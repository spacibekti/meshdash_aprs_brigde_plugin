import logging
import time
from types import SimpleNamespace

from aprs_bridge import registry
from aprs_bridge.ack_tracker import AckTracker, MsgnoGenerator
from aprs_bridge.config import BridgeConfig
from aprs_bridge.mesh_bridge import MeshToRfBridge
from aprs_bridge.protocol import ax25, aprs_message, kiss
from aprs_bridge.protocol.dedupe import DedupeCache
from aprs_bridge.protocol.ratelimit import RateLimiter

LOCAL_ID = "!local0001"


def _wait_until(predicate, timeout=5, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _make_config(**overrides):
    defaults = dict(
        tnc_mode="kiss_tcp",
        tnc_host="127.0.0.1",
        tnc_port=8001,
        kiss_port=0,
        gateway_callsign="W4BRD-13",
        aprs_tocall="APZBRD",
        digi_path=("WIDE1-1", "WIDE2-1"),
        mesh_channel_index=0,
        registry_db_path=":memory:",
        dedupe_ttl_sec=30.0,
        rate_limit_per_min=6000.0,  # effectively unlimited by default
        rate_limit_burst=1000.0,
        per_callsign_rate_limit_per_min=6000.0,
        per_callsign_rate_limit_burst=1000.0,
        ack_retry_intervals_sec=(30.0, 60.0, 120.0),
        ack_max_attempts=4,
        mesh_fanout_delay_sec=0.05,  # tiny in tests; production default is 2.0s
    )
    defaults.update(overrides)
    return BridgeConfig(**defaults)


def _make_bridge(tmp_path, fake_connection_manager, running_event_loop, mesh_nodes=None, **cfg_overrides):
    conn = registry.init_db(str(tmp_path / "reg.db"))
    cfg = _make_config(registry_db_path=str(tmp_path / "reg.db"), **cfg_overrides)
    sent_rf_frames = []

    def transport_send(data: bytes) -> bool:
        sent_rf_frames.append(data)
        return True

    meshtastic_data = SimpleNamespace(nodes=mesh_nodes or {}, local_node_id=LOCAL_ID)
    dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_sec)
    ack_tracker = AckTracker(
        transport_send=transport_send,
        logger=logging.getLogger("test.mesh_bridge.ack"),
        retry_intervals=cfg.ack_retry_intervals_sec,
        max_attempts=cfg.ack_max_attempts,
    )
    rate_limiter = RateLimiter(
        direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
        direction_capacity=cfg.rate_limit_burst,
        per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
        per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
    )

    bridge = MeshToRfBridge(
        cfg=cfg,
        registry_conn=conn,
        connection_manager=fake_connection_manager,
        meshtastic_data=meshtastic_data,
        event_loop=running_event_loop,
        logger=logging.getLogger("test.mesh_bridge"),
        transport_send=transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=rate_limiter,
        msgno_generator=MsgnoGenerator(),
    )
    return bridge, conn, sent_rf_frames, ack_tracker


def _dm_packet(from_id: str, text: str, channel: int = 0, to_id: str = LOCAL_ID) -> dict:
    # channel is included because real packets carry it, but the bridge no
    # longer inspects it for DMs -- confirmed on real hardware that
    # Meshtastic DMs don't carry usable channel-encryption metadata (see
    # mesh_bridge.py's docstring and CLAUDE.md), so it can't gate anything.
    return {
        "fromId": from_id,
        "toId": to_id,
        "channel": channel,
        "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": text},
    }


def _decode_last_rf_frame(sent_rf_frames):
    assert len(sent_rf_frames) == 1
    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    parsed = ax25.parse_ui_frame(ax25_bytes)
    return parsed, aprs_message.decode_message(parsed.info)


def test_register_command_creates_registration_and_replies(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    bridge.on_mesh_packet(_dm_packet("!node0001", "!register W4BRD-13", channel=0))

    assert registry.lookup_callsign_for_node(conn, "!node0001") == "W4BRD-13"
    assert sent_rf_frames == []  # registration never touches RF
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Registered W4BRD-13" in fake_connection_manager.sent[0]["text"]
    assert fake_connection_manager.sent[0]["destinationId"] == "!node0001"


def test_register_command_ignores_channel_field(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet(_dm_packet("!node0001", "!register W4BRD-13", channel=7))
    assert registry.lookup_callsign_for_node(conn, "!node0001") == "W4BRD-13"


def test_register_command_malformed_replies_with_error(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet(_dm_packet("!node0001", "!register not-valid-at-all", channel=0))

    assert registry.lookup_callsign_for_node(conn, "!node0001") is None
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Register failed" in fake_connection_manager.sent[0]["text"]


def test_unregister_command_removes_registration(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "!unregister", channel=0))

    assert registry.lookup_callsign_for_node(conn, "!node0001") is None
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Unregistered W4BRD-13" in fake_connection_manager.sent[0]["text"]


def test_unregister_command_when_not_registered(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet(_dm_packet("!node0001", "!unregister", channel=0))
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "no active registration" in fake_connection_manager.sent[0]["text"]


def test_unregistered_sender_reaches_rf_attributed_by_mesh_name(
    tmp_path, fake_connection_manager, running_event_loop
):
    # Third-party relay model (FCC Part 97.115): an unregistered/
    # unlicensed mesh sender can still reach RF -- the gateway's own
    # licensed callsign remains the sole AX.25 source, and the sender is
    # identified by their mesh long name rather than a claimed callsign.
    bridge, _conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={"!node0001": {"user": {"longName": "David's Pager", "shortName": "PGR"}}},
    )

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    parsed = ax25.parse_ui_frame(ax25_bytes)
    assert parsed.source == "W4BRD-13"  # gateway callsign, unchanged
    message = aprs_message.decode_message(parsed.info)
    assert message.addressee == "WU2Z"
    assert message.text == "David's Pager: hello"


def test_unregistered_sender_falls_back_to_short_name_then_node_id(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={"!node0001": {"user": {"shortName": "PGR"}}},  # no longName set
    )

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    parsed = ax25.parse_ui_frame(ax25_bytes)
    message = aprs_message.decode_message(parsed.info)
    assert message.text == "PGR: hello"  # falls back to short name


def test_unregistered_sender_falls_back_to_node_id_when_unnamed(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, _conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    parsed = ax25.parse_ui_frame(ax25_bytes)
    message = aprs_message.decode_message(parsed.info)
    assert message.text == "0001: hello"  # last 4 chars of the node id


def test_registered_sender_attributed_by_mesh_name_not_callsign(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop,
        mesh_nodes={"!node0001": {"user": {"longName": "David's Pager", "shortName": "PGR"}}},
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    parsed = ax25.parse_ui_frame(ax25_bytes)
    message = aprs_message.decode_message(parsed.info)
    # Attribution is the mesh name, not the registered callsign -- the
    # AX.25 source (asserted above as the gateway callsign) already
    # satisfies station ID; registration still gates rate-limiting/
    # last-correspondent tracking and RF->mesh delivery, just not the text.
    assert parsed.source == "W4BRD-13"
    assert message.text == "David's Pager: hello"


def test_registered_sender_with_explicit_addressee_reaches_rf(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: Testing 123", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert parsed.source == "W4BRD-13"  # gateway callsign, not the mesh user's
    assert parsed.destination == "APZBRD"
    assert message.addressee == "WU2Z"
    assert message.text == "0001: Testing 123"  # mesh name attribution, no callsign in payload


def test_registered_sender_reaches_rf_on_channel_0(tmp_path, fake_connection_manager, running_event_loop):
    # Regression guard: real Meshtastic DMs are always tagged channel 0
    # regardless of the sender's active channel context (confirmed on real
    # hardware). A registered sender's DM must still reach RF -- channel
    # value must never gate the DM path.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=0))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)


def test_hash_r_replies_to_last_correspondent(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")
    registry.set_last_correspondent(conn, "W4BRD-13", "WU2Z")

    bridge.on_mesh_packet(_dm_packet("!node0001", "#r just a reply, no callsign prefix", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert message.addressee == "WU2Z"
    assert message.text == "0001: just a reply, no callsign prefix"


def test_hash_r_case_insensitive(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")
    registry.set_last_correspondent(conn, "W4BRD-13", "WU2Z")

    bridge.on_mesh_packet(_dm_packet("!node0001", "#R hello again", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert message.addressee == "WU2Z"


def test_hash_r_with_no_last_correspondent_replies_with_error(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "#r no prior correspondent", channel=2))

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "No prior correspondent" in fake_connection_manager.sent[0]["text"]


def test_hash_r_with_no_text_replies_with_error(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")
    registry.set_last_correspondent(conn, "W4BRD-13", "WU2Z")

    bridge.on_mesh_packet(_dm_packet("!node0001", "#r", channel=2))

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "No message text" in fake_connection_manager.sent[0]["text"]


def test_bare_dm_with_no_recognized_form_is_left_untouched(
    tmp_path, fake_connection_manager, running_event_loop
):
    # The actual bug this guards against: other MeshDash plugins also see
    # every DM sent to the gateway node. A bare message matching neither
    # "CALLSIGN:" nor "#r" must not be auto-forwarded to RF (it might be
    # meant for a different plugin entirely) -- and we shouldn't even
    # reply, since that could itself interfere with whatever else is
    # meant to handle it.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")
    registry.set_last_correspondent(conn, "W4BRD-13", "WU2Z")

    bridge.on_mesh_packet(_dm_packet("!node0001", "yes", channel=2))

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert fake_connection_manager.sent == []


def test_sending_updates_last_correspondent(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: first message", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    assert registry.get_last_correspondent(conn, "W4BRD-13") == "WU2Z"


def test_sending_updates_last_active_node_for_registered_sender(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")
    registry.add_registration(conn, "W4BRD-13", "!node0002")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: first message", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    assert registry.get_last_active_node(conn, "W4BRD-13") == "!node0001"

    bridge.on_mesh_packet(_dm_packet("!node0002", "WU2Z: second message", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 2)
    assert registry.get_last_active_node(conn, "W4BRD-13") == "!node0002"


def test_unregistered_sender_does_not_touch_last_active_node(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    assert registry.get_last_active_node(conn, "W4BRD-13") is None


def test_sending_updates_conversation_node_regardless_of_registration(
    tmp_path, fake_connection_manager, running_event_loop
):
    # conversation_node is keyed by the RF correspondent, not the mesh
    # sender's callsign -- it's what lets an unregistered sender's RF
    # correspondent reply back to the right node (see bridge.py).
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    assert registry.get_conversation_node(conn, "WU2Z") == "!node0001"


def test_broadcast_and_non_dm_traffic_is_ignored(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    # A broadcast, not a DM to us.
    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: hello", channel=2, to_id="^all"))
    # A non-text packet.
    bridge.on_mesh_packet(
        {
            "fromId": "!node0001",
            "toId": LOCAL_ID,
            "channel": 2,
            "decoded": {"portnum": "POSITION_APP"},
        }
    )

    time.sleep(0.2)
    assert sent_rf_frames == []
    assert fake_connection_manager.sent == []


def test_malformed_packet_does_not_raise(tmp_path, fake_connection_manager, running_event_loop):
    bridge, _conn, _sent, _ack_tracker = _make_bridge(tmp_path, fake_connection_manager, running_event_loop)
    bridge.on_mesh_packet({})  # missing everything
    bridge.on_mesh_packet({"decoded": {}})


def test_outbound_message_carries_a_msgno_and_is_tracked_for_ack(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: Testing 123", channel=2))

    assert _wait_until(lambda: len(sent_rf_frames) == 1)
    _parsed, message = _decode_last_rf_frame(sent_rf_frames)
    assert message.msgno is not None
    assert ack_tracker.pending_count() == 1


def test_duplicate_mesh_packet_id_sent_only_once(tmp_path, fake_connection_manager, running_event_loop):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path, fake_connection_manager, running_event_loop
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    packet = _dm_packet("!node0001", "WU2Z: hello", channel=2)
    packet["id"] = 12345
    bridge.on_mesh_packet(packet)
    bridge.on_mesh_packet(dict(packet))  # identical packet id, as if pubsub fired twice

    time.sleep(0.2)
    assert len(sent_rf_frames) == 1


def test_rate_limit_exceeded_drops_send_and_replies(
    tmp_path, fake_connection_manager, running_event_loop
):
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path,
        fake_connection_manager,
        running_event_loop,
        per_callsign_rate_limit_per_min=60.0,
        per_callsign_rate_limit_burst=1.0,  # exactly one message allowed
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: first", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: second", channel=2))
    time.sleep(0.2)
    assert len(sent_rf_frames) == 1  # second send rate-limited
    # Successful sends generate no mesh reply; only the rate-limit error does.
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert "Rate limit" in fake_connection_manager.sent[-1]["text"]


def test_unrecognized_bare_dm_does_not_consume_rate_limit_budget(
    tmp_path, fake_connection_manager, running_event_loop
):
    # A message left untouched because it isn't ours (no "CALLSIGN:", no
    # "#r") shouldn't cost the sender any of their rate-limit budget --
    # it was never actually forwarded, so it shouldn't count against a
    # later genuine request.
    bridge, conn, sent_rf_frames, _ack_tracker = _make_bridge(
        tmp_path,
        fake_connection_manager,
        running_event_loop,
        per_callsign_rate_limit_per_min=60.0,
        per_callsign_rate_limit_burst=1.0,  # exactly one message allowed
    )
    registry.add_registration(conn, "W4BRD-13", "!node0001")

    for i in range(5):
        # Distinct text per attempt -- dedupe would otherwise collapse
        # identical repeats before they even reach _handle_dm.
        bridge.on_mesh_packet(_dm_packet("!node0001", f"not meant for us {i}", channel=2))
    time.sleep(0.2)
    assert sent_rf_frames == []
    assert fake_connection_manager.sent == []

    bridge.on_mesh_packet(_dm_packet("!node0001", "WU2Z: real request", channel=2))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

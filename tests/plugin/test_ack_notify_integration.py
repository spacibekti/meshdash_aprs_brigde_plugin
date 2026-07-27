import logging
import time
from types import SimpleNamespace

from aprs_bridge import registry
from aprs_bridge.ack_tracker import AckTracker, MsgnoGenerator
from aprs_bridge.bridge import RfToMeshBridge
from aprs_bridge.config import BridgeConfig
from aprs_bridge.mesh_bridge import MeshToRfBridge
from aprs_bridge.protocol import ax25, aprs_message, kiss
from aprs_bridge.protocol.dedupe import DedupeCache
from aprs_bridge.protocol.ratelimit import RateLimiter

LOCAL_ID = "!local0001"
SENDER_NODE = "!node0001"


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
        rate_limit_per_min=6000.0,
        rate_limit_burst=1000.0,
        per_callsign_rate_limit_per_min=6000.0,
        per_callsign_rate_limit_burst=1000.0,
        ack_retry_intervals_sec=(30.0, 60.0, 120.0),
        ack_max_attempts=4,
        mesh_fanout_delay_sec=0.05,  # tiny in tests; production default is 2.0s
    )
    defaults.update(overrides)
    return BridgeConfig(**defaults)


def _make_wired_bridges(tmp_path, fake_connection_manager, running_event_loop, notify_calls, **cfg_overrides):
    """Mirrors main.py's actual wiring: one shared registry connection,
    dedupe cache, and ack_tracker across both bridge directions, with
    on_acked/on_exhausted callbacks that record what they'd have sent."""
    conn = registry.init_db(str(tmp_path / "reg.db"))
    cfg = _make_config(registry_db_path=str(tmp_path / "reg.db"), **cfg_overrides)
    sent_rf_frames = []

    def transport_send(data: bytes) -> bool:
        sent_rf_frames.append(data)
        return True

    def on_acked(msgno, addressee, node_id):
        notify_calls.append(("acked", msgno, addressee, node_id))

    def on_exhausted(msgno, addressee, node_id):
        notify_calls.append(("exhausted", msgno, addressee, node_id))

    dedupe = DedupeCache(ttl_seconds=cfg.dedupe_ttl_sec)
    ack_tracker = AckTracker(
        transport_send=transport_send,
        logger=logging.getLogger("test.integration.ack"),
        retry_intervals=cfg.ack_retry_intervals_sec,
        max_attempts=cfg.ack_max_attempts,
        on_acked=on_acked,
        on_exhausted=on_exhausted,
    )
    msgno_generator = MsgnoGenerator()
    meshtastic_data = SimpleNamespace(nodes={}, local_node_id=LOCAL_ID)

    rf_to_mesh = RfToMeshBridge(
        cfg=cfg,
        registry_conn=conn,
        connection_manager=fake_connection_manager,
        meshtastic_data=meshtastic_data,
        event_loop=running_event_loop,
        logger=logging.getLogger("test.integration.rf_to_mesh"),
        transport_send=transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=RateLimiter(
            direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
            direction_capacity=cfg.rate_limit_burst,
            per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
            per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
        ),
    )
    mesh_to_rf = MeshToRfBridge(
        cfg=cfg,
        registry_conn=conn,
        connection_manager=fake_connection_manager,
        meshtastic_data=meshtastic_data,
        event_loop=running_event_loop,
        logger=logging.getLogger("test.integration.mesh_to_rf"),
        transport_send=transport_send,
        dedupe=dedupe,
        ack_tracker=ack_tracker,
        rate_limiter=RateLimiter(
            direction_rate_per_sec=cfg.rate_limit_per_min / 60.0,
            direction_capacity=cfg.rate_limit_burst,
            per_callsign_rate_per_sec=cfg.per_callsign_rate_limit_per_min / 60.0,
            per_callsign_capacity=cfg.per_callsign_rate_limit_burst,
        ),
        msgno_generator=msgno_generator,
    )
    return rf_to_mesh, mesh_to_rf, conn, sent_rf_frames, ack_tracker


def _dm_packet(from_id: str, text: str) -> dict:
    return {
        "fromId": from_id,
        "toId": LOCAL_ID,
        "channel": 0,
        "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": text},
    }


def test_mesh_sender_notified_when_real_recipient_acks(
    tmp_path, fake_connection_manager, running_event_loop
):
    notify_calls = []
    rf_to_mesh, mesh_to_rf, conn, sent_rf_frames, _ack_tracker = _make_wired_bridges(
        tmp_path, fake_connection_manager, running_event_loop, notify_calls
    )
    registry.add_registration(conn, "W4BRD-13", SENDER_NODE)

    # Mesh user sends a message out to RF.
    mesh_to_rf.on_mesh_packet(_dm_packet(SENDER_NODE, "WU2Z: hello there"))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[0])
    outbound = ax25.parse_ui_frame(ax25_bytes)
    outbound_msg = aprs_message.decode_message(outbound.info)
    assert outbound_msg.msgno is not None

    # The real APRS recipient (WU2Z) sends back a genuine ack, received
    # on the RF RX path (a different bridge instance, sharing ack_tracker).
    ack_info = aprs_message.build_ack("W4BRD-13", outbound_msg.msgno)
    ack_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], ack_info)
    rf_to_mesh.on_ax25_frame(ack_frame)

    assert notify_calls == [("acked", outbound_msg.msgno, "WU2Z", SENDER_NODE)]


def test_mesh_sender_notified_on_final_failure(tmp_path, fake_connection_manager, running_event_loop):
    notify_calls = []
    clock_box = {"now": 0.0}

    def fake_clock():
        return clock_box["now"]

    rf_to_mesh, mesh_to_rf, conn, sent_rf_frames, ack_tracker = _make_wired_bridges(
        tmp_path,
        fake_connection_manager,
        running_event_loop,
        notify_calls,
        ack_retry_intervals_sec=(10.0,),
        ack_max_attempts=2,
    )
    # Swap in a fake clock on the shared ack_tracker so we can fast-forward
    # past every retry interval deterministically instead of sleeping.
    ack_tracker._clock = fake_clock
    registry.add_registration(conn, "W4BRD-13", SENDER_NODE)

    mesh_to_rf.on_mesh_packet(_dm_packet(SENDER_NODE, "WU2Z: never acked"))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    clock_box["now"] += 10
    ack_tracker.poll()  # attempt 2 (== max_attempts) -> exhausted on the check that follows
    clock_box["now"] += 10
    ack_tracker.poll()  # now exhausted

    assert len(notify_calls) == 1
    kind, _msgno, addressee, node_id = notify_calls[0]
    assert kind == "exhausted"
    assert addressee == "WU2Z"
    assert node_id == SENDER_NODE


def test_unregistered_sender_receives_reply_via_conversation_tracking(
    tmp_path, fake_connection_manager, running_event_loop
):
    notify_calls = []
    rf_to_mesh, mesh_to_rf, conn, sent_rf_frames, _ack_tracker = _make_wired_bridges(
        tmp_path, fake_connection_manager, running_event_loop, notify_calls
    )
    # No registration -- SENDER_NODE is an unlicensed mesh user relayed
    # via third-party traffic, identified by mesh name rather than a
    # callsign.
    mesh_to_rf.on_mesh_packet(_dm_packet(SENDER_NODE, "WU2Z: hello there"))
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    # WU2Z replies. Every outbound frame's AX.25 source is always the
    # gateway's own callsign (never SENDER_NODE's, which doesn't have
    # one), so WU2Z's reply comes back addressed to the gateway callsign
    # -- conversation tracking is what lets it still find SENDER_NODE.
    reply_info = aprs_message.encode_message("W4BRD-13", "hi there", msgno="900")
    reply_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], reply_info)
    rf_to_mesh.on_ax25_frame(reply_frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    delivered = fake_connection_manager.sent[0]
    assert delivered["destinationId"] == SENDER_NODE
    assert delivered["text"] == "WU2Z: hi there"


def test_mesh_node_can_reply_bare_to_first_contact_from_rf(
    tmp_path, fake_connection_manager, running_event_loop
):
    # An RF station messages a registered mesh node first (mesh node has
    # never sent anything yet, so it has no last_correspondent of its
    # own on record). Its "#r" reply must still reach WU2Z -- delivery
    # itself has to seed last_correspondent, not just outbound sends.
    notify_calls = []
    rf_to_mesh, mesh_to_rf, conn, sent_rf_frames, _ack_tracker = _make_wired_bridges(
        tmp_path, fake_connection_manager, running_event_loop, notify_calls
    )
    registry.add_registration(conn, "W4BRD-13", SENDER_NODE)

    inbound_info = aprs_message.encode_message("W4BRD-13", "hello from RF", msgno="500")
    inbound_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], inbound_info)
    rf_to_mesh.on_ax25_frame(inbound_frame)
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 1)
    assert fake_connection_manager.sent[0]["destinationId"] == SENDER_NODE
    # The inbound message carried a msgno, so it already got an RF ack --
    # sent_rf_frames has that ack in it before the reply we care about.
    assert _wait_until(lambda: len(sent_rf_frames) == 1)

    mesh_to_rf.on_mesh_packet(_dm_packet(SENDER_NODE, "#r hi back, no callsign needed"))
    assert _wait_until(lambda: len(sent_rf_frames) == 2)

    _port, _cmd, ax25_bytes = kiss.decode_frame(sent_rf_frames[-1])
    outbound = ax25.parse_ui_frame(ax25_bytes)
    outbound_msg = aprs_message.decode_message(outbound.info)
    assert outbound_msg.addressee == "WU2Z"
    assert outbound_msg.text == "0001: hi back, no callsign needed"


def test_all_broadcast_then_reply_narrows_routing_to_the_replying_device(
    tmp_path, fake_connection_manager, running_event_loop
):
    # "!ALL" reaches every device registered to a callsign; whichever one
    # replies afterward should become the one further RF messages to that
    # callsign route to, without needing any extra "!ALL"-specific
    # bookkeeping -- mesh_bridge.py already records last_active_node on
    # every registered send, so this should fall out of existing logic.
    notify_calls = []
    rf_to_mesh, mesh_to_rf, conn, sent_rf_frames, _ack_tracker = _make_wired_bridges(
        tmp_path, fake_connection_manager, running_event_loop, notify_calls
    )
    node_a, node_b = "!nodeaaaa", "!nodebbbb"
    registry.add_registration(conn, "W4BRD-13", node_a)
    registry.add_registration(conn, "W4BRD-13", node_b)

    broadcast_info = aprs_message.encode_message("W4BRD-13", "!ALL check in", msgno="700")
    broadcast_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], broadcast_info)
    rf_to_mesh.on_ax25_frame(broadcast_frame)
    assert _wait_until(lambda: len(fake_connection_manager.sent) == 2)
    assert {s["destinationId"] for s in fake_connection_manager.sent} == {node_a, node_b}

    # node_b is the one that replies.
    mesh_to_rf.on_mesh_packet(_dm_packet(node_b, "#r here"))
    assert _wait_until(lambda: len(sent_rf_frames) == 2)  # ack of the broadcast + this reply

    # A follow-up RF message to the callsign, with no "!ALL", now goes
    # only to node_b -- not fanned out again.
    followup_info = aprs_message.encode_message("W4BRD-13", "good, thanks", msgno="701")
    followup_frame = ax25.build_ui_frame("APZ019", "WU2Z", ["WIDE1-1", "WIDE2-1"], followup_info)
    rf_to_mesh.on_ax25_frame(followup_frame)

    assert _wait_until(lambda: len(fake_connection_manager.sent) == 3)
    assert fake_connection_manager.sent[-1]["destinationId"] == node_b

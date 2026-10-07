from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Callable, List, Optional

from .ack_tracker import AckTracker
from . import commands
from .config import BridgeConfig
from .protocol import ax25, aprs_message, kiss
from .protocol.dedupe import DedupeCache
from .protocol.errors import ProtocolError
from .protocol.ratelimit import RateLimiter
from . import registry


class RfToMeshBridge:
    """RF -> mesh. Three ways an incoming APRS message gets routed to a
    mesh node, tried in order:

    1. Addressed to our own gateway callsign, and a conversation is on
       record for frame.source (registry.conversation_node, written by
       mesh_bridge.py on every mesh->RF send): since every outbound
       frame's AX.25 source is always the gateway's own callsign, not
       the originating mesh sender's, an RF correspondent's reply is
       always addressed back to us regardless of who they were actually
       talking to -- this is what lets that reply find its way back to
       an unregistered/unlicensed sender, who has no callsign of their
       own for the correspondent to address in the first place.
    2. Addressed to a registered callsign: delivered to the Meshtastic
       node(s) registered under it (an operator may register more than
       one device to the same callsign; a device itself still maps to
       exactly one callsign). With more than one registered device,
       delivery goes only to whichever sent mesh->RF under that callsign
       most recently (registry.last_active_node) -- falling back to
       every device if none has ever sent, or the last-active one was
       since unregistered. A sender can force delivery to every
       registered device regardless, by starting the message text with
       "!ALL" (commands.parse_broadcast_prefix strips it before
       delivery) -- e.g. to check in with everyone at once. Whichever
       device replies afterward becomes the new last-active one, since
       mesh_bridge.py records that on every registered send, so routing
       narrows back down on its own without any extra bookkeeping here.
    3. Addressed to something matching a live mesh node's short name, or
       the last 4 hex chars of its node id (the same fallback code shown
       as attribution when a node has no name -- see
       mesh_bridge.py's _mesh_long_name) -- lets an RF sender reach an
       unlicensed mesh user directly even without a prior conversation
       or registration (third-party relay; see mesh_bridge.py's
       docstring for the compliance model). The node-id code exists
       because a Meshtastic short name can be arbitrary unicode (even
       missing), which isn't always something an RF sender can type.

    The text delivered to mesh is prefixed with the RF sender's AX.25
    source callsign ("N0CALL-10: text") so the recipient can see who's
    messaging them. Sends an RF ACK back over the TNC if the message
    carried a message number -- as a plain ack (source=gateway_callsign)
    when the message was addressed to gateway_callsign itself, or wrapped
    in APRS third-party-traffic format ("}ADDRESSEE>TOCALL:...", see
    aprs_message.build_third_party_ack) otherwise, since standard APRS
    ack-matching on the sender's end expects the ack to come from
    whatever it addressed the message to -- confirmed live, on two
    different APRS clients, that a same-source ack for a message
    addressed to a mesh short name/node-id code left the sender
    retransmitting indefinitely despite having been acked. Also the RX side of the
    mesh->RF ACK loop: an incoming APRS message addressed to our own
    gateway callsign that decodes as "ackNNN" clears the matching
    pending send in ack_tracker instead of being treated as
    mesh-deliverable traffic. Runs on the TNC RX thread via
    on_ax25_frame(); mesh delivery is scheduled onto the MeshDash event
    loop since connection_manager.sendText is a coroutine. When more
    than one device is targeted (fan-out or "!ALL"), deliveries are
    sequenced one at a time with a gap between them
    (_deliver_to_all, cfg.mesh_fanout_delay_sec) rather than fired
    concurrently -- confirmed live that concurrent sendText calls to
    different destinations can race and silently drop one of them, and
    that too short a gap can still silently drop a later delivery even
    when sequenced (the earlier send's own ack-wait/retry cycle appears
    to still occupy the radio), under clean RF with nothing else to
    blame.

    A duplicate/retried receipt of an already-delivered message (a
    digipeated repeat, or the sender retrying because our first ack
    never reached them) is re-acked but not re-delivered: without this,
    a lost ack would leave the RF sender retrying indefinitely, since
    APRS has no other way for them to learn we got it."""

    def __init__(
        self,
        cfg: BridgeConfig,
        registry_conn: sqlite3.Connection,
        connection_manager,
        meshtastic_data,
        event_loop: asyncio.AbstractEventLoop,
        logger: logging.Logger,
        transport_send: Callable[[bytes], bool],
        dedupe: DedupeCache,
        ack_tracker: AckTracker,
        rate_limiter: RateLimiter,
    ) -> None:
        self._cfg = cfg
        self._registry_conn = registry_conn
        self._cm = connection_manager
        self._meshtastic_data = meshtastic_data
        self._loop = event_loop
        self._logger = logger
        self._transport_send = transport_send
        self._dedupe = dedupe
        self._ack_tracker = ack_tracker
        self._rate_limiter = rate_limiter

    def _lookup_node_by_short_name(self, name: str) -> Optional[str]:
        nodes = getattr(self._meshtastic_data, "nodes", {}) or {}
        name_upper = name.strip().upper()
        for node_id, nd in nodes.items():
            user = (nd.get("user") or {}) if isinstance(nd, dict) else {}
            short_name = user.get("shortName") or nd.get("short_name") or ""
            if short_name.strip().upper() == name_upper:
                return node_id
        return None

    def _lookup_node_by_code(self, name: str) -> Optional[str]:
        """Matches the last 4 hex chars of a node's id -- the same
        fallback code mesh_bridge.py's _mesh_long_name uses as
        attribution when a node has no long/short name set. Unlike a
        Meshtastic short name (which can be arbitrary, even non-ASCII,
        unicode, or missing), this is always present, always ASCII, and
        always short enough to type on RF -- so it's the reliable way
        for an RF sender to reach a specific node without needing to
        know (or be able to type) its display name."""
        nodes = getattr(self._meshtastic_data, "nodes", {}) or {}
        name_upper = name.strip().upper()
        for node_id in nodes:
            if node_id and node_id[-4:].upper() == name_upper:
                return node_id
        return None

    def on_ax25_frame(self, ax25_bytes: bytes) -> None:
        try:
            frame = ax25.parse_ui_frame(ax25_bytes)
        except ProtocolError as exc:
            self._logger.debug("aprs_bridge: ignoring non-UI/APRS frame: %s", exc)
            return

        if not aprs_message.is_message(frame.info):
            return

        try:
            message = aprs_message.decode_message(frame.info)
        except ProtocolError as exc:
            self._logger.debug("aprs_bridge: malformed APRS message from %s: %s", frame.source, exc)
            return

        # A leading "!ALL" forces delivery to every device registered
        # under the addressed callsign, overriding the last-active-device
        # narrowing below -- e.g. to check in with everyone at once.
        # Whichever device replies afterward becomes the new last-active
        # one for that callsign (mesh_bridge.py already records this on
        # every registered send), so routing narrows back down on its own.
        is_broadcast, delivery_text = commands.parse_broadcast_prefix(message.text)

        node_ids: List[str] = []

        if message.addressee == self._cfg.gateway_callsign:
            acked_msgno = aprs_message.parse_ack(message)
            if acked_msgno is not None:
                if self._ack_tracker.ack(acked_msgno):
                    self._logger.info(
                        "aprs_bridge: mesh->RF message %s acked by %s", acked_msgno, frame.source
                    )
                else:
                    self._logger.debug(
                        "aprs_bridge: ack %s from %s for unknown/already-cleared message",
                        acked_msgno, frame.source,
                    )
                return
            # Not an ack: since every mesh->RF frame uses the gateway's
            # own callsign as its AX.25 source (never the individual
            # sender's -- see mesh_bridge.py's docstring), this is most
            # likely frame.source replying to something we sent on
            # behalf of a mesh node. Route by conversation history
            # (whichever mesh node last sent TO frame.source) rather than
            # by addressee, since addressee here is just us regardless of
            # who the original mesh sender was -- this is what lets a
            # reply reach an unregistered/unlicensed sender, who has no
            # callsign of their own for an RF correspondent to address.
            # Skipped entirely for "!ALL": conversation history always
            # names a single node, which directly contradicts a request
            # to reach every device -- this matters whenever
            # gateway_callsign is itself also a registered callsign
            # (e.g. testing under one's own callsign), since without this
            # check a prior conversation would silently override "!ALL"
            # before the registered-callsign fan-out logic below ever runs.
            if not is_broadcast:
                conversation_node = registry.get_conversation_node(self._registry_conn, frame.source)
                if conversation_node is not None:
                    node_ids = [conversation_node]
            # No conversation on record (or broadcasting): fall through
            # below, which also covers gateway_callsign coincidentally
            # being a registered mesh user's callsign in its own right.

        if not node_ids:
            node_ids = registry.lookup_nodes_for_callsign(self._registry_conn, message.addressee)
            if len(node_ids) > 1 and not is_broadcast:
                # A callsign with several registered devices: route to
                # just the one that most recently sent mesh->RF under
                # this callsign, rather than fanning out to every device.
                # Falls back to fan-out if we've never seen an outbound
                # send from this callsign yet (registered-but-silent
                # devices), or if the last-active device has since been
                # unregistered -- or if the sender explicitly requested
                # "!ALL" (checked above).
                last_active = registry.get_last_active_node(self._registry_conn, message.addressee)
                if last_active in node_ids:
                    node_ids = [last_active]

        if not node_ids:
            # Not a registered callsign -- also allow reaching a mesh
            # node directly by its live short name, or by its 4-hex-char
            # node-id code (see _lookup_node_by_code), so an RF sender
            # can message an unlicensed mesh user too (third-party
            # relay; gateway_callsign remains the sole RF transmission
            # attribution regardless of who receives). The code is
            # checked second since a short name match is more likely to
            # be what the sender actually meant to type.
            matched_node = self._lookup_node_by_short_name(
                message.addressee
            ) or self._lookup_node_by_code(message.addressee)
            node_ids = [matched_node] if matched_node else []

        if not node_ids:
            self._logger.info(
                "aprs_bridge: dropping APRS message for unknown callsign/node name %r", message.addressee
            )
            return

        signature = (frame.source, message.addressee, message.text, message.msgno)
        if self._dedupe.seen(signature):
            # Same message already delivered -- most commonly a
            # digipeated repeat (same content, different AX.25 path) or
            # the sender retrying because our first ack was lost. Don't
            # re-deliver to mesh, but DO re-ack: an un-acked retry would
            # otherwise have the sender retry forever. Slide the TTL
            # forward so a burst of retries keeps getting acked rather
            # than the window expiring mid-retry-sequence and causing a
            # second real mesh delivery.
            self._dedupe.mark(signature)
            if message.msgno is not None:
                self._logger.debug(
                    "aprs_bridge: re-acking duplicate/retried message %s from %s (no re-delivery)",
                    message.msgno, frame.source,
                )
                self._send_ack(frame.source, message.msgno, message.addressee)
            return

        if not self._rate_limiter.allow(message.addressee):
            self._logger.warning(
                "aprs_bridge: rate limit exceeded delivering to %s; dropping RF message",
                message.addressee,
            )
            # Deliberately not marking dedupe here: an undelivered,
            # rate-limited message was never acked, so a genuine retry
            # must be free to try again (and succeed) once the rate
            # limit window reopens, rather than being mistaken for an
            # already-delivered duplicate.
            return

        self._dedupe.mark(signature)
        self._logger.info(
            "aprs_bridge: RF->mesh %s -> %s (nodes %s): %r",
            frame.source, message.addressee, ", ".join(node_ids), message.text,
        )
        # Prefix with the RF sender's callsign (mirrors the mesh->RF
        # "CALLSIGN: text" / "via <name>: text" attribution) so the mesh
        # recipient can see who's messaging them -- without it there's no
        # way to tell one RF correspondent from another. delivery_text
        # has any leading "!ALL" already stripped, so recipients see only
        # the actual message.
        mesh_text = f"{frame.source}: {delivery_text}"
        # A callsign may have several registered devices (e.g. an
        # operator running more than one mesh node); one incoming RF
        # message counts as one rate-limited/acked event regardless, and
        # fans out to every device registered under that callsign. Record
        # last_correspondent for every recipient up front (cheap, purely
        # local) so it's set correctly even if delivery to some of them
        # is still pending -- keyed the same way mesh_bridge.py computes
        # identity_key (registered callsign if any, else the node id), so
        # a bare-text reply from them, with no "CALLSIGN:" prefix, goes
        # straight back to whoever just messaged them. Without this, a
        # mesh node's very first reply to an RF message would fail ("No
        # recipient given and no prior correspondent") even though we
        # just told them exactly who's messaging them.
        for node_id in node_ids:
            recipient_identity = registry.lookup_callsign_for_node(self._registry_conn, node_id) or node_id
            registry.set_last_correspondent(self._registry_conn, recipient_identity, frame.source)
        asyncio.run_coroutine_threadsafe(self._deliver_to_all(node_ids, mesh_text), self._loop)

        if message.msgno is not None:
            self._send_ack(frame.source, message.msgno, message.addressee)

    async def _deliver_to_all(self, node_ids: List[str], text: str) -> None:
        # Delivered one at a time, awaited in sequence with a short gap
        # between each -- not fired concurrently. Confirmed live: firing
        # one run_coroutine_threadsafe per node (independent, unawaited)
        # let sendText calls to different destinations race each other,
        # and one node's message never arrived at all under clean RF
        # conditions with nothing else to blame. Sequencing them removes
        # that race regardless of what's happening inside
        # connection_manager/the local radio's send queue; the pacing gap
        # (cfg.mesh_fanout_delay_sec) additionally avoids stacking sends
        # faster than the one physical radio link can actually clear
        # them -- confirmed live that even a sequenced-but-too-short gap
        # (0.5s) could still silently drop a later delivery, apparently
        # because the earlier send's own ack-wait/retry cycle was still
        # occupying the radio.
        for i, node_id in enumerate(node_ids):
            if i > 0:
                await asyncio.sleep(self._cfg.mesh_fanout_delay_sec)
            await self._deliver(node_id, text)

    async def _deliver(self, node_id: str, text: str) -> None:
        if not self._cm.is_ready.is_set():
            self._logger.warning("aprs_bridge: connection_manager not ready; dropping message to %s", node_id)
            return
        try:
            await self._cm.sendText(
                text,
                destinationId=node_id,
                channelIndex=self._cfg.mesh_channel_index,
            )
        except Exception:
            self._logger.exception("aprs_bridge: sendText to %s failed", node_id)

    def _send_ack(self, rf_recipient_callsign: str, msgno: str, original_addressee: str) -> None:
        ack_text = "ack" + msgno
        if original_addressee == self._cfg.gateway_callsign:
            ack_info = aprs_message.build_ack(rf_recipient_callsign, msgno)
        else:
            # The message was addressed to something other than us (a mesh
            # short name, node-id code, or another registered callsign) --
            # standard APRS ack-matching on the sender's end expects the
            # ack to come from the callsign it addressed, not from us.
            # Wrap it in third-party-traffic format so the payload claims
            # to be "from" original_addressee, while the AX.25 frame
            # itself (built below) still has to carry the gateway's own
            # callsign as its real source regardless -- confirmed live
            # that a plain same-source ack here left the sender retrying
            # indefinitely, on two different APRS clients.
            ack_info = aprs_message.build_third_party_ack(
                original_addressee, rf_recipient_callsign, msgno, self._cfg.aprs_tocall
            )
        ax25_frame = ax25.build_ui_frame(
            self._cfg.aprs_tocall,
            self._cfg.gateway_callsign,
            self._cfg.digi_path,
            ack_info,
        )
        kiss_frame = kiss.encode_frame(ax25_frame, port=self._cfg.kiss_port)
        if self._transport_send(kiss_frame):
            # Mark our own ack's signature so a TNC echo / digipeat
            # loopback of this exact frame is recognized as self-
            # originated on RX, not re-processed as fresh traffic.
            self._dedupe.mark((self._cfg.gateway_callsign, rf_recipient_callsign, ack_text, None))
        else:
            self._logger.warning("aprs_bridge: failed to send RF ack to %s", rf_recipient_callsign)

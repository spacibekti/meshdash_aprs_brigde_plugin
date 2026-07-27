from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any, Callable

from . import commands, registry
from .ack_tracker import AckTracker, MsgnoGenerator
from .commands import CommandError
from .config import BridgeConfig
from .protocol import ax25, aprs_message, kiss
from .protocol.dedupe import DedupeCache
from .protocol.errors import ProtocolError
from .protocol.ratelimit import RateLimiter

_MAX_OUTBOUND_TEXT = 67  # APRS message text limit; enforced again defensively here.
_MAX_ATTRIBUTION_LEN = 30  # cap on the "name: " prefix so it can't crowd out the message text.


class MeshToRfBridge:
    """Mesh -> RF. Handles !register/!unregister DM commands (no RF
    involved) and forwards "CALLSIGN: text" or "#r text" (reply to last
    correspondent) DMs out over RF as APRS messages, tracked for
    ACK/retransmit via ack_tracker. A bare DM matching neither form is
    left completely untouched -- other MeshDash plugins also subscribe to
    every DM sent to the gateway node (pub/sub fans out to every
    subscriber; there's no ownership arbitration between plugins), so an
    unrecognized bare message could just as easily be meant for a
    different plugin's own command syntax. Confirmed live: auto-forwarding
    any bare DM to "whoever I last talked to on RF" occasionally hijacked
    a message meant for another plugin and transmitted it -- content the
    sender never intended to put on RF at all. Requiring an explicit "#r"
    removes that ambiguity at the cost of the sender having to type it
    instead of just replying with bare text.

    Compliance model (see CLAUDE.md -- this is the FCC Part 97.115
    third-party-traffic model, not a claim that every mesh sender holds
    a license):
    - The AX.25 source on every outbound frame is always the gateway's
      own licensed callsign, regardless of who originated the mesh-side
      content. That's the sole point of RF transmission accountability,
      unaffected by anything below.
    - Registration (!register CALLSIGN-SSID) still gates whether a
      sender can receive RF replies addressed to that callsign, and
      still keys their rate-limit bucket and last-correspondent
      tracking by that callsign rather than their node id. But the
      message-text attribution itself is always just the mesh long
      name ("LONGNAME: text", falling back to short name/node id if
      unset) for every sender, registered or not -- the
      station-callsign requirement is already met by the AX.25
      source on the outgoing frame (always the gateway's own licensed
      callsign, per the point above), so repeating a sender's callsign
      in the payload isn't needed.
    - Only genuine direct messages addressed to the gateway node reach
      this far at all (_handle_packet returns early on anything else) --
      broadcast/channel content, encrypted-default-channel included, is
      never inspected for RF-gating purposes. There is deliberately no
      channel-index check on top of that: confirmed on real hardware that
      Meshtastic DMs don't carry usable channel-encryption metadata (a DM
      sent from a non-default channel context was still tagged channel 0),
      so a channel allowlist can't discriminate anything for DMs.
    """

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
        msgno_generator: MsgnoGenerator,
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
        self._msgno_generator = msgno_generator

    def on_mesh_packet(self, packet: dict, interface: Any = None) -> None:
        try:
            self._handle_packet(packet)
        except Exception:
            self._logger.exception("aprs_bridge: mesh packet handling raised")

    def _handle_packet(self, packet: dict) -> None:
        decoded = packet.get("decoded", {}) or {}
        portnum = str(decoded.get("portnum", ""))
        if "TEXT_MESSAGE" not in portnum:
            return

        from_id = packet.get("fromId") or packet.get("from_id")
        to_id = packet.get("toId") or packet.get("to_id")
        if not from_id or not to_id:
            return

        local_id = getattr(self._meshtastic_data, "local_node_id", None)
        if not local_id or to_id != local_id:
            return  # Not a DM addressed to us; broadcasts/other traffic are not commands.

        text = (decoded.get("text") or "").strip()
        if not text:
            return

        # Prefer the mesh packet id (Meshtastic's own dedup key) when
        # present; fall back to (from_id, text) for synthetic/test
        # packets or the rare packet missing an id.
        packet_id = packet.get("id")
        signature = ("mesh", packet_id) if packet_id is not None else ("mesh", from_id, text)
        if self._dedupe.seen_or_mark(signature):
            self._logger.debug("aprs_bridge: dropping duplicate mesh packet %r", signature)
            return

        self._handle_dm(from_id, text)

    def _handle_dm(self, from_id: str, text: str) -> None:
        # Registration bookkeeping never touches RF and works on any
        # channel -- it's how a node gets onto the allowlist in the first
        # place, so it can't itself require prior registration.
        try:
            new_callsign = commands.parse_register_command(text)
        except CommandError as exc:
            self._reply(from_id, f"Register failed: {exc}")
            return
        if new_callsign is not None:
            registry.add_registration(self._registry_conn, new_callsign, from_id)
            self._logger.info("aprs_bridge: registered %s -> node %s", new_callsign, from_id)
            self._reply(from_id, f"Registered {new_callsign} to this node.")
            return

        if commands.is_unregister_command(text):
            removed = registry.remove_registration_by_node(self._registry_conn, from_id)
            if removed is None:
                self._reply(from_id, "This node has no active registration.")
            else:
                self._logger.info("aprs_bridge: unregistered %s (node %s)", removed, from_id)
                self._reply(from_id, f"Unregistered {removed}.")
            return

        # Is this DM actually meant for us at all? Other MeshDash plugins
        # also subscribe to every DM sent to the gateway node (pub/sub
        # fans a packet out to every subscriber; there's no ownership
        # arbitration), so a bare, unrecognized DM could just as easily
        # be meant for a different plugin's own command syntax. Confirmed
        # live: treating any bare DM as "reply to my last APRS
        # correspondent" occasionally hijacked a message meant for
        # another plugin and transmitted it on RF -- content the sender
        # never intended to send there at all. Only two forms are
        # unambiguously ours: an explicit "CALLSIGN: text" target, or an
        # explicit "#r text" request to reply to the last correspondent.
        # Anything else is left completely alone (no reply either, so we
        # don't interfere with whatever else is meant to handle it).
        addressee, message_text = commands.parse_outbound_request(text)
        is_explicit_reply = False
        if addressee is None:
            is_explicit_reply, message_text = commands.parse_reply_prefix(text)
            if not is_explicit_reply:
                return

        # Third-party relay model: !register'd status still determines
        # identity_key (the stable per-sender string used for
        # rate-limiting and last-correspondent tracking -- a registered
        # sender's real callsign, or their node id otherwise), but the
        # message-text attribution is always just the mesh long name
        # (falling back to short name, then node id, if unset -- see
        # _mesh_long_name). The station callsign requirement is already
        # satisfied by the AX.25 source on the outgoing frame (always the
        # gateway's own licensed callsign -- see the class docstring);
        # repeating a sender's callsign in the payload added nothing a
        # reader couldn't already see in the frame header, and the long
        # name is what actually identifies which mesh node/person sent it.
        sender_callsign = registry.lookup_callsign_for_node(self._registry_conn, from_id)
        identity_key = sender_callsign if sender_callsign is not None else from_id
        attribution = self._mesh_long_name(from_id)

        if not self._rate_limiter.allow(identity_key):
            self._logger.warning(
                "aprs_bridge: rate limit exceeded for %s; dropping mesh->RF request", identity_key
            )
            self._reply(from_id, "Rate limit exceeded; message not sent. Try again shortly.")
            return

        if is_explicit_reply:
            addressee = registry.get_last_correspondent(self._registry_conn, identity_key)
            if addressee is None:
                self._reply(from_id, "No prior correspondent to reply to. Use 'CALLSIGN: message' first.")
                return
            if not message_text:
                self._reply(from_id, "No message text given. Use '#r message text'.")
                return

        self._send_to_rf(from_id, attribution, addressee, message_text)
        registry.set_last_correspondent(self._registry_conn, identity_key, addressee)
        # Every outbound frame's AX.25 source is the gateway's own
        # callsign, so when `addressee` replies, their reply comes back
        # addressed to us, not to this sender specifically -- record who
        # sent it so that reply can find its way back to the right mesh
        # node (see bridge.py's on_ax25_frame). Unlike last_active_node
        # below, this isn't gated on registration: an unregistered sender
        # needs this route too, since they have no callsign RF users can
        # address directly in the first place.
        registry.set_conversation_node(self._registry_conn, addressee, from_id)
        if sender_callsign is not None:
            # A callsign can have several registered devices; remember
            # which one most recently sent, so an RF reply to this
            # callsign can be routed to just that device instead of
            # fanning out to all of them (see bridge.py's on_ax25_frame).
            registry.set_last_active_node(self._registry_conn, sender_callsign, from_id)

    def _mesh_long_name(self, node_id: str) -> str:
        nodes = getattr(self._meshtastic_data, "nodes", {}) or {}
        nd = nodes.get(node_id) or {}
        user = (nd.get("user") or {}) if isinstance(nd, dict) else {}
        return (
            user.get("longName")
            or nd.get("long_name")
            or user.get("shortName")
            or nd.get("short_name")
            or (node_id[-4:] if node_id else node_id)
        )

    def _send_to_rf(self, from_id: str, attribution: str, addressee: str, message_text: str) -> None:
        msgno = self._msgno_generator.next()
        # Meshtastic long names can run up to 40 bytes -- cap the
        # attribution itself so a long name can never eat the entire
        # 67-char APRS text budget and leave nothing for the message.
        prefix = f"{attribution[:_MAX_ATTRIBUTION_LEN]}: "
        # Reserve room for the trailing "{msgno" (1 + up to 5 chars) the
        # same way encode_message will append it, so the combined field
        # never exceeds the 67-char APRS message text limit.
        suffix_len = 1 + len(msgno)
        available = _MAX_OUTBOUND_TEXT - len(prefix) - suffix_len
        if available <= 0:
            self._logger.warning(
                "aprs_bridge: attribution %r alone leaves no room for message text",
                attribution,
            )
            return
        text = prefix + message_text[:available]

        try:
            info = aprs_message.encode_message(addressee, text, msgno=msgno)
        except ProtocolError as exc:
            self._logger.warning("aprs_bridge: could not encode outbound APRS message: %s", exc)
            return

        ax25_frame = ax25.build_ui_frame(
            self._cfg.aprs_tocall, self._cfg.gateway_callsign, self._cfg.digi_path, info
        )
        kiss_frame = kiss.encode_frame(ax25_frame, port=self._cfg.kiss_port)
        if self._transport_send(kiss_frame):
            self._logger.info(
                "aprs_bridge: mesh->RF %s -> %s (msg %s): %r",
                attribution, addressee, msgno, message_text,
            )
            self._ack_tracker.track(msgno, addressee, kiss_frame, from_id)
            # Mark our own transmission's signature so a TNC echo /
            # digipeat loopback is recognized as self-originated on RX,
            # not re-processed (and not double-counted against the
            # recipient's own rate-limit bucket) as fresh RF traffic.
            self._dedupe.mark((self._cfg.gateway_callsign, addressee, text, msgno))
        else:
            self._logger.warning("aprs_bridge: failed to send mesh->RF message to %s", addressee)

    def _reply(self, node_id: str, text: str) -> None:
        asyncio.run_coroutine_threadsafe(self._send_reply(node_id, text), self._loop)

    async def _send_reply(self, node_id: str, text: str) -> None:
        if not self._cm.is_ready.is_set():
            self._logger.warning(
                "aprs_bridge: connection_manager not ready; dropping reply to %s", node_id
            )
            return
        try:
            await self._cm.sendText(
                text, destinationId=node_id, channelIndex=self._cfg.mesh_channel_index
            )
        except Exception:
            self._logger.exception("aprs_bridge: reply sendText to %s failed", node_id)

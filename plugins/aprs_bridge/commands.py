from __future__ import annotations

import re
from typing import Optional, Tuple

# Basic amateur callsign-SSID sanity check: 3-7 alphanumeric chars (real
# callsigns are shorter, but this is deliberately a bit permissive rather
# than trying to fully encode ITU prefix rules), optional -0..-15 SSID.
# This is a syntax check only -- it says nothing about whether the
# callsign is actually licensed; that's what the registration table is for.
_CALLSIGN_RE = re.compile(r"^[A-Z0-9]{3,7}(-(?:[0-9]|1[0-5]))?$")

_REGISTER_RE = re.compile(r"^!register\b(.*)$", re.IGNORECASE)
_UNREGISTER_RE = re.compile(r"^!unregister\s*$", re.IGNORECASE)

# "!ALL message text" -- an RF sender addressing a callsign with several
# registered devices can force delivery to every one of them instead of
# just the most recently active one (see bridge.py's on_ax25_frame).
_ALL_PREFIX_RE = re.compile(r"^!all\b\s*(.*)$", re.IGNORECASE)

# "CALLSIGN: message text" -- the aprstastic addressing convention. Colon
# is required; the callsign part is validated with the same syntax check
# used for registration so "not really a callsign: this is just a message
# with a colon in it" doesn't get misparsed as an addressee.
_ADDRESSED_RE = re.compile(r"^([A-Za-z0-9-]{1,9}):\s*(.+)$")

# "#r message text" -- explicit "reply to my last RF correspondent". Other
# MeshDash plugins also subscribe to every DM sent to the gateway node
# (pub/sub fans out to all subscribers, there's no way to know who a bare
# message was "meant" for); silently treating any unrecognized bare DM as
# an outbound APRS request risks transmitting a message that was actually
# meant for a different plugin. Requiring this explicit marker removes that
# ambiguity -- a bare DM with neither this nor a "CALLSIGN:" prefix is left
# untouched instead of being guessed at.
_REPLY_PREFIX_RE = re.compile(r"^#r\b\s*(.*)$", re.IGNORECASE)


class CommandError(Exception):
    """Raised when text looks like a command but is malformed, so the
    caller can reply with a specific error instead of silently ignoring it
    or treating it as an ordinary outbound message."""


def normalize_callsign(callsign: str) -> str:
    return callsign.strip().upper()


def is_valid_callsign(callsign: str) -> bool:
    return bool(_CALLSIGN_RE.match(normalize_callsign(callsign)))


def parse_register_command(text: str) -> Optional[str]:
    """Returns the normalized callsign-SSID if text is a well-formed
    "!register CALLSIGN-SSID" command. Returns None if text isn't a
    !register command at all (caller should fall through to other
    handling). Raises CommandError if it looks like an attempted
    !register command but the callsign is missing/malformed."""
    match = _REGISTER_RE.match(text.strip())
    if match is None:
        return None
    arg = match.group(1).strip()
    if not arg or not is_valid_callsign(arg):
        raise CommandError(
            f"'{arg}' doesn't look like a valid callsign-SSID (e.g. W4BRD-13)"
        )
    return normalize_callsign(arg)


def is_unregister_command(text: str) -> bool:
    return bool(_UNREGISTER_RE.match(text.strip()))


def parse_broadcast_prefix(text: str) -> Tuple[bool, str]:
    """Detects a leading "!ALL" token (case-insensitive) in RF message
    text. Returns (is_broadcast, remaining_text) -- remaining_text has
    the marker stripped so mesh recipients see only the actual message,
    not the raw command."""
    match = _ALL_PREFIX_RE.match(text.strip())
    if match is None:
        return False, text
    return True, match.group(1).strip()


def parse_outbound_request(text: str) -> Tuple[Optional[str], str]:
    """Splits "CALLSIGN: message text" into (addressee, message). If text
    doesn't start with a valid-looking "CALLSIGN:" prefix, returns
    (None, text) unchanged -- the caller should then check
    parse_reply_prefix rather than assuming this is meant for us at all."""
    match = _ADDRESSED_RE.match(text.strip())
    if match is None:
        return None, text.strip()
    candidate = normalize_callsign(match.group(1))
    if not is_valid_callsign(candidate):
        return None, text.strip()
    return candidate, match.group(2).strip()


def parse_reply_prefix(text: str) -> Tuple[bool, str]:
    """Detects a leading "#r" token (case-insensitive) requesting a reply
    to the sender's last RF correspondent. Returns (is_reply,
    remaining_text) -- remaining_text has the marker stripped so the
    outgoing APRS message contains only the actual reply."""
    match = _REPLY_PREFIX_RE.match(text.strip())
    if match is None:
        return False, text
    return True, match.group(1).strip()

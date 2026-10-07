from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .errors import AprsMessageError

_FORBIDDEN_TEXT_CHARS = ("|", "~", "{")
_ADDRESSEE_WIDTH = 9
_MAX_TEXT_LEN = 67


@dataclass(frozen=True)
class AprsMessage:
    addressee: str
    text: str
    msgno: Optional[str] = None


def _validate_addressee(addressee: str) -> str:
    if len(addressee) > _ADDRESSEE_WIDTH:
        raise AprsMessageError(
            f"addressee {addressee!r} exceeds {_ADDRESSEE_WIDTH} characters"
        )
    return addressee


def _validate_text(text: str) -> str:
    if not 1 <= len(text) <= _MAX_TEXT_LEN:
        raise AprsMessageError(
            f"text must be 1..{_MAX_TEXT_LEN} characters, got {len(text)}"
        )
    for ch in _FORBIDDEN_TEXT_CHARS:
        if ch in text:
            raise AprsMessageError(f"text contains forbidden character {ch!r}")
    return text


def _validate_msgno(msgno: Optional[str]) -> Optional[str]:
    if msgno is None:
        return None
    if not 1 <= len(msgno) <= 5:
        raise AprsMessageError(f"msgno must be 1..5 characters, got {len(msgno)!r}")
    return msgno


def encode_message(addressee: str, text: str, msgno: Optional[str] = None) -> bytes:
    addressee = _validate_addressee(addressee)
    text = _validate_text(text)
    msgno = _validate_msgno(msgno)

    parts = [":", addressee.ljust(_ADDRESSEE_WIDTH), ":", text]
    if msgno is not None:
        parts.append("{" + msgno)
    return "".join(parts).encode("ascii")


def is_message(info: bytes) -> bool:
    return info[0:1] == b":"


def decode_message(info: bytes) -> AprsMessage:
    if info[0:1] != b":":
        raise AprsMessageError("info field does not start with ':'")
    if len(info) < _ADDRESSEE_WIDTH + 2 or info[_ADDRESSEE_WIDTH + 1 : _ADDRESSEE_WIDTH + 2] != b":":
        raise AprsMessageError("malformed addressee framing (expected ':' at byte 10)")

    # Callsigns/addressees are case-insensitive on-air; normalize to upper
    # at decode time so downstream routing/ack handling is not dependent on
    # whichever case an originating client happened to use.
    addressee = info[1 : _ADDRESSEE_WIDTH + 1].decode("ascii").strip().upper()
    # Some radios/software append a trailing CR (occasionally CRLF) to the
    # info field -- confirmed live against a real ack from a station in
    # the wild: ":W4BRD-13 :ack001\r". Not part of the APRS message
    # content; stripped before any parsing so it can't corrupt a msgno
    # (an unstripped "\r" made "ack001\r" parse as msgno "001\r", which
    # then never matched the tracked "001" and silently failed to clear
    # the pending ack).
    raw_tail = info[_ADDRESSEE_WIDTH + 2 :].rstrip(b"\r\n")

    text = raw_tail
    msgno: Optional[bytes] = None
    tail_window = raw_tail[-6:]
    if b"{" in tail_window:
        idx = raw_tail.rfind(b"{")
        candidate = raw_tail[idx + 1 :]
        if 1 <= len(candidate) <= 5:
            text = raw_tail[:idx]
            msgno = candidate

    return AprsMessage(
        addressee=addressee,
        text=text.decode("ascii"),
        msgno=msgno.decode("ascii") if msgno is not None else None,
    )


def build_ack(addressee: str, msgno: str) -> bytes:
    return encode_message(addressee, "ack" + msgno, msgno=None)


def build_third_party_ack(claimed_source: str, addressee: str, msgno: str, tocall: str) -> bytes:
    """Wraps an ack in APRS third-party-traffic format ("}SRC>DST:payload"),
    the same mechanism APRS-IS igates use to relay a packet under their own
    AX.25 source while presenting a different logical originating station.
    Standard APRS ack-matching on the sending station's end expects an ack
    to come from the callsign it addressed its message to; when that
    addressee isn't our own gateway_callsign (a mesh short name, node-id
    code, or any other third-party target) the sender won't recognize a
    plain ack (always sourced from gateway_callsign, per the hard
    compliance invariant) as satisfying its pending send, and keeps
    retransmitting. This lets the ack claim to be "from" claimed_source in
    the payload while the caller's actual AX.25 frame source is still
    whatever it legally has to be -- this function only builds the info
    field, it has no bearing on RF station identification."""
    for label, value in (("claimed_source", claimed_source), ("tocall", tocall)):
        if not value or any(ch in value for ch in (">", ":", "\r", "\n")):
            raise AprsMessageError(f"{label} {value!r} is not usable in a third-party header")
    inner = build_ack(addressee, msgno)
    header = f"}}{claimed_source}>{tocall}:".encode("ascii")
    return header + inner


def parse_ack(msg: AprsMessage) -> Optional[str]:
    if msg.msgno is not None:
        return None
    if not msg.text.lower().startswith("ack"):
        return None
    candidate = msg.text[3:]
    if 1 <= len(candidate) <= 5:
        return candidate
    return None

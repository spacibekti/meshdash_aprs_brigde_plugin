import pytest

from aprs_bridge.protocol import aprs_message
from aprs_bridge.protocol.aprs_message import AprsMessage
from aprs_bridge.protocol.errors import AprsMessageError

FIXTURE_INFO = b":WU2Z     :Testing{003"


def test_decode_message_known_good_fixture():
    msg = aprs_message.decode_message(FIXTURE_INFO)
    assert msg == AprsMessage(addressee="WU2Z", text="Testing", msgno="003")


def test_encode_message_known_good_fixture():
    encoded = aprs_message.encode_message("WU2Z", "Testing", "003")
    assert encoded == FIXTURE_INFO


def test_message_without_msgno_round_trips():
    encoded = aprs_message.encode_message("WU2Z", "Hello")
    assert encoded == b":WU2Z     :Hello"
    decoded = aprs_message.decode_message(encoded)
    assert decoded == AprsMessage(addressee="WU2Z", text="Hello", msgno=None)


def test_addressee_normalization_pads_and_strips():
    encoded = aprs_message.encode_message("WU2Z", "hi")
    assert encoded.startswith(b":WU2Z     :")  # padded to 9 chars
    decoded = aprs_message.decode_message(encoded)
    assert decoded.addressee == "WU2Z"


def test_text_at_max_length_encodes_fine():
    text = "x" * 67
    encoded = aprs_message.encode_message("WU2Z", text)
    assert aprs_message.decode_message(encoded).text == text


def test_text_over_max_length_raises():
    with pytest.raises(AprsMessageError):
        aprs_message.encode_message("WU2Z", "x" * 68)


def test_empty_text_raises():
    with pytest.raises(AprsMessageError):
        aprs_message.encode_message("WU2Z", "")


@pytest.mark.parametrize("bad_char", ["|", "~", "{"])
def test_forbidden_characters_raise(bad_char):
    with pytest.raises(AprsMessageError):
        aprs_message.encode_message("WU2Z", f"hello{bad_char}world")


def test_addressee_over_nine_chars_raises():
    with pytest.raises(AprsMessageError):
        aprs_message.encode_message("TOOLONGCALL", "hi")


def test_build_ack():
    assert aprs_message.build_ack("WU2Z", "003") == b":WU2Z     :ack003"


def test_build_third_party_ack():
    wrapped = aprs_message.build_third_party_ack("PGR", "N0CALL-10", "003", "APZBRD")
    assert wrapped == b"}PGR>APZBRD:" + b":N0CALL-10:ack003"


def test_build_third_party_ack_inner_payload_decodes_normally():
    # The part after the third-party header ("}SRC>DST:") is a normal,
    # independently valid ack payload -- a client that ignores/strips
    # third-party framing but still parses the remainder would see a
    # well-formed ack, not garbage.
    wrapped = aprs_message.build_third_party_ack("PGR", "N0CALL-10", "003", "APZBRD")
    header, inner_info = wrapped.split(b":", 1)
    assert header == b"}PGR>APZBRD"
    decoded = aprs_message.decode_message(inner_info)
    assert aprs_message.parse_ack(decoded) == "003"


@pytest.mark.parametrize("bad_value", [">", ":", "N0\r10", "N0\n10", ""])
def test_build_third_party_ack_rejects_unusable_claimed_source(bad_value):
    with pytest.raises(AprsMessageError):
        aprs_message.build_third_party_ack(bad_value, "N0CALL-10", "003", "APZBRD")


@pytest.mark.parametrize("bad_value", [">", ":", ""])
def test_build_third_party_ack_rejects_unusable_tocall(bad_value):
    with pytest.raises(AprsMessageError):
        aprs_message.build_third_party_ack("PGR", "N0CALL-10", "003", bad_value)


def test_parse_ack_on_ack_message():
    ack_info = aprs_message.build_ack("WU2Z", "003")
    msg = aprs_message.decode_message(ack_info)
    assert aprs_message.parse_ack(msg) == "003"


def test_parse_ack_accepts_mixed_case_ack_prefix():
    msg = aprs_message.decode_message(b":W4BRD-13 :Ack005")
    assert aprs_message.parse_ack(msg) == "005"


def test_parse_ack_on_ordinary_message_returns_none():
    msg = aprs_message.decode_message(FIXTURE_INFO)
    assert aprs_message.parse_ack(msg) is None


def test_is_message_true_for_message_info():
    assert aprs_message.is_message(FIXTURE_INFO) is True


def test_is_message_false_for_non_message_info():
    assert aprs_message.is_message(b"!4903.50N/07201.75W-Test") is False


def test_decode_message_no_leading_colon_raises():
    with pytest.raises(AprsMessageError):
        aprs_message.decode_message(b"not a message at all")


def test_decode_message_malformed_addressee_framing_raises():
    # Byte 10 should be ':'; here it's 'X' instead.
    with pytest.raises(AprsMessageError):
        aprs_message.decode_message(b":WU2Z     Xhello")


def test_decode_message_strips_trailing_cr_from_real_ack():
    # Real bytes captured live from an actual station's ack: some
    # radios/software append a trailing CR to the info field. Without
    # stripping it, this decoded msgno="001\r", which never matched the
    # tracked "001" and silently failed to clear the pending ack.
    real_ack_info = bytes.fromhex("3a57344252442d3133203a61636b3030310d")
    assert real_ack_info == b":W4BRD-13 :ack001\r"
    msg = aprs_message.decode_message(real_ack_info)
    assert msg == AprsMessage(addressee="W4BRD-13", text="ack001", msgno=None)
    assert aprs_message.parse_ack(msg) == "001"


def test_decode_message_strips_trailing_crlf_from_ordinary_text():
    msg = aprs_message.decode_message(b":WU2Z     :Testing\r\n")
    assert msg.text == "Testing"


def test_decode_message_strips_trailing_cr_after_explicit_msgno():
    msg = aprs_message.decode_message(b":WU2Z     :Testing{003\r")
    assert msg == AprsMessage(addressee="WU2Z", text="Testing", msgno="003")


def test_decode_message_normalizes_addressee_to_uppercase():
    msg = aprs_message.decode_message(b":w4brd-13 :hello")
    assert msg.addressee == "W4BRD-13"

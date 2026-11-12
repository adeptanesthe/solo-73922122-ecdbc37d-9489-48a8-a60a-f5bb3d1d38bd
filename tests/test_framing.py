"""Tests for MLLP framing edge cases and the ACK envelope."""

from __future__ import annotations

import pytest

from app.hl7 import (
    ERR_SEGMENT_SEQUENCE,
    ERR_TABLE_VALUE,
    ERR_UNSUPPORTED_MESSAGE_TYPE,
    ERR_UNSUPPORTED_VERSION,
    MLLPError,
    extract_envelope,
    mllp_frame,
    normalize_payload,
)

from tests.test_lis import make_message


def test_crlf_and_lf_become_cr_and_final_cr_preserved():
    text = make_message("N1", "S1", [("GLU", "5.0", "mmol/L", "F")])
    canonical = normalize_payload(text.encode())
    assert canonical == text.encode()
    assert normalize_payload(text.replace("\r", "\r\n").encode()) == text.encode()
    assert normalize_payload(text.replace("\r", "\n").encode()) == text.encode()
    assert canonical.endswith(b"\r")


def test_empty_and_control_payloads_rejected():
    for payload in (b"", b"\r\n", "MSH|é".encode(), b"MSH|a\x00b", b"MSH\x0babc"):
        with pytest.raises(MLLPError):
            normalize_payload(payload)
    with pytest.raises(MLLPError):
        normalize_payload(b"MSH|abc\x1cdef")  # stray FS inside frame


def test_envelope_extraction_survives_bad_body():
    text = "MSH|^~\\&|APP|FAC|LIS|HOSP|1||ORM^O01|BAD-1|P|2.5.1\rGARBAGE"
    env = extract_envelope(text.encode())
    assert env.control_id == "BAD-1"
    assert env.sending_app == "APP"
    assert env.receiving_facility == "HOSP"


def test_envelope_empty_for_non_msh_payload():
    env = extract_envelope(b"JUNK|1|2")
    assert env.control_id == ""
    assert env.version == "2.5.1"


def test_message_hash_is_line_ending_independent():
    from app.hl7 import message_hash
    text = make_message("N2", "S2", [("GLU", "5.0", "mmol/L", "F")])
    a = message_hash(normalize_payload(text.encode()))
    b = message_hash(normalize_payload(text.replace("\r", "\r\n").encode()))
    assert a == b


def test_mllp_frame_wraps_with_vt_fs_cr():
    assert mllp_frame(b"MSH|x\r") == b"\x0bMSH|x\r\x1c\r"


def test_ack_error_segment_carries_code_and_segment():
    from app.hl7 import build_ack
    env = extract_envelope(
        make_message("N3", "S3", [("GLU", "5", "x", "F")]).encode())
    ack = build_ack(env, "AE", "bad value", "102", "OBX").decode()
    parts = ack.rstrip("\r").split("\r")
    assert parts[1].startswith("MSA|AE|N3|")
    assert parts[2] == "ERR|OBX^102^HL70357||E"


def test_error_codes_are_distinct():
    assert len({ERR_SEGMENT_SEQUENCE, ERR_TABLE_VALUE,
                ERR_UNSUPPORTED_MESSAGE_TYPE, ERR_UNSUPPORTED_VERSION}) == 4

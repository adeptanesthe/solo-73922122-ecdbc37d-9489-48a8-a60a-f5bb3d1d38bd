"""Rejection table: every mutation must produce an AE and persist nothing."""

from __future__ import annotations

import pytest

from app.hl7 import HL7Error, normalize_payload, parse_message
from app.store import Store

from tests.test_lis import make_message


def base() -> str:
    return make_message("V1", "S1", [("GLU", "5.0", "mmol/L", "F"),
                                      ("ALT", "30", "U/L", "F")])


def drop_second_obx(text: str) -> str:
    lines = text.split("\r")
    keep = [line for line in lines if not line.startswith("OBX")]
    return "\r".join(keep) + "\r"


def set_obx11(text: str, new: str) -> str:
    head, *obs = text.split("\r")
    last = obs[-2]  # second OBX (trailing empty element follows the final CR)
    fields = last.split("|")
    fields[11] = new
    return "\r".join([head] + obs[:-2] + ["|".join(fields)] + obs[-1:])


def set_field(text: str, index: int, value: str, occurrence: int = 1) -> str:
    segments = [s for s in text.split("\r") if s]
    count = 0
    for i, segment in enumerate(segments):
        fields = segment.split("|")
        if fields[0] == "OBX":
            count += 1
            if count == occurrence:
                fields[index] = value
                segments[i] = "|".join(fields)
                break
    return "\r".join(segments) + "\r"


CASES = {
    "wrong message type": lambda t: t.replace("ORU^R01", "ORM^O01", 1),
    "wrong version": lambda t: t.replace("|2.5.1\r", "|2.4\r", 1),
    "missing MSH-10": lambda t: t.replace("|V1|", "||", 1),
    "missing OBR-3": lambda t: t.replace("|S1|", "||", 1),
    "non-numeric OBX-5": lambda t: t.replace("|5.0|", "|abc|", 1),
    "empty OBX-5": lambda t: set_field(t, 5, ""),
    "OBX-11 Z": lambda t: set_obx11(t, "Z"),
    "OBX-2 TX": lambda t: set_field(t, 2, "TX"),
    "no OBX": drop_second_obx,
    "duplicate OBX-3": lambda t: set_field(t, 3, "GLU^Analyte", 2),
    "21 OBX segments": lambda t: t + "\r".join(
        "OBX|{0}|NM|C{0}^x||1|U|||||F".format(i) for i in range(3, 22)) + "\r",
    "second OBR": lambda t: t + "OBR|2|S01|S1\r",
    "unsupported segment": lambda t: t + "ZZZ|1|x\r",
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_each_invalid_message_is_rejected_without_writes(name, tmp_path):
    text = CASES[name](base())
    store = Store(str(tmp_path / "r.db"))
    store.init()
    with pytest.raises(HL7Error):
        store.apply(parse_message(normalize_payload(text.encode())))
    assert store.sample_results("S1")["items"] == []
    assert store.message("V1") is None


def test_exactly_twenty_obx_is_accepted(tmp_path):
    items = [(f"C{i}", str(i), "U", "F") for i in range(1, 21)]
    text = make_message("V2", "S2", items)
    store = Store(str(tmp_path / "ok.db"))
    store.init()
    result = store.apply(parse_message(normalize_payload(text.encode())))
    assert result.applied
    assert len(store.sample_results("S2")["items"]) == 20

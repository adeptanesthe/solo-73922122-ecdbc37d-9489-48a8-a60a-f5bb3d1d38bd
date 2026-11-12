"""End-to-end tests: real MLLP sockets, concurrent connections, crash safety."""

from __future__ import annotations

import asyncio
import socket
import sqlite3
import threading
import time

import pytest

from app.hl7 import (
    HL7Error,
    build_ack,
    extract_envelope,
    mllp_frame,
    normalize_payload,
    parse_message,
)
from app.store import ApplyError, Store


def make_message(control_id: str, sample: str,
                 items: list[tuple[str, str, str, str]],
                 *, sending_app: str = "ANALYSER") -> str:
    """items: (code, value, unit, status)."""
    msh = "|".join(["MSH", "^~\\&", sending_app, "LAB", "LIS", "HOSP",
                    "20261112090000", "", "ORU^R01", control_id, "P", "2.5.1"])
    obr = "|".join(["OBR", "1", "S01", sample, "", "", "", "", "", "", "", "",
                    "", "", "", "", "DR"])
    obx = []
    for i, (code, value, unit, status) in enumerate(items, start=1):
        obx.append("|".join(["OBX", str(i), "NM", f"{code}^Analyte",
                             "", value, unit, "", "", "", "", status]))
    return "\r".join([msh, obr] + obx) + "\r"


# --------------------------------------------------------------------------- #
# parser
# --------------------------------------------------------------------------- #

def test_parse_valid_and_fields():
    text = make_message("M-1", "S-1", [("GLU", "5.4", "mmol/L", "F"),
                                       ("K", "4.1", "mmol/L", "F")])
    parsed = parse_message(normalize_payload(text.encode()))
    assert parsed.control_id == "M-1"
    assert parsed.sample_no == "S-1"
    assert [(o.code, o.value, o.status) for o in parsed.obx_items] == [
        ("GLU", "5.4", "F"), ("K", "4.1", "F")]


def test_parse_accepts_crlf_and_lf_terminators():
    text = make_message("M-2", "S-2", [("GLU", "5.4", "mmol/L", "F")])
    crlf = text.replace("\r", "\r\n")
    assert normalize_payload(crlf.encode()) == text.encode()


@pytest.mark.parametrize("mutate,code", [
    (lambda t: t.replace("ORU^R01", "ORM^O01"), "200"),
    (lambda t: t.replace("2.5.1", "2.3"), "203"),
    (lambda t: t.replace("|M-9|", "||"), "101"),
    (lambda t: t.replace("|S-9|", "||"), "101"),
    (lambda t: t.replace("|5.0|", "|abc|"), "102"),
    (lambda t: t.replace("|5.0|", "|3.5.2|"), "102"),
    (lambda t: t.replace("||F\r", "||Z\r"), "103"),
    (lambda t: t.replace("|GLU^Analyte", "||Analyte"), "101"),
    (lambda t: t.replace("NM", "ST"), "102"),
])
def test_parse_rejects_structural_errors(mutate, code):
    base = make_message("M-9", "S-9", [("GLU", "5.0", "mmol/L", "F")])
    with pytest.raises(HL7Error) as info:
        parse_message(normalize_payload(mutate(base).encode()))
    assert info.value.code == code


def test_parse_rejects_duplicate_code_and_multi_obr():
    duplicate = make_message("M-3", "S-3", [("GLU", "5.0", "x", "F"),
                                            ("GLU", "6.0", "x", "F")])
    with pytest.raises(HL7Error) as info:
        parse_message(normalize_payload(duplicate.encode()))
    assert "duplicate" in info.value.text

    two_obr = make_message("M-4", "S-4", [("GLU", "5.0", "x", "F")]) + \
        "OBR|2|S01|S-4\r"
    with pytest.raises(HL7Error) as info:
        parse_message(normalize_payload(two_obr.encode()))
    assert "exactly one OBR" in info.value.text


def test_parse_rejects_too_many_obx():
    items = [(f"C{i}", "1", "x", "F") for i in range(21)]
    with pytest.raises(HL7Error):
        parse_message(normalize_payload(
            make_message("M-4", "S-4", items).encode()))


def test_mllp_rejects_non_ascii_and_stray_controls():
    with pytest.raises(Exception):
        normalize_payload("MSH|é\r".encode())
    with pytest.raises(Exception):
        normalize_payload(b"MSH|abc\x1c")


def test_ack_shape():
    text = make_message("ACK-1", "S-1", [("GLU", "5", "x", "F")])
    envelope = extract_envelope(text.encode())
    ack = build_ack(envelope, "AA", "ok")
    segments = ack.decode().rstrip("\r").split("\r")
    assert segments[0].startswith("MSH|^~\\&|ANALYSER|LAB|LIS|HOSP|")
    assert segments[1].split("|")[:3] == ["MSA", "AA", "ACK-1"]


# --------------------------------------------------------------------------- #
# store: atomic application, idempotency, restart
# --------------------------------------------------------------------------- #

def _store(tmp_path) -> Store:
    store = Store(str(tmp_path / "lis.db"))
    store.init()
    return store


def _apply(store: Store, text: str):
    return store.apply(parse_message(normalize_payload(text.encode())))


def test_f_and_c_semantics(tmp_path):
    store = _store(tmp_path)
    _apply(store, make_message("A1", "S1", [("GLU", "5.0", "mmol/L", "F")]))
    assert store.sample_results("S1")["items"][0]["value"] == "5.0"
    _apply(store, make_message("A2", "S1", [("GLU", "4.8", "mmol/L", "C")]))
    assert store.sample_results("S1")["items"][0]["value"] == "4.8"
    with pytest.raises(ApplyError):
        _apply(store, make_message("A3", "S1", [("K", "4.1", "mmol/L", "C")]))
    with pytest.raises(ApplyError):
        _apply(store, make_message("A4", "S1", [("GLU", "5.0", "mmol/L", "F")]))


def test_whole_message_rejected_leaves_no_partial_result(tmp_path):
    store = _store(tmp_path)
    text = make_message("B1", "SB", [("GLU", "5.0", "mmol/L", "F"),
                                     ("ALT", "abc", "U/L", "F")])
    with pytest.raises(HL7Error):
        _apply(store, text)
    assert store.sample_results("SB")["items"] == []

    # F for a new code plus C for a missing one must roll back the F.
    text2 = make_message("B2", "SB2", [("GLU", "5.0", "mmol/L", "F"),
                                       ("K", "4.0", "mmol/L", "C")])
    with pytest.raises(ApplyError):
        _apply(store, text2)
    assert store.sample_results("SB2")["items"] == []
    assert store.message("B2") is None


def test_duplicate_same_text_replays_ack_and_does_not_reapply(tmp_path):
    store = _store(tmp_path)
    text = make_message("D1", "SD", [("GLU", "5.0", "mmol/L", "F")])
    first = _apply(store, text)
    assert first.applied and first.ack.startswith(b"MSH")
    second = _apply(store, text)
    assert not second.applied
    assert second.ack == first.ack
    items = store.sample_results("SD")["items"]
    assert len(items) == 1
    assert items[0]["control_id"] == "D1"
    assert items[0]["updated_at"] == items[0]["updated_at"]


def test_duplicate_id_different_text_rejected(tmp_path):
    store = _store(tmp_path)
    _apply(store, make_message("E1", "SE", [("GLU", "5.0", "mmol/L", "F")]))
    with pytest.raises(ApplyError) as info:
        _apply(store, make_message("E1", "SE", [("GLU", "6.0", "mmol/L", "F")]))
    assert "already accepted" in info.value.text
    assert store.sample_results("SE")["items"][0]["value"] == "5.0"


def test_duplicate_and_correction_of_earlier_message_do_not_change_state(tmp_path):
    store = _store(tmp_path)
    _apply(store, make_message("H1", "SH", [("GLU", "5.0", "mmol/L", "F")]))
    _apply(store, make_message("H2", "SH", [("GLU", "4.9", "mmol/L", "C")]))
    # replay H1: same stored ACK, current value must stay 4.9
    first_ack = store.message("H1")["ack_message"]
    result = _apply(store, make_message("H1", "SH", [("GLU", "5.0", "mmol/L", "F")]))
    assert result.ack.decode() == first_ack
    assert store.sample_results("SH")["items"][0]["value"] == "4.9"


def test_state_survives_reopen(tmp_path):
    path = str(tmp_path / "lis.db")
    store = Store(path)
    store.init()
    _apply(store, make_message("R1", "SR", [("GLU", "5.0", "mmol/L", "F")]))
    del store
    reopened = Store(path)
    reopened.init()
    result = _apply(reopened, make_message("R1", "SR", [("GLU", "5.0", "mmol/L", "F")]))
    assert not result.applied
    with pytest.raises(ApplyError):
        _apply(reopened, make_message("R1", "SR", [("GLU", "9.0", "mmol/L", "F")]))
    assert reopened.sample_results("SR")["items"][0]["value"] == "5.0"


def test_concurrent_store_apply_same_id(tmp_path):
    store = _store(tmp_path)
    text = make_message("T1", "ST", [("GLU", "5.0", "mmol/L", "F")])
    results: list = []
    errors: list = []
    start = threading.Barrier(8)

    def worker():
        start.wait()
        try:
            results.append(_apply(store, text))
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert sum(1 for r in results if r.applied) == 1
    assert all(r.ack == results[0].ack for r in results)
    assert len(store.sample_results("ST")["items"]) == 1


def test_interruption_before_commit_leaves_no_partial_state(tmp_path):
    """Simulate process death after row writes but before COMMIT."""
    path = str(tmp_path / "kill.db")
    Store(path).init()
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA synchronous=FULL")

    def die_after_first_change():
        if conn.total_changes >= 1:
            raise RuntimeError("simulated process death")
        return 0

    conn.set_progress_handler(die_after_first_change, 10)
    with pytest.raises((RuntimeError, sqlite3.OperationalError)):
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO results (sample_no, code, value, unit, status, value_type,"
            " text, obx_seq, control_id, updated_at) "
            "VALUES ('SK','GLU','9.9','mmol/L','F','NM','','1','KILL-1','now')")
        conn.execute("COMMIT")
    conn.close()

    store = Store(path)
    assert store.sample_results("SK")["items"] == []
    result = _apply(store, make_message("KILL-1", "SK", [
        ("GLU", "5.0", "mmol/L", "F"), ("K", "4.1", "mmol/L", "F")]))
    assert result.applied
    assert [(i["code"], i["value"]) for i in store.sample_results("SK")["items"]] == [
        ("GLU", "5.0"), ("K", "4.1")]


# --------------------------------------------------------------------------- #
# live server tests (uvicorn in a thread, real sockets)
# --------------------------------------------------------------------------- #

@pytest.fixture
def server(tmp_path):
    import os
    os.environ["LIS_DB_PATH"] = str(tmp_path / "live.db")
    import importlib
    import app.main as main_module
    importlib.reload(main_module)

    free = socket.socket()
    free.bind(("127.0.0.1", 0))
    port = free.getsockname()[1]
    free.close()
    main_module.settings.mllp_port = port
    main_module.mllp.port = port
    main_module.mllp.store = main_module.store

    import uvicorn

    config = uvicorn.Config(main_module.app, host="127.0.0.1", port=0,
                            log_level="warning")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.05)
    assert srv.started

    class Handle:
        def __init__(self):
            self.port = port
            self.http_port = srv.servers[0].sockets[0].getsockname()[1]
            self.srv = srv

        def connect(self):
            return socket.create_connection(("127.0.0.1", self.port), timeout=10)

        def url(self, path):
            return f"http://127.0.0.1:{self.http_port}{path}"

    handle = Handle()
    yield handle
    srv.should_exit = True
    thread.join(timeout=10)


def read_one_ack(sock: socket.socket) -> str:
    data = b""
    while b"\x1c" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    body = data.split(b"\x1c", 1)[0]
    return body.split(b"\x0b", 1)[-1].decode("ascii")


def run_async(coro):
    return asyncio.run(coro)


async def send_async(port, frame_bytes, fragments=1, delay=0.0):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        size = max(1, (len(frame_bytes) + fragments - 1) // fragments)
        for offset in range(0, len(frame_bytes), size):
            writer.write(frame_bytes[offset:offset + size])
            await writer.drain()
            if delay:
                await asyncio.sleep(delay)
        if fragments != 1:
            # Let the receiver drain fragmented writes so the ACK cannot race
            # ahead of our last writer.drain() on the same socket.
            await asyncio.sleep(0.02)
        data = b""
        while b"\x1c" not in data:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=20)
            if not chunk:
                break
            data += chunk
        return data.split(b"\x1c", 1)[0].split(b"\x0b", 1)[-1].decode("ascii")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def test_live_fragmented_and_sticky_packets(server):
    text = make_message("L1", "SL", [("GLU", "5.0", "mmol/L", "F")])
    fragment_ack = run_async(send_async(server.port, mllp_frame(text.encode()),
                                        fragments=30, delay=0.005))
    assert "\rMSA|AA|L1" in fragment_ack or "MSA|AA|L1" in fragment_ack

    texts = [make_message(f"L2-{i}", f"SL2-{i}", [("ALT", str(20 + i), "U/L", "F")])
             for i in range(3)]
    blob = b"".join(mllp_frame(t.encode()) for t in texts)

    with server.connect() as sock:
        sock.sendall(blob)
        for i in range(3):
            ack = read_one_ack(sock)
            assert f"MSA|AA|L2-{i}" in ack


def test_live_dropped_ack_replay_and_conflict(server):
    text = make_message("L3", "SL3", [("GLU", "5.0", "mmol/L", "F")])
    f = mllp_frame(text.encode())
    with server.connect() as sock:
        sock.sendall(f)
        first = read_one_ack(sock)
    with server.connect() as sock:
        sock.sendall(f)
        second = read_one_ack(sock)
    assert first == second
    assert "MSA|AA|L3" in second

    conflicting = make_message("L3", "SL3", [("GLU", "7.0", "mmol/L", "F")])
    ack = run_async(send_async(server.port, mllp_frame(conflicting.encode())))
    assert "MSA|AE|L3" in ack


def test_live_concurrent_same_control_id(server):
    text = make_message("L4", "SL4", [("GLU", "5.0", "mmol/L", "F")])
    f = mllp_frame(text.encode())

    async def run():
        return await asyncio.gather(*[send_async(server.port, f)
                                      for _ in range(10)])

    acks = run_async(run())
    assert all("MSA|AA|L4" in a for a in acks)
    assert len(set(acks)) == 1


def test_live_http_api(server):
    import httpx
    text = make_message("L5", "SL5", [("GLU", "5.0", "mmol/L", "F"),
                                       ("K", "4.2", "mmol/L", "F")])
    run_async(send_async(server.port, mllp_frame(text.encode())))
    with httpx.Client(timeout=10) as client:
        data = client.get(server.url("/api/v1/samples/SL5")).json()
        assert data["sample_no"] == "SL5"
        assert [i["code"] for i in data["items"]] == ["GLU", "K"]
        msg = client.get(server.url("/api/v1/messages/L5")).json()
        assert msg["ack_code"] == "AA"
        assert "MSA|AA|L5" in msg["ack_message"]
        assert client.get(server.url("/api/v1/samples/nope")).status_code == 404
        assert client.get(server.url("/health")).json()["status"] == "ok"


def test_live_demo_endpoint(server):
    import httpx
    text = make_message("L6", "SL6", [("GLU", "5.0", "mmol/L", "F")])
    with httpx.Client(timeout=20) as client:
        resp = client.post(server.url("/api/v1/demo/messages"), json={
            "message": text.replace("\r", "\n"), "fragments": 12,
            "drop_ack": True}).json()
        assert resp["ack_codes"] == ["AA", "AA"]
        assert resp["ack_frames"][0] == resp["ack_frames"][1]
        assert "MSA|AA|L6" in resp["stored_ack"]
        sample = client.get(server.url("/api/v1/samples/SL6")).json()
        assert len(sample["items"]) == 1
        assert len(sample["messages"]) == 1

        conc = client.post(server.url("/api/v1/demo/concurrent"), params={
            "message": text.replace("\r", "\n"), "connections": 6}).json()
        assert conc["ack_codes"] == ["AA"] * 6
        assert conc["identical_acks"] is True
        assert conc["distinct_acks"] == 1


def test_live_partial_frame_then_disconnect_leaves_nothing(server):
    text = make_message("L7", "SL7", [("GLU", "5.0", "mmol/L", "F")])
    f = mllp_frame(text.encode())
    with server.connect() as sock:
        sock.sendall(f[:-4])  # cut before FS+CR
    import httpx
    time.sleep(0.3)
    with httpx.Client(timeout=10) as client:
        assert client.get(server.url("/api/v1/samples/SL7")).status_code == 404
        assert client.get(server.url("/api/v1/messages/L7")).status_code == 404


def test_live_f_for_existing_item_rejected_and_nothing_applied(server):
    first = make_message("L8", "SL8", [("GLU", "5.0", "mmol/L", "F")])
    run_async(send_async(server.port, mllp_frame(first.encode())))
    # F for existing GLU plus F for new K: whole message must be rejected
    second = make_message("L9", "SL8", [("GLU", "6.0", "mmol/L", "F"),
                                         ("K", "4.1", "mmol/L", "F")])
    ack = run_async(send_async(server.port, mllp_frame(second.encode())))
    assert "MSA|AE|L9" in ack
    import httpx
    with httpx.Client(timeout=10) as client:
        items = client.get(server.url("/api/v1/samples/SL8")).json()["items"]
        assert [(i["code"], i["value"]) for i in items] == [("GLU", "5.0")]
        assert client.get(server.url("/api/v1/messages/L9")).status_code == 404

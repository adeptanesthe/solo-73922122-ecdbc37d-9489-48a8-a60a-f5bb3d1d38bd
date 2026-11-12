"""Restart/durability checks that operate on a real SQLite file."""

from __future__ import annotations

import threading

from app.hl7 import normalize_payload, parse_message
from app.store import Store

from tests.test_lis import make_message


def _open(path: str) -> Store:
    store = Store(path)
    store.init()
    return store


def _apply(store: Store, text: str):
    return store.apply(parse_message(normalize_payload(text.encode())))


def test_ack_bytes_identical_after_process_restart(tmp_path):
    path = str(tmp_path / "r.db")
    first_store = _open(path)
    result = _apply(first_store, make_message("RR-1", "SRR",
                                              [("GLU", "5.0", "mmol/L", "F")]))
    original_ack = result.ack
    del first_store

    reopened = _open(path)
    replay = _apply(reopened, make_message("RR-1", "SRR",
                                           [("GLU", "5.0", "mmol/L", "F")]))
    assert not replay.applied
    assert replay.ack == original_ack
    assert len(reopened.sample_results("SRR")["items"]) == 1


def test_writer_lock_serialises_two_store_instances(tmp_path):
    path = str(tmp_path / "c.db")
    _open(path)
    text = make_message("CC-1", "SCC", [("GLU", "5.0", "mmol/L", "F")])
    barrier = threading.Barrier(6)
    outcomes = []

    def worker():
        store = Store(path)
        barrier.wait()
        try:
            outcomes.append(store.apply(parse_message(
                normalize_payload(text.encode()))))
        except Exception as exc:  # pragma: no cover
            outcomes.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(not isinstance(o, Exception) for o in outcomes)
    assert sum(1 for o in outcomes if o.applied) == 1
    assert len({o.ack for o in outcomes}) == 1
    assert len(_open(path).sample_results("SCC")["items"]) == 1


def test_concurrent_correction_after_base_result(tmp_path):
    path = str(tmp_path / "c2.db")
    store = _open(path)
    _apply(store, make_message("CC2-F", "SCC2", [("GLU", "5.0", "mmol/L", "F")]))
    barrier = threading.Barrier(4)
    ack_codes = []

    def correct(control_id: str, value: str):
        s = Store(path)
        barrier.wait()
        r = s.apply(parse_message(normalize_payload(
            make_message(control_id, "SCC2", [("GLU", value, "mmol/L", "C")]
                         ).encode())))
        ack_codes.append(r.applied)

    threads = [threading.Thread(target=correct, args=(f"CC2-C{i}", v))
               for i, v in enumerate(["4.0", "4.1", "4.2", "4.3"])]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert ack_codes == [True, True, True, True]
    item = _open(path).sample_results("SCC2")["items"][0]
    assert item["value"] in {"4.0", "4.1", "4.2", "4.3"}
    assert item["status"] == "C"

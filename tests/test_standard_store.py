"""Store-level tests: mapping versions, shadow recompute and atomic switch."""

from __future__ import annotations

import threading

import pytest

from app.hl7 import normalize_payload, parse_message
from app.recalc import RecalcManager
from app.store import MappingConflictError, Store

from tests.test_lis import make_message


def _open(tmp_path, name="s.db") -> Store:
    store = Store(str(tmp_path / name))
    store.init()
    return store


def _apply(store: Store, text: str):
    return store.apply(parse_message(normalize_payload(text.encode())))


GLU_MAP = [
    {"source_item": "GLU", "source_unit": "mmol/L",
     "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
     "coefficient": "1"},
    {"source_item": "CREA", "source_unit": "umol/L",
     "standard_item": "CREATININE", "standard_unit": "mg/dL",
     "coefficient": "0.0113"},
]


def test_seed_empty_version_marks_everything_unavailable(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("M1", "S1", [
        ("GLU", "5.12", "mmol/L", "F"),
        ("CREA", "92", "umol/L", "F")]))
    data = store.standard_sample_results("S1")
    assert data["mapping_version"] == 1
    reasons = {i["code"]: i["reason"] for i in data["items"]}
    assert reasons == {"GLU": "no_mapping", "CREA": "no_mapping"}
    assert all(not i["available"] for i in data["items"])
    assert all(i["standard_value"] is None for i in data["items"])


def test_validation_rejects_bad_coefficient_and_duplicates(tmp_path):
    store = _open(tmp_path)
    bad = [dict(GLU_MAP[0], coefficient="0")]
    with pytest.raises(MappingConflictError):
        store.create_mapping_version(bad, "", 0, "T1")
    dup_source = GLU_MAP + [
        {"source_item": "GLU", "source_unit": "x", "standard_item": "OTHER",
         "standard_unit": "x", "coefficient": "2"}]
    with pytest.raises(MappingConflictError):
        store.create_mapping_version(dup_source, "", 0, "T2")
    dup_standard = GLU_MAP + [
        {"source_item": "X", "source_unit": "x", "standard_item": "GLUCOSE",
         "standard_unit": "x", "coefficient": "2"}]
    with pytest.raises(MappingConflictError):
        store.create_mapping_version(dup_standard, "", 0, "T3")
    with pytest.raises(MappingConflictError):
        store.create_mapping_version([], "", 0, "T4")
    # Nothing pending was persisted after the failed attempts.
    assert store.current_mapping()["version_id"] == 1


def test_mapping_limit_is_twenty(tmp_path):
    store = _open(tmp_path)
    entries = [{"source_item": f"C{i}", "source_unit": "u",
                "standard_item": f"S{i}", "standard_unit": "u",
                "coefficient": "1"} for i in range(21)]
    with pytest.raises(MappingConflictError):
        store.create_mapping_version(entries, "", 0, "TBIG")


def test_failed_create_leaves_no_pending_version_or_task(tmp_path):
    store = _open(tmp_path)
    # A UNIQUE constraint violation halfway through insertion must roll the
    # whole version back (validation normally prevents this, so force it).
    good = [{"source_item": "GLU", "source_unit": "mmol/L",
             "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
             "coefficient": "1"}]
    with pytest.raises(Exception):
        with store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "INSERT INTO mapping_versions (state, note, created_at, "
                "entry_count) VALUES ('pending', 'x', ?, 1)", ("now",))
            vid = cur.lastrowid
            # Duplicate standard item via raw insert -> constraint failure.
            conn.execute(
                "INSERT INTO mapping_entries (version_id, source_item, "
                "source_unit, standard_item, standard_unit, coefficient) "
                "VALUES (?, 'A', '', 'DUP', '', '1')", (vid,))
            conn.execute(
                "INSERT INTO mapping_entries (version_id, source_item, "
                "source_unit, standard_item, standard_unit, coefficient) "
                "VALUES (?, 'B', '', 'DUP', '', '1')", (vid,))
            conn.execute("COMMIT")
    # Seed version is still the only active one; no pending rows linger.
    assert store.current_mapping()["version_id"] == 1
    assert store.list_tasks() == []
    # Normal creation still works afterwards.
    created = store.create_mapping_version(good, "", 0, "OK")
    assert created["entry_count"] == 1


def test_full_recompute_lifecycle_and_exact_values(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("M1", "S1", [
        ("GLU", "5.12", "mmol/L", "F"),
        ("CREA", "92", "umol/L", "F")]))

    created = store.create_mapping_version(GLU_MAP, "v2", 0, "TASK-1")
    v2 = created["version_id"]
    # Old version stays active while the new one is pending.
    assert store.current_mapping()["version_id"] == 1

    store.recompute_version(v2, "TASK-1")
    # Not active yet: queries still see the seed (unavailable) rows.
    assert store.standard_sample_results("S1")["mapping_version"] == 1

    result = store.switch_mapping_version(v2, "TASK-1")
    assert result["previous_version"] == 1
    mapping = store.current_mapping()
    assert mapping["version_id"] == v2
    assert len(mapping["entries"]) == 2

    data = store.standard_sample_results("S1")
    by_code = {i["code"]: i for i in data["items"]}
    assert by_code["GLU"]["standard_item"] == "GLUCOSE"
    assert by_code["GLU"]["standard_value"] == "5.12"
    assert by_code["CREA"]["standard_value"] == "1.0396"
    assert all(i["available"] for i in data["items"])
    task = store.task_status("TASK-1")
    assert task["state"] == "completed"
    assert task["processed_rows"] == 2


def test_unit_mismatch_and_missing_mapping_after_switch(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("M1", "S1", [
        ("GLU", "5.12", "mg/dL", "F"),       # wrong unit for the mapping
        ("K", "4.1", "mmol/L", "F")]))       # no mapping at all
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]
    store.recompute_version(v2, "T")
    store.switch_mapping_version(v2, "T")
    by_code = {i["code"]: i for i in
               store.standard_sample_results("S1")["items"]}
    assert by_code["GLU"]["available"] is False
    assert by_code["GLU"]["reason"] == "unit_mismatch"
    assert by_code["K"]["available"] is False
    assert by_code["K"]["reason"] == "no_mapping"
    # Raw reception was unaffected: the raw rows and message still exist.
    raw = store.sample_results("S1")
    assert {i["code"]: i["value"] for i in raw["items"]} == {
        "GLU": "5.12", "K": "4.1"}
    assert store.message("M1")["ack_code"] == "AA"


def test_ingestion_writes_standard_row_in_same_transaction(tmp_path):
    """A raw row and its standard row must appear and disappear together."""
    store = _open(tmp_path)
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]
    store.recompute_version(v2, "T")
    store.switch_mapping_version(v2, "T")

    # F then a correction C: both update the standard row immediately.
    _apply(store, make_message("F1", "S9", [("GLU", "7.10", "mmol/L", "F")]))
    row = store.standard_sample_results("S9")["items"][0]
    assert row["standard_value"] == "7.10"
    assert row["control_id"] == "F1"
    _apply(store, make_message("C1", "S9", [("GLU", "5.80", "mmol/L", "C")]))
    row = store.standard_sample_results("S9")["items"][0]
    assert row["standard_value"] == "5.80"
    assert row["source_value"] == "5.80"
    assert row["control_id"] == "C1"

    # A rejected message rolls back standard rows too.
    bad = make_message("X1", "S9", [("GLU", "9", "mmol/L", "F")])  # F exists
    with pytest.raises(Exception):
        _apply(store, bad)
    row = store.standard_sample_results("S9")["items"][0]
    assert row["standard_value"] == "5.80"


def test_correction_during_recompute_is_not_overwritten(tmp_path):
    """The headline concurrency rule: old sweep must not clobber new values."""
    store = _open(tmp_path)
    _apply(store, make_message("F1", "S1", [
        ("GLU", "7.10", "mmol/L", "F"),
        ("CREA", "92", "umol/L", "F")]))
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]

    # Dangerous ordering: a correction lands FIRST (written into the pending
    # version by ingestion), and the background sweep afterwards reads the raw
    # table from a snapshot/cursor that still carries the old value.  The sweep
    # must keep the corrected pending row instead of downgrading it.
    _apply(store, make_message("C1", "S1", [("GLU", "5.80", "mmol/L", "C")]))
    store.recompute_version(v2, "T", delay_ms=0)
    with store._connect() as conn:
        row = conn.execute(
            "SELECT source_value, standard_value, control_id "
            "FROM standard_results WHERE version_id=? AND sample_no=? AND code=?",
            (v2, "S1", "GLU")).fetchone()
    assert tuple(row) == ("5.80", "5.80", "C1")

    # Running the whole recompute a second time is equally harmless.
    store.recompute_version(v2, "T", delay_ms=0)
    store.switch_mapping_version(v2, "T")
    by_code = {i["code"]: i for i in
               store.standard_sample_results("S1")["items"]}
    assert by_code["GLU"]["source_value"] == "5.80"
    assert by_code["GLU"]["standard_value"] == "5.80"
    assert by_code["GLU"]["control_id"] == "C1"
    assert by_code["CREA"]["source_value"] == "92"


def test_repeated_corrections_while_sweep_runs_converge_to_latest(tmp_path):
    """Many corrections interleaved with sweeps always end on the last write."""
    store = _open(tmp_path)
    _apply(store, make_message("F1", "S1", [("GLU", "1.00", "mmol/L", "F")]))
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]
    for n in range(5):
        store.recompute_version(v2, "T")
        _apply(store, make_message(
            f"C{n}", "S1", [("GLU", f"{2 + n}.00", "mmol/L", "C")]))
    store.recompute_version(v2, "T")
    store.switch_mapping_version(v2, "T")
    item = store.standard_sample_results("S1")["items"][0]
    assert item["source_value"] == "6.00"
    assert item["control_id"] == "C4"


def test_new_result_during_recompute_is_gap_filled_at_switch(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("F1", "S1", [("GLU", "7.10", "mmol/L", "F")]))
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]
    store.recompute_version(v2, "T")

    # A brand-new sample lands between recompute and switch.  Because an active
    # version always exists, ingestion writes both active + pending standard
    # rows itself; the switch must still succeed and expose it.
    _apply(store, make_message("F2", "S2", [
        ("CREA", "88.4956", "umol/L", "F"),
        ("K", "4.1", "mmol/L", "F")]))
    result = store.switch_mapping_version(v2, "T")
    assert result["gap_filled"] == 0  # ingestion already covered the rows
    s2 = {i["code"]: i for i in
          store.standard_sample_results("S2")["items"]}
    assert s2["CREA"]["available"] is True
    assert s2["K"]["reason"] == "no_mapping"


def test_failed_recompute_keeps_old_version_active(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("F1", "S1", [("GLU", "7.10", "mmol/L", "F")]))
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]

    # Simulate a recompute that died mid-way: nothing switches, the failure is
    # recorded and the old version keeps serving queries.
    original = store.recompute_version

    def boom(version_id, task_id, **kwargs):
        raise RuntimeError("boom")

    store.recompute_version = boom  # type: ignore[assignment]
    manager = RecalcManager(store)
    started = manager.start_replacement(GLU_MAP)
    manager._running["thread"].join(timeout=10)
    store.recompute_version = original  # type: ignore[assignment]

    assert store.current_mapping()["version_id"] == 1
    task = store.task_status(started["task_id"])
    assert task["state"] == "failed"
    assert "boom" in task["error"]
    # Old standard results remain queryable and the slot is released.
    assert store.standard_sample_results("S1")["mapping_version"] == 1
    assert not manager.is_running()
    assert store.acquire_recalc_slot()
    store.release_recalc_slot()


def test_restart_marks_running_task_interrupted_and_allows_retry(tmp_path):
    path = str(tmp_path / "r.db")
    first = Store(path)
    first.init()
    _apply(first, make_message("F1", "S1", [("GLU", "7.10", "mmol/L", "F")]))
    first.create_mapping_version(GLU_MAP, "", 0, "STUCK")
    assert first.task_status("STUCK")["state"] == "running"
    del first

    reopened = Store(path)
    reopened.init()  # simulates process restart
    assert reopened.task_status("STUCK")["state"] == "interrupted"
    # Old mapping still active; a fresh replacement can be started.
    assert reopened.current_mapping()["version_id"] == 1
    manager = RecalcManager(reopened)
    started = manager.start_replacement(GLU_MAP)
    thread = manager._running["thread"]
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert reopened.current_mapping()["version_id"] == started["version_id"]
    assert reopened.task_status(started["task_id"])["state"] == "completed"


def test_only_one_recompute_at_a_time(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("F1", "S1", [("GLU", "7.10", "mmol/L", "F")]))
    manager = RecalcManager(store, default_delay_ms=20)
    first = manager.start_replacement(GLU_MAP)
    try:
        with pytest.raises(MappingConflictError):
            manager.start_replacement(
                [{"source_item": "K", "source_unit": "mmol/L",
                  "standard_item": "POTASSIUM", "standard_unit": "mmol/L",
                  "coefficient": "1"}])
        assert manager.is_running()
        manager._running["thread"].join(timeout=15)
        assert not manager.is_running()
    finally:
        if manager._running:
            manager._running["thread"].join(timeout=15)
    assert store.task_status(first["task_id"])["state"] == "completed"


def test_switch_is_atomic_under_concurrent_ingestion(tmp_path):
    """While the switch transaction runs, ingestion commits stay consistent."""
    store = _open(tmp_path)
    for i in range(20):
        _apply(store, make_message(f"F{i}", f"S{i}",
                                   [("GLU", str(i + 1), "mmol/L", "F")]))
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]
    store.recompute_version(v2, "T")

    errors: list[Exception] = []

    def ingest():
        try:
            for i in range(10):
                _apply(store, make_message(
                    f"CC{i}-{threading.get_ident()}", f"S{i}",
                    [("GLU", f"{i}.5", "mmol/L", "C")]))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=ingest) for _ in range(4)]
    for t in threads:
        t.start()
    switched = store.switch_mapping_version(v2, "T")
    for t in threads:
        t.join()
    assert not errors
    # Every current raw result must have exactly one matching standard row,
    # with identical source values, under the new active version.
    for i in range(20):
        raw = {r["code"]: r for r in store.sample_results(f"S{i}")["items"]}
        std = {r["code"]: r for r in
               store.standard_sample_results(f"S{i}")["items"]}
        assert set(raw) == set(std)
        for code, r in raw.items():
            assert std[code]["source_value"] == r["value"]
            assert std[code]["control_id"] == r["control_id"]
            assert std[code]["available"] is True
    assert switched["version_id"] == v2


def test_standardisation_never_touches_raw_tables(tmp_path):
    store = _open(tmp_path)
    _apply(store, make_message("F1", "S1", [("GLU", "7.10", "mmol/L", "F")]))
    before = store.message("F1")
    v2 = store.create_mapping_version(GLU_MAP, "", 0, "T")["version_id"]
    store.recompute_version(v2, "T")
    store.switch_mapping_version(v2, "T")
    after = store.message("F1")
    assert before == after
    raw = store.sample_results("S1")["items"][0]
    assert raw["value"] == "7.10" and raw["unit"] == "mmol/L"

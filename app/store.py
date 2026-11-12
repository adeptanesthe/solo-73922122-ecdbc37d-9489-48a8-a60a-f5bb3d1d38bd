"""Durable result store: exactly-once application plus result standardisation.

All state lives in one SQLite database (WAL, ``synchronous=FULL``).

Raw ingestion (unchanged contract)
-----------------------------------
Every accepted HL7 message is applied in a single ``BEGIN IMMEDIATE``
transaction together with the ACK sent for it.  From this module onwards that
same transaction *also* writes the standardised result rows computed against the
currently active mapping version, so raw results, standard results and the
receipt are committed together.

* an ACK is only sent after ``COMMIT`` returned;
* a crash before commit leaves no partial results and no recorded message;
* re-delivery of an accepted MSH-10 with identical text replays the stored,
  byte-identical ACK without applying anything again;
* re-delivery with different text is rejected (AE);
* standardisation never mutates ``results``, ``messages`` or the stored ACK.

Mapping versions and background recompute
-----------------------------------------
Each mapping replacement creates a new *version*: its mapping rows and a task
row are persisted immediately, while the previous version stays active and
kept serving queries.  A background recompute (which never holds the writer
lock while doing decimal work — it uses short per-batch transactions) fills a
shadow copy of the standard results.  ``switch_mapping_version`` then performs
one short writer transaction that:

1. fills any rows missing for the new version — these are results inserted or
   corrected by live MLLP traffic *during* the recompute;
2. flips the active pointer, so the new mapping and every corresponding result
   become visible atomically;
3. retires the old version.

A recompute that fails or is interrupted (including a process restart) leaves
the old version active; the task can be restarted, which recomputes the version
from scratch into its shadow rows.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Optional

from .hl7 import (
    ERR_APPLICATION_ERROR,
    ERR_DUPLICATE_KEY,
    Envelope,
    ParsedMessage,
    build_ack,
)
from .standard import (
    MappingValidationError,
    convert,
    index_mappings,
    validate_coefficient,
)

BUSY_TIMEOUT_MS = 30000

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    control_id     TEXT PRIMARY KEY,
    raw_hash       TEXT NOT NULL,
    raw_message    TEXT NOT NULL,
    sample_no      TEXT NOT NULL,
    ack_code       TEXT NOT NULL,
    ack_message    BLOB NOT NULL,
    received_at    TEXT NOT NULL,
    applied_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    sample_no      TEXT NOT NULL,
    code           TEXT NOT NULL,
    value          TEXT NOT NULL,
    unit           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    value_type     TEXT NOT NULL DEFAULT '',
    text           TEXT NOT NULL DEFAULT '',
    obx_seq        INTEGER NOT NULL DEFAULT 0,
    control_id     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (sample_no, code)
);

CREATE INDEX IF NOT EXISTS idx_results_sample ON results(sample_no);

-- One row per mapping replacement.  Exactly one row is active=1 at a time.
CREATE TABLE IF NOT EXISTS mapping_versions (
    version_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    state          TEXT NOT NULL CHECK (state IN ('pending','active','retired','failed')),
    note           TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL,
    activated_at   TEXT,
    entry_count    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mapping_entries (
    version_id        INTEGER NOT NULL,
    source_item       TEXT NOT NULL,
    source_unit       TEXT NOT NULL DEFAULT '',
    standard_item     TEXT NOT NULL,
    standard_unit     TEXT NOT NULL DEFAULT '',
    coefficient       TEXT NOT NULL,
    PRIMARY KEY (version_id, source_item),
    UNIQUE (version_id, standard_item)
);

-- Background recompute task bookkeeping (at most one running in-process).
CREATE TABLE IF NOT EXISTS recalc_tasks (
    task_id        TEXT PRIMARY KEY,
    version_id     INTEGER NOT NULL,
    state          TEXT NOT NULL CHECK (state IN
                       ('running','completed','failed','interrupted')),
    delay_ms       INTEGER NOT NULL DEFAULT 0,
    total_rows     INTEGER NOT NULL DEFAULT 0,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    error          TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL,
    started_at     TEXT,
    finished_at    TEXT
);

-- Standardised results, versioned.  Queries read rows of the active version
-- only; shadow rows of a pending recompute coexist without being visible.
CREATE TABLE IF NOT EXISTS standard_results (
    version_id        INTEGER NOT NULL,
    sample_no         TEXT NOT NULL,
    code              TEXT NOT NULL,
    standard_item     TEXT,
    standard_unit     TEXT,
    standard_value    TEXT,
    coefficient       TEXT,
    available         INTEGER NOT NULL,
    reason            TEXT,
    source_value      TEXT NOT NULL,
    source_unit       TEXT NOT NULL DEFAULT '',
    control_id        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (version_id, sample_no, code)
);

CREATE INDEX IF NOT EXISTS idx_standard_sample
    ON standard_results(version_id, sample_no);
"""


class ApplyError(Exception):
    """A validly-formed message cannot be applied; maps to an AE ACK."""

    def __init__(self, text: str, err_code: str = ERR_APPLICATION_ERROR,
                 segment: str = ""):
        super().__init__(text)
        self.text = text
        self.err_code = err_code
        self.segment = segment


class DuplicateIdConflict(ApplyError):
    """MSH-10 already accepted with different message text."""

    def __init__(self, control_id: Optional[str]):
        super().__init__(
            f"MSH-10 '{control_id or ''}' was already accepted with different "
            "message content", ERR_DUPLICATE_KEY, "MSH")
        self.control_id = control_id


class MappingConflictError(Exception):
    """A recompute/switch request conflicts with current task state."""


@dataclass
class ApplyResult:
    ack: bytes
    applied: bool       # False means the stored ACK was replayed


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Store:
    def __init__(self, path: str):
        self.path = path
        # Serialises writer transactions in-process.  SQLite's own writer lock
        # still protects against other processes; this lock avoids busy retries
        # under concurrent MLLP connections and keeps the recompute switch in
        # the same serialisation order as live ingestion.
        self._write_lock = threading.Lock()
        # Held for the whole lifetime of a background recompute so only one
        # mapping version is recomputed at a time.  Never held during the
        # per-row conversion work together with _write_lock.
        self._recalc_lock = threading.Lock()

    # -- connection helpers ------------------------------------------------- #

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_MS / 1000,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            # WAL + synchronous=FULL: a completed COMMIT is fsync-durable.
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            conn.close()

    def init(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(SCHEMA)
        # A previous process may have died mid-recompute; its task must not
        # block a fresh attempt after restart.
        self.recover_interrupted_tasks()
        self.ensure_seed_version()

    # -- mapping versions --------------------------------------------------- #

    def active_version(self, conn: sqlite3.Connection) -> Optional[int]:
        row = conn.execute(
            "SELECT version_id FROM mapping_versions WHERE state='active' "
            "ORDER BY version_id DESC LIMIT 1").fetchone()
        return None if row is None else row["version_id"]

    def ensure_seed_version(self) -> int:
        """Make sure an active version exists (empty mapping on first boot)."""
        with self._write_lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                version_id = self.active_version(conn)
                if version_id is not None:
                    conn.execute("COMMIT")
                    return version_id
                now = _utc_now()
                cur = conn.execute(
                    "INSERT INTO mapping_versions (state, note, created_at, "
                    "activated_at, entry_count) VALUES ('active', ?, ?, ?, 0)",
                    ("seed empty mapping", now, now))
                version_id = cur.lastrowid
                conn.execute("COMMIT")
                return version_id
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def recover_interrupted_tasks(self) -> list[int]:
        """Mark stale 'running' tasks (e.g. after a restart) as interrupted."""
        with self._write_lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT task_id, version_id FROM recalc_tasks "
                    "WHERE state='running'").fetchall()
                for row in rows:
                    conn.execute(
                        "UPDATE recalc_tasks SET state='interrupted', "
                        "error=COALESCE(NULLIF(error,''), 'interrupted by restart'), "
                        "finished_at=? WHERE task_id=?",
                        (_utc_now(), row["task_id"]))
                conn.execute("COMMIT")
                return [r["version_id"] for r in rows]
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def get_mapping(self, version_id: int) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT source_item, source_unit, standard_item, "
                "standard_unit, coefficient FROM mapping_entries "
                "WHERE version_id=? ORDER BY source_item",
                (version_id,)).fetchall()
        return [dict(r) for r in rows]

    def current_mapping(self) -> dict:
        with self._connect() as conn:
            version_id = self.active_version(conn)
            if version_id is None:
                return {"version_id": None, "entries": []}
            version = conn.execute(
                "SELECT version_id, state, note, created_at, activated_at, "
                "entry_count FROM mapping_versions WHERE version_id=?",
                (version_id,)).fetchone()
            entries = conn.execute(
                "SELECT source_item, source_unit, standard_item, "
                "standard_unit, coefficient FROM mapping_entries "
                "WHERE version_id=? ORDER BY source_item",
                (version_id,)).fetchall()
        out = dict(version)
        out["entries"] = [dict(r) for r in entries]
        return out

    def create_mapping_version(self, entries: list[dict], note: str,
                               delay_ms: int, task_id: str) -> dict:
        """Validate and persist a new pending mapping plus its recalc task.

        Does not touch the active version: queries keep using the old mapping
        until the recompute switches atomically.
        """
        cleaned = self._validate_entries(entries)
        now = _utc_now()
        with self._write_lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "INSERT INTO mapping_versions (state, note, created_at, "
                    "entry_count) VALUES ('pending', ?, ?, ?)",
                    (note[:300], now, len(cleaned)))
                version_id = cur.lastrowid
                conn.executemany(
                    "INSERT INTO mapping_entries (version_id, source_item, "
                    "source_unit, standard_item, standard_unit, coefficient) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [(version_id, e["source_item"], e["source_unit"],
                      e["standard_item"], e["standard_unit"], e["coefficient"])
                     for e in cleaned])
                conn.execute(
                    "INSERT INTO recalc_tasks (task_id, version_id, state, "
                    "delay_ms, created_at) VALUES (?, ?, 'running', ?, ?)",
                    (task_id, version_id, max(0, int(delay_ms)), now))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return {"task_id": task_id, "version_id": version_id,
                "entry_count": len(cleaned)}

    @staticmethod
    def _validate_entries(entries: list[dict]) -> list[dict]:
        if not isinstance(entries, list) or not entries:
            raise MappingConflictError("mapping must contain at least one entry")
        if len(entries) > 20:
            raise MappingConflictError("a mapping may contain at most 20 entries")
        seen_sources: set[str] = set()
        seen_standards: set[str] = set()
        cleaned: list[dict] = []
        for raw in entries:
            if not isinstance(raw, dict):
                raise MappingConflictError("each mapping entry must be an object")
            try:
                source_item = str(raw["source_item"]).strip()
                source_unit = str(raw.get("source_unit", "")).strip()
                standard_item = str(raw["standard_item"]).strip()
                standard_unit = str(raw.get("standard_unit", "")).strip()
                coefficient = validate_coefficient(str(raw["coefficient"]))
            except KeyError as exc:
                raise MappingConflictError(
                    f"mapping entry is missing field {exc.args[0]}") from None
            except MappingValidationError as exc:
                raise MappingConflictError(str(exc)) from None
            if not source_item or not standard_item:
                raise MappingConflictError(
                    "source_item and standard_item must be non-empty")
            if len(source_item) > 64 or len(standard_item) > 64:
                raise MappingConflictError("item codes may be at most 64 characters")
            if len(source_unit) > 32 or len(standard_unit) > 32:
                raise MappingConflictError("units may be at most 32 characters")
            if source_item in seen_sources:
                raise MappingConflictError(
                    f"duplicate source_item {source_item!r}")
            if standard_item in seen_standards:
                raise MappingConflictError(
                    f"duplicate standard_item {standard_item!r}")
            seen_sources.add(source_item)
            seen_standards.add(standard_item)
            cleaned.append({
                "source_item": source_item,
                "source_unit": source_unit,
                "standard_item": standard_item,
                "standard_unit": standard_unit,
                "coefficient": coefficient,
            })
        return cleaned

    # -- background recompute ---------------------------------------------- #

    def acquire_recalc_slot(self) -> bool:
        """Non-blocking: only one recompute may run at a time."""
        return self._recalc_lock.acquire(blocking=False)

    def release_recalc_slot(self) -> None:
        try:
            self._recalc_lock.release()
        except RuntimeError:
            pass

    def recompute_version(self, version_id: int, task_id: str,
                          delay_ms: int = 0,
                          should_keep_running: Optional[callable] = None) -> dict:
        """Recompute all standard rows for ``version_id`` into shadow storage.

        Heavy decimal work happens *outside* the writer lock in short,
        per-batch transactions, so MLLP ingestion is never blocked by the
        conversion.  Shadow rows are written with ``INSERT OR IGNORE``: any row
        already present (written by live ingestion — an F/C committed during
        the recompute, which always sees newer raw state) is preserved, so an
        old sweep can never overwrite a newer value.  Rows still missing at the
        end are gap-filled atomically by :meth:`switch_mapping_version`.
        """
        with self._connect() as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS n FROM results").fetchone()["n"]
        self._task_mark_started(task_id, total)
        mapping = index_mappings(self.get_mapping(version_id))
        processed = 0
        batch: list[tuple] = []
        last_pk = ("", "")
        batch_size = 100
        while True:
            if should_keep_running is not None and not should_keep_running():
                raise RuntimeError("recompute cancelled")
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT sample_no, code, value, unit, control_id, updated_at "
                    "FROM results WHERE (sample_no, code) > (?, ?) "
                    "ORDER BY sample_no, code LIMIT ?",
                    (last_pk[0], last_pk[1], batch_size)).fetchall()
            if not rows:
                break
            for row in rows:
                outcome = convert(row["value"], row["unit"],
                                  mapping.get(row["code"]))
                batch.append((
                    version_id, row["sample_no"], row["code"],
                    outcome["standard_item"], outcome["standard_unit"],
                    outcome["standard_value"], outcome["coefficient"],
                    1 if outcome["available"] else 0, outcome["reason"],
                    row["value"], row["unit"], row["control_id"],
                    row["updated_at"],
                ))
                processed += 1
                if delay_ms:
                    # Sleep outside every lock: conversion must not stall
                    # ingestion.  Kept tiny per row; mainly for demos/tests.
                    time.sleep(delay_ms / 1000.0)
            # Shadow sweep yields to any row already produced by ingestion.
            self._bulk_upsert_standard(batch, replace=False)
            batch.clear()
            last_pk = (rows[-1]["sample_no"], rows[-1]["code"])
            self._task_set_progress(task_id, processed, total)
        self._task_set_progress(task_id, processed, total)
        return {"processed_rows": processed, "total_rows": total}

    def _bulk_upsert_standard(self, rows: list[tuple], replace: bool) -> None:
        if not rows:
            return
        # ``replace=True`` is reserved for ingestion writes (the newest raw
        # state); the background recompute always passes False.
        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        sql = (
            f"{verb} INTO standard_results (version_id, sample_no, code, "
            "standard_item, standard_unit, standard_value, coefficient, "
            "available, reason, source_value, source_unit, control_id, "
            "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(sql, rows)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def switch_mapping_version(self, version_id: int, task_id: str) -> dict:
        """Atomically activate ``version_id`` and retire the previous one.

        One short writer transaction: gap-fill results that arrived during the
        recompute (including corrections), flip the active pointer and close
        the task.  On any error everything rolls back and the old version
        stays active.
        """
        now = _utc_now()
        with self._write_lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                task = conn.execute(
                    "SELECT state FROM recalc_tasks WHERE task_id=?",
                    (task_id,)).fetchone()
                if task is None or task["state"] != "running":
                    raise MappingConflictError(
                        f"task {task_id} is not running; cannot switch version")
                version = conn.execute(
                    "SELECT state FROM mapping_versions WHERE version_id=?",
                    (version_id,)).fetchone()
                if version is None or version["state"] != "pending":
                    raise MappingConflictError(
                        f"version {version_id} is not pending")

                mapping_rows = conn.execute(
                    "SELECT source_item, source_unit, standard_item, "
                    "standard_unit, coefficient FROM mapping_entries "
                    "WHERE version_id=?", (version_id,)).fetchall()
                mapping = index_mappings([dict(r) for r in mapping_rows])

                # Gap-fill: every current result must have a row under the new
                # version.  Rows produced by live ingestion already exist
                # (written in the same transaction as the raw result) and are
                # skipped, so an old recompute sweep can never overwrite a
                # newer value.
                missing = conn.execute(
                    "SELECT r.sample_no, r.code, r.value, r.unit, "
                    "r.control_id, r.updated_at FROM results r "
                    "WHERE NOT EXISTS (SELECT 1 FROM standard_results s "
                    "WHERE s.version_id=? AND s.sample_no=r.sample_no "
                    "AND s.code=r.code) ORDER BY r.sample_no, r.code",
                    (version_id,)).fetchall()
                gap_rows = []
                for row in missing:
                    outcome = convert(row["value"], row["unit"],
                                      mapping.get(row["code"]))
                    gap_rows.append((
                        version_id, row["sample_no"], row["code"],
                        outcome["standard_item"], outcome["standard_unit"],
                        outcome["standard_value"], outcome["coefficient"],
                        1 if outcome["available"] else 0, outcome["reason"],
                        row["value"], row["unit"], row["control_id"],
                        row["updated_at"],
                    ))
                if gap_rows:
                    conn.executemany(
                        "INSERT INTO standard_results (version_id, sample_no, "
                        "code, standard_item, standard_unit, standard_value, "
                        "coefficient, available, reason, source_value, "
                        "source_unit, control_id, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        gap_rows)

                old_active = self.active_version(conn)
                if old_active is not None and old_active != version_id:
                    conn.execute(
                        "UPDATE mapping_versions SET state='retired' "
                        "WHERE version_id=?", (old_active,))
                conn.execute(
                    "UPDATE mapping_versions SET state='active', activated_at=? "
                    "WHERE version_id=?", (now, version_id))
                conn.execute(
                    "UPDATE recalc_tasks SET state='completed', "
                    "processed_rows=total_rows, finished_at=? WHERE task_id=?",
                    (now, task_id))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return {"version_id": version_id, "task_id": task_id,
                "gap_filled": len(gap_rows), "previous_version": old_active}

    def fail_task(self, task_id: str, error: str) -> None:
        """Record recompute failure; the pending version never becomes active."""
        with self._write_lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE recalc_tasks SET state='failed', error=?, "
                    "finished_at=? WHERE task_id=?",
                    (error[:500], _utc_now(), task_id))
                conn.execute(
                    "UPDATE mapping_versions SET state='failed' "
                    "WHERE version_id=("
                    "SELECT version_id FROM recalc_tasks WHERE task_id=?)",
                    (task_id,))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def task_status(self, task_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT task_id, version_id, state, delay_ms, total_rows, "
                "processed_rows, error, created_at, started_at, finished_at "
                "FROM recalc_tasks WHERE task_id=?", (task_id,)).fetchone()
        return None if row is None else dict(row)

    def latest_task(self) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT task_id, version_id, state, delay_ms, total_rows, "
                "processed_rows, error, created_at, started_at, finished_at "
                "FROM recalc_tasks ORDER BY created_at DESC, rowid DESC "
                "LIMIT 1").fetchone()
        return None if row is None else dict(row)

    def list_tasks(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT task_id, version_id, state, delay_ms, total_rows, "
                "processed_rows, error, created_at, started_at, finished_at "
                "FROM recalc_tasks ORDER BY created_at DESC, rowid DESC LIMIT ?",
                (max(1, min(limit, 500)),)).fetchall()
        return [dict(r) for r in rows]

    def _task_mark_started(self, task_id: str, total: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE recalc_tasks SET started_at=?, total_rows=? "
                "WHERE task_id=?", (_utc_now(), total, task_id))

    def _task_set_progress(self, task_id: str, processed: int, total: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE recalc_tasks SET processed_rows=?, total_rows=? "
                "WHERE task_id=?", (processed, total, task_id))

    # -- application -------------------------------------------------------- #

    def apply(self, parsed: ParsedMessage) -> ApplyResult:
        """Apply one validated message atomically, returning the ACK to send.

        Raw result changes, the standardised rows (against the active mapping
        version, plus any pending version being recomputed) and the persisted
        ACK all commit in one transaction.  The caller must only transmit the
        ACK after this method returns.
        """
        with self._write_lock:
            return self._apply_locked(parsed)

    def _apply_locked(self, parsed: ParsedMessage) -> ApplyResult:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                replay = conn.execute(
                    "SELECT raw_hash, ack_message FROM messages WHERE control_id=?",
                    (parsed.control_id,)).fetchone()
                if replay is not None:
                    if replay["raw_hash"] != parsed.raw_hash:
                        raise DuplicateIdConflict(parsed.control_id)
                    conn.execute("COMMIT")
                    return ApplyResult(bytes(replay["ack_message"]), applied=False)

                now = _utc_now()
                changed: list[dict] = []
                for item in parsed.obx_items:
                    existing = conn.execute(
                        "SELECT value, status FROM results "
                        "WHERE sample_no=? AND code=?",
                        (parsed.sample_no, item.code)).fetchone()
                    if item.status == "F":
                        if existing is not None:
                            raise ApplyError(
                                f"OBX-3.1 '{item.code}' already exists for sample "
                                f"'{parsed.sample_no}' but OBX-11 is F",
                                ERR_DUPLICATE_KEY, "OBX")
                        conn.execute(
                            "INSERT INTO results (sample_no, code, value, unit, "
                            "status, value_type, text, obx_seq, control_id, updated_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (parsed.sample_no, item.code, item.value, item.unit,
                             item.status, item.value_type, item.text, item.seq,
                             parsed.control_id, now))
                    else:  # C
                        if existing is None:
                            raise ApplyError(
                                f"OBX-3.1 '{item.code}' does not exist for sample "
                                f"'{parsed.sample_no}' so OBX-11 C cannot correct it",
                                ERR_APPLICATION_ERROR, "OBX")
                        conn.execute(
                            "UPDATE results SET value=?, unit=?, status=?, "
                            "value_type=?, text=?, obx_seq=?, control_id=?, updated_at=? "
                            "WHERE sample_no=? AND code=?",
                            (item.value, item.unit, item.status, item.value_type,
                             item.text, item.seq, parsed.control_id, now,
                             parsed.sample_no, item.code))
                    changed.append({
                        "sample_no": parsed.sample_no, "code": item.code,
                        "value": item.value, "unit": item.unit,
                        "control_id": parsed.control_id, "updated_at": now,
                    })

                # Standardise every changed item against every non-retired
                # version: the active one (served by queries) and any pending
                # recompute target (so corrections during a recompute land in
                # the shadow version immediately and cannot be overwritten).
                versions = [r["version_id"] for r in conn.execute(
                    "SELECT version_id FROM mapping_versions "
                    "WHERE state IN ('active','pending')").fetchall()]
                for version_id in versions:
                    mapping_rows = conn.execute(
                        "SELECT source_item, source_unit, standard_item, "
                        "standard_unit, coefficient FROM mapping_entries "
                        "WHERE version_id=?", (version_id,)).fetchall()
                    mapping = index_mappings([dict(r) for r in mapping_rows])
                    for ch in changed:
                        outcome = convert(ch["value"], ch["unit"],
                                          mapping.get(ch["code"]))
                        conn.execute(
                            "INSERT OR REPLACE INTO standard_results "
                            "(version_id, sample_no, code, standard_item, "
                            "standard_unit, standard_value, coefficient, "
                            "available, reason, source_value, source_unit, "
                            "control_id, updated_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (version_id, ch["sample_no"], ch["code"],
                             outcome["standard_item"], outcome["standard_unit"],
                             outcome["standard_value"], outcome["coefficient"],
                             1 if outcome["available"] else 0, outcome["reason"],
                             ch["value"], ch["unit"], ch["control_id"],
                             ch["updated_at"]))

                envelope = Envelope(
                    sending_app=parsed.sending_app,
                    sending_facility=parsed.sending_facility,
                    receiving_app=parsed.receiving_app,
                    receiving_facility=parsed.receiving_facility,
                    control_id=parsed.control_id,
                    version=parsed.version,
                )
                ack = build_ack(envelope, "AA", "Message accepted")
                conn.execute(
                    "INSERT INTO messages (control_id, raw_hash, raw_message, "
                    "sample_no, ack_code, ack_message, received_at, applied_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (parsed.control_id, parsed.raw_hash,
                     parsed.raw_bytes.decode("ascii"), parsed.sample_no,
                     "AA", ack, now, now))
                conn.execute("COMMIT")
                return ApplyResult(ack, applied=True)
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                raise

    # -- queries ------------------------------------------------------------ #

    def sample_results(self, sample_no: str) -> dict:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT sample_no, code, value, unit, status, value_type, text, "
                "obx_seq, control_id, updated_at FROM results "
                "WHERE sample_no=? ORDER BY obx_seq, code",
                (sample_no,)).fetchall()
            messages = conn.execute(
                "SELECT control_id, ack_code, received_at, applied_at FROM messages "
                "WHERE sample_no=? ORDER BY received_at",
                (sample_no,)).fetchall()
        return {
            "sample_no": sample_no,
            "items": [dict(r) for r in rows],
            "messages": [dict(m) for m in messages],
        }

    def standard_sample_results(self, sample_no: str) -> dict:
        """Return standardised rows of the active mapping version only.

        Values are precomputed at write/recompute time; nothing is converted
        during the query.
        """
        with self._connect() as conn:
            version_id = self.active_version(conn)
            if version_id is None:
                return {"sample_no": sample_no, "mapping_version": None,
                        "items": []}
            rows = conn.execute(
                "SELECT sample_no, code, standard_item, standard_unit, "
                "standard_value, coefficient, available, reason, source_value, "
                "source_unit, control_id, updated_at FROM standard_results "
                "WHERE version_id=? AND sample_no=? "
                "ORDER BY code", (version_id, sample_no)).fetchall()
            items = []
            for r in rows:
                item = dict(r)
                item["available"] = bool(item["available"])
                items.append(item)
        return {"sample_no": sample_no, "mapping_version": version_id,
                "items": items}

    def samples(self, limit: int = 200) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT sample_no, COUNT(*) AS item_count, MAX(updated_at) AS updated_at "
                "FROM results GROUP BY sample_no ORDER BY updated_at DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def message(self, control_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT control_id, raw_hash, raw_message, sample_no, ack_code, "
                "ack_message, received_at, applied_at FROM messages WHERE control_id=?",
                (control_id,)).fetchone()
        if row is None:
            return None
        out = dict(row)
        out["ack_message"] = out["ack_message"].decode("ascii")
        return out

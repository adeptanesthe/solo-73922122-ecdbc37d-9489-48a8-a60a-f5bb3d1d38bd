"""Durable result store and exactly-once message application.

All state lives in one SQLite database.  Every accepted message is applied in
a single ``BEGIN IMMEDIATE`` transaction together with the ACK that was sent
for it, so:

* an ACK is only sent after ``COMMIT`` returned (``synchronous=FULL`` + WAL);
* a crash before commit leaves no partial results and no recorded message,
  and the analyser's retransmission is applied afresh;
* re-delivery of an accepted MSH-10 with identical text returns the stored,
  byte-identical ACK without applying anything again;
* re-delivery of an accepted MSH-10 with different text is rejected (AE);
* concurrent deliveries of the same MSH-10 serialise on the SQLite writer lock,
  so exactly one applies and the others replay its ACK.

OBX-11 semantics enforced here, inside the same transaction:

* ``F`` adds a result item and fails if the item already exists for the sample;
* ``C`` corrects an existing item and fails if the item does not exist.
"""

from __future__ import annotations

import os
import sqlite3
import threading
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
        # still protects against other processes; the lock here avoids busy
        # retries under concurrent MLLP connections.
        self._write_lock = threading.Lock()

    # -- connection helpers ------------------------------------------------- #

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_MS / 1000,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            # WAL + synchronous=FULL: a completed COMMIT is fsync-durable, so
            # an AA may only be sent after the transaction has committed.
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

    # -- application -------------------------------------------------------- #

    def apply(self, parsed: ParsedMessage) -> ApplyResult:
        """Apply one validated message atomically, returning the ACK to send.

        The ACK is generated and persisted in the same transaction as the
        result changes; the caller must only transmit it after this method
        returns.  Raises :class:`ApplyError` (and commits nothing) when the
        message conflicts with existing state.
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
                             parsed.control_id, _utc_now()))
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
                             item.text, item.seq, parsed.control_id, _utc_now(),
                             parsed.sample_no, item.code))

                envelope = Envelope(
                    sending_app=parsed.sending_app,
                    sending_facility=parsed.sending_facility,
                    receiving_app=parsed.receiving_app,
                    receiving_facility=parsed.receiving_facility,
                    control_id=parsed.control_id,
                    version=parsed.version,
                )
                ack = build_ack(envelope, "AA", "Message accepted")
                now = _utc_now()
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

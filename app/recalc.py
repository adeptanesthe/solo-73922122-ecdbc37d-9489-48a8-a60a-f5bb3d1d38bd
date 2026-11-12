"""Background mapping-recompute orchestration.

A replacement request is served immediately (the pending mapping version and a
task row are durably stored), then this manager runs the recompute on a worker
thread so the HTTP call and the MLLP listener never block:

* only one recompute runs at a time — further requests get HTTP 409 while a
  task is running;
* MLLP F/C traffic is accepted normally throughout: each message still commits
  raw rows, standard rows and the ACK in one transaction, including into the
  pending version's shadow storage;
* decimal conversion runs without holding the database writer lock — only
  short per-batch transactions take it;
* on success the mapping and *all* its results switch in one transaction;
* on failure or interruption the old version stays active and queryable, and
  the task may be restarted (also automatically after a process restart, where
  a stale ``running`` task is first marked ``interrupted``).
"""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Optional

from .store import MappingConflictError, Store

log = logging.getLogger("lis.recalc")


class RecalcManager:
    def __init__(self, store: Store, default_delay_ms: int = 0):
        self.store = store
        self.default_delay_ms = max(0, int(default_delay_ms))
        # Guards _running and the worker bookkeeping.  The store-level
        # recalc slot lock is the durable cross-thread guarantee.
        self._guard = threading.Lock()
        self._running: Optional[dict] = None

    def is_running(self) -> bool:
        with self._guard:
            return self._running is not None

    def running_task(self) -> Optional[dict]:
        with self._guard:
            return None if self._running is None else dict(self._running)

    def start_replacement(self, entries: list[dict], note: str = "",
                          delay_ms: Optional[int] = None) -> dict:
        """Persist a new mapping version and start its background recompute."""
        effective_delay = self.default_delay_ms if delay_ms is None \
            else max(0, int(delay_ms))
        with self._guard:
            if self._running is not None:
                raise MappingConflictError(
                    f"recompute task {self._running['task_id']} is already "
                    "running; wait for it to finish before replacing the mapping")
            if not self.store.acquire_recalc_slot():
                # Defensive: another thread holds the slot without bookkeeping.
                raise MappingConflictError("another recompute is already running")
            task_id = "RCL-" + uuid.uuid4().hex[:16].upper()
            try:
                created = self.store.create_mapping_version(
                    entries, note, effective_delay, task_id)
            except BaseException:
                self.store.release_recalc_slot()
                raise
            worker = threading.Thread(
                target=self._run,
                args=(task_id, created["version_id"], effective_delay),
                name=f"recalc-{created['version_id']}", daemon=True)
            self._running = {
                "task_id": task_id,
                "version_id": created["version_id"],
                "thread": worker,
            }
            worker.start()
            return {
                "task_id": task_id,
                "version_id": created["version_id"],
                "entry_count": created["entry_count"],
                "state": "running",
                "delay_ms": effective_delay,
            }

    def _run(self, task_id: str, version_id: int, delay_ms: int) -> None:
        try:
            log.info("recompute %s for mapping version %s started",
                     task_id, version_id)
            self.store.recompute_version(
                version_id, task_id, delay_ms=delay_ms,
                should_keep_running=lambda: self._is_current(task_id))
            switched = self.store.switch_mapping_version(version_id, task_id)
            log.info("recompute %s switched active version %s -> %s "
                     "(gap-filled %s rows)", task_id,
                     switched["previous_version"], version_id,
                     switched["gap_filled"])
        except MappingConflictError as exc:
            log.error("recompute %s cannot switch: %s", task_id, exc)
            self._safe_fail(task_id, str(exc))
        except Exception as exc:  # noqa: BLE001 - worker must not die silently
            log.exception("recompute %s failed; old mapping stays active",
                          task_id)
            self._safe_fail(task_id, f"{type(exc).__name__}: {exc}")
        finally:
            self.store.release_recalc_slot()
            with self._guard:
                if self._running and self._running["task_id"] == task_id:
                    self._running = None

    def _is_current(self, task_id: str) -> bool:
        with self._guard:
            return (self._running is not None
                    and self._running["task_id"] == task_id)

    def _safe_fail(self, task_id: str, error: str) -> None:
        try:
            self.store.fail_task(task_id, error)
        except Exception:  # pragma: no cover - defensive
            log.exception("could not mark task %s failed", task_id)

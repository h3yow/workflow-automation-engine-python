"""In-memory run store with per-run locking for concurrent safety."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

from .models import RunRecord


class RunAlreadyExists(Exception):
    """Raised when ``create`` is called with a run_id that already exists."""


class RunStore:
    """Process-local run storage.

    Concurrency model:

    * a short-lived registry lock guards the records/locks dictionaries;
    * each run gets its own re-entrant lock. All read-modify-write sequences
      on a :class:`RunRecord` (including the duplicate-trigger decision) must
      happen while holding that run's lock.
    """

    def __init__(self) -> None:
        self._records: dict[str, RunRecord] = {}
        self._locks: dict[str, threading.RLock] = {}
        self._registry_lock = threading.Lock()

    @contextmanager
    def lock(self, run_id: str) -> Iterator[threading.RLock]:
        """Acquire the per-run lock (creating it atomically if needed)."""
        with self._registry_lock:
            run_lock = self._locks.get(run_id)
            if run_lock is None:
                run_lock = threading.RLock()
                self._locks[run_id] = run_lock
        run_lock.acquire()
        try:
            yield run_lock
        finally:
            run_lock.release()

    def create(self, record: RunRecord) -> None:
        """Atomically insert a new run; raise ``RunAlreadyExists`` on duplicate id."""
        with self._registry_lock:
            if record.run_id in self._records:
                raise RunAlreadyExists(record.run_id)
            self._records[record.run_id] = record

    def get(self, run_id: str) -> RunRecord | None:
        with self._registry_lock:
            return self._records.get(run_id)

    def save(self, record: RunRecord) -> None:
        """Persist a mutated record (same reference; kept for API symmetry)."""
        with self._registry_lock:
            self._records[record.run_id] = record

    def all(self) -> list[RunRecord]:
        with self._registry_lock:
            return list(self._records.values())

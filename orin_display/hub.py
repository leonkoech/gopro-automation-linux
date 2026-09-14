"""In-memory fan-out of the current display state to any number of SSE clients."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional


class StateHub:
    """Holds the latest display-state snapshot and wakes waiters when it changes.

    A source (Firestore watcher or the demo generator) calls ``publish``; each SSE
    connection sits in ``wait`` and gets the new version as soon as it lands.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._state: Dict[str, Any] = {
            "status": "starting",
            "gameId": None,
            "source": None,
        }
        self._version = 0
        self._updated_at = 0.0

    def publish(self, state: Dict[str, Any]) -> None:
        with self._cond:
            self._state = dict(state)
            self._version += 1
            self._updated_at = time.time()
            self._cond.notify_all()

    def snapshot(self) -> tuple[int, Dict[str, Any]]:
        with self._cond:
            return self._version, dict(self._state)

    def age_seconds(self) -> float:
        with self._cond:
            return time.time() - self._updated_at if self._updated_at else -1.0

    def wait(self, last_version: int, timeout: float) -> Optional[tuple[int, Dict[str, Any]]]:
        """Block until the state version moves past ``last_version`` or ``timeout``
        elapses. Returns ``(version, state)`` on a change, ``None`` on timeout."""
        with self._cond:
            if self._version <= last_version:
                self._cond.wait(timeout)
            if self._version > last_version:
                return self._version, dict(self._state)
            return None

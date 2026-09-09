"""State sources for the display hub: the live Firestore game, or a demo game.

Both call ``hub.publish(state)`` with the same flat schema the page consumes:

    {
      "status": "live" | "idle" | "completed" | "demo" | "error",
      "gameId": str | None,
      "left":  {"name","displayName","score","fouls","timeoutsUsed","jerseyColor"},
      "right": {...},
      "period": str,
      "possession": "left" | "right" | None,
      "clock": {"baseGameTime": ms, "isRunning": bool, "anchorMs": epoch_ms},
      "source": "firebase" | "demo",
      "serverEpochMs": int,      # so the page can correct its own clock skew
      "updatedAt": iso str | None,
    }
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .hub import StateHub

logger = logging.getLogger("orin_display.sources")

_EMPTY_TEAM = {
    "name": "", "displayName": "", "score": 0,
    "fouls": 0, "timeoutsUsed": 0, "jerseyColor": "#333333",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _team(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = raw or {}
    return {
        "name": raw.get("name") or "",
        "displayName": raw.get("displayName") or raw.get("name") or "",
        "score": int(raw.get("score") or 0),
        "fouls": int(raw.get("fouls") or 0),
        "timeoutsUsed": int(raw.get("timeoutsUsed") or 0),
        "jerseyColor": raw.get("jerseyColor") or "#333333",
    }


# --------------------------------------------------------------------------- #
# Firestore — read the same live/state doc the web TV display subscribes to
# --------------------------------------------------------------------------- #
class FirestoreSource:
    """Watches `basketball-games where status == 'active'` and, for the active
    game, its `live/state` subdoc. Read-only — opens snapshot listeners and
    never writes."""

    def __init__(self, hub: StateHub, credentials_path: Optional[str] = None):
        self.hub = hub
        self.credentials_path = credentials_path or os.getenv("FIREBASE_CREDENTIALS_PATH")
        self._db = None
        self._games_unsub = None
        self._live_unsub = None
        self._game_id: Optional[str] = None
        self._game_doc: Dict[str, Any] = {}
        self._live_doc: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        import firebase_admin
        from firebase_admin import credentials, firestore

        if not self.credentials_path or not os.path.exists(self.credentials_path):
            raise FileNotFoundError(
                f"Firebase credentials not found (FIREBASE_CREDENTIALS_PATH={self.credentials_path!r})"
            )
        if not firebase_admin._apps:
            firebase_admin.initialize_app(credentials.Certificate(self.credentials_path))
        self._db = firestore.client()

        query = self._db.collection("basketball-games").where("status", "==", "active")
        self._games_unsub = query.on_snapshot(self._on_games)
        logger.info("FirestoreSource watching for active games")
        # If nothing is active yet, show the idle screen right away.
        self._publish_idle()

    def stop(self) -> None:
        for unsub in (self._games_unsub, self._live_unsub):
            try:
                if unsub:
                    unsub.unsubscribe()
            except Exception:  # noqa: BLE001
                pass

    # ---- snapshot handlers ------------------------------------------------- #
    def _on_games(self, docs, changes, read_time) -> None:  # noqa: ANN001
        try:
            active = sorted(
                (d for d in docs),
                key=lambda d: (d.to_dict() or {}).get("createdAt") or "",
            )
            if not active:
                with self._lock:
                    self._teardown_live_locked()
                    self._game_id = None
                self._publish_idle()
                return
            chosen = active[-1]  # most recently created active game
            with self._lock:
                self._game_doc = chosen.to_dict() or {}
                if chosen.id != self._game_id:
                    self._teardown_live_locked()
                    self._game_id = chosen.id
                    ref = (self._db.collection("basketball-games")
                           .document(chosen.id).collection("live").document("state"))
                    self._live_unsub = ref.on_snapshot(self._on_live)
                    logger.info("now tracking active game %s", chosen.id)
            self._compose_and_publish()
        except Exception as e:  # noqa: BLE001
            logger.exception("games snapshot handler failed: %s", e)

    def _on_live(self, docs, changes, read_time) -> None:  # noqa: ANN001
        try:
            snap = docs[0] if docs else None
            with self._lock:
                self._live_doc = (snap.to_dict() if snap and snap.exists else {}) or {}
            self._compose_and_publish()
        except Exception as e:  # noqa: BLE001
            logger.exception("live snapshot handler failed: %s", e)

    def _teardown_live_locked(self) -> None:
        if self._live_unsub:
            try:
                self._live_unsub.unsubscribe()
            except Exception:  # noqa: BLE001
                pass
        self._live_unsub = None
        self._live_doc = {}

    # ---- compose --------------------------------------------------------- #
    def _publish_idle(self) -> None:
        self.hub.publish({
            "status": "idle", "gameId": None,
            "left": dict(_EMPTY_TEAM), "right": dict(_EMPTY_TEAM),
            "period": "", "possession": None,
            "clock": {"baseGameTime": 0, "isRunning": False, "anchorMs": _now_ms()},
            "source": "firebase", "serverEpochMs": _now_ms(), "updatedAt": None,
        })

    def _compose_and_publish(self) -> None:
        with self._lock:
            game = dict(self._game_doc)
            live = dict(self._live_doc)
            game_id = self._game_id

        # Prefer the slim live/state fields; fall back to the parent game doc.
        left = _team(live.get("leftTeam") or game.get("leftTeam"))
        right = _team(live.get("rightTeam") or game.get("rightTeam"))
        clock = live.get("clock") or {}
        status = "completed" if game.get("status") == "completed" else "live"

        self.hub.publish({
            "status": status,
            "gameId": game_id,
            "left": left,
            "right": right,
            "period": live.get("period") or game.get("period") or "",
            "possession": live.get("possession") if live.get("possession") in ("left", "right") else None,
            "clock": {
                "baseGameTime": int(clock.get("baseGameTime") or 0),
                "isRunning": bool(clock.get("isRunning")),
                "anchorMs": int(clock.get("anchorMs") or _now_ms()),
            },
            "source": "firebase",
            "serverEpochMs": _now_ms(),
            "updatedAt": live.get("updatedAt") or game.get("updatedAt"),
        })


# --------------------------------------------------------------------------- #
# Demo — a synthetic ticking game, no Firebase needed
# --------------------------------------------------------------------------- #
class DemoSource:
    """Generates a believable game so the page can be tested on the actual TV
    without a live game or Firebase access. ORIN_DISPLAY_DEMO=1."""

    HALF_MS = 10 * 60 * 1000

    def __init__(self, hub: StateHub):
        self.hub = hub
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, name="orin-display-demo", daemon=True)

    def start(self) -> None:
        self._t.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        left = {"name": "HOME", "displayName": "Home", "score": 0, "fouls": 0,
                "timeoutsUsed": 0, "jerseyColor": "#3b82f6"}
        right = {"name": "AWAY", "displayName": "Away", "score": 0, "fouls": 0,
                 "timeoutsUsed": 0, "jerseyColor": "#ef4444"}
        period = "1st"
        possession = "left"
        base = self.HALF_MS
        anchor = _now_ms()
        next_event = time.time() + random.uniform(6, 14)
        last_beat = 0.0

        def remaining() -> int:
            return max(0, base - (_now_ms() - anchor))

        def publish() -> None:
            self.hub.publish({
                "status": "demo", "gameId": "demo",
                "left": dict(left), "right": dict(right),
                "period": period, "possession": possession,
                "clock": {"baseGameTime": base, "isRunning": True, "anchorMs": anchor},
                "source": "demo", "serverEpochMs": _now_ms(), "updatedAt": _iso(),
            })

        publish()
        while not self._stop.wait(0.5):
            now = time.time()
            if remaining() <= 0:
                if period == "1st":
                    period, base, anchor = "2nd", self.HALF_MS, _now_ms()
                else:  # game over -> reset and loop
                    left["score"] = right["score"] = left["fouls"] = right["fouls"] = 0
                    left["timeoutsUsed"] = right["timeoutsUsed"] = 0
                    period, base, anchor = "1st", self.HALF_MS, _now_ms()
                publish()
                next_event = now + random.uniform(6, 14)
                continue
            if now >= next_event:
                roll = random.random()
                team = left if random.random() < 0.5 else right
                if roll < 0.72:
                    team["score"] += random.choice([2, 2, 2, 3])
                    possession = "right" if team is left else "left"
                elif roll < 0.9:
                    team["fouls"] += 1
                else:
                    team["timeoutsUsed"] = min(3, team["timeoutsUsed"] + 1)
                publish()
                next_event = now + random.uniform(8, 20)
                last_beat = now
            elif now - last_beat > 10:
                publish()  # periodic re-send so the page's "last update" stays fresh
                last_beat = now

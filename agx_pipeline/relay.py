"""
Firebase relay — lets the deployed HTTPS frontend control the AGX without
reaching its plain-HTTP :5000 service (mixed-content / no TLS proxy).

- Publishes device status to `agx-devices/{jetson_id}` (cameras, recording
  state, current ingestion) on a heartbeat.
- Polls `agx-commands` for pending start/stop commands targeting this device,
  executes them via the same handlers the HTTP routes use, and marks the result.

One background thread does both (~3s cadence).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

logger = logging.getLogger("agx.relay")

DEVICES = "agx-devices"
COMMANDS = "agx-commands"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Relay:
    def __init__(self, fb, jetson_id: str, state_fn: Callable[[], Dict],
                 on_start: Callable, on_stop: Callable,
                 auto_fn: Optional[Callable] = None, interval: float = 3.0,
                 on_preview: Optional[Callable] = None):
        self.fb = fb
        self.jetson_id = jetson_id
        self.state_fn = state_fn      # () -> device status dict
        self.on_start = on_start      # (game_id, label, force) -> (payload, status)
        self.on_stop = on_stop        # () -> (payload, status)
        self.on_preview = on_preview  # () -> (payload, status); camera snapshots
        self.auto_fn = auto_fn        # () -> None; auto start/stop from game lifecycle
        self.interval = interval
        self._stop = threading.Event()

    def publish(self) -> None:
        if not self.fb:
            return
        try:
            doc = self.state_fn()
            doc.update({"jetson_id": self.jetson_id, "online": True, "updated_at": _now()})
            self.fb.db.collection(DEVICES).document(self.jetson_id).set(doc)
        except Exception as e:  # noqa: BLE001
            logger.warning("status publish failed: %s", e)

    def _process_commands(self) -> None:
        if not self.fb:
            return
        try:
            q = (self.fb.db.collection(COMMANDS)
                 .where("jetson_id", "==", self.jetson_id)
                 .where("status", "==", "pending"))
            for d in q.stream():
                cmd = d.to_dict()
                action = cmd.get("action")
                logger.info("command %s: action=%s", d.id, action)
                d.reference.update({"status": "processing", "picked_at": _now()})
                try:
                    if action == "start":
                        payload, _ = self.on_start(cmd.get("firebase_game_id"), cmd.get("label"),
                                                   bool(cmd.get("force")))
                    elif action == "stop":
                        payload, _ = self.on_stop()
                    elif action == "preview":
                        payload, _ = (self.on_preview() if self.on_preview
                                      else ({"success": False, "error": "preview not supported"}, 501))
                    else:
                        payload = {"success": False, "error": f"unknown action {action}"}
                    d.reference.update({
                        "status": "done" if payload.get("success") else "error",
                        "result": {k: v for k, v in payload.items() if k != "sessions"},
                        "done_at": _now(),
                    })
                except Exception as e:  # noqa: BLE001
                    d.reference.update({"status": "error", "error": str(e)[:300], "done_at": _now()})
                    logger.error("command %s failed: %s", d.id, e)
        except Exception as e:  # noqa: BLE001
            logger.warning("command poll failed: %s", e)

    def _loop(self) -> None:
        while not self._stop.is_set():
            if self.auto_fn:
                try:
                    self.auto_fn()
                except Exception as e:  # noqa: BLE001
                    logger.warning("auto-follow failed: %s", e)
            self._process_commands()
            self.publish()
            self._stop.wait(self.interval)

    def start(self) -> None:
        self.publish()
        threading.Thread(target=self._loop, name="agx-relay", daemon=True).start()
        logger.info("relay started (device=%s)", self.jetson_id)

    def stop(self) -> None:
        self._stop.set()

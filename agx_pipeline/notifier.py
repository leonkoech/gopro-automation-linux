"""
Camera-disconnect email alerts (sprint sync 2026-07-16).

The scorekeeper isn't shown camera health; instead the UAI team is notified
internally by email when a camera drops (and when it recovers). Debounced —
one email per up->down transition, one per down->up recovery — and sent on a
background thread so it never blocks the heartbeat.

Config (env):
    ALERT_EMAIL_ENABLED  (default true)
    ALERT_EMAIL_TO       (default rohitkale523@gmail.com)
    ALERT_EMAIL_FROM     (default = ALERT_SMTP_USER)
    ALERT_SMTP_HOST      (default smtp.gmail.com)
    ALERT_SMTP_PORT      (default 587, STARTTLS)
    ALERT_SMTP_USER / ALERT_SMTP_PASS   (Gmail account + app password)

If SMTP isn't configured the transition is logged (a warning) instead of
emailed, so the box still runs; wire the creds into .env.agx to turn it on.
"""

from __future__ import annotations

import logging
import os
import smtplib
import ssl
import threading
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Dict, List, Set

logger = logging.getLogger("agx.notifier")


def _enabled() -> bool:
    return os.getenv("ALERT_EMAIL_ENABLED", "true").lower() in ("1", "true", "yes")


class CameraAlerter:
    """Tracks per-camera up/down and emails UAI on each transition."""

    def __init__(self, jetson_id: str, location: str):
        self.jetson_id = jetson_id
        self.location = location
        self.to_addr = os.getenv("ALERT_EMAIL_TO", "rohitkale523@gmail.com")
        self.smtp_host = os.getenv("ALERT_SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("ALERT_SMTP_PORT", "587"))
        self.smtp_user = os.getenv("ALERT_SMTP_USER", "")
        self.smtp_pass = os.getenv("ALERT_SMTP_PASS", "")
        self.from_addr = os.getenv("ALERT_EMAIL_FROM", self.smtp_user or "agx-alerts@uball")
        self._down: Set[str] = set()   # angles currently flagged down (already alerted)
        self._primed = False           # first check establishes the baseline silently
        self._lock = threading.Lock()

    def check(self, cameras: List[Dict]) -> None:
        """cameras: [{'angle','id','ip','up'}, …]. Emails on down and recovery."""
        if not _enabled():
            return
        now_down = {c["angle"] for c in cameras if not c.get("up")}
        with self._lock:
            if not self._primed:
                # Don't spam on startup / restart: take the current state as the
                # baseline and only alert on changes from here on.
                self._down = now_down
                self._primed = True
                return
            newly_down = now_down - self._down
            recovered = self._down - now_down
            self._down = now_down
        if not (newly_down or recovered):
            return
        by_angle = {c["angle"]: c for c in cameras}
        for angle in sorted(newly_down):
            c = by_angle.get(angle, {"angle": angle, "id": "?", "ip": "?"})
            self._send(f"[UAI AGX] Camera DOWN: {angle} ({c.get('id')} @ {c.get('ip')})",
                       self._body("disconnected", c, cameras))
        for angle in sorted(recovered):
            c = by_angle.get(angle, {"angle": angle, "id": "?", "ip": "?"})
            self._send(f"[UAI AGX] Camera recovered: {angle} ({c.get('id')})",
                       self._body("reconnected", c, cameras))

    def _body(self, verb: str, cam: Dict, cameras: List[Dict]) -> str:
        up = [c["angle"] for c in cameras if c.get("up")]
        down = [c["angle"] for c in cameras if not c.get("up")]
        return (
            f"Camera {cam.get('angle')} ({cam.get('id')} @ {cam.get('ip')}) {verb} on "
            f"{self.jetson_id} / {self.location} at "
            f"{datetime.now(timezone.utc).isoformat()}.\n\n"
            f"Cameras up:   {', '.join(up) or 'none'}\n"
            f"Cameras down: {', '.join(down) or 'none'}\n"
        )

    def _send(self, subject: str, body: str) -> None:
        if not (self.smtp_user and self.smtp_pass):
            logger.warning("camera alert (email not configured, logging instead): %s", subject)
            return
        threading.Thread(target=self._smtp_send, args=(subject, body),
                         name="cam-alert", daemon=True).start()

    def _smtp_send(self, subject: str, body: str) -> None:
        try:
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = self.from_addr
            msg["To"] = self.to_addr
            msg.set_content(body)
            ctx = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=20) as s:
                s.starttls(context=ctx)
                s.login(self.smtp_user, self.smtp_pass)
                s.send_message(msg)
            logger.info("camera alert emailed to %s: %s", self.to_addr, subject)
        except Exception as e:  # noqa: BLE001
            logger.error("camera alert email failed: %s", e)

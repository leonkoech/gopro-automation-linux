"""Live shot-value typing for the CV auto-scorecard.

Every CV auto-highlight (a detected make) already produces a trimmed 1080p clip
in cut_highlight — the calibration's native coordinate space (calib_arcs_*.json
is 1920x1080), verified consistent with master-based classification. This
module queues that clip through the FL/FR classifier so the live scorecard
shows REAL 2/3/4-point values (user directive: no makes-x2 — each make may
take time, but it gets its true shot type).

One serialized worker thread: ~20s per make idle, ~25-40s under game load; the
queue drains through timeouts/halftime and finishes by the end of the game.
The verdict lands on the game doc as `cv_points.{logId}` — a field the live
detector's shot_live rewrites never touch, and NEVER logs[] (the official
score stays the scorekeeper's).

Env: SHOT_LIVE_TYPING=true enables; SHOT_TYPING_CWD points at the proven
classify working dir (agx_classify.py + calib + weights + .env)."""
from __future__ import annotations

import glob
import os
import queue
import re
import subprocess
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

from logging_service import get_logger

logger = get_logger("agx.shot_typing_live")

TYPING_CWD = os.getenv("SHOT_TYPING_CWD", "/home/dev/shot_typing")
CLASSIFY_TIMEOUT_S = int(os.getenv("SHOT_TYPING_TIMEOUT_S", "240"))
_POINTS = {"2PT": 2, "3PT": 3, "4PT": 4}


def typing_enabled() -> bool:
    return os.getenv("SHOT_LIVE_TYPING", "false").strip().lower() in ("1", "true", "yes", "on")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify_env() -> Dict[str, str]:
    """The CUDA env agx_classify needs when spawned from the service (which
    itself runs without LD_LIBRARY_PATH — same recipe as the nightly typing)."""
    env = os.environ.copy()
    nvlibs = ":".join(glob.glob(
        "/home/dev/.local/lib/python3.10/site-packages/nvidia/*/lib"))
    env["LD_LIBRARY_PATH"] = (
        f"{nvlibs}:/usr/local/cuda-12.6/targets/aarch64-linux/lib:"
        f"/usr/local/cuda-12.6/lib64:" + env.get("LD_LIBRARY_PATH", ""))
    env["SHOT_ATTRIB"] = "possession"
    env["SHOT_FEET"] = "bbox"
    return env


class LiveTyper:
    """Serialized clip->shot-type queue. enqueue() never blocks the highlight
    path: a full queue drops the item with a warning (that make just stays
    'pending' on the scorecard — the nightly job still types its card)."""

    def __init__(self, fb):
        self.fb = fb
        self._q: "queue.Queue[Dict]" = queue.Queue(maxsize=64)
        self._thread = threading.Thread(target=self._run, name="shot-typing-live",
                                        daemon=True)
        self._thread.start()

    def enqueue(self, game_id: str, log_id: str, angle: str,
                clip_path: str, pre_s: float) -> None:
        item = {"game_id": game_id, "log_id": log_id, "angle": angle,
                "clip": clip_path, "pre": float(pre_s)}
        try:
            self._q.put_nowait(item)
            logger.info("typing queued %s (%s, depth=%d)", log_id, angle,
                        self._q.qsize())
        except queue.Full:
            logger.warning("typing queue FULL — dropped %s (stays pending; "
                           "nightly typing still covers its card)", log_id)

    # ---- worker ------------------------------------------------------------ #
    def _run(self) -> None:
        while True:
            item = self._q.get()
            try:
                self._type(item)
            except Exception as e:  # noqa: BLE001 — one bad clip never kills the queue
                logger.warning("typing failed for %s: %s", item.get("log_id"), e)

    def _type(self, item: Dict) -> None:
        log_id, angle, clip = item["log_id"], item["angle"], item["clip"]
        if not os.path.isfile(clip):
            logger.warning("typing skipped %s — clip missing (%s)", log_id, clip)
            return
        # The rim moment sits `pre` seconds into the trimmed clip; it is both
        # the shot ts and the precise rim anchor (no rim search needed — the
        # live detector's verdict IS the rim time).
        env = _classify_env()
        env["SHOT_RIM_TS"] = f"{item['pre']:.2f}"
        cp = subprocess.run(
            ["python3", "agx_classify.py", angle, clip, f"{item['pre']:.2f}", log_id],
            cwd=TYPING_CWD, env=env, capture_output=True, text=True,
            timeout=CLASSIFY_TIMEOUT_S)
        m_zone = re.search(r"ZONE_NEW=(\w+)", cp.stdout)
        m_who = re.search(r"WHO=#(\w+)", cp.stdout)
        m_proc = re.search(r"([\d.]+)s proc", cp.stdout)
        zone = m_zone.group(1) if m_zone else None
        if zone not in _POINTS:
            logger.warning("typing no-zone for %s (zone=%s rc=%d) — stays pending",
                           log_id, zone, cp.returncode)
            return
        who = m_who.group(1) if m_who and m_who.group(1) != "None" else None
        rec = {"zone": zone, "points": _POINTS[zone], "who": who,
               "angle": angle, "typed_at": _utcnow_iso(),
               "proc_s": float(m_proc.group(1)) if m_proc else None}
        try:
            self.fb.db.collection("basketball-games").document(item["game_id"]).set(
                {"cv_points": {log_id: rec}}, merge=True)
            logger.info("typing DONE %s -> %s (%dpt, who=%s, %.0fs)", log_id,
                        zone, _POINTS[zone], who, rec["proc_s"] or -1)
        except Exception as e:  # noqa: BLE001
            logger.warning("typing write failed for %s: %s", log_id, e)


_TYPER: Optional[LiveTyper] = None
_TYPER_LOCK = threading.Lock()


def get_typer(fb) -> LiveTyper:
    global _TYPER
    with _TYPER_LOCK:
        if _TYPER is None:
            _TYPER = LiveTyper(fb)
        return _TYPER

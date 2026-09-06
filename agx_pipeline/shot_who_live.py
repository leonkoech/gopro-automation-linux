"""Live WHO ("who shot it") for left-end makes — Thursday v1, gated.

For each left-side make, cuts a [-25s,+2s] NL window from the LEFT highlight
buffer (NL is already buffered there as FL's failover) and runs who_from_nl.py
in the classify working dir. The verdict lands as cv_points.{logId}.who_nl =
{number, conf} — number only; the frontend maps number->name via the game's
check-in roster. Answers only when the jersey vote is unambiguous (fleet-
measured 84% correct-when-spoken on the clean benchmark game); otherwise the
field simply never appears. Enable with SHOT_LIVE_WHO=true.
"""
from __future__ import annotations

import os
import queue
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from logging_service import get_logger
from shot_typing_live import TYPING_CWD, _classify_env

logger = get_logger("agx.shot_who_live")

WHO_PRE = float(os.getenv("SHOT_WHO_PRE_S", "25"))
WHO_POST = float(os.getenv("SHOT_WHO_POST_S", "2"))
WHO_TIMEOUT = int(os.getenv("SHOT_WHO_TIMEOUT_S", "300"))


def who_enabled() -> bool:
    return os.getenv("SHOT_LIVE_WHO", "false").strip().lower() in ("1", "true", "yes", "on")


class LiveWho:
    def __init__(self, fb, recorder):
        self.fb = fb
        self.recorder = recorder          # the LEFT HighlightRecorder (holds NL)
        self._q: "queue.Queue[Dict]" = queue.Queue(maxsize=32)
        threading.Thread(target=self._run, name="shot-who-live", daemon=True).start()

    def enqueue(self, game_id: str, log_id: str, ts_epoch: float, label: Optional[str]) -> None:
        try:
            self._q.put_nowait({"game_id": game_id, "log_id": log_id,
                                "t": float(ts_epoch), "label": label})
        except queue.Full:
            logger.warning("who queue FULL — dropped %s", log_id)

    def _run(self) -> None:
        while True:
            item = self._q.get()
            try:
                self._who(item)
            except Exception as e:  # noqa: BLE001
                logger.warning("who failed for %s: %s", item.get("log_id"), e)

    def _who(self, item: Dict) -> None:
        from highlight import _select_segments, _run, SEG_SEC  # late: avoid cycle
        log_id, t = item["log_id"], item["t"]
        t0, t1 = t - WHO_PRE, t + WHO_POST
        deadline = t1 + 2 * SEG_SEC + 20
        while time.time() < deadline:
            segs = [s for s in self.recorder.segments(item["label"]) if s[1] == "NL"]
            if segs and segs[-1][0] >= t1:
                break
            time.sleep(2)
        segs = [s for s in self.recorder.segments(item["label"]) if s[1] == "NL"]
        picked = _select_segments(segs, t0, t1)
        if not picked:
            logger.info("who skipped %s — no NL coverage", log_id)
            return
        work = os.path.join(TYPING_CWD, "who_work")
        os.makedirs(work, exist_ok=True)
        lst = os.path.join(work, f"{log_id}_nl.txt")
        merged = os.path.join(work, f"{log_id}_nl.mp4")
        with open(lst, "w") as f:
            f.writelines(f"file '{p}'\n" for _s, _a, p in picked)
        if not _run(["ffmpeg", "-nostdin", "-y", "-f", "concat", "-safe", "0",
                     "-i", lst, "-c", "copy", merged]):
            logger.warning("who concat failed for %s", log_id)
            return
        rim_in_clip = t - picked[0][0]
        env = _classify_env()
        cp = subprocess.run(
            ["python3", "who_from_nl.py", merged, f"{rim_in_clip:.2f}", log_id],
            cwd=TYPING_CWD, env=env, capture_output=True, text=True,
            timeout=WHO_TIMEOUT)
        m = re.search(r"WHO_NL=#(\w+) conf=([\d.]+)", cp.stdout)
        try:
            os.remove(merged); os.remove(lst)
        except OSError:
            pass
        if not m or m.group(1) == "None":
            logger.info("who unknown for %s (honest silence)", log_id)
            return
        rec = {"number": m.group(1), "conf": float(m.group(2)),
               "at": datetime.now(timezone.utc).isoformat()}
        self.fb.db.collection("basketball-games").document(item["game_id"]).set(
            {"cv_points": {log_id: {"who_nl": rec}}}, merge=True)
        logger.info("who DONE %s -> #%s (conf %.2f)", log_id, rec["number"], rec["conf"])


_WHO: Optional[LiveWho] = None
_LOCK = threading.Lock()


def get_who(fb, recorder) -> LiveWho:
    global _WHO
    with _LOCK:
        if _WHO is None:
            _WHO = LiveWho(fb, recorder)
        return _WHO

"""Hand finished CV highlight clips to the possession-tracker queue (service side).

The service never runs the tracker itself: it only drops one small JSON job file per clip into
TRACKER_QUEUE_DIR/pending/. A separate low-priority worker process
(possession_tracker/queue_worker.py) types the shot, renders the 2K highlight and, once a game
has ended and its queue is empty, publishes the game's reel to Core. So recording and live
detection never wait on the tracker, and a crashed worker loses nothing — its jobs stay queued.

Enabled with SHOT_TRACKER_QUEUE=true. Best-effort: a failure here never touches the clip.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("agx.tracker_queue")

QUEUE_DIR = os.getenv("TRACKER_QUEUE_DIR", "/home/dev/possession/queue")


def tracker_queue_enabled() -> bool:
    return os.getenv("SHOT_TRACKER_QUEUE", "false").strip().lower() in ("1", "true", "yes", "on")


def job_path(queue_dir: str, state: str, game_id: str, log_id: str) -> str:
    return os.path.join(queue_dir, state, "%s__%s.json" % (game_id, log_id))


def enqueue_tracker_job(game_id: str, log_id: str, angle: str, clip_path: str, pre_s: float,
                        s3_key: Optional[str], made: Optional[bool],
                        queue_dir: Optional[str] = None) -> Optional[str]:
    """Write the job atomically (tmp + rename) so the worker never reads half a file."""
    qd = queue_dir or QUEUE_DIR
    try:
        os.makedirs(os.path.join(qd, "pending"), exist_ok=True)
        dst = job_path(qd, "pending", game_id, log_id)
        job = {"game_id": game_id, "log_id": log_id, "angle": angle, "clip": clip_path,
               "pre": float(pre_s), "s3_key": s3_key, "made": made, "queued_at": time.time()}
        tmp = dst + ".tmp"
        with open(tmp, "w") as f:
            json.dump(job, f)
        os.replace(tmp, dst)
        logger.info("tracker queued %s/%s (%s)", game_id, log_id, angle)
        return dst
    except Exception as e:  # noqa: BLE001 — never fails the highlight cut
        logger.warning("tracker enqueue failed for %s: %s", log_id, e)
        return None

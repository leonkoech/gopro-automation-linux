"""Publish a whole-game highlight reel to UBall Core at end-of-game.

Reuses the AGX highlight clips already cut + uploaded during the game
(``basketball-games/{id}.highlights.{logId}`` — CloudFront URLs). No re-upload,
and NO dependency on the annotation "Sync to UBall" pipeline: the reel goes
straight to Core, which stores it in ``agx_game_highlights`` behind the admin
publish gate (a new reel lands in the pending queue, not live).

Best-effort + idempotent: Core upserts on ``firebase_game_id``, so re-running is
safe and never changes an already-published reel's state. A failure here never
fails the ingestion run. Disabled unless ``CORE_API_BASE_URL`` and
``AGX_SERVICE_TOKEN`` are set in the environment.

Ordering/labels mirror plays_sync so the reel plays scores in game order with
the same canonical play-type labels the annotation tool uses.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

import requests

logger = logging.getLogger("agx.core_highlight")

# Points -> canonical annotation label (must match plays_sync._MAKE_BY_POINTS).
_MAKE_BY_POINTS: Dict[int, str] = {1: "FREE_THROW_MAKE", 2: "FG_MAKE", 3: "3PT_MAKE", 4: "4PT_MAKE"}

_TIMEOUT = 20


def _include_misses() -> bool:
    """Temporary escape hatch: publish CV misses into the Core reel.

    Default OFF. Exists so the -3s/+3s miss window can be eyeballed on a real
    clip in the app; it is not a shipping configuration."""
    return os.getenv("CORE_REEL_INCLUDE_MISSES", "false").lower() in ("1", "true", "yes", "on")


def _play_type(log: Dict) -> Optional[str]:
    """Canonical play-type label for a score log, or None for non-scores."""
    if log.get("actionType") in ("score_added", "player_score_added"):
        return _MAKE_BY_POINTS.get((log.get("payload") or {}).get("points", 0))
    return None


def build_reel(firebase_game_id: str, game: Dict, game_date: Optional[str] = None) -> Optional[Dict]:
    """Assemble the reel payload from a Firebase game doc, or None if there are
    no ready highlight clips. Clips are ordered by their score's wall-clock time
    (``logs[].timestamp``); the highlight key is the score log's ``id``."""
    highlights = game.get("highlights") or {}
    if not highlights:
        return None

    logs = game.get("logs") or []
    by_id = {str(l.get("id")): l for l in logs if l.get("id")}

    # Running score at each scoring event (chronological), keyed by the log id →
    # (team1_score, team2_score) AFTER that shot — for the recap scorebug. Sum
    # operator-entered scoreboard points per side over ALL scores (incl. free
    # throws / non-clipped plays, so a clip's on-screen score reflects every
    # prior point); skip CV shadow events so totals reconcile with finalScore.
    running: Dict[str, tuple] = {}
    l_run = r_run = 0
    for lg in sorted(logs, key=lambda x: (x.get("timestamp") is None, x.get("timestamp") or "")):
        if lg.get("actionType") not in ("score_added", "player_score_added"):
            continue
        p = lg.get("payload") or {}
        if p.get("source") == "cv":
            continue
        pts = p.get("points", 0) or 0
        if lg.get("team") == "left":
            l_run += pts
        elif lg.get("team") == "right":
            r_run += pts
        if lg.get("id"):
            running[str(lg["id"])] = (l_run, r_run)

    clips: List[Dict] = []
    for log_id, h in highlights.items():
        if h.get("status") != "ready" or not h.get("url"):
            continue
        # A CV-detected MISS is cut and stored (the typing stage needs it) but
        # is not normally a highlight: Core cannot tell one from a make -- a clip
        # carries no outcome field, and a `cv_` basename with a null play_type
        # reads as "cv" either way -- so publishing misses would put missed shots
        # in the feed unlabelled.
        #
        # CORE_REEL_INCLUDE_MISSES=true lets them through anyway. That is a
        # DELIBERATE, TEMPORARY setting for verifying the -3s/+3s miss window on
        # a live clip; turn it back off once confirmed, or the public feed shows
        # missed shots as highlights.
        if h.get("made") is False and not _include_misses():
            continue
        log = by_id.get(str(log_id), {})
        score = running.get(str(log_id))
        clips.append({
            "url": h["url"],
            "play_type": _play_type(log),
            "team": log.get("team"),          # "left"/"right" (team identity)
            "ts": log.get("timestamp"),        # ISO — reel ordering key
            "angle": h.get("angle"),           # camera the clip was cut from
            "team1_score": score[0] if score else None,  # running, after this shot
            "team2_score": score[1] if score else None,
        })
    if not clips:
        return None
    clips.sort(key=lambda c: (c.get("ts") is None, c.get("ts") or ""))

    left = game.get("leftTeam") or {}
    right = game.get("rightTeam") or {}
    return {
        "firebase_game_id": firebase_game_id,
        "game_date": game_date,
        "team1_name": left.get("name"),
        "team2_name": right.get("name"),
        "team1_score": left.get("finalScore"),
        "team2_score": right.get("finalScore"),
        "team1_color": left.get("jerseyColor"),
        "team2_color": right.get("jerseyColor"),
        "clips": clips,
    }


def publish_core_highlight(firebase_game_id: str, game: Dict,
                           game_date: Optional[str] = None) -> int:
    """POST the reel to Core. Returns the number of clips published (0 if
    skipped or failed). Never raises."""
    base = os.getenv("CORE_API_BASE_URL", "").rstrip("/")
    token = os.getenv("AGX_SERVICE_TOKEN", "")
    if not base or not token:
        logger.info("core highlight publish skipped (CORE_API_BASE_URL/AGX_SERVICE_TOKEN unset)")
        return 0

    reel = build_reel(firebase_game_id, game, game_date)
    if not reel:
        logger.info("core highlight: no ready clips for %s — skipping", firebase_game_id)
        return 0

    try:
        resp = requests.post(
            f"{base}/games/agx-highlight",
            json=reel,
            headers={"X-AGX-Token": token},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        n = len(reel["clips"])
        logger.info("core highlight published %s: %d clips", firebase_game_id, n)
        return n
    except Exception as e:  # noqa: BLE001 — best-effort, never fail ingestion
        logger.error("core highlight publish failed (%s): %s", firebase_game_id, e)
        return 0

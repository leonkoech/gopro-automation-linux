"""Full-rate CONFIRM pass — recover the made shots the live loop could not see.

The live detector runs the ball model on every 4th frame (~30 samples/s on
~120fps FLIRs), but shot_detect/logic.py's gates were validated at FULL rate. A
clean swish spends 0.1-0.2s in the rim zone, leaves 2-5 samples at stride 4, and
is gated out as `skipped: gate (evidence N)`. Swishes are mostly 3PT, 4PT and
free throws, which is exactly the shape of the loss: 2PT made shots are detected
at 88%, 3PT at 50%, 4PT at 69%, free throws at 59% (two games, 2026-09-14).

It was root-caused on 2026-08-11 (commit 34c1d6c: "the coarse 30fps (stride-4)
scan skipping fast rim-crossings... a full-fps CONFIRM pass on candidate windows
... is the real fix") and never built.

It cannot run live — the loop is already at ~94% of real time with both cameras
and full rate costs ~3.6x — so the live loop KEEPS the segments worth a second
look (gated while the ball was at the rim, never scanned before game end, or
skipped as stale) and this re-decides them at ingest, when the GPU is idle and
before plays_sync turns shot_live into annotation cards.

Measured on identical frames (post-game footage, both cameras): made shots
29 -> 37, re-reading 16% of segments. Two recovered makes were checked by eye:
both clean swishes through the middle of the ring.

The decision itself is logic.decide — the same function, the same geometry the
live loop used (the manifest carries its rims). Only the stride changes.
"""
from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from logging_service import get_logger

logger = get_logger("agx.shot_confirm")

MANIFEST = "UNSCANNED.json"
FULL_RATE = 1


def enabled() -> bool:
    return os.getenv("SHOT_CONFIRM_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def read_manifest(seg_dir: str) -> Optional[Dict]:
    try:
        with open(os.path.join(seg_dir, MANIFEST)) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def window_wallclock(close: float, seg_real: Optional[float], t_shot: float,
                     seg_sec: float) -> float:
    """Wall-clock epoch of a shot inside a single-segment window.

    Mirrors live.py: a segment is cut on MEDIA time (seg_sec of frames at the
    nominal lock) but the FLIRs deliver fewer frames than that, so it really
    spans seg_real seconds of wall clock. The file's mtime is when it closed.
    """
    real = seg_real if seg_real and seg_real > 0 else seg_sec
    return (close - real) + t_shot * (real / seg_sec)


def dedup_new(found: List[Dict], existing: List[Dict], dedup_s: float) -> List[Dict]:
    """The confirmed shots that are genuinely new: not within dedup_s of a shot
    the live loop already has on that side, nor of one another."""
    def epoch(s: Dict) -> Optional[float]:
        try:
            return datetime.fromisoformat(s["wallclock"]).timestamp()
        except (KeyError, TypeError, ValueError):
            return None

    kept: List[Dict] = []
    have = [(s.get("side"), epoch(s)) for s in existing]
    for s in sorted(found, key=lambda x: epoch(x) or 0.0):
        e = epoch(s)
        if e is None:
            continue
        if any(side == s.get("side") and t is not None and abs(t - e) <= dedup_s
               for side, t in have):
            continue
        kept.append(s)
        have.append((s.get("side"), e))
    return kept


def confirm_segments(seg_dir: str, fb=None, game_id: Optional[str] = None,
                     keep_going: Optional[Callable[[], bool]] = None,
                     dry_run: bool = False) -> Optional[Dict]:
    """Re-decide every kept segment at full rate; merge the new shots.

    Returns a summary, or None when there is nothing to do. Never raises: it
    runs inside ingest, and a failed confirm must leave ingest untouched.
    """
    man = read_manifest(seg_dir)
    if not man or not man.get("segments"):
        return None
    game_id = game_id or man.get("game_id")
    t0 = time.time()
    try:
        from agx_pipeline.shot_detect import logic
        from agx_pipeline.shot_detect.backtest import scan
        from agx_pipeline.shot_detect.live import (_HOOP_SIDE, _SEG_RE, DEDUP_SEC,
                                                   SHADOW_CAP)
        from agx_pipeline.shot_detect.node import _VALIDATOR
        from agx_pipeline.shot_recording import SHOT_SEGMENT_SEC
        detector, rims_json = _VALIDATOR.get()
    except Exception as e:  # noqa: BLE001 — runtime missing => leave the footage
        logger.warning("shot-confirm unavailable (%s) — segments kept", e)
        return None

    fps = float(man.get("fps") or 119.9)
    imgsz = int(os.getenv("SHOT_DET_IMGSZ", "640"))
    found: List[Dict] = []
    n_read = 0
    aborted = False
    for fn in man["segments"]:
        if keep_going is not None and not keep_going():
            aborted = True               # a game started: stop, keep what's left
            break
        m = _SEG_RE.match(fn)
        path = os.path.join(seg_dir, fn)
        if not m or not os.path.exists(path):
            continue
        idx, angle = int(m.group(1)), m.group(2)
        rim = (man.get("rims") or {}).get(angle) or (rims_json or {}).get(angle)
        close = (man.get("close") or {}).get(fn)
        if rim is None or close is None:
            continue
        G = logic.Geo.from_rim(rim, fps)
        try:
            track, _ = scan.scan_ball_and_hoops(detector.model, path, detector.device,
                                                stride=FULL_RATE, imgsz=imgsz)
        except Exception as e:  # noqa: BLE001 — one bad file never kills the pass
            logger.warning("shot-confirm scan failed %s: %s", fn, e)
            continue
        n_read += 1
        reason = "unscanned" if fn in set(man.get("unscanned") or []) else "gated_at_rim"
        for v in logic.decide(G, track):
            if "verdict" not in v:
                continue
            wc = window_wallclock(float(close), (man.get("seg_real") or {}).get(angle),
                                  float(v.get("t", 0.0)), float(SHOT_SEGMENT_SEC))
            found.append({
                "cam": angle, "side": _HOOP_SIDE.get(angle), "seg": idx,
                "t_shot": round(float(v.get("t", 0.0)), 3),
                "made": v["verdict"] == "MAKE", "verdict": v["verdict"],
                "rho": v.get("rho"),
                "wallclock": datetime.fromtimestamp(wc, timezone.utc).isoformat(),
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "source": "full_rate_confirm", "confirm_reason": reason,
            })

    existing: List[Dict] = []
    if fb and game_id:
        try:
            doc = fb.db.collection("basketball-games").document(game_id).get().to_dict() or {}
            existing = (doc.get("shot_live") or {}).get("shots") or []
        except Exception as e:  # noqa: BLE001
            logger.warning("shot-confirm could not read shot_live for %s: %s", game_id, e)
    new = dedup_new(found, existing, DEDUP_SEC)
    summary = {"segments_read": n_read, "found": len(found), "new": len(new),
               "new_makes": sum(1 for s in new if s["made"]),
               "aborted": aborted, "secs": round(time.time() - t0, 1)}

    if fb and game_id and new and not dry_run:
        merged = sorted(existing + new, key=lambda s: s.get("wallclock") or "")
        n_make = sum(1 for s in merged if s.get("made"))
        try:
            fb.db.collection("basketball-games").document(game_id).set({"shot_live": {
                "shots": merged[-SHADOW_CAP:], "n_shots": len(merged),
                "n_make": n_make, "n_miss": len(merged) - n_make,
                "n_make_left": sum(1 for s in merged if s.get("made") and s.get("side") == "left"),
                "n_make_right": sum(1 for s in merged if s.get("made") and s.get("side") == "right"),
                "confirm": summary,
            }}, merge=True)
        except Exception as e:  # noqa: BLE001
            logger.warning("shot-confirm write failed for %s: %s", game_id, e)
            summary["write_failed"] = True
    if not (aborted or dry_run or summary.get("write_failed")):
        shutil.rmtree(seg_dir, ignore_errors=True)
    logger.info("shot-confirm %s: read %d segments, %d shots, %d NEW (%d makes)%s in %.0fs",
                game_id, n_read, len(found), len(new), summary["new_makes"],
                " [aborted: game started]" if aborted else "", summary["secs"])
    return summary

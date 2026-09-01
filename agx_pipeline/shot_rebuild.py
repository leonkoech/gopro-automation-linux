"""Re-derive a game's shots from the SL/SR masters, after the game, at ingest.

The live detector runs against a deadline: it must keep pace with real time
while the same GPU encodes video, so when it falls behind it drops segments
rather than queue them. That is the right trade for a green button — a late clip
is worthless — but it means shots go missing. On 2026-08-24 one game's live pass
found 4 shots against 196 real ones, because one shot camera produced nothing at
all for the whole game.

This pass has no deadline. It reads the whole master, so nothing can be dropped
for being late, and the recording itself is the queue — every frame is already
durably on disk, which is why no segment buffering is needed to avoid loss.

It also sidesteps the live clock entirely. On a segment, `cross_frame` is an
index inside a short window and live.py reconstructs a game time as
`segment_index * nominal_seconds`; that arithmetic put clips minutes from their
shot. On a master, `cross_frame` IS the position:

    real_seconds = cross_frame / measured_fps        (nb_frames / duration)

measured, never the 120 lock — the shot cams deliver 118.5-119.9 and the value
moves per game with exposure, which alone is up to 37 s of error by full time.

The clip is cut from the tracking camera at that same number, with no offset:
SL->FL and SR->FR was measured (3/3 and 2/2 against controls of 0/3 and 0/2),
and 11 anchors spread across a full game put the offset at -0.10 s with -1.2 s
of drift. Scored against a hand-annotated game afterwards, the times land within
0.78 s of what a human marked.

MAKES ONLY, per the product call: a reel is made shots, and a miss needs a
timestamp (for its annotation card), not a clip.

The live clips are NOT deleted. They did their job during the game; this only
replaces what Core and the cards READ, so nothing that happened live regresses.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from logging_service import get_logger

logger = get_logger("agx.shot_rebuild")

WIN_S = 8.0            # scan window
OVERLAP_S = 2.0        # so a shot straddling a boundary is whole in one window
DEDUP_S = 1.5
STRIDE = int(os.getenv("SHOT_REBUILD_STRIDE", "4"))
IMGSZ = int(os.getenv("SHOT_REBUILD_IMGSZ", "960"))
_HOOP_SIDE = {"SL": "left", "SR": "right"}
_SIDE_ANGLE = {"left": "FL", "right": "FR"}


def enabled() -> bool:
    return os.getenv("SHOT_REBUILD_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def cut_clip(src: str, t: float, log_id: str, angle: str, date: str,
             game_id: str, work_dir: str) -> Optional[Dict]:
    """Cut one make clip from the 1080p tracking angle and upload it.

    Same key layout, bucket and CDN as the live cutter, so a rebuilt clip is
    indistinguishable to everything downstream — Core infers "cv" from the
    basename, and the review screen does not need to know which pass made it.

    Window comes from live.clip_window so there is one definition of it; only
    makes reach here, so only the make window is ever used.
    """
    from agx_pipeline.highlight import (CLOUDFRONT_DOMAIN, S3_BUCKET, S3_PREFIX,
                                        AWS_REGION)
    from agx_pipeline.shot_detect.live import clip_window

    pre, post = clip_window(True)
    t0 = max(0.0, t - pre)
    dst = os.path.join(work_dir, f"{log_id}_{angle}.mp4")
    r = subprocess.run(
        ["ffmpeg", "-nostdin", "-y", "-loglevel", "error",
         "-ss", f"{t0:.3f}", "-i", src, "-t", f"{pre + post:.3f}",
         "-c:v", "libx264", "-preset", "veryfast", "-crf", "24",
         "-pix_fmt", "yuv420p", "-an", "-movflags", "+faststart", dst],
        capture_output=True, text=True, timeout=420)
    if r.returncode != 0 or not os.path.exists(dst) or os.path.getsize(dst) < 20_000:
        logger.warning("shot-rebuild: cut failed for %s: %s",
                       log_id, r.stderr.strip()[-140:])
        return None
    try:
        import boto3
        key = f"{S3_PREFIX}/{date}/{game_id}/{log_id}_{angle}.mp4"
        boto3.client("s3", region_name=AWS_REGION).upload_file(
            dst, S3_BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})
        url = (f"https://{CLOUDFRONT_DOMAIN}/{key}" if CLOUDFRONT_DOMAIN
               else f"https://{S3_BUCKET}.s3.amazonaws.com/{key}")
        return {"s3_key": key, "url": url, "duration": pre + post}
    except Exception as e:  # noqa: BLE001
        logger.warning("shot-rebuild: upload failed for %s: %s", log_id, e)
        return None
    finally:
        try:
            os.unlink(dst)
        except OSError:
            pass


def _recording_now() -> Optional[bool]:
    """Is the box capturing a game right now? None when it cannot be determined."""
    import json as _json
    import urllib.request
    try:
        with urllib.request.urlopen("http://localhost:5000/health", timeout=5) as r:
            return bool(_json.loads(r.read().decode()).get("recording"))
    except Exception:  # noqa: BLE001
        return None


def _yield_to_recording(angle: str) -> None:
    """Block while a game is being captured. Capture always wins.

    This pass is GPU-bound and takes ~1x realtime per camera, so a backlog run
    can still be going when the next fixture starts. The transcode is already
    protected by nice/ionice and cpu-shares, but inference is not: it would
    compete with the LIVE shot detector for the same GPU, degrading the thing
    that has a deadline in order to speed up the thing that does not.

    An unknown state (health unreachable) is treated as safe-to-continue rather
    than blocking forever — a backlog job that silently never finishes is its own
    failure, and the caller already gated on `recording:false` at start.
    """
    waited = 0
    while True:
        rec = _recording_now() is True
        # Also yield to a FRESH ingestion. Backlog work must not slow down the
        # game that just finished: when recording stops, that game's own
        # transcode starts, and a backlog rebuild resuming at the same moment
        # would compete with it for the GPU. Tonight's game reaches the
        # annotators first; the backlog fills the gaps afterwards.
        busy = False
        if not rec:
            try:
                from agx_pipeline.ingest import is_transcoding   # lazy: ingest imports us
                busy = is_transcoding()
            except Exception:  # noqa: BLE001
                busy = False
        if not (rec or busy):
            break
        if waited == 0:
            logger.warning("shot-rebuild %s: pausing — %s owns the GPU", angle,
                           "a game is RECORDING" if rec else "a fresh ingestion")
        time.sleep(60)
        waited += 60
        if waited % 900 == 0:
            logger.info("shot-rebuild %s: still paused (%d min, recording=%s "
                        "transcoding=%s)", angle, waited // 60, rec, busy)
    if waited:
        logger.info("shot-rebuild %s: resuming after %d min paused",
                    angle, waited // 60)


def _measured_fps(path: str) -> Optional[tuple]:
    """(fps, frames, duration) straight from the container. None if unreadable."""
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=nb_frames,duration", "-of", "json", path],
        capture_output=True, text=True, timeout=120)
    try:
        s = json.loads(r.stdout)["streams"][0]
        nb, dur = float(s["nb_frames"]), float(s["duration"])
        if nb <= 0 or dur <= 0:
            return None
        return nb / dur, nb, dur
    except Exception:  # noqa: BLE001
        return None


def detect_master(path: str, angle: str, work_dir: str) -> List[Dict]:
    """Every rim crossing in one SL/SR master, timestamped in real seconds."""
    from agx_pipeline.shot_detect import logic
    from agx_pipeline.shot_detect.node import _VALIDATOR
    from agx_pipeline.shot_detect.backtest import scan as scan_mod

    probe = _measured_fps(path)
    if not probe:
        logger.warning("shot-rebuild %s: cannot read fps/duration, skipping", angle)
        return []
    fps, nframes, dur = probe
    logger.info("shot-rebuild %s: %.0f frames / %.1fs -> measured %.3f fps "
                "(the 120 lock would be %.0fs out over this game)",
                angle, nframes, dur, fps, abs(dur - nframes / 120.0))

    detector, rims = _VALIDATOR.get()
    rim = (rims or {}).get(angle)
    if rim is None:
        logger.warning("shot-rebuild %s: no rim in rims.json, skipping", angle)
        return []
    # Geo gets the MEASURED rate so its frame->second maths matches reality
    G = logic.Geo.from_rim(rim, float(fps))

    tmp = os.path.join(work_dir, f"_rebuild_{angle}.mp4")
    found: List[Dict] = []
    t, nwin = 0.0, 0
    while t < dur:
        seg = min(WIN_S, dur - t)
        if seg < 1.0:
            break
        cut = subprocess.run(
            ["ffmpeg", "-nostdin", "-y", "-loglevel", "error", "-ss", f"{t:.3f}",
             "-i", path, "-t", f"{seg:.3f}", "-c", "copy", tmp],
            capture_output=True, text=True, timeout=300)
        if cut.returncode == 0 and os.path.exists(tmp):
            try:
                track, _hoops = scan_mod.scan_ball_and_hoops(
                    detector.model, tmp, detector.device,
                    stride=STRIDE, imgsz=IMGSZ)
                for v in logic.decide(G, track):
                    if "verdict" not in v:
                        continue
                    found.append({"t": round(t + float(v.get("t", 0.0)), 3),
                                  "cam": angle, "side": _HOOP_SIDE[angle],
                                  "verdict": v["verdict"], "geo": v.get("geo")})
            except Exception as e:  # noqa: BLE001 — one bad window must not stop the sweep
                logger.warning("shot-rebuild %s window %.0fs failed: %s", angle, t, e)
        nwin += 1
        if nwin % 10 == 0:
            _yield_to_recording(angle)
        if nwin % 50 == 0:
            logger.info("shot-rebuild %s: %.0fs/%.0fs, %d crossings",
                        angle, t, dur, len(found))
        t += WIN_S - OVERLAP_S
    try:
        os.unlink(tmp)
    except OSError:
        pass

    found.sort(key=lambda r: r["t"])
    out: List[Dict] = []
    for r in found:
        if out and r["t"] - out[-1]["t"] < DEDUP_S:
            continue          # same shot seen in two overlapping windows
        out.append(r)
    makes = sum(1 for r in out if r["verdict"] == "MAKE")
    logger.info("shot-rebuild %s: %d crossings (%d make / %d miss)",
                angle, len(out), makes, len(out) - makes)
    return out


def _game_start_epoch(fb, firebase_game_id: str) -> Optional[float]:
    """Wall-clock epoch the game began, for naming clips the way live does.

    Clip ids are `cv_<epoch_seconds>_<side>` and downstream treats that number as
    a real wall-clock time: the recap reel walks the score logs against it to put
    the running score on each clip. A game-relative number sorts correctly but
    sits ~800 years before any score log, so every clip would caption 0-0.
    """
    try:
        g = fb.db.collection("basketball-games").document(firebase_game_id).get()
        raw = (g.to_dict() or {}).get("createdAt")
        if not raw:
            return None
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
    except Exception:  # noqa: BLE001
        return None


def rebuild(fb, firebase_game_id: str, date: str, shot_files: List[Dict],
            tr: Dict[str, Dict], work_dir: str) -> Dict:
    """Detect on SL/SR, re-cut FL/FR clips for the makes, and replace what Core
    and the cards read. Returns a summary; never raises."""
    summary = {"detected": 0, "makes": 0, "clips": 0, "angles": []}
    shots: List[Dict] = []
    for f in shot_files:
        ang = f.get("angle")
        if ang not in _HOOP_SIDE or not os.path.exists(f.get("path", "")):
            continue
        shots += detect_master(f["path"], ang, work_dir)
        summary["angles"].append(ang)
    if not shots:
        logger.warning("shot-rebuild: no shots detected, leaving live data alone")
        return summary
    shots.sort(key=lambda s: s["t"])
    summary["detected"] = len(shots)
    summary["makes"] = sum(1 for s in shots if s["verdict"] == "MAKE")

    # Clips for MAKES only; misses travel as timestamps for their cards.
    now = datetime.now(timezone.utc).isoformat()
    game_start = _game_start_epoch(fb, firebase_game_id)
    if game_start is None:
        logger.warning("shot-rebuild: no game start time — clip ids will be "
                       "game-relative, so the recap will caption them 0-0")
    highlights: Dict[str, Dict] = {}
    for s in shots:
        if s["verdict"] != "MAKE":
            continue
        ang = _SIDE_ANGLE[s["side"]]
        src = (tr.get(ang) or {}).get("dst")
        if not src or not os.path.exists(src):
            continue
        # Same id shape as the live cutter: cv_<epoch_seconds>_<side>. Downstream
        # reads that number as wall-clock (the recap reel walks the score logs
        # against it), so a game-relative value would order fine and caption
        # every clip 0-0. Falls back to game-relative only if the start time is
        # unknown, where wrong ordering would be worse than a wrong score.
        stamp = int(round((game_start + s["t"]) if game_start else s["t"] * 1000))
        log_id = f"cv_{stamp}_{s['side']}"
        cut = cut_clip(src, s["t"], log_id, ang, date, firebase_game_id, work_dir)
        if not cut:
            continue
        highlights[log_id] = {
            "angle": ang, "duration": cut["duration"], "s3_key": cut["s3_key"],
            "status": "ready", "updatedAt": now, "url": cut["url"],
            "made": True, "verdict": "MAKE", "t": s["t"], "source": "rebuild",
        }
    summary["clips"] = len(highlights)

    # shot_live carries EVERY shot (makes and misses) — plays_sync builds the
    # annotation cards off it, and a miss card is the whole reason misses are
    # still tracked after we stopped clipping them.
    shot_live = {
        "shots": [{"cam": s["cam"], "side": s["side"], "verdict": s["verdict"],
                   "made": s["verdict"] == "MAKE", "video_ts": s["t"],
                   "source": "rebuild"} for s in shots],
        "n_shots": len(shots), "n_make": summary["makes"],
        "n_miss": len(shots) - summary["makes"],
        "n_sl": sum(1 for s in shots if s["cam"] == "SL"),
        "n_sr": sum(1 for s in shots if s["cam"] == "SR"),
        "source": "rebuild", "rebuilt_at": now,
    }
    try:
        fb.db.collection("basketball-games").document(firebase_game_id).update(
            {"highlights": highlights, "shot_live": shot_live})
        logger.info("shot-rebuild: replaced highlights (%d make clips) and "
                    "shot_live (%d shots) for %s",
                    len(highlights), len(shots), firebase_game_id)
    except Exception as e:  # noqa: BLE001
        logger.error("shot-rebuild: firebase update failed: %s", e)
    return summary

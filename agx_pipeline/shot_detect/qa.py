"""Post-game shot-detection QA (deferred, GPU-gated) — validate the scorekeeper's
scored makes against the high-fps SL/SR footage.

MULTI-GAME SAFE. Recording + live highlights ALWAYS have priority on the AGX GPU.
So QA is fully decoupled from the synchronous ingest flow:

  1. ENQUEUE (during ingest, STAGE 4.5): read the timing sidecar + confirm scored
     makes exist, then write a `shot-qa-queue/{game_id}` job and return. No GPU,
     no blocking — ingest cleans up + completes normally (SL/SR go to S3).
  2. WORKER (a daemon thread in the service): only when the GPU is FREE (nothing
     recording AND nothing ingesting) does it pull the oldest job, download that
     game's SL/SR from S3, and validate. It re-checks between every shot and
     ABORTS (re-queues) the moment a game starts recording — so a running QA can
     never delay a new game's capture. One job at a time (bounded memory).

SHADOW/QA ONLY — never mutates scores, cards, or plays. Best-effort throughout;
any failure just logs. Gated by SHOT_QA_ENABLED (default false).

Env: SHOT_QA_ENABLED, SHOT_VALIDATION_N_BEFORE_S/M_AFTER_S/LATENCY_S,
     SHOT_DET_WEIGHT / SHOT_DET_IMGSZ (see detect.py), UPLOAD_BUCKET.
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
from datetime import datetime, timezone
from typing import Callable, List, Optional

from logging_service import get_logger

logger = get_logger("agx.shot_qa")

QUEUE = "shot-qa-queue"
BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")
_MAKE_SUFFIX = "_MAKE"


def enabled() -> bool:
    return os.getenv("SHOT_QA_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _score_triggers(game: dict) -> List[dict]:
    """Scored makes from the game log -> [{ts, team, period, logId, points}].
    Only `score_added` (a new made basket); needs team + timestamp + a log id."""
    out: List[dict] = []
    for log in (game.get("logs") or []):
        if log.get("actionType") != "score_added":
            continue
        team, ts, log_id = log.get("team"), log.get("timestamp"), log.get("id")
        if not (team and ts and log_id):
            continue
        out.append({"ts": ts, "team": team, "period": log.get("period"),
                    "logId": str(log_id),
                    "points": (log.get("payload") or {}).get("points")})
    out.sort(key=lambda t: t["ts"])
    return out


def _prewarm(detector) -> None:
    try:
        import numpy as np
        detector._ball_track([np.zeros((540, 720, 3), dtype=np.uint8), ] * 2)
    except Exception as e:  # noqa: BLE001
        logger.info("shot-qa prewarm skipped: %s", e)


def _set_ms(run, *args, **kwargs) -> None:
    """Milestone on the IngestionRun (frontend node), best-effort."""
    try:
        if run is not None:
            run.set_shot_qa(*args, **kwargs)
    except Exception:  # noqa: BLE001
        pass


def _ingestion_ms(fb, pipeline_id: Optional[str], **fields) -> None:
    """Update ingestion-runs/{pipeline_id}.shot_qa directly (the worker has no
    live IngestionRun object). Best-effort."""
    if not (fb and pipeline_id):
        return
    doc = {"status": "pending", "n_scored": 0, "n_confirmed": 0, "n_disagree": 0,
           "secs": None, "error": None}
    doc.update(fields)
    try:
        fb.db.collection("ingestion-runs").document(pipeline_id).update({"shot_qa": doc})
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------- #
# 1. ENQUEUE (ingest STAGE 4.5) — no GPU, never blocks ingest
# --------------------------------------------------------------------------- #
def enqueue(fb, cfg, game_id: str, label: Optional[str], s3_prefix: str,
            date: str, folder: str, starting_side: Optional[str],
            pipeline_id: Optional[str], run=None) -> None:
    """Queue a game for deferred QA. Reads the sidecar (still local) + confirms
    scored makes; writes shot-qa-queue/{game_id}. Best-effort, no GPU."""
    if not enabled():
        _set_ms(run, "disabled")
        return
    try:
        if not (fb and label):
            _set_ms(run, "skipped", error="no label / firebase")
            return
        session_dir = os.path.join(cfg.output_dir, label)
        sidecar_path = os.path.join(session_dir, f"{label}_shot_timing.json")
        if not os.path.exists(sidecar_path):
            _set_ms(run, "skipped", error="no timing sidecar")
            return
        with open(sidecar_path) as fh:
            sidecar = json.load(fh)
        triggers = _score_triggers(fb.get_game(game_id) or {})
        if not triggers:
            _set_ms(run, "skipped", error="no scored makes")
            return
        fb.db.collection(QUEUE).document(game_id).set({
            "game_id": game_id, "label": label, "s3_prefix": s3_prefix.rstrip("/"),
            "date": date, "folder": folder, "sidecar": sidecar,
            "starting_side": starting_side, "pipeline_id": pipeline_id,
            "n_scored": len(triggers), "status": "queued", "created_at": _now(),
        })
        _set_ms(run, "queued", n_scored=len(triggers))
        logger.info("shot-qa: queued %s (%d scored makes) for deferred QA",
                    game_id, len(triggers))
    except Exception as e:  # noqa: BLE001 — enqueue must never break ingestion
        logger.warning("shot-qa enqueue failed: %s", e)
        _set_ms(run, "skipped", error=str(e)[:120])


# --------------------------------------------------------------------------- #
# 2. WORKER — runs only when the GPU is free; aborts if a game starts recording
# --------------------------------------------------------------------------- #
def _download_footage(cfg, s3_prefix: str, date: str, folder: str,
                      cams=("SL", "SR")) -> tuple:
    """Download this game's SL/SR from S3 to a scratch dir; return (dir, video_for)."""
    import boto3
    work = os.path.join(getattr(cfg, "output_dir", "/tmp"), "shot_qa_work", folder)
    os.makedirs(work, exist_ok=True)
    client = boto3.client("s3")
    for cam in cams:
        fn = f"{date}_{folder}_{cam}.mp4"
        dst = os.path.join(work, fn)
        if not (os.path.exists(dst) and os.path.getsize(dst) > 0):
            client.download_file(BUCKET, f"{s3_prefix}/{fn}", dst)

    def video_for(angle: str) -> str:
        return os.path.join(work, f"{date}_{folder}_{angle}.mp4")
    return work, video_for


def _qa_core(fb, game_id: str, sidecar: dict, video_for: Callable[[str], str],
             starting_side: Optional[str],
             keep_going: Optional[Callable[[], bool]] = None) -> Optional[dict]:
    """Validate each scored make. `keep_going()` is polled between shots — if it
    returns False (a game started recording), abort so the GPU frees immediately;
    the summary carries `aborted: True` and the caller re-queues."""
    from agx_pipeline.shot_detect.node import _VALIDATOR, _f
    from agx_pipeline.shot_detect.validate import validate_shot, write_validation

    triggers = _score_triggers(fb.get_game(game_id) or {})
    if not triggers:
        return None
    detector, rims = _VALIDATOR.get()
    _prewarm(detector)
    n_before = _f("SHOT_VALIDATION_N_BEFORE_S", 8.0)
    m_after = _f("SHOT_VALIDATION_M_AFTER_S", 2.0)
    latency = _f("SHOT_VALIDATION_LATENCY_S", 0.0)

    t0 = time.time()
    n_confirmed = n_verdict = 0
    disagreements: List[dict] = []
    aborted = False
    for trig in triggers:
        if keep_going is not None and not keep_going():
            aborted = True
            break
        val = validate_shot(detector, sidecar, video_for, trig, starting_side, rims,
                            n_before_s=n_before, m_after_s=m_after,
                            pipeline_latency_s=latency, stream=True)
        if val is None:
            continue
        write_validation(fb, game_id, trig["logId"], val)
        n_verdict += 1
        if val.get("agrees"):
            n_confirmed += 1
        else:
            disagreements.append({"logId": trig["logId"], "team": trig["team"],
                                  "ts": trig["ts"], "cam": val.get("cam"),
                                  "n_make": val.get("n_make"), "n_miss": val.get("n_miss")})
    summary = {
        "n_scored": len(triggers), "n_with_verdict": n_verdict,
        "n_confirmed": n_confirmed, "n_disagree": len(disagreements),
        "disagreements": disagreements[:50], "secs": round(time.time() - t0, 1),
        "aborted": aborted, "updated_at": _now(),
    }
    if not aborted:  # only publish the game-level tally on a complete pass
        try:
            fb.db.collection("basketball-games").document(game_id).update(
                {"shot_qa": summary})
        except Exception as e:  # noqa: BLE001
            logger.warning("shot-qa summary write failed: %s", e)
    return summary


def _process_job(fb, cfg, job: dict, is_gpu_free: Callable[[], bool]) -> None:
    gid = job["game_id"]
    pid = job.get("pipeline_id")
    n_scored = int(job.get("n_scored") or 0)
    _ingestion_ms(fb, pid, status="running", n_scored=n_scored)
    _q(fb, gid, {"status": "running", "started_at": _now()})
    work = None
    try:
        work, video_for = _download_footage(cfg, job["s3_prefix"], job["date"], job["folder"])
        summary = _qa_core(fb, gid, job.get("sidecar") or {}, video_for,
                           job.get("starting_side"), keep_going=is_gpu_free)
        # Phase 2 (dormant unless SHOT_AUTO_ENABLED): auto-detect ALL shots on the
        # same footage while we have it + the GPU is free. Also abortable.
        auto = None
        try:
            from agx_pipeline.shot_detect.autodetect import auto_enabled, run_autodetect
            if auto_enabled() and summary is not None and not summary.get("aborted"):
                auto = run_autodetect(fb, cfg, gid, job.get("sidecar") or {}, video_for,
                                      job.get("starting_side"), keep_going=is_gpu_free)
        except Exception as e:  # noqa: BLE001 — Phase-2 never breaks the QA worker
            logger.warning("shot-auto step failed for %s: %s", gid, e)
        if summary is None:
            _ingestion_ms(fb, pid, status="skipped", n_scored=n_scored)
            _q(fb, gid, {"status": "skipped"})
        elif summary.get("aborted") or (auto is not None and auto.get("aborted")):
            # a game started recording — yield the GPU, retry this job later
            logger.info("shot-qa %s: aborted mid-job for recording — re-queued", gid)
            _q(fb, gid, {"status": "queued"})
            _ingestion_ms(fb, pid, status="queued", n_scored=n_scored)
        else:
            _ingestion_ms(fb, pid, status="done", n_scored=n_scored,
                          n_confirmed=summary["n_confirmed"],
                          n_disagree=summary["n_disagree"], secs=summary["secs"])
            _q(fb, gid, {"status": "done", "summary": {k: summary[k] for k in
                ("n_scored", "n_with_verdict", "n_confirmed", "n_disagree", "secs")}})
            logger.info("shot-qa %s: %d/%d makes confirmed, %d flagged (%.0fs)",
                        gid, summary["n_confirmed"], summary["n_scored"],
                        summary["n_disagree"], summary["secs"])
    except Exception as e:  # noqa: BLE001
        logger.warning("shot-qa job %s failed: %s", gid, e)
        _ingestion_ms(fb, pid, status="failed", n_scored=n_scored, error=str(e)[:150])
        _q(fb, gid, {"status": "failed", "error": str(e)[:200]})
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)


def _q(fb, game_id: str, fields: dict) -> None:
    try:
        fb.db.collection(QUEUE).document(game_id).update(fields)
    except Exception:  # noqa: BLE001
        pass


def _next_job(fb) -> Optional[dict]:
    """Oldest queued job (Python-sorted — no composite index needed)."""
    try:
        docs = list(fb.db.collection(QUEUE).where("status", "==", "queued").stream())
    except Exception as e:  # noqa: BLE001
        logger.warning("shot-qa queue read failed: %s", e)
        return None
    jobs = [{**d.to_dict(), "game_id": d.id} for d in docs]
    jobs.sort(key=lambda j: j.get("created_at") or "")
    return jobs[0] if jobs else None


def qa_worker_loop(fb, cfg, is_gpu_free: Callable[[], bool], poll_s: float = 45.0) -> None:
    logger.info("shot-qa worker started (poll %ss)", poll_s)
    while True:
        try:
            time.sleep(poll_s)
            if not enabled() or not is_gpu_free():
                continue                              # recording/ingest -> defer
            job = _next_job(fb)
            if job:
                logger.info("shot-qa worker: GPU free — processing %s", job["game_id"])
                _process_job(fb, cfg, job, is_gpu_free)
        except Exception as e:  # noqa: BLE001 — the worker must never die
            logger.warning("shot-qa worker cycle error: %s", e)


def start_worker(fb, cfg, is_gpu_free: Callable[[], bool]) -> None:
    """Spawn the deferred-QA worker daemon (no-op without Firebase)."""
    if not fb:
        return
    threading.Thread(target=qa_worker_loop, args=(fb, cfg, is_gpu_free),
                     name="shot-qa-worker", daemon=True).start()

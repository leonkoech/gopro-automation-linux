#!/usr/bin/env python3
"""AWS Batch entrypoint for the uball-cv-merge container.

Flow:
  1. Read env config (GAME_ID, per-side result S3 keys, buckets, etc).
  2. Download both per-side detection_results.json from S3.
  3. Fetch the Firebase basketball-games doc.
  4. Skip if cv_emitted_at is already set (idempotency).
  5. Merge shots cross-side (prefer higher confidence on overlap).
  6. Attribute team per shot using startingSideTeam1 + halftime timestamp.
  7. Build cv_shot log entries and ArrayUnion them into Firebase logs[]
     (or cv_logs_staging[] when CV_EMIT_TARGET=cv_logs_staging).
  8. Drive plays_sync.create_plays_from_firebase_logs to emit UBall plays.
  9. Stamp cv_emitted_at.

ENV (required):
  GAME_ID                    Firebase basketball-games doc ID
  SUPABASE_GAME_ID           Supabase games.id (UUID) — the UBall game_id
  SIDE_A_RESULT_S3_KEY       S3 key of Side A detection_results.json
  SIDE_B_RESULT_S3_KEY       S3 key of Side B detection_results.json
  RESULTS_BUCKET             S3 bucket holding the detection JSONs
  FIREBASE_CREDENTIALS_PATH  Path to Firebase Admin SDK JSON (mounted)

  (UBall sync is only attempted when CV_EMIT_TARGET=logs — shadow mode skips it)
  UBALL_BACKEND_URL
  UBALL_AUTH_EMAIL
  UBALL_AUTH_PASSWORD

ENV (optional):
  AWS_REGION           default: us-east-1
  CV_EMIT_TARGET       'logs' (prod) or 'cv_logs_staging' (shadow). default: logs
  TEMPORAL_WINDOW      default: 1.0  (cross-side dedup tolerance, seconds)
  MODEL_VERSION        default: v1   (stamped onto each log)
  CV_RUN_ID            default: $AWS_BATCH_JOB_ID or a uuid4
  LOG_LEVEL            default: INFO

ENV (local test mode):
  LOCAL_MODE=true      skip S3 downloads; use LOCAL_SIDE_A_PATH / LOCAL_SIDE_B_PATH
  LOCAL_SIDE_A_PATH    local path to a Side A detection_results.json
  LOCAL_SIDE_B_PATH    local path to a Side B detection_results.json
  DRY_RUN=true         don't write to Firebase or UBall — print what would happen
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [cv-merge] %(message)s",
)
log = logging.getLogger("cv-merge")


# -----------------------------------------------------------------------------
# env helpers
# -----------------------------------------------------------------------------
def _env(key: str, default: Optional[str] = None, *, required: bool = False) -> Optional[str]:
    val = os.environ.get(key, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# -----------------------------------------------------------------------------
# input loading
# -----------------------------------------------------------------------------
def _load_side_result(
    *, bucket: Optional[str], key: Optional[str], local_path: Optional[str], label: str
) -> Dict[str, Any]:
    if local_path:
        p = Path(local_path)
        if not p.exists():
            raise RuntimeError(f"{label} local path not found: {p}")
        log.info("[%s] loading local %s", label, p)
        return json.loads(p.read_text())

    if not (bucket and key):
        raise RuntimeError(f"{label}: neither S3 bucket/key nor LOCAL_SIDE_*_PATH provided")

    import boto3
    s3 = boto3.client("s3", region_name=_env("AWS_REGION", "us-east-1"))
    dest = Path("/tmp") / f"{label}_{Path(key).name}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("[%s] s3://%s/%s -> %s", label, bucket, key, dest)
    s3.download_file(bucket, key, str(dest))
    return json.loads(dest.read_text())


# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
def main() -> int:
    # Ensure the app source dir is importable (cv_merge, firebase_service, etc.)
    sys.path.insert(0, "/app")
    # Also support host/local execution from the repo root.
    repo_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(repo_root))

    game_id = _env("GAME_ID", required=True)
    supabase_game_id = _env("SUPABASE_GAME_ID", required=True)

    emit_target = _env("CV_EMIT_TARGET", "logs")
    if emit_target not in ("logs", "cv_logs_staging"):
        raise RuntimeError(f"CV_EMIT_TARGET must be 'logs' or 'cv_logs_staging', got {emit_target!r}")

    temporal_window = float(_env("TEMPORAL_WINDOW", "1.0"))
    model_version = _env("MODEL_VERSION", "v1")
    cv_run_id = (
        _env("CV_RUN_ID")
        or os.environ.get("AWS_BATCH_JOB_ID")
        or f"local-{uuid.uuid4().hex[:12]}"
    )
    dry_run = _env_bool("DRY_RUN")
    is_local = _env_bool("LOCAL_MODE")

    log.info(
        "start game_id=%s supabase_game=%s cv_run_id=%s emit_target=%s dry_run=%s local_mode=%s",
        game_id, supabase_game_id, cv_run_id, emit_target, dry_run, is_local,
    )

    # Metrics emitter — swallows errors, noop under DRY_RUN/LOCAL_MODE if we
    # don't want to hit CloudWatch. The module no-ops cleanly when AWS creds
    # aren't available.
    import cv_metrics
    _merge_started_at = time.time()

    # ---- download per-side results ----
    if is_local:
        side_a_data = _load_side_result(
            bucket=None, key=None,
            local_path=_env("LOCAL_SIDE_A_PATH", required=True),
            label="sideA",
        )
        side_b_data = _load_side_result(
            bucket=None, key=None,
            local_path=_env("LOCAL_SIDE_B_PATH", required=True),
            label="sideB",
        )
    else:
        bucket = _env("RESULTS_BUCKET", required=True)
        side_a_data = _load_side_result(
            bucket=bucket, key=_env("SIDE_A_RESULT_S3_KEY", required=True),
            local_path=None, label="sideA",
        )
        side_b_data = _load_side_result(
            bucket=bucket, key=_env("SIDE_B_RESULT_S3_KEY", required=True),
            local_path=None, label="sideB",
        )

    side_a_shots = side_a_data.get("shots") or []
    side_b_shots = side_b_data.get("shots") or []
    log.info("loaded %d side-A shots, %d side-B shots", len(side_a_shots), len(side_b_shots))

    # ---- imports that need sys.path set up ----
    from cv_merge import (
        attribute_team,
        build_cv_shot_log,
        emit_cv_logs,
        find_halftime_seconds,
        is_cv_already_emitted,
        merge,
    )

    # ---- fetch Firebase game doc ----
    if dry_run:
        # For dry-run, accept a minimal synthetic game from env for testing.
        game: Dict[str, Any] = {
            "createdAt": _env("LOCAL_GAME_CREATED_AT", "2026-04-15T00:00:00Z"),
            "startingSideTeam1": _env("LOCAL_STARTING_SIDE", "left"),
            "leftTeam": {"name": _env("LOCAL_LEFT_TEAM", "Team 1")},
            "rightTeam": {"name": _env("LOCAL_RIGHT_TEAM", "Team 2")},
            "logs": [],
            "cv_emitted_at": None,
        }
        log.info("dry_run: synthesized minimal game doc (no Firebase call)")
    else:
        from firebase_service import FirebaseService
        fb = FirebaseService()
        game = fb.get_game(game_id)
        if not game:
            raise RuntimeError(f"basketball-games/{game_id} not found")
        if is_cv_already_emitted(game):
            log.info("cv_emitted_at already set (%s) — skipping", game.get("cv_emitted_at"))
            return 0

    starting_side = game.get("startingSideTeam1")
    left_team_name = (game.get("leftTeam") or {}).get("name", "Team 1")
    right_team_name = (game.get("rightTeam") or {}).get("name", "Team 2")
    halftime_ts = find_halftime_seconds(game)
    needs_review = starting_side not in ("left", "right")

    log.info(
        "startingSideTeam1=%s halftime=%s needs_review=%s",
        starting_side, f"{halftime_ts:.1f}s" if halftime_ts is not None else "unknown",
        needs_review,
    )

    # ---- merge + attribute ----
    merged = merge(side_a_shots, side_b_shots, temporal_window=temporal_window)
    log.info(
        "merged: %d shots (from %d+%d after cross-side dedup)",
        len(merged), len(side_a_shots), len(side_b_shots),
    )

    game_start_iso = game.get("createdAt")
    if not game_start_iso:
        raise RuntimeError(f"basketball-games/{game_id} missing createdAt")

    new_logs = []
    made = missed = 0
    for shot in merged:
        team_label = attribute_team(
            side=shot.side,
            timestamp_seconds=shot.timestamp_seconds,
            starting_side_team1=starting_side,
            halftime_ts=halftime_ts,
        )
        team_name = left_team_name if team_label == "left" else right_team_name
        new_logs.append(build_cv_shot_log(
            shot=shot,
            team_label=team_label,
            team_name=team_name,
            game_start_iso=game_start_iso,
            halftime_ts=halftime_ts,
            cv_run_id=cv_run_id,
            model_version=model_version,
        ))
        if shot.outcome == "made":
            made += 1
        elif shot.outcome == "missed":
            missed += 1

    log.info("cv_shot summary: made=%d missed=%d total=%d", made, missed, len(new_logs))

    # --- CloudWatch metrics (emit once per merge run) ---
    cv_metrics.emit('CVMergeShotsMade', made, dimensions={'Stage': 'merge'})
    cv_metrics.emit('CVMergeShotsMissed', missed, dimensions={'Stage': 'merge'})
    cv_metrics.emit('CVMergeShotsTotal', len(new_logs), dimensions={'Stage': 'merge'})
    cv_metrics.emit('CVMergeNeedsReview', 1 if needs_review else 0,
                    dimensions={'Stage': 'merge'})
    if new_logs:
        avg_conf = sum(shot.confidence for shot in merged) / max(len(merged), 1)
        cv_metrics.emit('CVMergeMeanConfidence', avg_conf,
                        unit='None', dimensions={'Stage': 'merge'})

    # ---- emit ----
    if dry_run:
        log.info("dry_run: would emit %d logs to %s.%s — first entry:\n%s",
                 len(new_logs), game_id, emit_target,
                 json.dumps(new_logs[0], indent=2) if new_logs else "(empty)")
        return 0

    emit_cv_logs(fb.db, game_id, new_logs, emit_target=emit_target)

    # ---- UBall plays sync ----
    if emit_target == "logs" and new_logs:
        from plays_sync import create_plays_from_firebase_logs
        from uball_client import UballClient

        ubc = UballClient()  # reads UBALL_* env vars
        updated = fb.get_game(game_id) or game
        plays_added = create_plays_from_firebase_logs(ubc, supabase_game_id, updated)
        log.info("UBall plays created: %d", plays_added)
    else:
        log.info("skipping UBall sync (emit_target=%s, logs=%d)", emit_target, len(new_logs))

    log.info("done")
    cv_metrics.emit('CVJobSuccess', 1, dimensions={'Stage': 'merge'})
    cv_metrics.emit('CVJobDurationSeconds', time.time() - _merge_started_at,
                    unit='Seconds', dimensions={'Stage': 'merge'})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log.exception("cv-merge entrypoint failed: %s", e)
        try:
            import cv_metrics as _m
            _m.emit('CVJobFailure', 1, dimensions={'Stage': 'merge'})
        except Exception:
            pass
        sys.exit(1)

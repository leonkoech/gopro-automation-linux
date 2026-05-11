"""S3-event-triggered CV dispatch Lambda.

Fires when the transcode pipeline writes one of the 4 angle 1080p mp4s for a
game into `s3://uball-videos-production/court-*/<date>/<uuid4>/*.mp4`. On
each invocation:

  1. Parse the S3 key to identify the game prefix + angle.
  2. HEAD-check that all 4 angles (FL, FR, NL, NR) for the same game prefix
     exist in S3. If fewer than 4 are present, exit cleanly — the next S3
     event for a sibling angle will retry.
  3. Debounce briefly so near-simultaneous puts (e.g. 4 transcode jobs
     finishing within seconds) collapse to a single dispatch.
  4. Idempotency — skip if a cv-fusion job already exists for this game's
     uuid4 in any non-terminal state.
  5. Submit Side A + Side B fusion jobs + a merge job with `dependsOn` via
     the shared `CVBatchDispatcher` (same code path as the Flask polling
     endpoint in `main.py`).
  6. Emit CloudWatch metrics under the `UBall/CV` namespace.

Notes:
  * The Lambda receives only the truncated `uuid4` (first 4 hyphen segments
    of the Supabase UUID) — it does NOT talk to Firebase, so it cannot
    resolve the full `firebase_game_id` here. We pass `uuid4` as both
    `game_uuid` and `firebase_game_id`; the merge container is expected to
    reconcile by looking up `basketball-games` where `uballGameId`
    starts with `uuid4`. This keeps the Lambda lean (no Firebase deps in
    the deployment package) and keeps Firebase access concentrated in the
    merge container that already needs it for `cv_shot` log emit.
  * The companion Flask path in `main.py` (`/api/cv/dispatch-pending`)
    remains as the cron-safe scanner for back-fills and ops re-runs.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3

# Shared dispatcher + metric helper. Bundled into the Lambda zip at
# build time by the Makefile (sam build copies cv_batch_dispatch.py and
# cv_metrics.py from the repo root into the artifact dir).
from cv_batch_dispatch import CVBatchDispatcher, GameVideoKeys  # type: ignore[import-not-found]
import cv_metrics  # type: ignore[import-not-found]

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# S3 key format produced by the transcode pipeline:
#   {location}/{YYYY-MM-DD}/{uuid4}/{YYYY-MM-DD}_{uuid4}_{angle}.mp4
# where {location} is e.g. "court-a", {uuid4} is the first 4 hyphen-separated
# segments of the full Supabase UUID, and {angle} is one of FL/FR/NL/NR.
_KEY_RE = re.compile(
    r"^(?P<location>court-[^/]+)/"
    r"(?P<date>\d{4}-\d{2}-\d{2})/"
    r"(?P<uuid4>[0-9a-fA-F-]+)/"
    r"(?P=date)_(?P=uuid4)_(?P<angle>FL|FR|NL|NR)\.mp4$"
)

INPUTS_BUCKET = os.environ.get("INPUTS_BUCKET", "uball-videos-production")
DEBOUNCE_SECONDS = int(os.environ.get("DEBOUNCE_SECONDS", "60"))
DISABLE_DEBOUNCE = os.environ.get("DISABLE_DEBOUNCE", "").lower() in ("1", "true")


# ---------------------------------------------------------------- helpers
def _parse_s3_event(event: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Return [(bucket, key), ...] tuples from either:

    * EventBridge S3 events — the production trigger shape:
      ``{"source": "aws.s3", "detail-type": "Object Created",
         "detail": {"bucket": {"name": ...}, "object": {"key": ...}}}``
    * Direct S3 notification events — used by unit tests:
      ``{"Records": [{"s3": {"bucket": {"name": ...},
                              "object": {"key": ...}}}]}``
    """
    out: List[Tuple[str, str]] = []

    # EventBridge shape (one record per invocation).
    if event.get("source") == "aws.s3" and event.get("detail-type") in (
        "Object Created",
        "Object Deleted",  # tolerated for forwards-compat; we filter to Created below
    ):
        detail = event.get("detail") or {}
        bucket = (detail.get("bucket") or {}).get("name")
        key = (detail.get("object") or {}).get("key")
        if bucket and key and event["detail-type"] == "Object Created":
            out.append((bucket, key))
        return out

    # Direct S3 notification shape (multiple records possible).
    for record in event.get("Records", []):
        s3 = record.get("s3", {})
        bucket = (s3.get("bucket") or {}).get("name")
        key = (s3.get("object") or {}).get("key")
        if bucket and key:
            out.append((bucket, key))
    return out


def _key_components(key: str) -> Optional[Dict[str, str]]:
    """Extract (location, date, uuid4, angle) from a transcoded-mp4 S3 key."""
    match = _KEY_RE.match(key)
    return match.groupdict() if match else None


def _all_four_angles_present(
    s3_client,
    bucket: str,
    location: str,
    date: str,
    uuid4: str,
) -> Tuple[bool, List[str]]:
    """HEAD-check all 4 angles for this game prefix.

    Returns ``(all_present, missing_angles)``. Treats S3 404s as missing;
    other errors propagate.
    """
    missing: List[str] = []
    for angle in ("FL", "FR", "NL", "NR"):
        key = f"{location}/{date}/{uuid4}/{date}_{uuid4}_{angle}.mp4"
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            status = getattr(e, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if code in ("404", "NoSuchKey", "NotFound") or status == 404:
                missing.append(angle)
            else:
                raise
    return (not missing, missing)


def _existing_jobs_for_game(batch_client, queue: str, name_pattern: str) -> List[str]:
    """Find non-terminal Batch jobs whose name contains ``name_pattern``.

    Used to short-circuit re-dispatch if a previous Lambda invocation
    already submitted jobs for this game.
    """
    found: List[str] = []
    for status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
        try:
            page = batch_client.list_jobs(jobQueue=queue, jobStatus=status, maxResults=100)
            for j in page.get("jobSummaryList", []):
                if name_pattern in j.get("jobName", ""):
                    found.append(j["jobId"])
        except Exception as e:
            logger.warning("list_jobs status=%s failed: %s", status, e)
    return found


def _build_game_keys(comps: Dict[str, str]) -> GameVideoKeys:
    angle_keys = {
        a: f"{comps['location']}/{comps['date']}/{comps['uuid4']}/"
        f"{comps['date']}_{comps['uuid4']}_{a}.mp4"
        for a in ("FL", "FR", "NL", "NR")
    }
    return GameVideoKeys(
        game_uuid=comps["uuid4"],
        game_uuid4=comps["uuid4"],
        # The Lambda has no Firebase access. The merge container is expected
        # to resolve the full game from `basketball-games` where
        # `uballGameId` starts with `uuid4`. See module docstring.
        firebase_game_id=comps["uuid4"],
        location=comps["location"],
        date=comps["date"],
        angle_keys=angle_keys,
    )


# ---------------------------------------------------------------- handler
def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("cv-dispatch event: %s", json.dumps(event)[:500])

    s3 = boto3.client("s3")
    dispatcher = CVBatchDispatcher()
    batch = dispatcher.batch_client

    submitted = 0
    skipped_waiting = 0
    skipped_already_running = 0
    skipped_unparseable = 0
    errors: List[Dict[str, str]] = []

    # Deduplicate by (location, date, uuid4) — a multi-record S3 event can
    # carry several angles for the same game.
    seen: set = set()

    for bucket, key in _parse_s3_event(event):
        comps = _key_components(key)
        if not comps:
            logger.info("skipping non-matching key: %s", key)
            skipped_unparseable += 1
            continue
        game_id = (comps["location"], comps["date"], comps["uuid4"])
        if game_id in seen:
            continue
        seen.add(game_id)

        # 1. Are all 4 angles in S3?
        all_present, missing = _all_four_angles_present(
            s3, bucket, comps["location"], comps["date"], comps["uuid4"]
        )
        if not all_present:
            logger.info(
                "skipping %s — waiting on angle(s): %s",
                comps["uuid4"],
                missing,
            )
            skipped_waiting += 1
            continue

        # 2. Debounce so near-simultaneous puts settle.
        if DEBOUNCE_SECONDS > 0 and not DISABLE_DEBOUNCE:
            logger.info(
                "debouncing %ds for %s before final dispatch",
                DEBOUNCE_SECONDS,
                comps["uuid4"],
            )
            time.sleep(DEBOUNCE_SECONDS)

        # 3. Idempotency — skip if a job is already in flight.
        existing = _existing_jobs_for_game(
            batch, dispatcher.fusion_queue, f"cv-fusion-{comps['uuid4']}"
        )
        if existing:
            logger.info(
                "skipping %s — %d non-terminal job(s) already in flight: %s",
                comps["uuid4"],
                len(existing),
                existing,
            )
            skipped_already_running += 1
            continue

        # 4. Submit Side A + Side B + merge.
        try:
            result = dispatcher.submit_game(_build_game_keys(comps))
            logger.info(
                "submitted %s: fusion_a=%s fusion_b=%s merge=%s",
                comps["uuid4"],
                result["fusion_a"]["jobId"],
                result["fusion_b"]["jobId"],
                result["merge"]["jobId"],
            )
            submitted += 1
        except Exception as e:  # pragma: no cover — defensive
            logger.exception("submit failed for %s: %s", comps["uuid4"], e)
            errors.append({"uuid4": comps["uuid4"], "error": str(e)})

    # 5. Metrics. Errors swallowed inside cv_metrics so emit can't break
    # the data pipeline.
    cv_metrics.emit(
        "CVDispatchSubmitted", submitted, dimensions={"Stage": "lambda"}
    )
    cv_metrics.emit(
        "CVDispatchWaitingForAngles",
        skipped_waiting,
        dimensions={"Stage": "lambda"},
    )
    cv_metrics.emit(
        "CVDispatchSkippedAlreadyProcessed",
        skipped_already_running,
        dimensions={"Stage": "lambda"},
    )
    cv_metrics.emit(
        "CVDispatchErrors", len(errors), dimensions={"Stage": "lambda"}
    )

    return {
        "submitted": submitted,
        "skipped_waiting": skipped_waiting,
        "skipped_already_running": skipped_already_running,
        "skipped_unparseable": skipped_unparseable,
        "errors": errors,
    }

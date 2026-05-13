"""CV Shot-Detection Batch Dispatcher.

Sibling of ``aws_batch_transcode.py`` — same boto3 client / IAM creds / retry
semantics, different job definitions. Submits:

  - 2x ``cv-fusion`` GPU jobs per game (Side A = FR+NR, Side B = FL+NL)
  - 1x ``cv-merge``  CPU job that ``dependsOn`` both fusion jobs

Submission is driven by the Flask endpoint ``POST /api/cv/dispatch-pending``
defined in ``main.py``, which is in turn hit by the same 5-min cron that
already hits ``/api/batch/register-completed``.

Environment Variables:
    AWS_BATCH_REGION            AWS region for Batch (default: us-east-1)
    AWS_REGION                  Fallback region
    UPLOAD_BUCKET               S3 bucket holding 1080p inputs (default:
                                uball-videos-production)
    CV_FUSION_JOB_DEFINITION    default: cv-fusion
    CV_MERGE_JOB_DEFINITION     default: cv-merge
    CV_FUSION_JOB_QUEUE         default: cv-shot-detection-queue
    CV_MERGE_JOB_QUEUE          default: cv-merge-queue
    CV_MODELS_BUCKET            default: uball-cv-models
    CV_RESULTS_BUCKET           default: uball-cv-results
    CV_MODEL_VERSION            default: v1
    CV_EMIT_TARGET              default: logs (alt: cv_logs_staging)
    UBALL_BACKEND_URL, UBALL_AUTH_EMAIL, UBALL_AUTH_PASSWORD
                                needed by the merge container; passed through
"""

from __future__ import annotations

import json
import os
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# SSL workaround (same as aws_batch_transcode.py)
os.environ.setdefault("OPENSSL_CONF", "/dev/null")

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from logging_service import get_logger

logger = get_logger("gopro.cv_batch_dispatch")


# Physical camera→side mapping matches cv_merge.team_attribution.SIDE_TO_HOOP.
# Side A watches the RIGHT hoop (cameras: FR, NR).
# Side B watches the LEFT  hoop (cameras: FL, NL).
SIDE_A_ANGLES = ("FR", "NR")  # (far, near)
SIDE_B_ANGLES = ("FL", "NL")  # (far, near)


@dataclass
class GameVideoKeys:
    """S3 keys for one game's four 1080p angle videos, plus metadata."""

    game_uuid: str                # Full Supabase UUID
    game_uuid4: str               # First 4 hyphen-separated segments (used in the S3 folder)
    firebase_game_id: str         # basketball-games doc ID
    location: str                 # e.g. "court-a"
    date: str                     # e.g. "2026-04-15"
    angle_keys: Dict[str, str] = field(default_factory=dict)  # angle -> s3_key

    def has_all_four_angles(self) -> bool:
        return all(a in self.angle_keys for a in ("FL", "FR", "NL", "NR"))


class CVBatchDispatcher:
    """Submits cv-fusion + cv-merge Batch jobs."""

    def __init__(
        self,
        *,
        region: Optional[str] = None,
        inputs_bucket: Optional[str] = None,
        models_bucket: Optional[str] = None,
        results_bucket: Optional[str] = None,
        fusion_job_definition: Optional[str] = None,
        merge_job_definition: Optional[str] = None,
        fusion_queue: Optional[str] = None,
        merge_queue: Optional[str] = None,
        model_version: Optional[str] = None,
        emit_target: Optional[str] = None,
        batch_client: Optional[Any] = None,  # injectable for tests
    ):
        self.region = region or os.getenv("AWS_BATCH_REGION", os.getenv("AWS_REGION", "us-east-1"))
        self.inputs_bucket = inputs_bucket or os.getenv("UPLOAD_BUCKET", "uball-videos-production")
        self.models_bucket = models_bucket or os.getenv("CV_MODELS_BUCKET", "uball-cv-models")
        self.results_bucket = results_bucket or os.getenv("CV_RESULTS_BUCKET", "uball-cv-results")
        self.fusion_job_definition = fusion_job_definition or os.getenv("CV_FUSION_JOB_DEFINITION", "cv-fusion")
        self.merge_job_definition = merge_job_definition or os.getenv("CV_MERGE_JOB_DEFINITION", "cv-merge")
        self.fusion_queue = fusion_queue or os.getenv("CV_FUSION_JOB_QUEUE", "cv-shot-detection-queue")
        self.merge_queue = merge_queue or os.getenv("CV_MERGE_JOB_QUEUE", "cv-merge-queue")
        self.model_version = model_version or os.getenv("CV_MODEL_VERSION", "v1")
        self.emit_target = emit_target or os.getenv("CV_EMIT_TARGET", "logs")

        if batch_client is None:
            boto_config = BotoConfig(
                retries={"max_attempts": 3, "mode": "adaptive"},
                max_pool_connections=1,
            )
            self.batch_client = boto3.client("batch", region_name=self.region, config=boto_config, verify=False)
        else:
            self.batch_client = batch_client

        logger.info(
            "CVBatchDispatcher initialized: region=%s fusion_queue=%s merge_queue=%s "
            "fusion_def=%s merge_def=%s model_version=%s emit_target=%s",
            self.region, self.fusion_queue, self.merge_queue,
            self.fusion_job_definition, self.merge_job_definition,
            self.model_version, self.emit_target,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def truncate_uuid(game_uuid: str) -> str:
        """First 4 hyphen-separated segments of a UUID — matches the folder
        convention in ``aws_batch_transcode`` (video_processing)."""
        parts = game_uuid.split("-")[:4]
        return "-".join(parts)

    def build_result_prefix(self, game_keys: GameVideoKeys, side: str) -> str:
        """S3 prefix where fusion writes detection_results.json for this side."""
        return (
            f"cv-results/{game_keys.location}/{game_keys.date}/"
            f"{game_keys.game_uuid4}/side-{side}"
        )

    # ------------------------------------------------------------------
    # submit_fusion_job
    # ------------------------------------------------------------------
    def submit_fusion_job(
        self,
        *,
        game_keys: GameVideoKeys,
        side: str,  # "A" or "B"
    ) -> Dict[str, Any]:
        """Submit one cv-fusion Batch job for Side A or Side B."""
        if side not in ("A", "B"):
            raise ValueError(f"side must be 'A' or 'B', got: {side!r}")

        far_angle, near_angle = (SIDE_A_ANGLES if side == "A" else SIDE_B_ANGLES)
        try:
            far_key = game_keys.angle_keys[far_angle]
            near_key = game_keys.angle_keys[near_angle]
        except KeyError as e:
            raise ValueError(
                f"Side {side} requires angle {e.args[0]} but it's missing from "
                f"game {game_keys.game_uuid}'s angle_keys: "
                f"{sorted(game_keys.angle_keys.keys())}"
            ) from e

        result_prefix = self.build_result_prefix(game_keys, side)
        job_name = f"cv-fusion-{side}-{game_keys.game_uuid4}-{int(time.time())}"

        env = [
            {"name": "GAME_ID",          "value": game_keys.game_uuid},
            {"name": "SIDE",             "value": side},
            {"name": "NEAR_S3_KEY",      "value": near_key},
            {"name": "FAR_S3_KEY",       "value": far_key},
            {"name": "INPUTS_BUCKET",    "value": self.inputs_bucket},
            {"name": "RESULTS_BUCKET",   "value": self.results_bucket},
            {"name": "MODELS_BUCKET",    "value": self.models_bucket},
            {"name": "RESULT_S3_PREFIX", "value": result_prefix},
            {"name": "MODEL_VERSION",    "value": self.model_version},
        ]

        logger.info(
            "submit fusion job name=%s side=%s game=%s near=%s far=%s",
            job_name, side, game_keys.game_uuid, near_key, far_key,
        )

        try:
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.fusion_queue,
                jobDefinition=self.fusion_job_definition,
                containerOverrides={"environment": env},
                tags={
                    "game_uuid":         game_keys.game_uuid,
                    "firebase_game_id":  game_keys.firebase_game_id,
                    "side":              side,
                    "service":           "cv-shot-detection",
                    "pipeline":          "v1",
                    "stage":             "fusion",
                },
            )
        except ClientError as e:
            logger.error("submit_job fusion failed for %s side=%s: %s",
                         game_keys.game_uuid, side, e)
            raise

        return {
            "jobId": response["jobId"],
            "jobName": response.get("jobName", job_name),
            "jobQueue": self.fusion_queue,
            "jobDefinition": self.fusion_job_definition,
            "side": side,
            "game_uuid": game_keys.game_uuid,
            "result_s3_prefix": result_prefix,
            "near_s3_key": near_key,
            "far_s3_key": far_key,
            "submitted_at": time.time(),
        }

    # ------------------------------------------------------------------
    # submit_merge_job
    # ------------------------------------------------------------------
    def submit_merge_job(
        self,
        *,
        game_keys: GameVideoKeys,
        side_a_result_prefix: str,
        side_b_result_prefix: str,
        depends_on_job_ids: List[str],
        cv_emit_target: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit the cv-merge job; ``dependsOn`` both fusion jobs."""
        if not depends_on_job_ids:
            raise ValueError("depends_on_job_ids must not be empty")

        emit_target = cv_emit_target or self.emit_target
        job_name = f"cv-merge-{game_keys.game_uuid4}-{int(time.time())}"

        side_a_key = f"{side_a_result_prefix}/detection_results.json"
        side_b_key = f"{side_b_result_prefix}/detection_results.json"

        env = [
            {"name": "GAME_ID",             "value": game_keys.firebase_game_id},
            {"name": "SUPABASE_GAME_ID",    "value": game_keys.game_uuid},
            {"name": "SIDE_A_RESULT_S3_KEY", "value": side_a_key},
            {"name": "SIDE_B_RESULT_S3_KEY", "value": side_b_key},
            {"name": "RESULTS_BUCKET",      "value": self.results_bucket},
            {"name": "CV_EMIT_TARGET",      "value": emit_target},
            {"name": "MODEL_VERSION",       "value": self.model_version},
        ]

        depends_on = [{"jobId": jid, "type": "SEQUENTIAL"} for jid in depends_on_job_ids]

        logger.info(
            "submit merge job name=%s game=%s depends_on=%s emit_target=%s",
            job_name, game_keys.game_uuid, depends_on_job_ids, emit_target,
        )

        try:
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.merge_queue,
                jobDefinition=self.merge_job_definition,
                dependsOn=depends_on,
                containerOverrides={"environment": env},
                tags={
                    "game_uuid":         game_keys.game_uuid,
                    "firebase_game_id":  game_keys.firebase_game_id,
                    "service":           "cv-shot-detection",
                    "pipeline":          "v1",
                    "stage":             "merge",
                },
            )
        except ClientError as e:
            logger.error("submit_job merge failed for %s: %s", game_keys.game_uuid, e)
            raise

        return {
            "jobId": response["jobId"],
            "jobName": response.get("jobName", job_name),
            "jobQueue": self.merge_queue,
            "jobDefinition": self.merge_job_definition,
            "game_uuid": game_keys.game_uuid,
            "depends_on": list(depends_on_job_ids),
            "side_a_key": side_a_key,
            "side_b_key": side_b_key,
            "submitted_at": time.time(),
        }

    # ------------------------------------------------------------------
    # Convenience: full submission (used by the Flask dispatch endpoint)
    # ------------------------------------------------------------------
    def submit_game(
        self,
        game_keys: GameVideoKeys,
        *,
        cv_emit_target: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit the full 3-job pipeline for one game: 2 fusion jobs + 1 merge."""
        if not game_keys.has_all_four_angles():
            raise ValueError(
                f"Cannot submit — game {game_keys.game_uuid} missing angles. "
                f"Have: {sorted(game_keys.angle_keys.keys())}"
            )

        side_a = self.submit_fusion_job(game_keys=game_keys, side="A")
        side_b = self.submit_fusion_job(game_keys=game_keys, side="B")
        merge = self.submit_merge_job(
            game_keys=game_keys,
            side_a_result_prefix=side_a["result_s3_prefix"],
            side_b_result_prefix=side_b["result_s3_prefix"],
            depends_on_job_ids=[side_a["jobId"], side_b["jobId"]],
            cv_emit_target=cv_emit_target,
        )
        return {
            "game_uuid": game_keys.game_uuid,
            "firebase_game_id": game_keys.firebase_game_id,
            "fusion_a": side_a,
            "fusion_b": side_b,
            "merge": merge,
        }

"""Shared CloudWatch metric emitter for the CV shot-detection pipeline.

Called from:
  - main.py (Flask dispatch endpoint)
  - cv-merge container entrypoint
  - (copied as deploy/cv_metrics.py) cv-fusion container entrypoint

Design rules:
  - Errors are swallowed — metric emission MUST NOT break the data pipeline.
  - `DISABLE_CV_METRICS=true` turns the module into a no-op (useful in
    local dev + unit tests without AWS credentials).
  - All metrics live under the single `UBall/CV` namespace.
  - Dimensions are intentionally minimal (Stage, Side) so alarms and
    dashboards stay cheap and readable.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional

# Match the SSL workaround used everywhere else in this repo for Jetson/ARM.
os.environ.setdefault("OPENSSL_CONF", "/dev/null")

logger = logging.getLogger("cv-metrics")

NAMESPACE = "UBall/CV"

_client = None


def _cloudwatch():
    """Lazy-init a CloudWatch client. Returns None if boto3 is unavailable or
    if DISABLE_CV_METRICS is set."""
    if _is_disabled():
        return None
    global _client
    if _client is not None:
        return _client
    try:
        import boto3
        from botocore.config import Config as BotoConfig
        region = os.getenv("AWS_REGION", "us-east-1")
        _client = boto3.client(
            "cloudwatch",
            region_name=region,
            config=BotoConfig(
                retries={"max_attempts": 2, "mode": "adaptive"},
                max_pool_connections=1,
            ),
            verify=False,
        )
        return _client
    except Exception as e:
        logger.warning("boto3 cloudwatch client init failed (%s) — metrics disabled", e)
        return None


def _is_disabled() -> bool:
    return os.environ.get("DISABLE_CV_METRICS", "").strip().lower() in ("1", "true", "yes", "on")


def _build_datum(
    name: str,
    value: float,
    *,
    unit: str,
    dimensions: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    dims: List[Dict[str, str]] = []
    for k, v in (dimensions or {}).items():
        if v is None:
            continue
        dims.append({"Name": str(k), "Value": str(v)})
    datum: Dict[str, Any] = {
        "MetricName": name,
        "Value": float(value),
        "Unit": unit,
    }
    if dims:
        datum["Dimensions"] = dims
    return datum


def emit(
    name: str,
    value: float,
    *,
    unit: str = "Count",
    dimensions: Optional[Dict[str, str]] = None,
) -> None:
    """Send a single metric datum; never raises.

    Unit: one of CloudWatch's supported units. See:
    https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
    """
    if _is_disabled():
        return
    client = _cloudwatch()
    if client is None:
        return
    try:
        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[_build_datum(name, value, unit=unit, dimensions=dimensions)],
        )
    except Exception as e:
        logger.warning("emit %s=%s failed: %s", name, value, e)


def emit_many(datums: Iterable[Dict[str, Any]]) -> None:
    """Batch-emit pre-built datums; never raises. CloudWatch caps at 20 per call."""
    if _is_disabled():
        return
    client = _cloudwatch()
    if client is None:
        return
    batch = [d for d in datums if d]
    if not batch:
        return
    try:
        for i in range(0, len(batch), 20):
            client.put_metric_data(Namespace=NAMESPACE, MetricData=batch[i:i + 20])
    except Exception as e:
        logger.warning("emit_many (%d datums) failed: %s", len(batch), e)


@contextmanager
def timed(
    name: str,
    *,
    dimensions: Optional[Dict[str, str]] = None,
):
    """Context manager that emits a Seconds timing metric on exit."""
    start = time.time()
    try:
        yield
    finally:
        emit(name, time.time() - start, unit="Seconds", dimensions=dimensions)

"""
Best-effort Grafana annotations for the highlight-clip pipeline.

Posts a point-in-time marker to Grafana (via its HTTP annotation API) at each
stage boundary of a highlight clip's life — detected, queued, segments ready,
transcode start/done, trim start/done, upload start/done, ready/error. These
show up as vertical lines on any Grafana panel covering that time range,
including the jetson_nvenc_utilization_percent / jetson_nvdec_utilization_percent
panels fed by tegrastats_engine_exporter.py, so a slow clip can be visually
lined up against GPU/encoder contention at the moment it happened.

Configuration (env vars — unset GRAFANA_URL or GRAFANA_TOKEN disables this
entirely, silently):
    GRAFANA_URL    e.g. http://monitoring-box:3000 (Tailscale hostname/IP)
    GRAFANA_TOKEN  a Grafana service-account token with Annotations:Write

This module must NEVER affect the pipeline it's observing: every call is
wrapped, has a short timeout, and failures are logged at debug level only.
"""

from __future__ import annotations

import logging
import os
import time
from typing import List, Optional

logger = logging.getLogger("agx.grafana_annotate")

GRAFANA_URL = os.getenv("GRAFANA_URL", "").rstrip("/")
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN")
_TIMEOUT_SEC = float(os.getenv("GRAFANA_ANNOTATE_TIMEOUT_SEC", "2"))

_enabled = bool(GRAFANA_URL and GRAFANA_TOKEN)
if not _enabled:
    logger.info("Grafana annotations disabled (GRAFANA_URL/GRAFANA_TOKEN not set)")


def annotate(text: str, tags: Optional[List[str]] = None,
             time_ms: Optional[int] = None) -> None:
    """Post one point-in-time annotation. Best-effort, non-blocking-ish
    (short timeout), and NEVER raises — a monitoring call must not be able
    to break clip cutting, ingestion, or recording."""
    if not _enabled:
        return
    try:
        import requests  # local import: keep this off the hot path / import graph
        requests.post(
            f"{GRAFANA_URL}/api/annotations",
            headers={"Authorization": f"Bearer {GRAFANA_TOKEN}"},
            json={
                "time": time_ms if time_ms is not None else int(time.time() * 1000),
                "tags": tags or [],
                "text": text,
            },
            timeout=_TIMEOUT_SEC,
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("Grafana annotation failed (non-fatal): %s", e)

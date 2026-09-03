"""
Best-effort Grafana annotations for the highlight-clip pipeline.

Posts a point-in-time marker to Grafana (via its HTTP annotation API) at each
stage boundary of a highlight clip's life — detected, queued, segments ready,
concat done, transcode start/done, trim start/done, upload start/done,
ready/error. These show up as vertical lines on any Grafana panel covering that
time range, including the jetson_nvenc_utilization_percent /
jetson_nvdec_utilization_percent panels fed by the engine exporter, so a slow
clip can be visually lined up against GPU/encoder contention at the moment it
happened.

Configuration (env vars — unset GRAFANA_URL or GRAFANA_TOKEN disables this
entirely, silently):
    GRAFANA_URL    e.g. http://monitoring-box:3000 (Tailscale hostname/IP)
    GRAFANA_TOKEN  a Grafana service-account token with Annotations:Write
    GRAFANA_ANNOTATE_TIMEOUT_SEC  per-request HTTP timeout, default 2.0

This module must NEVER affect the pipeline it's observing. The network POST runs
on a single background worker draining a bounded queue: `annotate()` only
enqueues (or drops, if Grafana is unreachable and the queue backs up) and
returns immediately, so a wedged monitoring route cannot add latency to the
very stage timings we're trying to measure.

Tags are kept low-cardinality on purpose — `["highlight", "<stage>"]` only. The
per-clip log_id is high-cardinality (hundreds a night) and would bloat Grafana's
global tag list, so it goes in the annotation *text* instead, not the tags.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from typing import List, Optional

logger = logging.getLogger("agx.grafana_annotate")


def _env_float(name: str, default: float) -> float:
    """Parse a float env var, falling back to `default` on unset/blank/garbage
    rather than raising at import time (which would take the service down)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("%s=%r is not a number, using %s", name, raw, default)
        return default


GRAFANA_URL = os.getenv("GRAFANA_URL", "").rstrip("/")
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN")
_TIMEOUT_SEC = _env_float("GRAFANA_ANNOTATE_TIMEOUT_SEC", 2.0)
_QUEUE_MAX = 256

_enabled = bool(GRAFANA_URL and GRAFANA_TOKEN)
if not _enabled:
    logger.info("Grafana annotations disabled (GRAFANA_URL/GRAFANA_TOKEN not set)")

_queue: "queue.Queue[dict]" = queue.Queue(maxsize=_QUEUE_MAX)
_worker_started = False
_worker_lock = threading.Lock()
_dropped = 0


def _worker() -> None:
    """Drain the queue, one POST at a time, on a reused Session. Never dies:
    any per-request failure is swallowed at debug level."""
    import requests  # local import: keep this off the service's import graph

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {GRAFANA_TOKEN}"})
    url = f"{GRAFANA_URL}/api/annotations"
    while True:
        payload = _queue.get()
        try:
            session.post(url, json=payload, timeout=_TIMEOUT_SEC)
        except Exception as e:  # noqa: BLE001
            logger.debug("Grafana annotation POST failed (non-fatal): %s", e)
        finally:
            _queue.task_done()


def _ensure_worker() -> None:
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if _worker_started:
            return
        threading.Thread(target=_worker, name="grafana-annotate",
                         daemon=True).start()
        _worker_started = True


def annotate(text: str, tags: Optional[List[str]] = None,
             time_ms: Optional[int] = None) -> None:
    """Enqueue one point-in-time Grafana annotation. Returns immediately —
    never blocks on the network and NEVER raises, so a monitoring call cannot
    slow down or break clip cutting, ingestion, or recording. If the send queue
    is full (Grafana unreachable and backing up) the annotation is dropped."""
    global _dropped
    if not _enabled:
        return
    try:
        _ensure_worker()
        payload = {
            "time": time_ms if time_ms is not None else int(time.time() * 1000),
            "tags": [str(t) for t in (tags or [])],
            "text": text,
        }
        try:
            _queue.put_nowait(payload)
        except queue.Full:
            _dropped += 1
            if _dropped % 50 == 1:
                logger.debug("Grafana annotation queue full; dropped %d so far",
                             _dropped)
    except Exception as e:  # noqa: BLE001
        logger.debug("Grafana annotate enqueue failed (non-fatal): %s", e)

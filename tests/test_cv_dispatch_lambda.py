"""Unit tests for the cv-dispatch Lambda handler.

Focus areas:
  * Event parsing — both EventBridge ("Object Created") and direct S3
    ("Records[].s3") shapes
  * S3 key parsing — pulls (location, date, uuid4, angle) and rejects
    keys that don't match the transcode-pipeline convention
  * 4-angle wait — handler exits cleanly when some angles are missing
  * Idempotency — handler skips when a non-terminal job already exists
    for this game
  * Happy path — submits exactly one fusion-A + fusion-B + merge bundle
  * Multi-record dedup — same game across multiple records in a single
    event collapses to one dispatch

All AWS calls are stubbed via monkeypatch — these tests run with no
boto3 credentials and no network.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Make the Lambda + repo-root modules importable in the test runner.
# Production runtime gets them in the deployment zip via Makefile.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))                          # cv_batch_dispatch.py, cv_metrics.py
sys.path.insert(0, str(REPO_ROOT / "lambda" / "cv_dispatch"))  # handler.py

# Suppress real metric emission across the suite.
os.environ.setdefault("DISABLE_CV_METRICS", "1")
os.environ.setdefault("DEBOUNCE_SECONDS", "0")
os.environ.setdefault("DISABLE_DEBOUNCE", "1")
os.environ.setdefault("AWS_REGION", "us-east-1")

import handler as h  # noqa: E402


# ---------------------------------------------------------------- fixtures
@pytest.fixture(autouse=True)
def _disable_debounce(monkeypatch):
    """Make sure no test ever blocks on the production debounce sleep."""
    monkeypatch.setattr(h.time, "sleep", lambda _s: None)


@pytest.fixture
def fake_s3_with_all_angles():
    """S3 client mock that reports all 4 angles present for any game."""
    m = MagicMock()
    m.head_object.return_value = {}
    return m


@pytest.fixture
def fake_s3_with_one_missing():
    """S3 client mock that reports the NR angle missing."""
    class _Err(Exception):
        response = {"Error": {"Code": "404"},
                    "ResponseMetadata": {"HTTPStatusCode": 404}}

    def _head(Bucket, Key):
        if Key.endswith("_NR.mp4"):
            raise _Err()
        return {}

    m = MagicMock()
    m.head_object.side_effect = _head
    return m


@pytest.fixture
def fake_dispatcher(monkeypatch):
    """Replace CVBatchDispatcher with a stub that records submit_game calls."""
    submitted: list = []
    batch_mock = MagicMock()
    batch_mock.list_jobs.return_value = {"jobSummaryList": []}

    class _StubDispatcher:
        fusion_queue = "cv-shot-detection-queue"

        def __init__(self, *a, **kw):
            self.batch_client = batch_mock

        def submit_game(self, game_keys):
            submitted.append(game_keys)
            return {
                "fusion_a": {"jobId": "fa-id"},
                "fusion_b": {"jobId": "fb-id"},
                "merge":    {"jobId": "merge-id"},
            }

    monkeypatch.setattr(h, "CVBatchDispatcher", _StubDispatcher)
    monkeypatch.setattr(h, "boto3", MagicMock())
    return submitted, batch_mock


# ---------------------------------------------------------------- helpers
def _eventbridge_event(key: str, bucket: str = "uball-videos-production") -> dict:
    return {
        "version": "0",
        "source": "aws.s3",
        "detail-type": "Object Created",
        "region": "us-east-1",
        "detail": {
            "bucket": {"name": bucket},
            "object": {"key": key},
            "reason": "PutObject",
        },
    }


def _records_event(*keys: str, bucket: str = "uball-videos-production") -> dict:
    return {
        "Records": [
            {"s3": {"bucket": {"name": bucket}, "object": {"key": k}}}
            for k in keys
        ]
    }


GAME_KEY_FL = "court-a/2026-05-08/abcd-1234-ef56-7890/2026-05-08_abcd-1234-ef56-7890_FL.mp4"
GAME_KEY_FR = "court-a/2026-05-08/abcd-1234-ef56-7890/2026-05-08_abcd-1234-ef56-7890_FR.mp4"
GAME_KEY_NL = "court-a/2026-05-08/abcd-1234-ef56-7890/2026-05-08_abcd-1234-ef56-7890_NL.mp4"
GAME_KEY_NR = "court-a/2026-05-08/abcd-1234-ef56-7890/2026-05-08_abcd-1234-ef56-7890_NR.mp4"


# ---------------------------------------------------------------- parsing
def test_parse_eventbridge_event_extracts_bucket_and_key():
    parsed = h._parse_s3_event(_eventbridge_event(GAME_KEY_FL))
    assert parsed == [("uball-videos-production", GAME_KEY_FL)]


def test_parse_records_event_extracts_all_records():
    parsed = h._parse_s3_event(_records_event(GAME_KEY_FL, GAME_KEY_FR))
    assert len(parsed) == 2
    assert {k for _, k in parsed} == {GAME_KEY_FL, GAME_KEY_FR}


def test_parse_event_ignores_object_deleted_eventbridge():
    e = _eventbridge_event(GAME_KEY_FL)
    e["detail-type"] = "Object Deleted"
    assert h._parse_s3_event(e) == []


def test_key_components_parses_canonical_key():
    c = h._key_components(GAME_KEY_FL)
    assert c == {
        "location": "court-a",
        "date":     "2026-05-08",
        "uuid4":    "abcd-1234-ef56-7890",
        "angle":    "FL",
    }


def test_key_components_rejects_unrelated_key():
    assert h._key_components("raw-chapters/some/path/file.MP4") is None


def test_key_components_rejects_wrong_angle():
    bad = "court-a/2026-05-08/abcd-1234-ef56-7890/2026-05-08_abcd-1234-ef56-7890_XX.mp4"
    assert h._key_components(bad) is None


# ---------------------------------------------------------------- gates
def test_handler_skips_when_angles_missing(fake_s3_with_one_missing, fake_dispatcher, monkeypatch):
    submitted, _ = fake_dispatcher
    monkeypatch.setattr(h.boto3, "client", lambda svc: fake_s3_with_one_missing)

    out = h.handler(_eventbridge_event(GAME_KEY_FL), context=None)

    assert out["submitted"] == 0
    assert out["skipped_waiting"] == 1
    assert submitted == []


def test_handler_submits_when_all_4_angles_present(fake_s3_with_all_angles, fake_dispatcher, monkeypatch):
    submitted, _ = fake_dispatcher
    monkeypatch.setattr(h.boto3, "client", lambda svc: fake_s3_with_all_angles)

    out = h.handler(_eventbridge_event(GAME_KEY_FL), context=None)

    assert out["submitted"] == 1
    assert out["skipped_waiting"] == 0
    assert len(submitted) == 1
    assert submitted[0].location == "court-a"
    assert submitted[0].game_uuid == "abcd-1234-ef56-7890"
    # All 4 expected angle keys reconstructed by the handler:
    assert set(submitted[0].angle_keys) == {"FL", "FR", "NL", "NR"}


def test_handler_dedupes_same_game_across_records(fake_s3_with_all_angles, fake_dispatcher, monkeypatch):
    """A single S3 event can carry several records for the same game (e.g. 4
    angles landing within the same multi-put). The handler should collapse
    them to one dispatch."""
    submitted, _ = fake_dispatcher
    monkeypatch.setattr(h.boto3, "client", lambda svc: fake_s3_with_all_angles)

    event = _records_event(GAME_KEY_FL, GAME_KEY_FR, GAME_KEY_NL, GAME_KEY_NR)
    out = h.handler(event, context=None)

    assert out["submitted"] == 1
    assert len(submitted) == 1


def test_handler_skips_when_jobs_already_in_flight(fake_s3_with_all_angles, fake_dispatcher, monkeypatch):
    submitted, batch_mock = fake_dispatcher
    monkeypatch.setattr(h.boto3, "client", lambda svc: fake_s3_with_all_angles)

    # Pretend a fusion job is already running for this game.
    batch_mock.list_jobs.return_value = {
        "jobSummaryList": [
            {"jobId": "in-flight", "jobName": "cv-fusion-abcd-1234-ef56-7890-A"}
        ]
    }

    out = h.handler(_eventbridge_event(GAME_KEY_FL), context=None)

    assert out["submitted"] == 0
    assert out["skipped_already_running"] == 1
    assert submitted == []


def test_handler_counts_unparseable_keys(fake_dispatcher, monkeypatch):
    monkeypatch.setattr(h.boto3, "client", lambda svc: MagicMock())
    event = {
        "Records": [
            {"s3": {"bucket": {"name": "uball-videos-production"},
                    "object": {"key": "raw-chapters/garbage.MP4"}}}
        ]
    }
    out = h.handler(event, context=None)
    assert out["submitted"] == 0
    assert out["skipped_unparseable"] == 1

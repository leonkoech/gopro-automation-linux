"""Tests for ghost-session de-dup logic in video_processing.py.

The bug this guards against: a second Start-press created a Firebase
recording-session doc that stayed at status='recording', endedAt=None,
chapterFiles=None, s3Prefix=None forever. The old de-dup (latest startedAt
wins per angle) picked the ghost over the real completed session because
the ghost's startedAt happened to be later, so every game in
process_game_videos failed with 'No videos were processed'.

These tests pin down:
  * _session_dedup_rank gives ghosts rank 0 and real uploaded sessions a
    higher rank
  * de-dup driven by (rank, parsed_start) prefers the real session even
    when the ghost has a later startedAt
  * same-rank ties still fall through to the "latest startedAt wins"
    behaviour, which is the legit case the old code was trying to handle
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# video_processing imports from logging_service at module load; stub it so
# the test doesn't need the real firebase/boto3 stack to import the function.
sys.modules.setdefault('logging_service', MagicMock(get_logger=lambda *_: MagicMock()))

from video_processing import _session_dedup_rank  # noqa: E402


# --- _session_dedup_rank --------------------------------------------------


def test_rank_fully_populated_uploaded_session_is_highest():
    s = {
        'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX010090.MP4'}],
        's3Prefix': 'raw-chapters/foo_FR_20260421/',
        'endedAt': '2026-04-22T03:49:36Z',
    }
    assert _session_dedup_rank(s) == 185


def test_rank_stopped_with_chapters_no_s3():
    s = {'status': 'stopped', 'totalChapters': 13, 'endedAt': '2026-04-21T21:00:00Z'}
    # 100 (stopped) + 25 (totalChapters>0) + 10 (endedAt) = 135
    assert _session_dedup_rank(s) == 135


def test_rank_ghost_session_is_zero():
    """The exact shape of the ghost sessions we saw in Firebase."""
    s = {
        'status': 'recording',
        'chapterFiles': None,
        's3Prefix': None,
        'endedAt': None,
    }
    assert _session_dedup_rank(s) == 0


def test_rank_recording_with_totalChapters_gets_partial_credit():
    """An actively-recording session that has already written chapters shouldn't be
    treated as a ghost — it has data, even if endedAt is not yet set."""
    s = {'status': 'recording', 'totalChapters': 5}
    # 0 (status not uploaded/stopped) + 25 (totalChapters>0) = 25
    assert _session_dedup_rank(s) == 25


def test_rank_chapterFiles_empty_list_does_not_score():
    """An empty list should NOT score — same as None."""
    s = {'status': 'uploaded', 'chapterFiles': [], 'totalChapters': 0}
    assert _session_dedup_rank(s) == 100  # only 'status' bonus


def test_rank_handles_missing_fields_gracefully():
    assert _session_dedup_rank({}) == 0


def test_rank_totalChapters_stored_as_string():
    """Defensive: Firebase sometimes round-trips ints as strings."""
    s = {'status': 'stopped', 'totalChapters': '13'}
    # int('13') > 0 → +25
    assert _session_dedup_rank(s) == 125


# --- de-dup behavior (simulating the inline code in process_game_videos) ----


def _parsed(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def _dedup(sessions):
    """Mirror the dedup block in video_processing.process_game_videos."""
    best_per_angle = {}
    for s in sessions:
        angle = s.get('angleCode', 'UNKNOWN')
        existing = best_per_angle.get(angle)
        s_key = (_session_dedup_rank(s), s['parsed_start'])
        if existing is None or s_key > (
            _session_dedup_rank(existing),
            existing['parsed_start'],
        ):
            best_per_angle[angle] = s
    return list(best_per_angle.values())


def test_dedup_prefers_uploaded_over_later_ghost_same_angle():
    """Regression for tonight's incident: ghost has NEWER startedAt but
    empty data; the uploaded session with older startedAt must still win."""
    good = {
        'id': 'hY7Ev8xc', 'angleCode': 'FR', 'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX010090.MP4'}],
        's3Prefix': 'raw-chapters/FR_session/',
        'endedAt': '2026-04-22T03:49:36Z',
        'parsed_start': _parsed('2026-04-21T20:17:58Z'),
    }
    ghost = {
        'id': 'oWiYAzTh', 'angleCode': 'FR', 'status': 'recording',
        'chapterFiles': None, 's3Prefix': None, 'endedAt': None,
        'parsed_start': _parsed('2026-04-21T21:23:38Z'),  # ~66 min LATER than good
    }
    winners = _dedup([good, ghost])
    assert len(winners) == 1
    assert winners[0]['id'] == 'hY7Ev8xc'


def test_dedup_latest_wins_among_equally_valid_sessions():
    """Legit use case the original code was written for: older-night session
    with similar completed state loses to newer-night session at same angle.
    Same-rank ties fall through to parsed_start."""
    older = {
        'id': 'prev-night', 'angleCode': 'FR', 'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX010080.MP4'}],
        's3Prefix': 'raw-chapters/prev/',
        'endedAt': '2026-04-17T23:00:00Z',
        'parsed_start': _parsed('2026-04-17T20:00:00Z'),
    }
    newer = {
        'id': 'tonight', 'angleCode': 'FR', 'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX010090.MP4'}],
        's3Prefix': 'raw-chapters/tonight/',
        'endedAt': '2026-04-22T03:00:00Z',
        'parsed_start': _parsed('2026-04-21T20:17:58Z'),
    }
    winners = _dedup([older, newer])
    assert len(winners) == 1
    assert winners[0]['id'] == 'tonight'


def test_dedup_multiple_angles_each_picks_best():
    good_fr = {
        'id': 'fr-good', 'angleCode': 'FR', 'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX010090.MP4'}],
        'endedAt': '2026-04-22T03:00:00Z',
        'parsed_start': _parsed('2026-04-21T20:17:58Z'),
    }
    ghost_fr = {
        'id': 'fr-ghost', 'angleCode': 'FR', 'status': 'recording',
        'chapterFiles': None, 's3Prefix': None, 'endedAt': None,
        'parsed_start': _parsed('2026-04-21T21:23:38Z'),
    }
    good_nl = {
        'id': 'nl-good', 'angleCode': 'NL', 'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX018518.MP4'}],
        'endedAt': '2026-04-22T03:00:00Z',
        'parsed_start': _parsed('2026-04-21T21:23:49Z'),
    }
    ghost_nl = {
        'id': 'nl-ghost', 'angleCode': 'NL', 'status': 'recording',
        'chapterFiles': None, 's3Prefix': None, 'endedAt': None,
        'parsed_start': _parsed('2026-04-21T20:18:08Z'),
    }
    winners = {w['angleCode']: w['id'] for w in _dedup([good_fr, ghost_fr, good_nl, ghost_nl])}
    assert winners == {'FR': 'fr-good', 'NL': 'nl-good'}


def test_dedup_single_session_per_angle_passes_through():
    solo = {
        'id': 'only-fr', 'angleCode': 'FR', 'status': 'uploaded',
        'chapterFiles': [{'filename': 'GX010090.MP4'}],
        'parsed_start': _parsed('2026-04-21T20:17:58Z'),
    }
    assert _dedup([solo]) == [solo]


def test_dedup_all_ghosts_no_good_session_picks_one_of_them():
    """Edge case: if the ONLY sessions for an angle are ghosts (no good session),
    we still pick one (the latest) so the pipeline can at least try. It'll fail
    later with 'no chapters found' but that's better than silently dropping the
    angle."""
    g1 = {
        'id': 'g1', 'angleCode': 'FR', 'status': 'recording',
        'chapterFiles': None, 's3Prefix': None, 'endedAt': None,
        'parsed_start': _parsed('2026-04-21T20:00:00Z'),
    }
    g2 = {
        'id': 'g2', 'angleCode': 'FR', 'status': 'recording',
        'chapterFiles': None, 's3Prefix': None, 'endedAt': None,
        'parsed_start': _parsed('2026-04-21T21:00:00Z'),
    }
    winners = _dedup([g1, g2])
    assert len(winners) == 1
    # Both have rank 0 → tiebreak by latest parsed_start → g2
    assert winners[0]['id'] == 'g2'

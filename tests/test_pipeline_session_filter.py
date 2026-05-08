"""Tests for ``pipeline_session_filter``.

These pin the regression that made Jetson-1 email 17 games on 2026-05-08:
a stale 'stopped' recording-session from May-4 with totalChapters=0 was
swept into tonight's pipeline (96h cutoff was the old setting), expanding
the game-detection timerange across four days.

The filter must:
  * skip non-'stopped' sessions entirely (they don't belong to a finished run)
  * skip empty stopped sessions (0 chapters, no s3Prefix) — these are the
    failed-recording corpses that caused the bug
  * skip sessions older than the age cutoff
  * keep good sessions, including sessions with no startedAt (defensive)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline_session_filter import (  # noqa: E402
    DEFAULT_AGE_HOURS,
    FilteredSessions,
    filter_pipeline_sessions,
    format_skip_log,
    session_label,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

NOW = datetime(2026, 5, 8, 5, 0, tzinfo=timezone.utc)
AGE_CUTOFF = NOW - timedelta(hours=DEFAULT_AGE_HOURS)  # = 2026-05-07 17:00 UTC


def _session(
    *,
    sid: str = 'sess1',
    status: str = 'stopped',
    started: datetime | str | None = NOW - timedelta(hours=2),
    total_chapters: int = 4,
    chapter_files: int | list = 4,
    s3_prefix: str | None = 'raw-chapters/foo/',
    angle: str = 'FR',
) -> dict:
    """Build a fake recording-session doc with sensible defaults."""
    files = chapter_files if isinstance(chapter_files, list) else (
        [{'filename': f'GX{i:02d}.MP4'} for i in range(chapter_files)]
    )
    return {
        'id': sid,
        'status': status,
        'startedAt': started,
        'totalChapters': total_chapters,
        'chapterFiles': files,
        's3Prefix': s3_prefix,
        'angleCode': angle,
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_keeps_recent_stopped_session_with_chapters():
    s = _session(started=NOW - timedelta(hours=2))
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]
    assert out.stale_sessions == []
    assert out.empty_sessions == []


def test_keeps_session_with_only_s3prefix_and_no_chapter_files():
    """Already-uploaded sessions arrive with totalChapters>0 and a s3Prefix,
    sometimes with chapterFiles wiped. They must still be kept."""
    s = _session(total_chapters=4, chapter_files=0, s3_prefix='raw-chapters/x/')
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


def test_keeps_session_with_chapter_files_but_no_total_chapters_field():
    """Edge case: totalChapters missing but chapterFiles populated."""
    s = _session(total_chapters=0, chapter_files=3, s3_prefix=None)
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


# ---------------------------------------------------------------------------
# Status filter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('status', ['recording', 'failed', 'uploaded', 'pending', None])
def test_drops_non_stopped_sessions(status):
    s = _session(status=status)
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == []
    assert out.stale_sessions == []
    assert out.empty_sessions == []


# ---------------------------------------------------------------------------
# Empty session filter — the regression
# ---------------------------------------------------------------------------


def test_regression_jetson1_may4_empty_session_is_skipped():
    """The exact session that caused the 17-game Jetson-1 email."""
    s = _session(
        sid='BPVGJdQW60zy5a2UkcWq',
        status='stopped',
        started=datetime(2026, 5, 4, 23, 44, 33, tzinfo=timezone.utc),
        total_chapters=0,
        chapter_files=0,
        s3_prefix=None,
    )
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == []
    assert out.empty_sessions == [s]
    # Stale-by-age would also catch it, but empty wins (more specific reason).
    assert out.stale_sessions == []


def test_empty_session_skipped_even_when_recent():
    """An empty session within the age window is still useless."""
    s = _session(
        started=NOW - timedelta(minutes=30),
        total_chapters=0,
        chapter_files=0,
        s3_prefix=None,
    )
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.empty_sessions == [s]
    assert out.sessions == []


def test_empty_classification_runs_before_age_classification():
    """If a session is BOTH empty and stale, it should be classified as
    empty — that tells the operator the more useful thing."""
    s = _session(
        started=NOW - timedelta(days=4),
        total_chapters=0,
        chapter_files=0,
        s3_prefix=None,
    )
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.empty_sessions == [s]
    assert out.stale_sessions == []


# ---------------------------------------------------------------------------
# Age filter
# ---------------------------------------------------------------------------


def test_drops_session_older_than_cutoff():
    s = _session(started=NOW - timedelta(hours=DEFAULT_AGE_HOURS + 1))
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.stale_sessions == [s]
    assert out.sessions == []


def test_keeps_session_exactly_at_cutoff():
    """Boundary: ``started == age_cutoff`` should be inclusive (not stale)."""
    s = _session(started=AGE_CUTOFF)
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


def test_iso_string_started_at_is_parsed():
    s = _session(started=(NOW - timedelta(hours=2)).isoformat())
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


def test_iso_string_with_z_suffix_is_parsed():
    s = _session(started='2026-05-08T03:00:00Z')
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


def test_naive_datetime_is_treated_as_utc():
    naive = (NOW - timedelta(hours=2)).replace(tzinfo=None)
    s = _session(started=naive)
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


def test_unparseable_started_at_is_kept():
    """Defensive: if we can't parse startedAt, keep the session — silently
    dropping it would be worse than including it."""
    s = _session(started='not-a-date')
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


def test_missing_started_at_is_kept():
    s = _session(started=None)
    out = filter_pipeline_sessions([s], age_cutoff=AGE_CUTOFF)
    assert out.sessions == [s]


# ---------------------------------------------------------------------------
# Multi-session realistic scenarios
# ---------------------------------------------------------------------------


def test_mixed_input_partitions_correctly():
    good = _session(sid='good', started=NOW - timedelta(hours=1))
    stale = _session(sid='stale', started=NOW - timedelta(days=2))
    empty = _session(
        sid='empty',
        started=NOW - timedelta(hours=1),
        total_chapters=0, chapter_files=0, s3_prefix=None,
    )
    other_status = _session(sid='rec', status='recording')

    out = filter_pipeline_sessions(
        [good, stale, empty, other_status],
        age_cutoff=AGE_CUTOFF,
    )
    assert [s['id'] for s in out.sessions] == ['good']
    assert [s['id'] for s in out.stale_sessions] == ['stale']
    assert [s['id'] for s in out.empty_sessions] == ['empty']


def test_jetson1_2026_05_07_scenario():
    """Reproduces tonight's Jetson-1 input: one empty May-4 ghost +
    one good May-7 session. The pipeline must see only the good one."""
    ghost_may_4 = _session(
        sid='BPVGJdQW60zy5a2UkcWq',
        started=datetime(2026, 5, 4, 23, 44, tzinfo=timezone.utc),
        total_chapters=0, chapter_files=0, s3_prefix=None,
    )
    good_may_7 = _session(
        sid='Rzvy5xcrttjD3pBqe7Ac',
        started=datetime(2026, 5, 7, 23, 47, tzinfo=timezone.utc),
        total_chapters=4,
        chapter_files=4,
        s3_prefix='raw-chapters/enxd43260dc857e_FR_20260507_194726/',
    )
    out = filter_pipeline_sessions([ghost_may_4, good_may_7], age_cutoff=AGE_CUTOFF)
    assert [s['id'] for s in out.sessions] == ['Rzvy5xcrttjD3pBqe7Ac']
    assert [s['id'] for s in out.empty_sessions] == ['BPVGJdQW60zy5a2UkcWq']


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_session_label_short_id():
    s = {'angleCode': 'FR', 'id': 'BPVGJdQW60zy5a2UkcWq'}
    assert session_label(s) == 'FR(BPVGJdQW)'


def test_session_label_handles_missing_fields():
    assert session_label({}) == '?(?)'


def test_format_skip_log_includes_count_and_labels():
    sessions = [
        {'angleCode': 'FR', 'id': 'aaaaaaaa11111111'},
        {'angleCode': 'FL', 'id': 'bbbbbbbb22222222'},
    ]
    msg = format_skip_log('stale (>12h old)', sessions)
    assert 'Skipping 2 stale (>12h old)' in msg
    assert 'FR(aaaaaaaa)' in msg
    assert 'FL(bbbbbbbb)' in msg


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


def test_filter_returns_dataclass_instance():
    out = filter_pipeline_sessions([], age_cutoff=AGE_CUTOFF)
    assert isinstance(out, FilteredSessions)
    assert out.sessions == []
    assert out.stale_sessions == []
    assert out.empty_sessions == []

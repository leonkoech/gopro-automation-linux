"""Tests for calculate_extraction_params gap-aware offset logic.

The bug this guards against: jetson-1 Apr 21/22 pipeline extracted
5 "live" games from ~65 min after the real game time because FR's
recording-session doc mixed chapters from 3 different days (Apr 17,
Apr 20, Apr 21 false-starts, Apr 21 real recording) with a 65 min
gap between the false-start chapters and recording 0090. The old
math assumed all chapters were contiguous from session.startedAt,
so the offset walked through the false-starts and landed inside the
real recording at the wrong wall-clock position.

These tests pin down:
  * Per-chapter ``creation_time`` drives the game→chapter mapping
  * Games inside a single chapter compute offset = game_start − chapter.start
  * Games spanning two contiguous chapters return both + trailing buffer
  * Games falling in a gap between recordings yield no chapters (graceful
    failure instead of extracting from the wrong wall-clock time)
  * Gaps between overlapping chapters are logged but don't break math
  * Missing ``creation_time`` on any chapter triggers legacy fallback
  * Tonight's exact FR scenario (ch7–ch13 on jetson-1) produces the
    correct offsets for Titans (ch9, 519s) and Blessed (ch13, 133s)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

sys.modules.setdefault('logging_service', MagicMock(get_logger=lambda *_: logging.getLogger('test')))

from video_processing import VideoProcessor  # noqa: E402


@pytest.fixture
def vp(tmp_path):
    storage = tmp_path / 'storage'
    segments = tmp_path / 'segments'
    storage.mkdir()
    segments.mkdir()
    return VideoProcessor(str(storage), str(segments))


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace('Z', '+00:00'))


def _chapter(filename: str, creation_time: str, duration_seconds: float) -> dict:
    return {
        'filename': filename,
        'creation_time': creation_time,
        'duration_seconds': duration_seconds,
        'source': 's3',
    }


# --- Single-chapter extraction ---------------------------------------------


def test_offset_within_single_chapter(vp):
    """Game entirely inside one chapter: offset = game_start - recording_start
    (first chapter is anchored to recording_start, not creation_time, so
    cross-camera clock drift doesn't leak into the offset)."""
    chapters = [_chapter('GX010090.MP4', '2026-04-21T21:23:37Z', 4868.9)]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:32:16Z'),
        game_end=_dt('2026-04-21T22:22:16Z'),
        recording_start=_dt('2026-04-21T21:23:37Z'),
        chapters=chapters,
    )
    assert params['offset_seconds'] == pytest.approx(519, abs=1)
    assert params['duration_seconds'] == pytest.approx(3000, abs=1)
    assert [c['filename'] for c in params['chapters_needed']] == ['GX010090.MP4']


def test_game_starts_exactly_at_chapter_boundary(vp):
    chapters = [_chapter('c.MP4', '2026-04-21T21:00:00Z', 600)]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:00:00Z'),
        game_end=_dt('2026-04-21T21:05:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    assert params['offset_seconds'] == 0.0
    assert params['duration_seconds'] == 300.0


# --- Multi-chapter extraction ----------------------------------------------


def test_game_spans_two_contiguous_chapters(vp):
    """Game crosses a chapter boundary; both chapters returned. Chapters share
    creation_time (one recording) so ch2 starts at ch1.end in wall-clock."""
    chapters = [
        _chapter('GX010090.MP4', '2026-04-21T21:23:37Z', 4868.9),  # shared ct
        _chapter('GX020090.MP4', '2026-04-21T21:23:37Z', 4868.9),  # shared ct
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T22:30:00Z'),
        game_end=_dt('2026-04-21T23:00:00Z'),
        recording_start=_dt('2026-04-21T21:23:37Z'),
        chapters=chapters,
    )
    names = [c['filename'] for c in params['chapters_needed']]
    assert 'GX010090.MP4' in names and 'GX020090.MP4' in names
    # Offset into first chapter (22:30 − 21:23:37 = 66m23s = 3983s)
    assert params['offset_seconds'] == pytest.approx(3983, abs=1)


def test_trailing_buffer_chapter_added(vp):
    """When a later chapter exists immediately after the last needed one, include it."""
    chapters = [
        _chapter('c1.MP4', '2026-04-21T21:00:00Z', 600),  # 21:00–21:10
        _chapter('c2.MP4', '2026-04-21T21:10:00Z', 600),  # 21:10–21:20 (buffer)
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:02:00Z'),
        game_end=_dt('2026-04-21T21:05:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    names = [c['filename'] for c in params['chapters_needed']]
    assert names == ['c1.MP4', 'c2.MP4']


# --- Gap handling ----------------------------------------------------------


def test_game_falls_entirely_inside_gap_returns_no_chapters(vp):
    """Jetson-1 Apr 21/22 scenario: game window lands between recordings."""
    chapters = [
        _chapter('false_start.MP4', '2026-04-21T20:17:47Z', 6.0),   # 20:17:47–53
        _chapter('real.MP4',         '2026-04-21T21:23:37Z', 4868.9),  # 21:23:37–22:44:46
    ]
    # Game at 20:30 — between the 6-sec false start and the real recording
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T20:30:00Z'),
        game_end=_dt('2026-04-21T20:40:00Z'),
        recording_start=_dt('2026-04-21T20:17:47Z'),
        chapters=chapters,
    )
    assert params['chapters_needed'] == []
    assert params['chapters_to_process'] == 0


def test_game_after_all_chapters_end_fails_gracefully(vp):
    """Old bug: pipeline's offset math would pick a random chapter here."""
    chapters = [
        _chapter('c.MP4', '2026-04-21T21:00:00Z', 600),  # 21:00–21:10
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-22T02:45:00Z'),
        game_end=_dt('2026-04-22T03:00:00Z'),
        recording_start=_dt('2026-04-21T20:00:00Z'),
        chapters=chapters,
    )
    assert params['chapters_needed'] == []


def test_gap_between_needed_chapters_logs_warning(vp, caplog):
    """When the game span crosses a gap, both chapters are selected but a
    warning is logged — downstream clip will jump over the gap."""
    chapters = [
        _chapter('c1.MP4', '2026-04-21T21:00:00Z', 600),   # 21:00–21:10
        _chapter('c2.MP4', '2026-04-21T22:00:00Z', 600),   # 22:00–22:10 (50-min gap!)
    ]
    with caplog.at_level(logging.WARNING):
        params = vp.calculate_extraction_params(
            game_start=_dt('2026-04-21T21:05:00Z'),
            game_end=_dt('2026-04-21T22:05:00Z'),
            recording_start=_dt('2026-04-21T21:00:00Z'),
            chapters=chapters,
        )
    assert len(params['chapters_needed']) >= 2
    assert any('GAP' in r.message for r in caplog.records)


# --- Fallback to legacy math -----------------------------------------------


def test_missing_creation_time_falls_back_to_legacy(vp):
    """If ANY chapter lacks creation_time, old contiguous math is used."""
    chapters = [
        {'filename': 'c1.MP4', 'duration_seconds': 600},  # no creation_time
        {'filename': 'c2.MP4', 'duration_seconds': 600},
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:05:00Z'),
        game_end=_dt('2026-04-21T21:08:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    # Legacy math treats c1 as [0,600], so offset 5min = 300s
    assert params['offset_seconds'] == pytest.approx(300, abs=1)
    assert params['chapters_needed'][0]['filename'] == 'c1.MP4'


def test_zero_duration_chapter_triggers_legacy_fallback(vp):
    """A chapter with duration_seconds=0 is insufficient metadata — fall back."""
    chapters = [
        _chapter('c1.MP4', '2026-04-21T21:00:00Z', 600),
        {'filename': 'c2.MP4', 'creation_time': '2026-04-21T21:10:00Z', 'duration_seconds': 0},
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:02:00Z'),
        game_end=_dt('2026-04-21T21:05:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    # Legacy math applied; duration=0 chapter estimated as 900s
    assert params['chapters_needed']


# --- Naive datetime input --------------------------------------------------


def test_accepts_naive_datetimes(vp):
    """Callers sometimes pass naive datetimes; treat them as UTC."""
    chapters = [_chapter('c.MP4', '2026-04-21T21:00:00Z', 600)]
    params = vp.calculate_extraction_params(
        game_start=datetime(2026, 4, 21, 21, 2, 0),
        game_end=datetime(2026, 4, 21, 21, 5, 0),
        recording_start=datetime(2026, 4, 21, 21, 0, 0),
        chapters=chapters,
    )
    assert params['offset_seconds'] == pytest.approx(120, abs=1)


# --- Real-world regression: tonight's jetson-1 FR session ------------------


# Tonight's jetson-1 FR chapters AS GoPro writes them: every chapter in a
# recording shares the recording's creation_time (the CODE must compute the
# real per-chapter wall-clock start by accumulating durations within a
# shared-creation_time group).
FR_TONIGHT_RAW = [
    # False-start recordings (kept by _filter_old_gopro_chapters, within session window)
    _chapter('GX010088.MP4', '2026-04-21T20:17:47Z', 6.0),     # 20:17:47–53
    _chapter('GX010089.MP4', '2026-04-21T20:17:55Z', 2.2),     # 20:17:55–57
    # Recording 0090: 5 chapters, all stamped 21:23:37 by GoPro
    _chapter('GX010090.MP4', '2026-04-21T21:23:37Z', 4868.9),  # ch9:  21:23:37–22:44:46
    _chapter('GX020090.MP4', '2026-04-21T21:23:37Z', 4868.9),  # ch10: 22:44:46–00:05:55
    _chapter('GX030090.MP4', '2026-04-21T21:23:37Z', 4804.8),  # ch11: 00:05:55–01:26:00
    _chapter('GX040090.MP4', '2026-04-21T21:23:37Z', 4612.6),  # ch12: 01:26:00–02:42:52
    _chapter('GX050090.MP4', '2026-04-21T21:23:37Z', 3874.6),  # ch13: 02:42:52–03:47:26
]


def test_tonight_game1_titans_picks_ch9_with_507s_offset(vp):
    """Titans vs Black Team C (21:32:16Z): lands in GX010090.

    Offset is ~507s (not 519s as under PR #30). The shift is the 11s camera
    clock skew: FR camera's creation_time 21:23:37 is 11s before the Jetson's
    session.startedAt 20:17:58 + accumulated gaps (20:18:09 end of ch8) +
    camera-clock gap (3939.8s) → 21:23:48.95. Anchoring to session.startedAt
    means ch9 wall-clock start is 21:23:48.95, so the offset for a game at
    21:32:16 is 507s in real wall-clock, not 519s in camera-clock.
    """
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:32:16Z'),
        game_end=_dt('2026-04-21T22:22:00Z'),
        recording_start=_dt('2026-04-21T20:17:58.948787Z'),
        chapters=FR_TONIGHT_RAW,
    )
    names = [c['filename'] for c in params['chapters_needed']]
    assert names[0] == 'GX010090.MP4'
    assert params['offset_seconds'] == pytest.approx(507, abs=2)


def test_tonight_game6_blessed_picks_ch13_with_121s_offset(vp):
    """Blessed & Highly Favored vs Akatsuki (02:45:05Z → 03:35:12Z): lands in
    GX050090. Under the old contiguous math (pre-PR #30) this failed entirely
    ('No videos were processed'). Under PR #31 with session.startedAt
    anchoring, ch7's wall-clock start is 02:43:04.15, so the offset for a
    game at 02:45:05 is ~121s.
    """
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-22T02:45:05Z'),
        game_end=_dt('2026-04-22T03:35:12Z'),
        recording_start=_dt('2026-04-21T20:17:58.948787Z'),
        chapters=FR_TONIGHT_RAW,
    )
    names = [c['filename'] for c in params['chapters_needed']]
    assert names[0] == 'GX050090.MP4'
    assert params['offset_seconds'] == pytest.approx(121, abs=2)
    # Game fits within ch13 — no additional overlap chapter needed
    assert params['chapters_to_process'] <= 2  # ch13 + optional trailing buffer (none here)


def test_tonight_game3_ortega_spans_chapters_correctly(vp):
    """Ortega vs Miracle Leaf (00:38:56Z → ~01:25Z): starts inside GX030090
    at offset ~1969s under session-anchored math."""
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-22T00:38:56Z'),
        game_end=_dt('2026-04-22T01:25:00Z'),
        recording_start=_dt('2026-04-21T20:17:58.948787Z'),
        chapters=FR_TONIGHT_RAW,
    )
    names = [c['filename'] for c in params['chapters_needed']]
    assert names[0] == 'GX030090.MP4'
    assert params['offset_seconds'] == pytest.approx(1969, abs=2)


# --- Shared creation_time within a recording ------------------------------


def test_shared_creation_time_chapters_accumulate_durations(vp):
    """GoPro stamps every chapter of a recording with the recording's start
    time. Our code must accumulate durations *within* a shared-creation_time
    run to recover each chapter's true wall-clock start."""
    chapters = [
        _chapter('GX01.MP4', '2026-04-21T21:00:00Z', 600),  # 21:00:00–21:10:00
        _chapter('GX02.MP4', '2026-04-21T21:00:00Z', 600),  # 21:10:00–21:20:00 (shared ct)
        _chapter('GX03.MP4', '2026-04-21T21:00:00Z', 600),  # 21:20:00–21:30:00 (shared ct)
    ]
    # Game squarely in ch2
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:12:00Z'),
        game_end=_dt('2026-04-21T21:17:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    assert params['chapters_needed'][0]['filename'] == 'GX02.MP4'
    assert params['offset_seconds'] == pytest.approx(120, abs=1)  # 21:12:00 − 21:10:00 = 120s


def test_gap_between_recordings_detected_via_creation_time_change(vp):
    """Two recordings (different creation_times) = new recording begins at its
    stamped creation_time, not at previous recording's end. Gap is real."""
    chapters = [
        # Recording A: 21:00:00–21:20:00 (2 chapters, shared ct)
        _chapter('GX010100.MP4', '2026-04-21T21:00:00Z', 600),
        _chapter('GX020100.MP4', '2026-04-21T21:00:00Z', 600),
        # 40-min gap (camera was off)
        # Recording B: 22:00:00–22:10:00 (new ct → not a continuation)
        _chapter('GX010101.MP4', '2026-04-21T22:00:00Z', 600),
    ]
    # Game at 21:30 (in the gap) — no chapter should overlap
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:30:00Z'),
        game_end=_dt('2026-04-21T21:45:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    assert params['chapters_needed'] == []


# --- Out-of-order input ----------------------------------------------------


def test_chapters_unsorted_by_filename_still_resolved_by_creation_time(vp):
    """Defensive: sort by creation_time so filename disorder doesn't matter."""
    chapters = [
        _chapter('b.MP4', '2026-04-21T21:10:00Z', 600),  # 21:10–21:20
        _chapter('a.MP4', '2026-04-21T21:00:00Z', 600),  # 21:00–21:10
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:02:00Z'),
        game_end=_dt('2026-04-21T21:05:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    # Game is in chapter 'a' based on creation_time, not in filename-first 'b'
    assert params['chapters_needed'][0]['filename'] == 'a.MP4'
    assert params['offset_seconds'] == pytest.approx(120, abs=1)


def test_shared_creation_time_reverse_filename_order_still_accumulates_correctly(vp):
    """Regression for a sort-before-walk bug: chapters of a single recording
    (shared creation_time) passed in REVERSE filename order must still get
    wall-clock starts accumulated in filename order, not list order."""
    chapters = [
        _chapter('GX030090.MP4', '2026-04-21T21:23:37Z', 4804.8),
        _chapter('GX010090.MP4', '2026-04-21T21:23:37Z', 4868.9),
        _chapter('GX020090.MP4', '2026-04-21T21:23:37Z', 4868.9),
    ]
    # Game at 22:30 should land in GX010090 (not GX020090 or GX030090)
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T22:30:00Z'),
        game_end=_dt('2026-04-21T22:35:00Z'),
        recording_start=_dt('2026-04-21T21:23:37Z'),
        chapters=chapters,
    )
    assert params['chapters_needed'][0]['filename'] == 'GX010090.MP4'
    # 22:30 − 21:23:37 = 66m23s = 3983s — offset into ch1, not into ch3
    assert params['offset_seconds'] == pytest.approx(3983, abs=2)


def test_trailing_buffer_skipped_when_next_chapter_is_after_a_gap(vp):
    """A 'trailing buffer' chapter must only be appended when it really
    buffers the boundary (<=2s gap). A chapter from a later recording gives
    no buffer value — just wastes bytes over HTTP concat."""
    chapters = [
        _chapter('c1.MP4', '2026-04-21T21:00:00Z', 600),   # 21:00–21:10
        _chapter('c2.MP4', '2026-04-21T22:00:00Z', 600),   # 22:00–22:10 (50-min gap!)
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:02:00Z'),
        game_end=_dt('2026-04-21T21:05:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    assert [c['filename'] for c in params['chapters_needed']] == ['c1.MP4']


def test_sub_second_creation_time_drift_treated_as_continuation(vp):
    """ffprobe sometimes reports consecutive chapters with 1s apart ctimes.
    That must not split a recording into two phantom recordings."""
    chapters = [
        _chapter('c1.MP4', '2026-04-21T21:00:00Z', 600),  # 21:00:00–21:10:00
        _chapter('c2.MP4', '2026-04-21T21:00:01Z', 600),  # 1s drift — same recording
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:12:00Z'),
        game_end=_dt('2026-04-21T21:15:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    # Game should fall in c2 at offset 120s (21:12 − 21:10), not rejected
    # for a phantom gap
    assert params['chapters_needed'][0]['filename'] == 'c2.MP4'
    assert params['offset_seconds'] == pytest.approx(120, abs=2)


# --- Cross-camera sync (the Apr 22/23 regression) -------------------------


def test_cross_camera_sync_despite_clock_drift(vp):
    """Two cameras recording the same event at the same Jetson-commanded moment
    but with different internal-clock offsets MUST resolve to the same game
    wall-clock time on both angles. Under PR #30 this test failed — FR extracted
    5s ahead of FL because each anchored to its own camera's creation_time."""
    # Both Jetsons commanded 'start' at 22:24:44.3 (shared NTP reference).
    # FR camera clock is 4.3s behind wall-clock; FL camera clock is 0.7s ahead.
    recording_start = _dt('2026-04-22T22:24:44.305Z')

    # FR chapters (shared creation_time 22:24:40, camera 4.3s behind Jetson)
    fr_chapters = [
        _chapter('GX010091.MP4', '2026-04-22T22:24:40Z', 4868.9),
        _chapter('GX020091.MP4', '2026-04-22T22:24:40Z', 4868.9),
    ]
    # FL chapters (shared creation_time 22:24:45, camera 0.7s ahead of Jetson)
    fl_chapters = [
        _chapter('GX010123.MP4', '2026-04-22T22:24:45Z', 3075.1),
        _chapter('GX020123.MP4', '2026-04-22T22:24:45Z', 2818.8),
        _chapter('GX030123.MP4', '2026-04-22T22:24:45Z', 2626.6),
    ]

    # Game starts 23:33:42.505 (wall-clock) — Black Team C vs 305 Turnovers
    # from the Apr 22/23 incident.
    game_start = _dt('2026-04-22T23:33:42.505Z')
    game_end = _dt('2026-04-23T00:20:09.156Z')

    fr_params = vp.calculate_extraction_params(game_start, game_end, recording_start, fr_chapters)
    fl_params = vp.calculate_extraction_params(game_start, game_end, recording_start, fl_chapters)

    # The frame each angle extracts must correspond to the same wall-clock
    # moment. Compute extracted wall-clock per angle = first-needed-chapter
    # wall-clock start + offset_seconds. Both must equal game_start.
    def extracted_walltime(params, chapters, rs):
        # Replicate the wall-clock computation the implementation did.
        first = params['chapters_needed'][0]
        # Find first in the input list to get its index-ordered wall-clock start
        idx = chapters.index(first)
        # All these test chapters share creation_time → ch[i].start = rs + sum(ch[0..i-1].duration)
        cum = sum(c['duration_seconds'] for c in chapters[:idx])
        return rs + timedelta(seconds=cum + params['offset_seconds'])

    fr_extracted = extracted_walltime(fr_params, fr_chapters, recording_start)
    fl_extracted = extracted_walltime(fl_params, fl_chapters, recording_start)

    # Both must extract within 0.5s of the game's actual wall-clock start
    assert abs((fr_extracted - game_start).total_seconds()) < 0.5, \
        f"FR extracted at {fr_extracted.isoformat()}, expected {game_start.isoformat()}"
    assert abs((fl_extracted - game_start).total_seconds()) < 0.5, \
        f"FL extracted at {fl_extracted.isoformat()}, expected {game_start.isoformat()}"
    # Cross-camera sync — the bug this PR fixes — must be sub-second
    assert abs((fr_extracted - fl_extracted).total_seconds()) < 0.5


def test_single_recording_session_anchors_to_session_startedat(vp):
    """With a single-recording session, first chapter's wall-clock start MUST
    equal recording_start regardless of the camera's creation_time reading.
    This is the core fix for the Apr 22/23 cross-camera drift bug."""
    # Camera clock is 4.3s behind Jetson clock (realistic drift)
    chapters = [
        _chapter('GX010091.MP4', '2026-04-22T22:24:40Z', 4868.9),
        _chapter('GX020091.MP4', '2026-04-22T22:24:40Z', 4868.9),
    ]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-22T22:32:39.122Z'),  # ~8 min into session
        game_end=_dt('2026-04-22T23:24:03.378Z'),
        recording_start=_dt('2026-04-22T22:24:44.305Z'),
        chapters=chapters,
    )
    # Expected offset = game_start − recording_start = 474.8s (not 479s as
    # under PR #30 which used creation_time-anchored ch1.start of 22:24:40)
    assert params['offset_seconds'] == pytest.approx(474.8, abs=0.5)


def test_large_clock_skew_logs_warning_but_still_anchors_to_session(vp, caplog):
    """If camera creation_time is >60s off from session.startedAt, log a
    warning but still anchor to session.startedAt so cross-camera sync holds."""
    chapters = [_chapter('c.MP4', '2026-04-22T22:30:00Z', 600)]  # 5min 15s ahead
    with caplog.at_level(logging.WARNING):
        params = vp.calculate_extraction_params(
            game_start=_dt('2026-04-22T22:32:00Z'),
            game_end=_dt('2026-04-22T22:35:00Z'),
            recording_start=_dt('2026-04-22T22:24:45Z'),
            chapters=chapters,
        )
    assert any('unusually large' in r.message.lower() for r in caplog.records)
    # Offset still based on session.startedAt anchor
    assert params['offset_seconds'] == pytest.approx(435, abs=1)  # 22:32 − 22:24:45


# --- Regression field check -----------------------------------------------


def test_returns_none_for_legacy_fields_in_new_path(vp):
    """offset_from_recording_start and first_chapter_start_time have
    different semantics per-path and must not be consumed downstream. The
    new path nulls them so any stale consumer fails loudly."""
    chapters = [_chapter('c.MP4', '2026-04-21T21:00:00Z', 600)]
    params = vp.calculate_extraction_params(
        game_start=_dt('2026-04-21T21:02:00Z'),
        game_end=_dt('2026-04-21T21:05:00Z'),
        recording_start=_dt('2026-04-21T21:00:00Z'),
        chapters=chapters,
    )
    assert params['offset_from_recording_start'] is None
    assert params['first_chapter_start_time'] is None

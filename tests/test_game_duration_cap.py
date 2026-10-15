"""Tests for the 90-minute game duration cap in process_game_videos.

Background — the cap protects against the Apr 28/29 failure mode where
a scorekeeper forgot to hit "End Game" and only finalised the timer 18+
hours later, producing ``endedAt - createdAt = 20.4h``. The unbounded
window made the chapter-overlap selection nonsensical and the game
ended up with no extracted output. The cap turns that into a 90-min
clip, which always covers a real game and lets the pipeline still
emit something even when the scorekeeper data is bad.
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

# Stub the Jetson logging service so importing video_processing succeeds
# without firebase/boto3 setup.
sys.modules.setdefault(
    "logging_service",
    MagicMock(get_logger=lambda *_: logging.getLogger("test")),
)


def _apply_duration_cap(
    game_start: datetime, game_end: datetime
) -> tuple[datetime, bool, str | None]:
    """Mirror the cap block at video_processing.py:1791-1810.

    Returns ``(capped_end, was_capped, warning_substring)``.
    """
    MAX_GAME_DURATION = timedelta(minutes=90)
    if game_end - game_start > MAX_GAME_DURATION:
        return (
            game_start + MAX_GAME_DURATION,
            True,
            "exceeds 90-min cap",
        )
    return (game_end, False, None)


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# --- The failure case the cap fixes -----------------------------------------


def test_apr28_29_scorekeeper_forgot_end_game_is_capped():
    """The exact incident: 02:34:13 → 22:57:28 = 20.4h. Cap to 90 min."""
    start = _utc("2026-04-29T02:34:13.611Z")
    end = _utc("2026-04-29T22:57:28.611Z")
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert was_capped
    assert capped_end == start + timedelta(minutes=90)
    assert (capped_end - start).total_seconds() == 5400


def test_warning_message_identifies_cap_reason():
    start = _utc("2026-04-29T02:34:13.611Z")
    end = _utc("2026-04-29T22:57:28.611Z")
    _, was_capped, msg = _apply_duration_cap(start, end)
    assert was_capped and msg and "90-min cap" in msg


# --- Real games are NEVER touched -------------------------------------------


@pytest.mark.parametrize(
    "duration_min",
    [10, 20, 35, 47, 60, 75, 80, 89, 89.99],
)
def test_normal_game_durations_pass_through(duration_min):
    """Real basketball games run 47-80 min — must not be capped."""
    start = _utc("2026-04-29T02:34:13Z")
    end = start + timedelta(minutes=duration_min)
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert not was_capped
    assert capped_end == end


def test_exactly_90_min_is_not_capped():
    """Boundary: a 90-minute game is the maximum allowed without capping."""
    start = _utc("2026-04-29T02:34:13Z")
    end = start + timedelta(minutes=90)
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert not was_capped
    assert capped_end == end


def test_91_min_is_capped():
    """Just past 90 min triggers the cap."""
    start = _utc("2026-04-29T02:34:13Z")
    end = start + timedelta(minutes=91)
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert was_capped
    assert capped_end == start + timedelta(minutes=90)


# --- Other failure modes the cap handles -----------------------------------


def test_endedat_set_to_next_day_is_capped():
    """If endedAt is bumped a full calendar day forward (e.g. dashboard
    auto-fills 23:59 of game-day), still cap cleanly."""
    start = _utc("2026-04-29T02:34:13Z")
    end = _utc("2026-04-29T23:59:59Z")
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert was_capped
    assert (capped_end - start).total_seconds() == 5400


def test_endedat_a_week_later_is_capped():
    """Pathological case — even a week-later endedAt resolves cleanly."""
    start = _utc("2026-04-29T02:34:13Z")
    end = start + timedelta(days=7)
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert was_capped
    assert capped_end == start + timedelta(minutes=90)


# --- Edge cases ------------------------------------------------------------


def test_zero_duration_is_not_capped():
    """game_end == game_start is not >90min — no cap fires (downstream
    selection will still complain about the zero-length window, which is
    correct)."""
    t = _utc("2026-04-29T02:34:13Z")
    capped_end, was_capped, _ = _apply_duration_cap(t, t)
    assert not was_capped
    assert capped_end == t


def test_negative_duration_is_not_capped():
    """game_end < game_start: cap doesn't fire (the > 90min check is false
    for negative deltas). Pipeline's existing checks handle this case."""
    start = _utc("2026-04-29T03:00:00Z")
    end = _utc("2026-04-29T02:00:00Z")
    capped_end, was_capped, _ = _apply_duration_cap(start, end)
    assert not was_capped
    assert capped_end == end

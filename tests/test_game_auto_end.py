"""Tests for ``game_auto_end.evaluate_auto_end``.

These pin the server-side auto-end guard (UBA-269 follow-up): scorekeepers
sometimes never press "End Game" (confirmed 2026-06-11: game
eh0CVfQIqwSDSJNVHDZA stayed In Progress all night), so the guard must end
games whose timeline has been idle past a threshold — and must never guess
when timestamps are unparseable.

All tests are pure: no Firebase, no network.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from game_auto_end import evaluate_auto_end  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

NOW = datetime(2026, 6, 11, 8, 0, 0, tzinfo=timezone.utc)
IDLE = timedelta(minutes=20)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', '') + 'Z'


def _event(*, minutes_ago: float, action: str = 'score_added',
           team: str | None = 'left', period: str | None = '1st',
           payload: dict | None = None, timestamp: str | None = None) -> dict:
    return {
        'timestamp': timestamp if timestamp is not None else _iso(NOW - timedelta(minutes=minutes_ago)),
        'actionType': action,
        'period': period,
        'team': team,
        'payload': payload or {},
    }


def _game(*, ended_at=None, created_at=_iso(NOW - timedelta(hours=3)),
          logs=None, final_period=None) -> dict:
    game = {
        'createdAt': created_at,
        'endedAt': ended_at,
        'leftTeam': {'displayName': 'Sharks', 'finalScore': 0},
        'rightTeam': {'displayName': 'Jets', 'finalScore': 0},
        'logs': logs if logs is not None else [],
    }
    if final_period is not None:
        game['finalPeriod'] = final_period
    return game


# ---------------------------------------------------------------------------
# Idle in-progress game → auto-end payload
# ---------------------------------------------------------------------------

def test_idle_game_produces_full_payload():
    last_event_age = 45  # minutes
    logs = [
        _event(minutes_ago=120, action='game_started', team=None, period='1st'),
        _event(minutes_ago=100, action='score_added', team='left', period='1st',
               payload={'newScore': 10, 'points': 2, 'oldScore': 8}),
        _event(minutes_ago=90, action='foul_added', team='right', period='1st',
               payload={'newFouls': 3}),
        _event(minutes_ago=60, action='score_added', team='right', period='2nd',
               payload={'newScore': 41, 'points': 3, 'oldScore': 38}),
        _event(minutes_ago=50, action='foul_added', team='left', period='2nd',
               payload={'newFouls': 8}),
        _event(minutes_ago=last_event_age, action='score_added', team='left', period='2nd',
               payload={'newScore': 54, 'points': 1, 'oldScore': 53}),
    ]
    game = _game(logs=logs)

    update = evaluate_auto_end(game, NOW, IDLE, jetson_id='jetson-1')

    assert update is not None
    last_activity = NOW - timedelta(minutes=last_event_age)
    assert update['endedAt'] == _iso(last_activity + timedelta(seconds=60))
    assert update['finalPeriod'] == '2nd'
    # Latest score_added per side wins; latest foul_added per side wins.
    assert update['leftTeam.finalScore'] == 54
    assert update['leftTeam.finalFouls'] == 8
    assert update['rightTeam.finalScore'] == 41
    assert update['rightTeam.finalFouls'] == 3
    assert update['autoEnded'] is True
    audit = update['autoEndAudit']
    assert audit['by'] == 'jetson-auto-end-guard'
    assert audit['jetsonId'] == 'jetson-1'
    assert audit['firedAt'] == _iso(NOW)
    assert audit['lastEventAt'] == _iso(last_activity)
    assert audit['idleMinutes'] == last_event_age


def test_team_maps_updated_via_dotted_keys_only():
    """The whole leftTeam/rightTeam maps must never be replaced wholesale."""
    game = _game(logs=[_event(minutes_ago=30)])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert 'leftTeam' not in update
    assert 'rightTeam' not in update
    assert {'leftTeam.finalScore', 'leftTeam.finalFouls',
            'rightTeam.finalScore', 'rightTeam.finalFouls'} <= set(update)


def test_ended_at_iso_format_has_milliseconds_and_z():
    game = _game(logs=[_event(minutes_ago=30)])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    # e.g. '2026-06-11T07:31:00.000Z'
    assert update['endedAt'].endswith('Z')
    assert '.' in update['endedAt']
    assert len(update['endedAt'].rsplit('.', 1)[1]) == 4  # 'mmmZ'


# ---------------------------------------------------------------------------
# Skip conditions → None
# ---------------------------------------------------------------------------

def test_recent_activity_is_not_ended():
    game = _game(logs=[_event(minutes_ago=5)])
    assert evaluate_auto_end(game, NOW, IDLE) is None


def test_activity_just_under_threshold_is_not_ended():
    game = _game(logs=[_event(minutes_ago=19.5)])
    assert evaluate_auto_end(game, NOW, IDLE) is None


def test_already_ended_game_is_skipped():
    game = _game(ended_at=_iso(NOW - timedelta(hours=1)),
                 logs=[_event(minutes_ago=600)])
    assert evaluate_auto_end(game, NOW, IDLE) is None


def test_no_parseable_timestamps_returns_none():
    """Never guess: garbage log timestamps AND garbage createdAt → None."""
    game = _game(created_at='not-a-date',
                 logs=[_event(minutes_ago=0, timestamp='also-not-a-date'),
                       _event(minutes_ago=0, timestamp=None)])
    game['logs'][1]['timestamp'] = None
    assert evaluate_auto_end(game, NOW, IDLE) is None


def test_missing_created_at_and_no_logs_returns_none():
    game = _game(created_at=None, logs=[])
    assert evaluate_auto_end(game, NOW, IDLE) is None


# ---------------------------------------------------------------------------
# Fallbacks
# ---------------------------------------------------------------------------

def test_no_logs_falls_back_to_created_at():
    created = NOW - timedelta(hours=3)
    game = _game(created_at=_iso(created), logs=[])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['endedAt'] == _iso(created + timedelta(seconds=60))
    assert update['autoEndAudit']['lastEventAt'] == _iso(created)


def test_unparseable_log_timestamps_fall_back_to_created_at():
    created = NOW - timedelta(hours=2)
    game = _game(created_at=_iso(created),
                 logs=[_event(minutes_ago=0, timestamp='garbage')])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['autoEndAudit']['lastEventAt'] == _iso(created)


def test_scores_and_fouls_default_to_zero_when_no_events():
    game = _game(logs=[_event(minutes_ago=40, action='timer_started',
                              team=None, period='1st')])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['leftTeam.finalScore'] == 0
    assert update['leftTeam.finalFouls'] == 0
    assert update['rightTeam.finalScore'] == 0
    assert update['rightTeam.finalFouls'] == 0


def test_score_event_missing_payload_fields_defaults_to_zero():
    game = _game(logs=[
        _event(minutes_ago=50, action='score_added', team='left', payload={}),
        _event(minutes_ago=45, action='score_added', team='right',
               payload={'points': 2}),  # no newScore
        _event(minutes_ago=40, action='foul_added', team='left', payload=None),
    ])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['leftTeam.finalScore'] == 0
    assert update['rightTeam.finalScore'] == 0
    assert update['leftTeam.finalFouls'] == 0


def test_final_period_falls_back_to_game_field_then_2nd():
    # Events without a period → use game.finalPeriod
    game = _game(final_period='1st',
                 logs=[_event(minutes_ago=40, period=None)])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['finalPeriod'] == '1st'

    # No events with a period and no game.finalPeriod → '2nd'
    game = _game(logs=[_event(minutes_ago=40, period=None)])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['finalPeriod'] == '2nd'


def test_final_period_comes_from_latest_event_with_period():
    game = _game(logs=[
        _event(minutes_ago=90, period='1st'),
        _event(minutes_ago=60, period='2nd'),
        _event(minutes_ago=40, action='timer_started', team=None, period=None),
    ])
    update = evaluate_auto_end(game, NOW, IDLE)
    assert update is not None
    assert update['finalPeriod'] == '2nd'


def test_out_of_order_logs_use_max_timestamp_for_idle():
    """last_activity is the max parseable timestamp, not the last array entry."""
    game = _game(logs=[
        _event(minutes_ago=5),    # recent event first in array
        _event(minutes_ago=90),   # stale event last
    ])
    assert evaluate_auto_end(game, NOW, IDLE) is None

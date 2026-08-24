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

import game_auto_end  # noqa: E402
from game_auto_end import (  # noqa: E402
    AutoEndGuard,
    evaluate_auto_end,
    _has_active_recording,
)


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
    # UBA-306: must mark completed so the client stops treating it as active
    # (getUnendedGame gate) and the TV Display tears down.
    assert update['status'] == 'completed'
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


# ---------------------------------------------------------------------------
# _sweep write-race coverage (UBA-304)
#
# _sweep() must never clobber a real final score/period when a game is ended
# (scorekeeper "End Game" or UBA-269 auto-end) in the multi-minute window
# between the sweep's read and its write. The fix wraps the write in a
# Firestore transaction that re-reads endedAt. These tests stay pure by
# substituting a direct-call runner for the real transactional decorator.
# ---------------------------------------------------------------------------

class _FakeSnapshot:
    def __init__(self, doc_id, data, reference):
        self.id = doc_id
        self._data = data
        self.reference = reference

    def to_dict(self):
        return dict(self._data)


class _FakeDocRef:
    """A Firestore doc reference whose committed state can diverge from the
    stale snapshot the sweep first streamed (simulating a concurrent write)."""

    def __init__(self, doc_id, state):
        self.id = doc_id
        self._state = state
        self.applied = []  # updates the guard actually wrote

    def get(self, transaction=None):
        return _FakeSnapshot(self.id, dict(self._state), self)

    def update(self, fields):
        self.applied.append(fields)
        self._state.update(fields)


class _FakeTransaction:
    def __init__(self):
        self.writes = []

    def update(self, ref, fields):
        self.writes.append((ref, fields))
        ref.update(fields)


class _FakeQuery:
    def __init__(self, docs):
        self._docs = docs

    def where(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def stream(self):
        return list(self._docs)


class _FakeDb:
    def __init__(self, docs):
        self._docs = docs
        self.transactions = []

    def collection(self, name):
        return _FakeQuery(self._docs)

    def transaction(self):
        txn = _FakeTransaction()
        self.transactions.append(txn)
        return txn


class _FakeFirebaseService:
    def __init__(self, docs, sessions=None):
        self.db = _FakeDb(docs)
        # _sweep queries recording sessions for the UBA-305 liveness gate. Serve
        # them here so the sweep tests exercise the real path rather than
        # silently landing in the fail-open `except` branch.
        self._sessions = sessions or []

    def get_recording_sessions(self, jetson_id=None, limit=100):
        return list(self._sessions)


def _direct_runner(transaction, fn):
    """Stand-in for firebase's @transactional: just run the body once."""
    return fn(transaction)


def _make_guard(svc):
    return AutoEndGuard(svc, jetson_id='jetson-1')


# --- transaction body in isolation ---------------------------------------

def test_transaction_body_skips_when_ended_concurrently():
    """The core race: endedAt is already set when the transaction re-reads →
    the guard must return None and write nothing (real score preserved)."""
    fresh = _game(ended_at=_iso(NOW - timedelta(minutes=1)),
                  logs=[_event(minutes_ago=45)])
    fresh['leftTeam'] = {'displayName': 'Sharks', 'finalScore': 88}
    doc_ref = _FakeDocRef('g1', fresh)
    txn = _FakeTransaction()

    guard = _make_guard(_FakeFirebaseService([]))
    result = guard._end_game_in_transaction(txn, doc_ref, NOW)

    assert result is None
    assert txn.writes == []
    assert doc_ref.applied == []
    assert doc_ref._state['leftTeam']['finalScore'] == 88  # untouched


def test_transaction_body_writes_when_still_idle():
    state = _game(logs=[_event(minutes_ago=45, team='left', period='2nd',
                               payload={'newScore': 54})])
    doc_ref = _FakeDocRef('g1', state)
    txn = _FakeTransaction()

    guard = _make_guard(_FakeFirebaseService([]))
    result = guard._end_game_in_transaction(txn, doc_ref, NOW)

    assert result is not None
    update, game = result
    assert update['autoEnded'] is True
    assert len(txn.writes) == 1
    ref, fields = txn.writes[0]
    assert ref is doc_ref and fields is update
    assert doc_ref._state['endedAt']  # now ended


# --- full _sweep path -----------------------------------------------------

def test_sweep_does_not_clobber_score_ended_during_window(monkeypatch):
    monkeypatch.setattr(game_auto_end, '_run_transaction', _direct_runner)

    # Stale snapshot the sweep streams: still idle, not ended → passes pre-filter.
    stale = _game(logs=[_event(minutes_ago=45)])
    # Committed state: a real "End Game" already set endedAt + the true score.
    fresh = _game(ended_at=_iso(NOW - timedelta(minutes=1)),
                  logs=[_event(minutes_ago=45)])
    fresh['leftTeam'] = {'displayName': 'Sharks', 'finalScore': 88}
    doc_ref = _FakeDocRef('g1', fresh)
    streamed = _FakeSnapshot('g1', stale, doc_ref)

    svc = _FakeFirebaseService([streamed])
    _make_guard(svc)._sweep()

    assert doc_ref.applied == []                      # guard never wrote
    assert svc.db.transactions[0].writes == []        # transaction wrote nothing
    assert doc_ref._state['leftTeam']['finalScore'] == 88  # real score intact
    assert doc_ref._state['endedAt'] == fresh['endedAt']   # real endedAt intact


def test_sweep_ends_idle_game_via_transaction(monkeypatch):
    monkeypatch.setattr(game_auto_end, '_run_transaction', _direct_runner)

    state = _game(logs=[_event(minutes_ago=45, team='left', period='2nd',
                               payload={'newScore': 54})])
    doc_ref = _FakeDocRef('g1', state)
    streamed = _FakeSnapshot('g1', dict(state), doc_ref)

    svc = _FakeFirebaseService([streamed])
    _make_guard(svc)._sweep()

    assert len(doc_ref.applied) == 1
    written = doc_ref.applied[0]
    assert written['autoEnded'] is True
    assert written['leftTeam.finalScore'] == 54
    assert doc_ref._state['endedAt']  # game is now ended


def test_sweep_skips_already_ended_without_opening_transaction(monkeypatch):
    monkeypatch.setattr(game_auto_end, '_run_transaction', _direct_runner)

    ended = _game(ended_at=_iso(NOW - timedelta(hours=1)),
                  logs=[_event(minutes_ago=45)])
    doc_ref = _FakeDocRef('g1', ended)
    streamed = _FakeSnapshot('g1', dict(ended), doc_ref)

    svc = _FakeFirebaseService([streamed])
    _make_guard(svc)._sweep()

    assert svc.db.transactions == []  # pre-filter short-circuits, no txn opened
    assert doc_ref.applied == []
# Liveness gate (UBA-305): never auto-end while a camera is still recording
# ---------------------------------------------------------------------------

MAX_SESSION_AGE = timedelta(hours=6)


def _session(*, status: str = 'recording', started_minutes_ago: float = 60,
             ended_at=None) -> dict:
    return {
        'status': status,
        'startedAt': _iso(NOW - timedelta(minutes=started_minutes_ago)),
        'endedAt': ended_at,
    }


def test_active_recording_blocks_when_started_before_last_activity():
    # Camera rolling since before the last logged event → still filming.
    last_activity = NOW - timedelta(minutes=40)
    sessions = [_session(status='recording', started_minutes_ago=120)]
    assert _has_active_recording(sessions, last_activity, NOW, MAX_SESSION_AGE) is True


def test_recording_started_after_last_activity_does_not_block():
    # Open session belongs to a *later* game (started after this game's silence).
    last_activity = NOW - timedelta(minutes=90)
    sessions = [_session(status='recording', started_minutes_ago=30)]
    assert _has_active_recording(sessions, last_activity, NOW, MAX_SESSION_AGE) is False


def test_only_finished_sessions_do_not_block():
    last_activity = NOW - timedelta(minutes=40)
    sessions = [
        _session(status='stopped', started_minutes_ago=120),
        _session(status='cancelled', started_minutes_ago=120),
        _session(status='failed', started_minutes_ago=120),
        _session(status='uploaded', started_minutes_ago=120),
    ]
    assert _has_active_recording(sessions, last_activity, NOW, MAX_SESSION_AGE) is False


def test_ghost_recording_older_than_max_age_does_not_block():
    # 'recording' with no endedAt for >6h → crashed camera, must not block forever.
    last_activity = NOW - timedelta(minutes=40)
    sessions = [_session(status='recording', started_minutes_ago=7 * 60)]
    assert _has_active_recording(sessions, last_activity, NOW, MAX_SESSION_AGE) is False


def test_session_with_ended_at_does_not_block_even_if_status_recording():
    # Stale status but endedAt set → recording is done.
    last_activity = NOW - timedelta(minutes=40)
    sessions = [_session(status='recording', started_minutes_ago=120,
                         ended_at=_iso(NOW - timedelta(minutes=10)))]
    assert _has_active_recording(sessions, last_activity, NOW, MAX_SESSION_AGE) is False


def test_empty_and_malformed_sessions_do_not_block():
    last_activity = NOW - timedelta(minutes=40)
    assert _has_active_recording([], last_activity, NOW, MAX_SESSION_AGE) is False
    assert _has_active_recording(None, last_activity, NOW, MAX_SESSION_AGE) is False
    bad = ['not-a-dict', {}, {'status': 'recording', 'startedAt': 'garbage'}]
    assert _has_active_recording(bad, last_activity, NOW, MAX_SESSION_AGE) is False


def test_evaluate_auto_end_skips_when_active_recording():
    # Idle well past threshold, but a camera is still rolling → no auto-end.
    game = _game(logs=[_event(minutes_ago=90)])
    assert evaluate_auto_end(game, NOW, IDLE, active_recording=True) is None
    # Same game with no active recording still ends.
    assert evaluate_auto_end(game, NOW, IDLE, active_recording=False) is not None

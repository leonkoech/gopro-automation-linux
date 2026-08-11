"""Server-side auto-end guard for basketball games left "In Progress".

Scorekeepers sometimes never press "End Game", leaving ``endedAt=None`` on the
``basketball-games`` doc. The frontend auto-end (UBA-269) is client-side only
and gated on a schedule slot + clock==0, so it frequently can't fire (e.g. tab
closed, no schedule slot). An un-ended game has no extraction window, so the
video pipeline skips it entirely.

This module runs inside the Jetson Flask service:

* :func:`evaluate_auto_end` — pure decision function (no Firebase), unit-tested.
* :class:`AutoEndGuard` — daemon thread that periodically sweeps recent games
  and ends those whose timeline has been idle past a threshold.

Exactly one Jetson fires by default (leader-gated via ``AUTO_END_LEADER``).
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger('gopro.game_auto_end')

BASKETBALL_GAMES_COLLECTION = 'basketball-games'

# How long after the last timeline event the game is considered to have ended.
ENDED_AT_GRACE_SECONDS = 60

# Recovery-net threshold. The client (gopro-automation-wb, UBA-270) is the
# authoritative transition — it ends the prior game before starting the next.
# This guard only catches games the client can't: the last game of a session or
# a crash with no subsequent start. logs[] is a sparse discrete-event array with
# no heartbeat, so a live game routinely goes 20+ min silent (halftime + a
# stoppage). 45 min keeps the guard clear of normal in-game silence; the
# liveness gate below is what actually prevents ending a live game (UBA-305).
DEFAULT_IDLE_MINUTES = 45
DEFAULT_POLL_SECONDS = 300
DEFAULT_LOOKBACK_HOURS = 36
DEFAULT_LEADER = 'jetson-1'

# A 'recording' session with no endedAt this old is treated as a ghost (camera
# crashed without a stop) and ignored for liveness, so it can't block auto-end
# forever. Mirrors the ghost-session handling in video_processing.py.
DEFAULT_MAX_SESSION_AGE_HOURS = 6


def _parse_iso(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 string (with optional trailing 'Z') to an aware UTC datetime."""
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_z(dt: datetime) -> str:
    """Format an aware datetime as ISO string with milliseconds + 'Z' (UTC)."""
    return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', '') + 'Z'


def _sorted_events(logs: List[Dict[str, Any]]) -> List[Tuple[datetime, Dict[str, Any]]]:
    """Return (timestamp, event) pairs for events with parseable timestamps, oldest first."""
    parsed = []
    for index, event in enumerate(logs or []):
        if not isinstance(event, dict):
            continue
        ts = _parse_iso(event.get('timestamp'))
        if ts is not None:
            parsed.append((ts, index, event))
    parsed.sort(key=lambda item: (item[0], item[1]))
    return [(ts, event) for ts, _, event in parsed]


def _latest_payload_value(events: List[Tuple[datetime, Dict[str, Any]]],
                          action_type: str, team: str, key: str) -> int:
    """Value of ``payload[key]`` from the latest ``action_type`` event for ``team`` (0 if none)."""
    for _, event in reversed(events):
        if event.get('actionType') != action_type or event.get('team') != team:
            continue
        payload = event.get('payload')
        if isinstance(payload, dict) and isinstance(payload.get(key), (int, float)):
            return int(payload[key])
    return 0


def _latest_period(events: List[Tuple[datetime, Dict[str, Any]]], game: Dict[str, Any]) -> str:
    """Period of the latest event that has one; fallback to game finalPeriod or '2nd'."""
    for _, event in reversed(events):
        period = event.get('period')
        if period:
            return period
    return game.get('finalPeriod') or '2nd'


def _game_last_activity(game: Dict[str, Any],
                        events: List[Tuple[datetime, Dict[str, Any]]]) -> Optional[datetime]:
    """Timestamp of the newest log event, or ``createdAt`` when logs are empty.

    ``None`` when nothing is parseable — callers must never guess in that case.
    """
    if events:
        return events[-1][0]
    return _parse_iso(game.get('createdAt'))


def _has_active_recording(sessions: List[Dict[str, Any]], last_activity: datetime,
                          now: datetime, max_session_age: timedelta) -> bool:
    """True if a camera is still rolling over this game's window (UBA-305).

    ``logs[]`` silence can't distinguish halftime from a finished game, so we
    gate on a real liveness signal: a still-open recording session
    (``status=='recording'`` and ``endedAt is None``) that began at/before the
    game's last logged event is, by definition, still filming this court right
    now — so the game is not over. Sessions that started *after* ``last_activity``
    belong to a later game and must not block. Sessions open longer than
    ``max_session_age`` are treated as ghosts (crashed without a stop) and
    ignored, so they can't block auto-end forever.
    """
    grace = timedelta(seconds=ENDED_AT_GRACE_SECONDS)
    for session in sessions or []:
        if not isinstance(session, dict):
            continue
        if session.get('status') != 'recording' or session.get('endedAt'):
            continue
        started = _parse_iso(session.get('startedAt'))
        if started is None:
            continue
        if now - started > max_session_age:
            continue
        if started <= last_activity + grace:
            return True
    return False


def evaluate_auto_end(game: Dict[str, Any], now: datetime,
                      idle_threshold: timedelta,
                      jetson_id: str = 'unknown',
                      active_recording: bool = False) -> Optional[Dict[str, Any]]:
    """Decide whether an in-progress game should be auto-ended.

    Returns None if the game should NOT be auto-ended; otherwise returns the
    Firestore update payload (dotted-field keys for team maps so the rest of
    each team map is untouched).

    ``active_recording`` short-circuits to None: never auto-end while a camera
    is still recording this game (UBA-305). Kept as a bool param so this
    decision function stays pure/Firebase-free — the caller (``_sweep``)
    computes it from ``_has_active_recording``.
    """
    if game.get('endedAt'):
        return None

    events = _sorted_events(game.get('logs') or [])

    last_activity = _game_last_activity(game, events)

    if last_activity is None:
        # No parseable timestamps anywhere — never guess.
        return None

    # Never auto-end while a camera is still recording this game (UBA-305).
    if active_recording:
        return None

    if now - last_activity < idle_threshold:
        return None

    idle_minutes = int((now - last_activity).total_seconds() // 60)

    update: Dict[str, Any] = {
        'endedAt': _iso_z(last_activity + timedelta(seconds=ENDED_AT_GRACE_SECONDS)),
        'finalPeriod': _latest_period(events, game),
        'autoEnded': True,
        'autoEndAudit': {
            'by': 'jetson-auto-end-guard',
            'jetsonId': jetson_id,
            'firedAt': _iso_z(now),
            'lastEventAt': _iso_z(last_activity),
            'idleMinutes': idle_minutes,
        },
    }

    for side in ('left', 'right'):
        update[f'{side}Team.finalScore'] = _latest_payload_value(events, 'score_added', side, 'newScore')
        update[f'{side}Team.finalFouls'] = _latest_payload_value(events, 'foul_added', side, 'newFouls')

    return update


class AutoEndGuard:
    """Daemon thread that auto-ends idle in-progress games.

    Only the leader Jetson (``AUTO_END_LEADER``, default 'jetson-1') runs the
    loop, so exactly one device fires fleet-wide by default.
    """

    def __init__(self, firebase_service, jetson_id: str):
        self.firebase_service = firebase_service
        self.jetson_id = jetson_id
        self.idle_threshold = timedelta(
            minutes=int(os.getenv('AUTO_END_IDLE_MINUTES', str(DEFAULT_IDLE_MINUTES))))
        self.poll_seconds = int(os.getenv('AUTO_END_POLL_SECONDS', str(DEFAULT_POLL_SECONDS)))
        self.lookback = timedelta(
            hours=int(os.getenv('AUTO_END_LOOKBACK_HOURS', str(DEFAULT_LOOKBACK_HOURS))))
        self.max_session_age = timedelta(
            hours=int(os.getenv('AUTO_END_MAX_SESSION_AGE_HOURS',
                                str(DEFAULT_MAX_SESSION_AGE_HOURS))))
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Spawn the polling daemon thread (no-op when disabled or not leader)."""
        enabled = os.getenv('AUTO_END_ENABLED', 'true').lower() == 'true'
        leader = os.getenv('AUTO_END_LEADER', DEFAULT_LEADER)
        if not enabled:
            logger.info("Auto-end guard disabled via AUTO_END_ENABLED — not starting")
            return
        if self.jetson_id != leader:
            logger.info(
                f"Auto-end guard not starting on {self.jetson_id} (leader is {leader})")
            return

        self._thread = threading.Thread(
            target=self._run_loop, name='game-auto-end-guard', daemon=True)
        self._thread.start()
        logger.info(
            f"Auto-end guard started on {self.jetson_id} "
            f"(idle={int(self.idle_threshold.total_seconds() // 60)}m, "
            f"poll={self.poll_seconds}s, lookback={int(self.lookback.total_seconds() // 3600)}h)")

    def _run_loop(self) -> None:
        while True:
            try:
                self._sweep()
            except Exception as e:
                logger.error(f"Auto-end sweep failed: {e}", exc_info=True)
            time.sleep(self.poll_seconds)

    def _sweep(self) -> None:
        now = datetime.now(timezone.utc)
        cutoff_iso = _iso_z(now - self.lookback)

        # Fleet-wide (jetson_id=None): the leader must see every court's cameras
        # to know whether a game is still being filmed. A query failure must not
        # abort the sweep — fail open to the pre-liveness behaviour (an empty
        # list simply skips the gate) rather than crash.
        try:
            active_sessions = self.firebase_service.get_recording_sessions(
                jetson_id=None, limit=100)
        except Exception as e:
            logger.warning(f"Auto-end: recording-session query failed, fail-open: {e}")
            active_sessions = []

        # Firestore can't reliably query endedAt==None across docs missing the
        # field, so pull recent games by createdAt and filter in Python.
        games_ref = self.firebase_service.db.collection(BASKETBALL_GAMES_COLLECTION)
        query = (games_ref
                 .where('createdAt', '>=', cutoff_iso)
                 .order_by('createdAt', direction='DESCENDING')
                 .limit(50))

        for doc in query.stream():
            try:
                game = doc.to_dict() or {}
                if game.get('endedAt'):
                    continue

                # Liveness gate (UBA-305): never end a game whose camera is
                # still rolling (e.g. a live game silent at halftime).
                events = _sorted_events(game.get('logs') or [])
                last_activity = _game_last_activity(game, events)
                if last_activity is not None and _has_active_recording(
                        active_sessions, last_activity, now, self.max_session_age):
                    logger.info(
                        f"Skip auto-end {doc.id}: recording still active")
                    continue

                update = evaluate_auto_end(
                    game, now, self.idle_threshold, jetson_id=self.jetson_id,
                    active_recording=False)
                if not update:
                    continue
                doc.reference.update(update)
                left = (game.get('leftTeam') or {}).get('displayName', '?')
                right = (game.get('rightTeam') or {}).get('displayName', '?')
                logger.info(
                    f"AUTO-ENDED game {doc.id} ({left} vs {right}): timeline idle "
                    f"{update['autoEndAudit']['idleMinutes']}m, endedAt set to "
                    f"{update['endedAt']} "
                    f"(scores {update['leftTeam.finalScore']}-{update['rightTeam.finalScore']}, "
                    f"period {update['finalPeriod']})")
            except Exception as e:
                logger.error(f"Auto-end failed for game {doc.id}: {e}", exc_info=True)

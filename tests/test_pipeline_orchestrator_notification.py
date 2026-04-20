"""Tests for the pipeline orchestrator's annotator-notification wiring.

The high-risk failure mode is silent: the in-memory pipeline is evicted 5
minutes after completion, but AWS Batch transcoding can take longer, so by
the time the batch poller calls ``_send_annotator_notification`` the pipeline
state may be gone. These tests pin down the snapshot + batch-result path
that survives that eviction.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline_orchestrator import _apply_batch_result_to_games  # noqa: E402


# --- _apply_batch_result_to_games (pure function) ------------------------------


def test_batch_result_flips_batch_submitted_to_completed_when_all_succeed():
    games = {
        'g1': {
            'status': 'batch_submitted',
            'batch_jobs': ['job-a', 'job-b'],
            'team_a_name': 'Alpha',
            'team_b_name': 'Bravo',
        }
    }
    batch_result = {
        'completed_jobs': [{'job_id': 'job-a'}, {'job_id': 'job-b'}],
        'failed_jobs': [],
    }
    out = _apply_batch_result_to_games(games, batch_result)
    assert out['g1']['status'] == 'completed'


def test_batch_result_flips_to_failed_when_any_job_failed():
    games = {
        'g1': {
            'status': 'batch_submitted',
            'batch_jobs': ['job-a', 'job-b'],
        }
    }
    batch_result = {
        'completed_jobs': [{'job_id': 'job-a'}],
        'failed_jobs': [{'job_id': 'job-b'}],
    }
    out = _apply_batch_result_to_games(games, batch_result)
    assert out['g1']['status'] == 'failed'


def test_batch_result_leaves_status_unchanged_for_non_batch_submitted_games():
    """Games that already have status='completed' (SKIP-GAME path) or 'failed'
    must not be re-flipped by the batch poller."""
    games = {
        'skipped': {'status': 'completed', 'batch_jobs': []},
        'failed_early': {'status': 'failed', 'batch_jobs': []},
    }
    batch_result = {'completed_jobs': [], 'failed_jobs': []}
    out = _apply_batch_result_to_games(games, batch_result)
    assert out['skipped']['status'] == 'completed'
    assert out['failed_early']['status'] == 'failed'


def test_batch_result_leaves_status_unchanged_when_jobs_still_pending():
    """If a batch_submitted game has jobs that didn't appear in either list,
    we don't know the outcome — leave the game's status alone."""
    games = {
        'g1': {
            'status': 'batch_submitted',
            'batch_jobs': ['job-a', 'job-missing'],
        }
    }
    batch_result = {
        'completed_jobs': [{'job_id': 'job-a'}],
        'failed_jobs': [],
    }
    out = _apply_batch_result_to_games(games, batch_result)
    assert out['g1']['status'] == 'batch_submitted'


def test_batch_result_returns_fresh_dict_not_mutating_input():
    games = {
        'g1': {'status': 'batch_submitted', 'batch_jobs': ['job-a']},
    }
    batch_result = {
        'completed_jobs': [{'job_id': 'job-a'}],
        'failed_jobs': [],
    }
    _apply_batch_result_to_games(games, batch_result)
    # Original input untouched.
    assert games['g1']['status'] == 'batch_submitted'


# --- PipelineOrchestrator._send_annotator_notification -------------------------


def _make_orchestrator():
    """Build a PipelineOrchestrator with all external collaborators stubbed.

    The notification method only uses ``self._pipelines``, ``self._lock`` and
    imports email_notifier — no Firebase / Uball / S3 calls.
    """
    from pipeline_orchestrator import PipelineOrchestrator

    class _Stub:
        pass

    return PipelineOrchestrator(
        jetson_id='jetson-2',
        firebase_service=_Stub(),
        upload_service=_Stub(),
        video_processor=_Stub(),
        uball_client=_Stub(),
    )


def test_notification_uses_snapshot_when_pipeline_evicted():
    """Regression: if the pipeline was evicted from memory (5-min retention
    elapsed while AWS Batch ran), the snapshot path must still fire the
    email. Before the fix, the method read from ``self._pipelines`` and
    silently returned when it was None."""
    orch = _make_orchestrator()
    # Intentionally NOT populating self._pipelines — simulating eviction.

    snapshot = {
        'games': {
            'game-1': {
                'game_number': 1,
                'team_a_name': '305 Turnovers',
                'team_b_name': '?',
                'uball_game_id': 'uuid-1',
                'status': 'batch_submitted',
                'batch_jobs': ['job-a', 'job-b'],
            },
            # SKIP-GAME'd siblings from the same pipeline — already completed.
            'game-2': {
                'game_number': 2,
                'team_a_name': '4C',
                'team_b_name': 'Los Sazoneros',
                'uball_game_id': 'uuid-2',
                'status': 'completed',
                'batch_jobs': [],
            },
        },
        'jetson_name': 'jetson-2',
        'recording_start': '2026-04-18T00:30:02.970941Z',
    }
    batch_result = {
        'completed_jobs': [{'job_id': 'job-a'}, {'job_id': 'job-b'}],
        'failed_jobs': [],
    }

    with patch('email_notifier.send_games_ready_email', return_value=True) as mock_send:
        orch._send_annotator_notification(
            'pipeline-abc', snapshot=snapshot, batch_result=batch_result
        )

    assert mock_send.call_count == 1
    kwargs = mock_send.call_args.kwargs
    assert kwargs['jetson_name'] == 'jetson-2'
    assert kwargs['recording_date'] == '2026-04-18'
    ready_game_ids = [g.uball_game_id for g in kwargs['ready_games']]
    # Both games ready: game-1 flipped via batch_result, game-2 was already completed.
    assert sorted(ready_game_ids) == ['uuid-1', 'uuid-2']
    assert kwargs['failed_games'] == []


def test_notification_reports_failed_games_when_batch_fails():
    orch = _make_orchestrator()
    snapshot = {
        'games': {
            'game-1': {
                'game_number': 1,
                'team_a_name': 'A', 'team_b_name': 'B',
                'uball_game_id': 'uuid-1',
                'status': 'batch_submitted',
                'batch_jobs': ['job-a'],
                'error': None,
            },
        },
        'jetson_name': 'jetson-1',
        'recording_start': '2026-04-18T00:30:00Z',
    }
    batch_result = {
        'completed_jobs': [],
        'failed_jobs': [{'job_id': 'job-a'}],
    }

    with patch('email_notifier.send_games_ready_email', return_value=True) as mock_send:
        orch._send_annotator_notification(
            'p', snapshot=snapshot, batch_result=batch_result
        )

    kwargs = mock_send.call_args.kwargs
    assert kwargs['ready_games'] == []
    assert len(kwargs['failed_games']) == 1
    assert kwargs['failed_games'][0].uball_game_id == 'uuid-1'


def test_notification_no_op_when_no_snapshot_and_pipeline_gone():
    """When the batch-poller path is taken but no snapshot was captured (the
    pipeline was already gone when the poller thread started), we shouldn't
    attempt to send an empty email — we should log and return."""
    orch = _make_orchestrator()
    with patch('email_notifier.send_games_ready_email') as mock_send:
        orch._send_annotator_notification(
            'pipeline-missing', snapshot=None, batch_result=None
        )
    mock_send.assert_not_called()


def test_notification_synchronous_path_uses_in_memory_state():
    """When snapshot is None (no-batch completion) the method must still work
    against self._pipelines — this is the existing synchronous code path."""
    orch = _make_orchestrator()
    orch._pipelines['p-sync'] = {
        'jetson_name': 'jetson-2',
        'recording_start': '2026-04-18T00:30:00Z',
        'games': {
            'g1': {
                'game_number': 1,
                'team_a_name': 'Home', 'team_b_name': 'Away',
                'uball_game_id': 'uuid-sync',
                'status': 'completed',
            },
        },
    }
    with patch('email_notifier.send_games_ready_email', return_value=True) as mock_send:
        orch._send_annotator_notification('p-sync')
    kwargs = mock_send.call_args.kwargs
    assert [g.uball_game_id for g in kwargs['ready_games']] == ['uuid-sync']

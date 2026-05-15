"""Unit tests for the CV_PLAYS_ENABLED feature gate in plays_sync.

The flag controls whether CV-emitted events (payload.source == "cv") flow
through to the annotation tool as Supabase plays.  Defaults to off until
the V1 far-angle model retrain lands.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from plays_sync import _cv_plays_enabled, create_plays_from_firebase_logs  # noqa: E402


# ============================================================================
# _cv_plays_enabled — env-var parsing
# ============================================================================
class TestCvPlaysEnabledFlag:
    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", "on", "  true  "])
    def test_truthy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("CV_PLAYS_ENABLED", value)
        assert _cv_plays_enabled() is True

    @pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "anything-else"])
    def test_falsy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("CV_PLAYS_ENABLED", value)
        assert _cv_plays_enabled() is False

    def test_unset_defaults_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CV_PLAYS_ENABLED", raising=False)
        assert _cv_plays_enabled() is False


# ============================================================================
# create_plays_from_firebase_logs — CV filtering integration
# ============================================================================
def _make_game(logs: list[dict]) -> dict:
    """Minimal Firebase basketball-games doc with the given logs."""
    return {
        "createdAt": "2026-05-15T08:00:00Z",
        "leftTeam":  {"name": "Team Left"},
        "rightTeam": {"name": "Team Right"},
        "logs": logs,
    }


def _operator_make(t_seconds: int = 30) -> dict:
    """Operator-entered (no `payload.source`) score_added log."""
    ts = f"2026-05-15T08:00:{t_seconds:02d}Z"
    return {
        "actionType": "score_added",
        "timestamp":  ts,
        "team":       "left",
        "payload":    {"points": 2},
    }


def _cv_make(t_seconds: int = 30) -> dict:
    """CV-emitted score_added log (payload.source == 'cv')."""
    ts = f"2026-05-15T08:00:{t_seconds:02d}Z"
    return {
        "actionType": "score_added",
        "timestamp":  ts,
        "team":       "right",
        "payload":    {"points": 2, "source": "cv", "confidence": 0.82},
    }


def _mock_client_no_existing_plays():
    """UballClient stub: empty plays list, create_play succeeds."""
    client = MagicMock()
    client.list_plays.return_value = []
    client.create_play.return_value = {"id": "fake-play-id"}
    return client


class TestCvPlaysFiltering:
    def test_cv_plays_skipped_when_flag_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CV_PLAYS_ENABLED", "false")
        client = _mock_client_no_existing_plays()
        game = _make_game([_operator_make(10), _cv_make(20), _cv_make(40)])

        created = create_plays_from_firebase_logs(client, "uball-game-id", game)

        # Only the 1 operator play, both CV plays filtered out
        assert created == 1
        assert client.create_play.call_count == 1
        # Sanity: the surviving play is the operator one
        play_data = client.create_play.call_args_list[0].args[0]
        assert "(CV)" not in play_data["note"]

    def test_cv_plays_kept_when_flag_on(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CV_PLAYS_ENABLED", "true")
        client = _mock_client_no_existing_plays()
        game = _make_game([_operator_make(10), _cv_make(20), _cv_make(40)])

        created = create_plays_from_firebase_logs(client, "uball-game-id", game)

        # All 3 plays created — 1 operator + 2 CV
        assert created == 3
        assert client.create_play.call_count == 3
        # CV plays carry the (CV) badge in the note
        cv_calls = [
            c for c in client.create_play.call_args_list
            if "(CV)" in c.args[0]["note"]
        ]
        assert len(cv_calls) == 2

    def test_operator_plays_unaffected_by_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Operator events always create plays regardless of CV_PLAYS_ENABLED."""
        for flag_value in ("false", "true"):
            monkeypatch.setenv("CV_PLAYS_ENABLED", flag_value)
            client = _mock_client_no_existing_plays()
            game = _make_game([_operator_make(10), _operator_make(20)])

            created = create_plays_from_firebase_logs(client, "uball-game-id", game)

            assert created == 2, f"flag={flag_value}: expected both operator plays to land"
            assert client.create_play.call_count == 2

    def test_default_unset_filters_cv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With CV_PLAYS_ENABLED unset, CV plays are filtered out (safe default)."""
        monkeypatch.delenv("CV_PLAYS_ENABLED", raising=False)
        client = _mock_client_no_existing_plays()
        game = _make_game([_cv_make(10), _cv_make(20)])

        created = create_plays_from_firebase_logs(client, "uball-game-id", game)

        assert created == 0
        client.create_play.assert_not_called()

    def test_mixed_cv_and_operator_with_flag_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Realistic case: flag off, mix of operator + CV events; only operator survives."""
        monkeypatch.setenv("CV_PLAYS_ENABLED", "false")
        client = _mock_client_no_existing_plays()
        game = _make_game([
            _operator_make(10),
            _cv_make(15),
            _operator_make(30),
            _cv_make(45),
            _operator_make(55),
        ])

        created = create_plays_from_firebase_logs(client, "uball-game-id", game)

        assert created == 3  # only the 3 operator plays
        # Every created play is operator-source (no CV badge)
        for call in client.create_play.call_args_list:
            assert "(CV)" not in call.args[0]["note"]

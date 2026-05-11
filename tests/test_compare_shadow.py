"""Unit tests for `scripts/cv_infra/ops/compare_shadow.py`.

Pure-function coverage on the extraction + matching logic + the
CSV/table writers. No firebase_admin or network — tests build the
Firebase-doc shape in fixtures.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "cv_infra" / "ops"))

import compare_shadow as cs  # noqa: E402


# ---------------------------------------------------------------- fixtures
GAME_START = "2026-04-15T18:00:00Z"


def _ts(seconds_from_start: float) -> str:
    """Build an ISO timestamp seconds_from_start seconds after GAME_START."""
    from datetime import datetime, timedelta
    start = datetime.fromisoformat(GAME_START.replace("Z", "+00:00"))
    return (start + timedelta(seconds=seconds_from_start)).isoformat()


def _op_score(seconds: float, team: str = "left", points: int = 2) -> dict:
    return {
        "actionType": "score_added",
        "timestamp": _ts(seconds),
        "team": team,
        "payload": {"points": points},
    }


def _op_miss(seconds: float, team: str = "left") -> dict:
    return {
        "actionType": "shot_missed",
        "timestamp": _ts(seconds),
        "team": team,
        "payload": {},
    }


def _cv_shot(seconds: float, team: str, outcome: str = "made",
             confidence: float = 0.9) -> dict:
    return {
        "actionType": "cv_shot",
        "timestamp": _ts(seconds),
        "team": team,
        "payload": {"outcome": outcome, "confidence": confidence},
    }


def _game(*, logs=None, cv_logs=None) -> dict:
    return {
        "id": "test-game-1",
        "createdAt": GAME_START,
        "leftTeam":  {"name": "Lefts"},
        "rightTeam": {"name": "Rights"},
        "logs":             logs    or [],
        "cv_logs_staging":  cv_logs or [],
    }


# ============================================================================
# Extraction
# ============================================================================
class TestExtractOperatorShots:
    def test_score_added_becomes_made_shot(self):
        game = _game(logs=[_op_score(12.0, "left", points=3)])
        shots = cs.extract_operator_shots(game)
        assert len(shots) == 1
        assert shots[0].outcome == "made"
        assert shots[0].team == "left"
        assert shots[0].timestamp_seconds == pytest.approx(12.0)

    def test_shot_missed_becomes_missed_shot(self):
        game = _game(logs=[_op_miss(20.0, "right")])
        shots = cs.extract_operator_shots(game)
        assert len(shots) == 1
        assert shots[0].outcome == "missed"
        assert shots[0].team == "right"

    def test_unrelated_actiontypes_ignored(self):
        game = _game(logs=[
            {"actionType": "foul_added", "timestamp": _ts(5.0), "team": "left"},
            {"actionType": "game_started", "timestamp": _ts(0.0), "team": "left"},
            _op_score(10.0),
        ])
        assert len(cs.extract_operator_shots(game)) == 1

    def test_invalid_team_dropped(self):
        game = _game(logs=[
            {"actionType": "score_added", "timestamp": _ts(5.0),
             "team": "unknown", "payload": {"points": 2}},
        ])
        assert cs.extract_operator_shots(game) == []

    def test_unparseable_timestamp_dropped(self):
        game = _game(logs=[
            {"actionType": "score_added", "timestamp": "not-a-date",
             "team": "left", "payload": {"points": 2}},
        ])
        assert cs.extract_operator_shots(game) == []

    def test_results_sorted_by_timestamp(self):
        game = _game(logs=[_op_score(30.0), _op_score(10.0), _op_score(20.0)])
        ts_list = [s.timestamp_seconds for s in cs.extract_operator_shots(game)]
        assert ts_list == [10.0, 20.0, 30.0]


class TestExtractCvShots:
    def test_made_with_confidence(self):
        game = _game(cv_logs=[_cv_shot(12.0, "left", "made", 0.87)])
        shots = cs.extract_cv_shots(game)
        assert len(shots) == 1
        assert shots[0].outcome == "made"
        assert shots[0].confidence == pytest.approx(0.87)

    def test_missed(self):
        game = _game(cv_logs=[_cv_shot(20.0, "right", "missed", 0.62)])
        shots = cs.extract_cv_shots(game)
        assert shots[0].outcome == "missed"

    def test_invalid_outcome_dropped(self):
        game = _game(cv_logs=[_cv_shot(5.0, "left", "undetermined", 0.5)])
        assert cs.extract_cv_shots(game) == []

    def test_reads_alternate_array(self):
        """After Phase 7 cutover, CV shots land in `logs` rather than
        `cv_logs_staging`. The CLI's --cv-source flag handles this."""
        game = _game(logs=[_cv_shot(5.0, "left")])
        assert cs.extract_cv_shots(game, source_array="logs"), \
            "should read from `logs` when source_array overridden"


# ============================================================================
# Matching
# ============================================================================
class TestMatchShots:
    def test_exact_timestamp_match_counts_as_matched(self):
        op = [cs.Shot(timestamp_seconds=10.0, team="left",
                      outcome="made", source="operator")]
        cv = [cs.Shot(timestamp_seconds=10.0, team="left",
                      outcome="made", source="cv", confidence=0.9)]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        assert len(matched) == 1
        assert op_only == [] and cv_only == []

    def test_within_window_matches(self):
        op = [cs.Shot(10.0, "left", "made", "operator")]
        cv = [cs.Shot(13.5, "left", "made", "cv", 0.9)]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        assert len(matched) == 1

    def test_outside_window_does_not_match(self):
        op = [cs.Shot(10.0, "left", "made", "operator")]
        cv = [cs.Shot(16.0, "left", "made", "cv", 0.9)]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        assert matched == []
        assert len(op_only) == 1 and len(cv_only) == 1

    def test_different_team_does_not_match(self):
        op = [cs.Shot(10.0, "left", "made", "operator")]
        cv = [cs.Shot(10.0, "right", "made", "cv", 0.9)]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        assert matched == []
        assert len(op_only) == 1 and len(cv_only) == 1

    def test_disagreement_still_matches(self):
        """Operator says made, CV says missed — that's a useful
        DISAGREEMENT to surface, not a non-match."""
        op = [cs.Shot(10.0, "left", "made", "operator")]
        cv = [cs.Shot(10.5, "left", "missed", "cv", 0.6)]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        assert len(matched) == 1
        assert op_only == [] and cv_only == []
        # Caller decides outcome agreement separately.

    def test_greedy_picks_closest_cv(self):
        op = [cs.Shot(10.0, "left", "made", "operator")]
        cv = [
            cs.Shot(8.0,  "left", "made", "cv", 0.9),
            cs.Shot(10.2, "left", "made", "cv", 0.95),   # closer
            cs.Shot(13.0, "left", "made", "cv", 0.85),
        ]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        assert len(matched) == 1
        op_match, cv_match = matched[0]
        assert cv_match.timestamp_seconds == pytest.approx(10.2)
        # The other 2 fall through as cv_only.
        assert len(cv_only) == 2

    def test_each_cv_can_only_match_once(self):
        op = [
            cs.Shot(10.0, "left", "made", "operator"),
            cs.Shot(10.1, "left", "made", "operator"),
        ]
        cv = [cs.Shot(10.0, "left", "made", "cv", 0.9)]
        matched, op_only, cv_only = cs.match_shots(op, cv, window=5.0)
        # Only one operator shot matches the lone CV shot.
        assert len(matched) == 1
        assert len(op_only) == 1


# ============================================================================
# End-to-end build_report + CSV
# ============================================================================
class TestBuildReportAndCsv:
    def test_summary_row_aggregates_correctly(self):
        game = _game(
            logs=[_op_score(10.0), _op_score(20.0), _op_score(30.0)],
            cv_logs=[
                _cv_shot(10.2, "left", "made"),    # matches op[0]
                _cv_shot(20.1, "left", "made"),    # matches op[1]
                _cv_shot(40.0, "right", "made"),   # cv_only
            ],
        )
        report = cs.build_report("g1", game, window=5.0)
        assert len(report.matched) == 2
        assert len(report.op_only) == 1
        assert len(report.cv_only) == 1

        buf = io.StringIO()
        cs.write_csv([report], buf)
        text = buf.getvalue()
        assert "summary,g1," in text
        assert "matched=2" in text
        assert "op_only=1" in text
        assert "cv_only=1" in text

    def test_table_output_includes_per_shot_rows(self):
        game = _game(
            logs=[_op_score(10.0)],
            cv_logs=[_cv_shot(10.2, "left", "made", confidence=0.91)],
        )
        report = cs.build_report("g1", game, window=5.0)
        buf = io.StringIO()
        cs.write_table([report], buf)
        text = buf.getvalue()
        assert "=== game g1 ===" in text
        assert "matched" in text
        assert "0.910" in text   # confidence rendered to 3 dp

    def test_main_via_from_json(self, tmp_path):
        game = _game(
            logs=[_op_score(10.0), _op_score(20.0)],
            cv_logs=[_cv_shot(10.2, "left"), _cv_shot(30.0, "left")],
        )
        json_path = tmp_path / "game.json"
        json_path.write_text(json.dumps(game))
        out_path = tmp_path / "out.csv"

        rc = cs.main([
            "--from-json", str(json_path),
            "--output",    str(out_path),
            "--window",    "5.0",
        ])
        assert rc == 0
        text = out_path.read_text()
        assert "summary," in text
        # 1 matched + 1 op_only + 1 cv_only = 3 detail rows + 1 summary + 1 header
        non_empty = [line for line in text.splitlines() if line.strip()]
        assert len(non_empty) >= 4

"""Unit tests for `scripts/cv_infra/ops/accuracy_report.py`.

Covers:
  * extract_human_shots_from_plays — Supabase plays → Shot[]
  * compute_metrics — TP/FP/FN + made/missed agreement
  * AccuracyMetrics properties — precision, recall, classification_accuracy
  * build_report end-to-end
  * to_markdown / to_csv_rows output sanity
  * main() via --cv-from-json + --plays-from-json (offline mode)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "cv_infra" / "ops"))

import accuracy_report as ar  # noqa: E402
from compare_shadow import Shot  # noqa: E402


# ---------------------------------------------------------------- fixtures
GAME_START = "2026-04-15T18:00:00Z"


def _ts(seconds_from_start: float) -> str:
    from datetime import datetime, timedelta
    start = datetime.fromisoformat(GAME_START.replace("Z", "+00:00"))
    return (start + timedelta(seconds=seconds_from_start)).isoformat()


def _human_play(ts_seconds: float, team: str = "left",
                classification: str = "2PT_MAKE",
                source: str = "manual") -> dict:
    return {
        "id": f"play-{int(ts_seconds * 100)}",
        "timestamp_seconds": ts_seconds,
        "team_side": team,
        "classification": classification,
        "source": source,
    }


def _cv_log(ts_seconds: float, team: str = "left",
            outcome: str = "made", confidence: float = 0.9) -> dict:
    return {
        "actionType": "cv_shot",
        "timestamp": _ts(ts_seconds),
        "team": team,
        "payload": {"outcome": outcome, "confidence": confidence},
    }


def _firebase_game(*, cv_logs=None) -> dict:
    return {
        "id": "test-game-1",
        "createdAt": GAME_START,
        "leftTeam":  {"name": "Lefts"},
        "rightTeam": {"name": "Rights"},
        "logs": [],
        "cv_logs_staging": cv_logs or [],
    }


# ============================================================================
# extract_human_shots_from_plays
# ============================================================================
class TestExtractHumanShots:
    def test_made_classifications_become_made(self):
        plays = [
            _human_play(10.0, classification="FG_MAKE"),
            _human_play(20.0, classification="2PT_MAKE"),
            _human_play(30.0, classification="3PT_MAKE"),
            _human_play(40.0, classification="FT_MAKE"),
        ]
        shots = ar.extract_human_shots_from_plays(plays)
        assert len(shots) == 4
        assert all(s.outcome == "made" for s in shots)

    def test_missed_classifications_become_missed(self):
        plays = [
            _human_play(10.0, classification="FG_MISS"),
            _human_play(20.0, classification="2PT_MISS"),
            _human_play(30.0, classification="3PT_MISS"),
            _human_play(40.0, classification="FT_MISS"),
        ]
        shots = ar.extract_human_shots_from_plays(plays)
        assert len(shots) == 4
        assert all(s.outcome == "missed" for s in shots)

    def test_non_shot_classifications_ignored(self):
        plays = [
            _human_play(10.0, classification="FOUL"),
            _human_play(20.0, classification="TIPOFF"),
            _human_play(30.0, classification="FG_MAKE"),
        ]
        shots = ar.extract_human_shots_from_plays(plays)
        assert len(shots) == 1
        assert shots[0].outcome == "made"

    def test_source_cv_filtered_out(self):
        plays = [
            _human_play(10.0, classification="FG_MAKE", source="cv"),
            _human_play(20.0, classification="FG_MAKE", source="manual"),
        ]
        shots = ar.extract_human_shots_from_plays(plays)
        assert len(shots) == 1
        assert shots[0].timestamp_seconds == pytest.approx(20.0)

    def test_invalid_team_dropped(self):
        plays = [
            _human_play(10.0, team="middle", classification="FG_MAKE"),
            _human_play(20.0, team="left",   classification="FG_MAKE"),
        ]
        shots = ar.extract_human_shots_from_plays(plays)
        assert len(shots) == 1
        assert shots[0].team == "left"

    def test_results_sorted_by_timestamp(self):
        plays = [_human_play(30.0), _human_play(10.0), _human_play(20.0)]
        shots = ar.extract_human_shots_from_plays(plays)
        assert [s.timestamp_seconds for s in shots] == [10.0, 20.0, 30.0]


# ============================================================================
# compute_metrics
# ============================================================================
class TestComputeMetrics:
    def _shot(self, ts: float, outcome: str, team: str = "left") -> Shot:
        return Shot(timestamp_seconds=ts, team=team, outcome=outcome, source="x")

    def test_perfect_match(self):
        matched = [
            (self._shot(10.0, "made"),  self._shot(10.0, "made")),
            (self._shot(20.0, "missed"), self._shot(20.0, "missed")),
        ]
        m = ar.compute_metrics(matched, cv_only=[], human_only=[])
        assert m.tp == 2
        assert m.fp == 0
        assert m.fn == 0
        assert m.matched_made_correct == 1
        assert m.matched_missed_correct == 1
        assert m.matched_disagreements == 0
        assert m.precision == 1.0
        assert m.recall == 1.0
        assert m.classification_accuracy == 1.0

    def test_with_false_positives_and_negatives(self):
        matched = [(self._shot(10.0, "made"), self._shot(10.0, "made"))]
        m = ar.compute_metrics(
            matched,
            cv_only=[self._shot(15.0, "made"), self._shot(25.0, "missed")],
            human_only=[self._shot(35.0, "made")],
        )
        assert m.tp == 1
        assert m.fp == 2
        assert m.fn == 1
        # precision = 1 / (1 + 2) = 1/3
        assert m.precision == pytest.approx(1 / 3)
        # recall = 1 / (1 + 1) = 1/2
        assert m.recall == pytest.approx(1 / 2)

    def test_disagreement_counted_in_classification_only(self):
        matched = [
            (self._shot(10.0, "made"),   self._shot(10.0, "missed")),  # disagree
            (self._shot(20.0, "made"),   self._shot(20.0, "made")),    # agree
        ]
        m = ar.compute_metrics(matched, cv_only=[], human_only=[])
        assert m.tp == 2                       # both matched
        assert m.matched_disagreements == 1
        # 1 out of 2 matched agree on made/missed
        assert m.classification_accuracy == 0.5

    def test_zero_division_safe(self):
        m = ar.compute_metrics([], cv_only=[], human_only=[])
        assert m.precision == 0.0
        assert m.recall == 0.0
        assert m.classification_accuracy == 0.0


# ============================================================================
# build_report end-to-end
# ============================================================================
class TestBuildReport:
    def test_high_accuracy_game(self):
        cv_shots = [
            Shot(10.0, "left",  "made",   "cv", confidence=0.91),
            Shot(20.0, "right", "made",   "cv", confidence=0.88),
            Shot(30.0, "left",  "missed", "cv", confidence=0.72),
        ]
        human_shots = [
            Shot(10.2, "left",  "made",   "human"),
            Shot(20.1, "right", "made",   "human"),
            Shot(30.5, "left",  "missed", "human"),
        ]
        r = ar.build_report(game_id="g1", cv_shots=cv_shots,
                            human_shots=human_shots, window=1.0)
        assert r.metrics.tp == 3
        assert r.metrics.fp == 0
        assert r.metrics.fn == 0
        assert r.metrics.precision == 1.0
        assert r.metrics.recall == 1.0

    def test_loose_window_does_not_match_far_shots(self):
        cv_shots = [Shot(10.0, "left", "made", "cv", confidence=0.9)]
        human_shots = [Shot(15.0, "left", "made", "human")]  # 5s apart, window=1.0
        r = ar.build_report(game_id="g1", cv_shots=cv_shots,
                            human_shots=human_shots, window=1.0)
        assert r.metrics.tp == 0
        assert r.metrics.fp == 1
        assert r.metrics.fn == 1


# ============================================================================
# Formatters
# ============================================================================
class TestFormatters:
    def _toy_report(self) -> ar.AccuracyReport:
        cv = [Shot(10.0, "left", "made", "cv", confidence=0.91)]
        human = [Shot(10.2, "left", "made", "human"),
                 Shot(40.0, "right", "missed", "human")]
        return ar.build_report(
            game_id="game-xyz",
            cv_shots=cv,
            human_shots=human,
            window=1.0,
            game_meta={"leftTeam": {"name": "Lefts"},
                       "rightTeam": {"name": "Rights"},
                       "createdAt": GAME_START},
        )

    def test_markdown_includes_metrics_block(self):
        md = ar.to_markdown(self._toy_report())
        assert "# CV accuracy report" in md
        assert "**Precision**" in md
        assert "**Recall**" in md
        assert "Lefts" in md and "Rights" in md
        # We have 1 TP, 0 FP, 1 FN — precision should be 100%, recall 50%
        assert "100.0%" in md
        assert "50.0%" in md

    def test_csv_includes_summary_and_detail(self):
        report = self._toy_report()
        rows = list(ar.to_csv_rows(report))
        types = [r[0] for r in rows]
        assert "summary" in types
        assert "matched" in types
        assert "fn_human_only" in types


# ============================================================================
# main() CLI via offline JSON
# ============================================================================
class TestMainOffline:
    def test_main_offline_writes_md_and_csv(self, tmp_path):
        game = _firebase_game(cv_logs=[
            _cv_log(10.0, "left", "made", 0.91),
            _cv_log(20.0, "right", "made", 0.85),
            _cv_log(50.0, "left", "missed", 0.65),   # extra CV → FP
        ])
        plays = [
            _human_play(10.2, "left",  "FG_MAKE"),
            _human_play(20.1, "right", "FG_MAKE"),
            _human_play(40.0, "left",  "FG_MAKE"),   # no CV → FN
        ]
        cv_json = tmp_path / "game.json"
        plays_json = tmp_path / "plays.json"
        out_dir = tmp_path / "out"
        cv_json.write_text(json.dumps(game))
        plays_json.write_text(json.dumps(plays))

        rc = ar.main([
            "--cv-from-json",    str(cv_json),
            "--plays-from-json", str(plays_json),
            "--window",          "1.0",
            "--out-dir",         str(out_dir),
        ])
        assert rc == 0

        md_path = out_dir / "test-game-1.md"
        csv_path = out_dir / "test-game-1.csv"
        assert md_path.exists()
        assert csv_path.exists()

        md = md_path.read_text()
        # 2 matched / 1 CV-only / 1 human-only -> precision = 2/3, recall = 2/3
        assert "66.7%" in md

    def test_main_offline_requires_both_json_args(self, tmp_path):
        cv_json = tmp_path / "game.json"
        cv_json.write_text("{}")
        rc = ar.main(["--cv-from-json", str(cv_json)])
        assert rc == 2

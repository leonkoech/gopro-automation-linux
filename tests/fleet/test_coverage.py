"""Coverage records must make an absent stage visible, not just a slow one.

Both real incidents looked identical from the outside: the box was up, the logs
were plausible, and nothing anywhere held the number the work should have
matched. These tests pin the properties that fix that -- a denominator on every
stage, and an enabled stage that never reported showing up as a fact in the
record rather than as a missing key.
"""

import importlib
import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "agx_pipeline"))


@pytest.fixture()
def cov(tmp_path, monkeypatch):
    monkeypatch.setenv("SHOT_COVERAGE_DIR", str(tmp_path / "coverage"))
    for flag in ("SHOT_LIVE_ENABLED", "SHOT_LIVE_TYPING", "SHOT_LIVE_WHO_SCAN"):
        monkeypatch.delenv(flag, raising=False)
    from agx_pipeline import coverage  # noqa: PLC0415
    importlib.reload(coverage)
    coverage._sources.clear()
    return coverage


@pytest.mark.unit
def test_a_record_carries_both_numbers(cov):
    cov.record("g1", "shot_detect", expected=1966, processed=1939, shots=163)
    e = cov.read("g1")["stages"]["shot_detect"]
    assert (e["expected"], e["processed"]) == (1966, 1939)
    assert e["complete"] is False
    assert e["detail"]["shots"] == 163


@pytest.mark.unit
def test_complete_is_true_only_when_the_work_matches(cov):
    cov.record("g1", "typing", expected=47, processed=47)
    assert cov.read("g1")["stages"]["typing"]["complete"] is True


@pytest.mark.unit
def test_an_unknown_denominator_is_unknown_not_zero(cov):
    """A stage that cannot know its total says so. Guessing a denominator is
    worse than admitting the gap -- a wrong one reads as healthy."""
    cov.record("g1", "who", expected=None, processed=12)
    e = cov.read("g1")["stages"]["who"]
    assert e["expected"] is None
    assert e["complete"] is None


@pytest.mark.unit
def test_stages_accumulate_into_one_record(cov):
    cov.record("g1", "shot_detect", expected=10, processed=10)
    cov.record("g1", "typing", expected=3, processed=2)
    assert set(cov.read("g1")["stages"]) == {"shot_detect", "typing"}


@pytest.mark.unit
def test_an_enabled_stage_that_never_reported_is_marked_silent(cov, monkeypatch):
    """The fifteen-day failure, as a record. Typing was ON and produced nothing;
    that has to be a value in the document, because a checker cannot assert on a
    key that simply is not there."""
    monkeypatch.setenv("SHOT_LIVE_TYPING", "true")
    cov.record("g1", "shot_detect", expected=10, processed=10)
    doc = cov.finalize("g1")
    assert doc["stages"]["typing"]["silent"] is True
    assert doc["stages"]["typing"]["processed"] is None


@pytest.mark.unit
def test_a_disabled_stage_is_not_marked_silent(cov):
    """Off is a valid state; only being on and mute is not."""
    cov.record("g1", "shot_detect", expected=10, processed=10)
    assert "typing" not in cov.finalize("g1")["stages"]


@pytest.mark.unit
def test_finalize_pulls_from_a_registered_source(cov, monkeypatch):
    monkeypatch.setenv("SHOT_LIVE_TYPING", "true")
    cov.register_source("typing", lambda: (47, 47, {"failed": 0}))
    doc = cov.finalize("g1")
    assert doc["stages"]["typing"]["processed"] == 47
    assert "silent" not in doc["stages"]["typing"]


@pytest.mark.unit
def test_a_source_that_raises_loses_only_itself(cov, monkeypatch):
    """One broken stage must not cost the record for every other stage."""
    monkeypatch.setenv("SHOT_LIVE_TYPING", "true")

    def boom():
        raise RuntimeError("nope")

    cov.register_source("typing", boom)
    cov.record("g1", "shot_detect", expected=10, processed=10)
    doc = cov.finalize("g1")
    assert doc["stages"]["shot_detect"]["processed"] == 10
    assert doc["stages"]["typing"]["silent"] is True


@pytest.mark.unit
def test_a_write_failure_never_raises_into_the_pipeline(cov, monkeypatch):
    """Coverage is instrumentation. Instrumentation that can take the game down
    is worse than none."""
    monkeypatch.setenv("SHOT_COVERAGE_DIR", "/proc/cannot/write/here")
    cov.record("g1", "shot_detect", expected=1, processed=1)   # must not raise
    assert cov.finalize("g1") == {}                            # reports nothing, breaks nothing


@pytest.mark.unit
def test_the_record_is_valid_json_on_disk(cov):
    cov.record("g1", "shot_detect", expected=2, processed=1)
    with open(cov.path_for("g1")) as fh:
        assert json.load(fh)["game_id"] == "g1"


@pytest.mark.unit
def test_a_game_id_cannot_escape_the_coverage_directory(cov):
    cov.record("../../etc/passwd", "shot_detect", expected=1, processed=1)
    written = list(Path(cov.root()).glob("*.json"))
    assert len(written) == 1
    assert written[0].parent == Path(cov.root())


@pytest.mark.unit
def test_a_record_exists_before_the_game_ends(cov):
    """The killed-mid-game case. If the record were only written at stop, a box
    that lost power would leave nothing at all -- indistinguishable from a night
    with no game, which is the confusion this is meant to remove."""
    cov.record("g1", "shot_detect", expected=40, processed=31)
    doc = cov.read("g1")
    assert doc["status"] == "running"
    assert "closed_at" not in doc


@pytest.mark.unit
def test_finalize_marks_the_record_closed(cov):
    cov.record("g1", "shot_detect", expected=40, processed=40)
    doc = cov.finalize("g1")
    assert doc["status"] == "closed"
    assert doc["closed_at"]


@pytest.mark.unit
def test_the_three_cases_are_distinguishable(cov):
    """Nothing started, started and died, finished cleanly -- a checker has to
    tell these apart, and only the middle one is new."""
    assert cov.read("never_ran") == {}
    cov.record("died", "shot_detect", expected=40, processed=31)
    cov.record("finished", "shot_detect", expected=40, processed=40)
    cov.finalize("finished")
    assert cov.read("died")["status"] == "running"
    assert cov.read("finished")["status"] == "closed"


@pytest.mark.unit
def test_a_game_still_being_played_is_not_a_problem(cov):
    """The check is polled every few minutes; work still in progress must not
    read as a fault."""
    cov.record("g1", "shot_detect", expected=40, processed=12)
    assert cov.problems() == []


@pytest.mark.unit
def test_a_run_that_stopped_without_closing_is_a_problem(cov, monkeypatch):
    monkeypatch.setattr(cov, "STALE_AFTER_MIN", 0.0)
    cov.record("g1", "shot_detect", expected=40, processed=12)
    found = cov.problems()
    assert any("never finished" in p for p in found), found


@pytest.mark.unit
def test_a_short_stage_is_a_problem_once_the_game_closes(cov):
    cov.record("g1", "shot_detect", expected=1966, processed=1939)
    cov.finalize("g1")
    found = cov.problems()
    assert any("1939 of 1966" in p for p in found), found


@pytest.mark.unit
def test_a_silent_stage_is_a_problem(cov, monkeypatch):
    monkeypatch.setenv("SHOT_LIVE_TYPING", "true")
    cov.record("g1", "shot_detect", expected=10, processed=10)
    cov.finalize("g1")
    found = cov.problems()
    assert any("typing was enabled and never reported" in p for p in found), found


@pytest.mark.unit
def test_a_complete_game_reports_nothing(cov):
    cov.record("g1", "shot_detect", expected=40, processed=40)
    cov.finalize("g1")
    assert cov.problems() == []


@pytest.mark.unit
def test_problems_ignores_records_outside_the_window(cov):
    cov.record("g1", "shot_detect", expected=40, processed=1)
    cov.finalize("g1")
    assert cov.problems(hours=0) == []


@pytest.mark.unit
def test_the_live_publisher_is_a_stage_coverage_knows_about(cov, monkeypatch):
    """It is env-gated like the others, so an enabled publisher that reports
    nothing has to show up the same way."""
    monkeypatch.setenv("LIVE_STREAM_ENABLED", "true")
    assert cov.enabled_stages()["live_stream"] is True
    cov.record("g1", "shot_detect", expected=10, processed=10)
    assert cov.finalize("g1")["stages"]["live_stream"]["silent"] is True


@pytest.mark.unit
def test_one_angle_of_two_is_a_problem(cov):
    """The FR failure: one angle published, the other died at launch with no
    segments and no retry."""
    cov.record("g1", "live_stream", expected=2, processed=1,
               per_angle={"FL": {"alive": True, "segments": 37},
                          "FR": {"alive": False, "segments": 0}})
    cov.finalize("g1")
    found = cov.problems()
    assert any("live_stream covered 1 of 2" in p for p in found), found


@pytest.mark.unit
def test_both_angles_publishing_is_not_a_problem(cov):
    cov.record("g1", "live_stream", expected=2, processed=2)
    cov.finalize("g1")
    assert cov.problems() == []


@pytest.mark.unit
def test_a_closed_record_stays_closed_when_a_slow_stage_reports_late(cov):
    """Stages stop on their own threads. A late number is worth keeping, but it
    must not put a finished game back into "running" and make it look dead."""
    cov.record("g1", "live_stream", expected=2, processed=2)
    cov.finalize("g1")
    cov.record("g1", "shot_detect", expected=40, processed=40)   # arrives after
    doc = cov.read("g1")
    assert doc["status"] == "closed"
    assert doc["stages"]["shot_detect"]["processed"] == 40
    assert cov.problems() == []


@pytest.mark.unit
def test_an_unattached_recording_writes_nothing(cov):
    """No game id means no game in the annotation tool to attribute this to."""
    cov.record(None, "live_stream", expected=2, processed=2)
    assert cov.finalize(None) == {}
    assert not list(Path(cov.root()).glob("*.json")) if Path(cov.root()).exists() else True


@pytest.mark.unit
def test_summarize_names_the_silent_stage(cov, monkeypatch):
    monkeypatch.setenv("SHOT_LIVE_TYPING", "true")
    cov.record("g1", "shot_detect", expected=10, processed=9)
    line = cov.summarize(cov.finalize("g1"))
    assert "shot_detect 9/10" in line
    assert "typing SILENT" in line

"""The event stream has to carry what the detector already knows.

These pin the properties the schema proposal turns on, because the dual write
exists to falsify it: a sport-neutral observation separate from the basketball
interpretation of it, an instant that is still expressed as an interval, a
derived id that survives a rescan, and nothing invented to fill a field the
detector cannot answer.
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

WC = "2026-09-14T19:22:31.880Z"
REC = {"cam": "SL", "side": "left", "seg": 418, "t_shot": 2.417, "made": True,
       "verdict": "MAKE", "rho": 0.71, "wallclock": WC, "detected_at": WC,
       "latency_s": 16.2, "scan_s": 3.9}
VERDICT = {"verdict": "MAKE", "rho": 0.71, "geo": "GEO_MAKE",
           "decided_by": "geometry"}


@pytest.fixture()
def ev(tmp_path, monkeypatch):
    monkeypatch.setenv("EVENT_STREAM_DIR", str(tmp_path / "events"))
    monkeypatch.setenv("EVENT_STREAM_ENABLED", "true")
    from agx_pipeline import events  # noqa: PLC0415
    return importlib.reload(events)


def shot(ev, **over):
    rec = dict(REC, **over)
    return ev.from_shot(rec, VERDICT, "g1", "riverside", "court-a", "v3.engine")


@pytest.mark.unit
def test_nothing_is_written_unless_the_flag_is_on(tmp_path, monkeypatch):
    """A side-channel under test must be opt-in."""
    monkeypatch.setenv("EVENT_STREAM_DIR", str(tmp_path / "events"))
    monkeypatch.delenv("EVENT_STREAM_ENABLED", raising=False)
    from agx_pipeline import events  # noqa: PLC0415
    events = importlib.reload(events)
    events.emit("g1", [{"id": "x"}])
    assert events.read("g1") == []


@pytest.mark.unit
def test_a_make_produces_an_observation_and_an_interpretation(ev):
    kinds = [e["kind"] for e in shot(ev)]
    assert kinds == ["observation.plane_cross", "interpretation.score"]


@pytest.mark.unit
def test_a_miss_produces_only_an_observation(ev):
    """Something crossed the plane either way; only one of them scored."""
    assert len(shot(ev, made=False, verdict="MISS")) == 1


@pytest.mark.unit
def test_the_observation_says_nothing_about_basketball(ev):
    """It is the half that has to survive into another sport."""
    obs = shot(ev)[0]
    blob = json.dumps(obs)
    assert "made" not in blob and "MAKE" not in blob
    assert obs["where"]["structure"] == "goal_left"


@pytest.mark.unit
def test_rho_stays_in_evidence_rather_than_becoming_a_confidence(ev):
    """It is a distance from the rim centre, not a probability, and it is not
    comparable with typing's number."""
    obs = shot(ev)[0]
    assert obs["evidence"]["offset"] == 0.71
    assert obs["confidence"] == {"value": None, "scale": "none"}


@pytest.mark.unit
def test_an_instant_is_still_expressed_as_an_interval(ev):
    """A shot is a moment and a rally is not. The pair is why volleyball fits."""
    t = shot(ev)[0]["t"]
    assert t["start"] == t["end"] == WC


@pytest.mark.unit
def test_the_interpretation_links_back_and_claims_nothing_it_cannot_know(ev):
    obs, interp = shot(ev)
    assert interp["derived_from"] == [obs["id"]]
    # Which team benefits depends on the period and the starting end, and
    # side_attribution.py owns that. Point value is typing's, and arrives later.
    assert interp["credit"]["team"] is None
    assert interp["value"]["points"] is None


@pytest.mark.unit
def test_the_id_is_derived_so_a_rescan_matches_instead_of_duplicating(ev):
    """Reprocessing is routine here — backfills, re-cuts, the deferred scan."""
    assert shot(ev)[0]["id"] == shot(ev)[0]["id"]


@pytest.mark.unit
def test_a_different_hoop_is_a_different_event(ev):
    assert shot(ev)[0]["id"] != shot(ev, side="right")[0]["id"]


@pytest.mark.unit
def test_no_wallclock_means_no_event(ev):
    """Without a wall-clock anchor nothing downstream could place it."""
    assert shot(ev, wallclock=None) == []


@pytest.mark.unit
def test_events_append_rather_than_replace(ev):
    """A killed run keeps what it already wrote."""
    ev.emit("g1", shot(ev))
    ev.emit("g1", shot(ev, side="right", made=False))
    assert len(ev.read("g1")) == 3


@pytest.mark.unit
def test_a_torn_final_line_does_not_lose_the_rest(ev):
    """A kill mid-write should cost one event, not the game."""
    ev.emit("g1", shot(ev))
    with open(ev.path_for("g1"), "a") as fh:
        fh.write('{"id": "half-writ')
    assert len(ev.read("g1")) == 2


@pytest.mark.unit
def test_a_write_failure_never_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("EVENT_STREAM_ENABLED", "true")
    monkeypatch.setenv("EVENT_STREAM_DIR", "/proc/cannot/write/here")
    from agx_pipeline import events  # noqa: PLC0415
    events = importlib.reload(events)
    events.emit("g1", [{"id": "x"}])          # must not raise
    assert events.read("g1") == []


@pytest.mark.unit
def test_a_game_id_cannot_escape_the_event_directory(ev):
    ev.emit("../../etc/passwd", shot(ev))
    written = list(Path(ev.root()).glob("*.jsonl"))
    assert len(written) == 1
    assert written[0].parent == Path(ev.root())

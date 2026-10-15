"""The detector check must not call a box ready when it is quietly on the .pt.

The detector falls back to the .pt whenever the TensorRT engine cannot be used.
That is the right behaviour and it is also what hides: the box works, at about
half the speed, and says so only in a log line at startup.
"""

import importlib
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))


def _load(**env):
    for key in ("SHOT_LIVE_TYPING", "SHOT_LIVE_WHO_SCAN", "SHOT_WHO_ROSTER",
                "SHOT_DET_WEIGHT", "SHOT_DET_ENGINE"):
        os.environ.pop(key, None)
    os.environ.update(env)
    import stage_check  # noqa: PLC0415
    return importlib.reload(stage_check)


@pytest.fixture()
def pt(tmp_path):
    w = tmp_path / "ball.pt"
    w.touch()
    return w


@pytest.mark.unit
def test_the_checked_weight_is_the_one_production_loads():
    """Re-deriving the default instead of importing it is how a checker starts
    reporting on a file nobody runs."""
    from agx_pipeline.shot_detect import node

    sc = _load()
    assert sc._detector_weight() == node._DEFAULT_WEIGHT


@pytest.mark.unit
def test_a_missing_pt_is_blocked_because_it_is_the_fallback(tmp_path):
    sc = _load(SHOT_DET_WEIGHT=str(tmp_path / "gone.pt"))
    status, detail = sc.check_detector()
    assert status == sc.BLOCKED
    assert "no fallback" in detail


@pytest.mark.unit
def test_a_dangling_pt_symlink_is_named_as_one(tmp_path):
    """The fault that took typing out for fifteen days. isfile() is False for a
    dangling link, so it must not be reported as merely missing -- the fix is
    different."""
    link = tmp_path / "ball.pt"
    link.symlink_to(tmp_path / "gone" / "ball.pt")
    sc = _load(SHOT_DET_WEIGHT=str(link))
    status, detail = sc.check_detector()
    assert status == sc.BLOCKED
    assert "DANGLING SYMLINK" in detail


@pytest.mark.unit
def test_no_engine_configured_is_ready_and_says_so(pt):
    """Running the .pt is not a fault. The report is where someone finds out the
    engine is not in use on this box."""
    sc = _load(SHOT_DET_WEIGHT=str(pt))
    status, detail = sc.check_detector()
    assert status == sc.OK
    assert "SHOT_DET_ENGINE is not set" in detail


@pytest.mark.unit
def test_an_engine_configured_but_missing_is_blocked(pt, tmp_path):
    """The box would run, at about half speed, with only a startup log to say
    so. A deploy that set SHOT_DET_ENGINE did not intend that."""
    sc = _load(SHOT_DET_WEIGHT=str(pt), SHOT_DET_ENGINE=str(tmp_path / "gone.engine"))
    status, detail = sc.check_detector()
    assert status == sc.BLOCKED
    assert "half speed" in detail


@pytest.mark.unit
def test_the_gpu_fault_marker_is_blocked_and_names_the_file(pt, tmp_path, monkeypatch):
    """Written when the engine faults the GPU mid-run. The box then stays on the
    .pt across restarts until someone deletes it, which is exactly the state
    nobody notices."""
    eng = tmp_path / "ball.engine"
    eng.touch()
    mark = tmp_path / "shot_det_engine.DISABLED"
    mark.touch()
    monkeypatch.setenv("SHOT_DET_ENGINE_DISABLED_MARK", str(mark))
    import agx_pipeline.shot_detect.detect as detect  # noqa: PLC0415
    importlib.reload(detect)
    sc = _load(SHOT_DET_WEIGHT=str(pt), SHOT_DET_ENGINE=str(eng),
               SHOT_DET_ENGINE_DISABLED_MARK=str(mark))
    status, detail = sc.check_detector()
    assert status == sc.BLOCKED
    assert str(mark) in detail


@pytest.mark.unit
def test_a_present_engine_is_ready_without_probing(pt, tmp_path):
    eng = tmp_path / "ball.engine"
    eng.touch()
    sc = _load(SHOT_DET_WEIGHT=str(pt), SHOT_DET_ENGINE=str(eng))
    status, detail = sc.check_detector()
    assert status == sc.OK
    assert "--load" in detail


@pytest.mark.unit
def test_load_defers_to_the_detectors_own_probe(pt, tmp_path, monkeypatch):
    """Reusing the detector's probe is what stops this check and production
    disagreeing about whether an engine is usable."""
    eng = tmp_path / "ball.engine"
    eng.touch()
    import agx_pipeline.shot_detect.detect as detect  # noqa: PLC0415
    monkeypatch.setattr(detect, "_probe_in_subprocess", lambda p: "probe ball not found")
    sc = _load(SHOT_DET_WEIGHT=str(pt), SHOT_DET_ENGINE=str(eng))
    status, detail = sc.check_detector(load=True)
    assert status == sc.BLOCKED
    assert "probe ball not found" in detail
    assert "build_shot_engine.py" in detail


@pytest.mark.unit
def test_a_passing_probe_is_ready(pt, tmp_path, monkeypatch):
    eng = tmp_path / "ball.engine"
    eng.touch()
    import agx_pipeline.shot_detect.detect as detect  # noqa: PLC0415
    monkeypatch.setattr(detect, "_probe_in_subprocess", lambda p: None)
    sc = _load(SHOT_DET_WEIGHT=str(pt), SHOT_DET_ENGINE=str(eng))
    assert sc.check_detector(load=True)[0] == sc.OK


@pytest.mark.unit
def test_a_broken_detector_makes_the_script_exit_nonzero(tmp_path):
    """So --load can gate a deploy."""
    sc = _load(SHOT_DET_WEIGHT=str(tmp_path / "gone.pt"))
    assert sc.main([]) == 1

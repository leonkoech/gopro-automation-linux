"""The detector-weight check must not call a weight ready that cannot run.

A1 switches SHOT_DET_WEIGHT from a .pt to a .engine. That is a config change
with no code behind it, so the only thing standing between a typo (or an engine
built on another box) and a silent game is this check.
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
                "SHOT_DET_WEIGHT"):
        os.environ.pop(key, None)
    os.environ.update(env)
    import stage_check  # noqa: PLC0415
    return importlib.reload(stage_check)


@pytest.mark.unit
def test_the_checked_path_is_the_one_production_loads():
    """Re-deriving the default instead of importing it is how a checker starts
    reporting on a file nobody runs."""
    from agx_pipeline.shot_detect import node

    sc = _load()
    assert sc._detector_weight() == node._DEFAULT_WEIGHT


@pytest.mark.unit
def test_env_override_wins(tmp_path):
    w = tmp_path / "ball.engine"
    w.touch()
    sc = _load(SHOT_DET_WEIGHT=str(w))
    assert sc._detector_weight() == str(w)


@pytest.mark.unit
def test_a_missing_weight_is_blocked(tmp_path):
    sc = _load(SHOT_DET_WEIGHT=str(tmp_path / "nope.engine"))
    status, detail = sc.check_detector()
    assert status == sc.BLOCKED
    assert "not found" in detail


@pytest.mark.unit
def test_a_dangling_weight_symlink_is_blocked(tmp_path):
    """The same fault that took typing out for fifteen days, one directory over.
    isfile() is False for a dangling link, so it must be named as a dangling
    link and not as 'not found' -- the fix is different."""
    link = tmp_path / "ball.engine"
    link.symlink_to(tmp_path / "gone" / "ball.engine")
    sc = _load(SHOT_DET_WEIGHT=str(link))
    status, detail = sc.check_detector()
    assert status == sc.BLOCKED
    assert "DANGLING SYMLINK" in detail


@pytest.mark.unit
def test_a_present_engine_is_ready_without_loading(tmp_path):
    w = tmp_path / "ball.engine"
    w.touch()
    sc = _load(SHOT_DET_WEIGHT=str(w))
    status, detail = sc.check_detector()
    assert status == sc.OK
    assert "TensorRT engine" in detail


@pytest.mark.unit
def test_a_pt_weight_is_ready_but_says_it_is_still_pytorch(tmp_path):
    """Being on .pt is not a fault, so it must not block -- but the report is
    where someone finds out A1 has not actually been adopted on this box."""
    w = tmp_path / "ball.pt"
    w.touch()
    sc = _load(SHOT_DET_WEIGHT=str(w))
    status, detail = sc.check_detector()
    assert status == sc.OK
    assert ".engine" in detail


@pytest.mark.unit
def test_load_reports_blocked_when_the_weight_cannot_run(tmp_path):
    """An empty file is not a model. Whatever ultralytics raises, the check owns
    it and reports BLOCKED rather than letting the exception escape."""
    pytest.importorskip("ultralytics")
    w = tmp_path / "ball.engine"
    w.write_bytes(b"not an engine")
    sc = _load(SHOT_DET_WEIGHT=str(w))
    status, detail = sc.check_detector(load=True)
    assert status == sc.BLOCKED
    assert "did not run here" in detail


@pytest.mark.unit
def test_a_broken_weight_makes_the_script_exit_nonzero(tmp_path):
    """So --load can gate a deploy."""
    sc = _load(SHOT_DET_WEIGHT=str(tmp_path / "nope.engine"))
    assert sc.main([]) == 1

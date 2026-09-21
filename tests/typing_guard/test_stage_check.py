"""stage_check must refuse to call a stage ready when it cannot actually run.

The point of the script is to be believed. If it reports READY for a stage that
silently produces nothing, it is worse than having no check at all -- that is
precisely how live typing went fifteen days unnoticed.
"""

import importlib
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))


def _load(cwd: Path, **env):
    """stage_check reads its paths at import time, so each case reloads it."""
    os.environ["SHOT_TYPING_CWD"] = str(cwd)
    for key in ("SHOT_LIVE_TYPING", "SHOT_LIVE_WHO_SCAN", "SHOT_WHO_ROSTER"):
        os.environ.pop(key, None)
    os.environ.update(env)
    import stage_check  # noqa: PLC0415
    return importlib.reload(stage_check)


def _who_ready_dir(tmp_path: Path) -> Path:
    """A working dir with every WHO artefact present."""
    (tmp_path / "runs" / "jersey").mkdir(parents=True)
    (tmp_path / "uball_cc").mkdir()
    for name in ("agx_classify.py", "who_scan_live.py", "unified_yolo26s.pt"):
        (tmp_path / name).touch()
    for name in ("legibility_resnet18.pt", "number_localizer_yolo11n.pt",
                 "parseq_jersey.pt"):
        (tmp_path / "runs" / "jersey" / name).touch()
    return tmp_path


@pytest.mark.unit
def test_a_disabled_stage_is_reported_but_does_not_fail_the_run(tmp_path):
    """Off is a valid state. Only silence about being broken is not."""
    sc = _load(tmp_path)
    status, _ = sc.check_typing()
    assert status == sc.OFF


@pytest.mark.unit
def test_typing_enabled_with_a_dangling_classifier_is_blocked(tmp_path):
    """The Sep-3 incident: the link is there, its target is gone."""
    (tmp_path / "agx_classify.py").symlink_to(tmp_path / "gone" / "agx_classify.py")
    sc = _load(tmp_path, SHOT_LIVE_TYPING="true")
    status, detail = sc.check_typing()
    assert status == sc.BLOCKED
    assert "DANGLING SYMLINK" in detail


@pytest.mark.unit
def test_typing_enabled_with_a_real_classifier_is_ready(tmp_path):
    (tmp_path / "agx_classify.py").write_text("print('hi')\n")
    sc = _load(tmp_path, SHOT_LIVE_TYPING="true")
    assert sc.check_typing()[0] == sc.OK


@pytest.mark.unit
def test_who_is_blocked_when_its_models_are_missing(tmp_path):
    (tmp_path / "agx_classify.py").touch()
    (tmp_path / "who_scan_live.py").touch()
    sc = _load(tmp_path, SHOT_LIVE_WHO_SCAN="true", SHOT_WHO_ROSTER="2,3")
    status, detail = sc.check_who()
    assert status == sc.BLOCKED
    assert "parseq_jersey.pt" in detail


@pytest.mark.unit
def test_who_is_blocked_when_the_roster_is_not_wired(tmp_path):
    """Everything on disk, gate on, and still not ready: an empty roster turns
    off the per-team filter and costs roughly 56% against 90%. The scan would
    answer -- just far less reliably -- so 'ready' would be a lie."""
    sc = _load(_who_ready_dir(tmp_path), SHOT_LIVE_WHO_SCAN="true")
    status, detail = sc.check_who()
    assert status == sc.BLOCKED
    assert "SHOT_WHO_ROSTER" in detail


@pytest.mark.unit
def test_who_is_ready_only_with_models_and_roster_together(tmp_path):
    sc = _load(_who_ready_dir(tmp_path), SHOT_LIVE_WHO_SCAN="true",
               SHOT_WHO_ROSTER="2,3,6,8,11")
    assert sc.check_who()[0] == sc.OK


@pytest.mark.unit
def test_a_blocked_stage_makes_the_script_exit_nonzero(tmp_path):
    """So it can gate a deploy."""
    (tmp_path / "agx_classify.py").symlink_to(tmp_path / "gone" / "x.py")
    sc = _load(tmp_path, SHOT_LIVE_TYPING="true")
    assert sc.main() == 1


@pytest.mark.unit
def test_all_stages_off_exits_zero(tmp_path):
    sc = _load(tmp_path)
    assert sc.main() == 0

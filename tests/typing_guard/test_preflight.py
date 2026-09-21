"""The dead-symlink case that silently disabled live typing for fifteen days.

agx_classify.py in SHOT_TYPING_CWD was a symlink into a scratch directory a disk
cleanup had deleted. Every spawn died on python's "can't open file" (rc=2) and
the only trace was that number in a warning. preflight() must name the cause.
"""
import importlib
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "agx_pipeline"))


def _load(cwd):
    os.environ["SHOT_TYPING_CWD"] = str(cwd)
    import shot_typing_live
    return importlib.reload(shot_typing_live)


def test_healthy_directory_passes(tmp_path):
    (tmp_path / "agx_classify.py").write_text("print('hi')\n")
    assert _load(tmp_path).preflight() is None


def test_dangling_symlink_is_named(tmp_path):
    """The real incident: link present, target gone."""
    (tmp_path / "agx_classify.py").symlink_to(tmp_path / "gone" / "agx_classify.py")
    reason = _load(tmp_path).preflight()
    assert reason is not None
    assert "DANGLING SYMLINK" in reason


def test_missing_classifier_is_named(tmp_path):
    reason = _load(tmp_path).preflight()
    assert reason is not None and "not found" in reason


def test_missing_directory_is_named(tmp_path):
    reason = _load(tmp_path / "nope").preflight()
    assert reason is not None and "does not exist" in reason


def test_unreadable_classifier_is_named(tmp_path):
    p = tmp_path / "agx_classify.py"
    p.write_text("x\n")
    p.chmod(0o000)
    try:
        if os.access(str(p), os.R_OK):
            pytest.skip("running as root — permission bits not enforced")
        reason = _load(tmp_path).preflight()
        assert reason is not None and "not readable" in reason
    finally:
        p.chmod(0o644)

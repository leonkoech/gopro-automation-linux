"""The detector must not silently destroy footage it never scanned.

cb9e1294 (2026-09-14) stopped 442s behind real time. The stop path rmtree'd the
whole segment directory and logged only `shots=163`, so the final 7.4 minutes —
16 ground-truth shots, 37% of that game's coverage gap — were both undetected
and unrecoverable, with nothing in the logs saying so.

These pin the two halves of that fix: unscanned segments survive, and scanned
ones do not accumulate.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from agx_pipeline.shot_detect.live import LiveShotScorer, _SEG_RE  # noqa: E402


def _seg(d: Path, idx: int, angle: str) -> Path:
    p = d / f"seg_{idx:05d}_{angle}.mp4"
    p.write_bytes(b"x" * 16)
    return p


def _live() -> LiveShotScorer:
    """The helper under test touches no Firebase and no config."""
    return LiveShotScorer.__new__(LiveShotScorer)


@pytest.mark.unit
def test_segment_name_pattern_is_what_we_assume(tmp_path):
    assert _SEG_RE.match("seg_00078_SL.mp4")
    assert not _SEG_RE.match("UNSCANNED.json")


@pytest.mark.unit
def test_unscanned_segments_are_kept(tmp_path):
    keep_a, keep_b = _seg(tmp_path, 10, "SL"), _seg(tmp_path, 10, "SR")
    leftover = [(10, "SL", str(keep_a)), (10, "SR", str(keep_b))]
    kept = _live()._keep_unscanned(str(tmp_path), leftover, "game1")
    assert kept == 2
    assert keep_a.exists() and keep_b.exists()


@pytest.mark.unit
def test_already_scanned_segments_are_deleted(tmp_path):
    """Otherwise a whole game (~6GB) accumulates for the sake of a few minutes."""
    scanned = [_seg(tmp_path, i, "SL") for i in range(5)]
    pending = _seg(tmp_path, 9, "SL")
    kept = _live()._keep_unscanned(str(tmp_path), [(9, "SL", str(pending))], "game1")
    assert kept == 1
    assert pending.exists()
    assert not any(p.exists() for p in scanned)


@pytest.mark.unit
def test_a_manifest_names_the_game_and_the_segments(tmp_path):
    """The directory is named by recording label, so without this a deferred
    scanner cannot tell which game the footage belongs to."""
    pending = _seg(tmp_path, 7, "SR")
    _seg(tmp_path, 1, "SL")          # scanned, should be pruned
    _live()._keep_unscanned(str(tmp_path), [(7, "SR", str(pending))], "abc123")
    manifest = tmp_path / "UNSCANNED.json"
    assert manifest.exists()
    got = json.loads(manifest.read_text())
    assert got["game_id"] == "abc123"
    assert got["segments"] == ["seg_00007_SR.mp4"]
    assert got["n_segments"] == 1
    assert got["written_at"] and got["reason"]


@pytest.mark.unit
def test_the_manifest_is_not_mistaken_for_a_segment(tmp_path):
    """A second stop must not delete the manifest or miscount it."""
    pending = _seg(tmp_path, 3, "SL")
    _live()._keep_unscanned(str(tmp_path), [(3, "SL", str(pending))], "g")
    kept = _live()._keep_unscanned(str(tmp_path), [(3, "SL", str(pending))], "g")
    assert kept == 1
    assert (tmp_path / "UNSCANNED.json").exists()


@pytest.mark.unit
def test_a_missing_directory_never_raises(tmp_path):
    """This runs on the way out of the detector thread; raising there would
    lose the stop log entirely."""
    gone = tmp_path / "not-there"
    assert _live()._keep_unscanned(str(gone), [], "g") == 0


@pytest.mark.unit
def test_nothing_unscanned_keeps_nothing(tmp_path):
    for i in range(3):
        _seg(tmp_path, i, "SL")
    assert _live()._keep_unscanned(str(tmp_path), [], "g") == 0
    assert not list(tmp_path.glob("*.mp4"))


@pytest.mark.unit
def test_unscanned_seconds_counts_time_slots_not_files(tmp_path):
    """Both cameras produce a file per slot, so counting files would double the
    reported loss. The figure must describe seconds of play."""
    from agx_pipeline.shot_detect.live import SHOT_SEGMENT_SEC
    leftover = [(10, "SL", "a"), (10, "SR", "b"), (11, "SL", "c"), (11, "SR", "d")]
    unscanned_s = len({idx for idx, _, _ in leftover}) * SHOT_SEGMENT_SEC
    assert unscanned_s == 2 * SHOT_SEGMENT_SEC

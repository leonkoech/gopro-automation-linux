"""The scan-window track cache must produce the same window as re-scanning.

live.py's _process_window concatenates the previous segment onto the current one
so a shot straddling the 4s boundary is whole in one window, then scans the pair.
Every segment is therefore decoded AND inferred twice. The cache scans each
segment once and joins TRACKS instead of files.

Track entries are (true_frame_index, ...) and logic.decide turns those indices
into t_shot, which sets where the clip is cut. So an index-offset error here is
SILENTLY WRONG -- the shot is found, at the wrong time. That is what these
assert against.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agx_pipeline.shot_detect.live import (  # noqa: E402
    _join_window_track, _split_window_track,
)

SEG = 480          # frames per 4s segment at a 120fps lock


def _track(*idxs):
    """Detections shaped like scan_ball_and_hoops': (idx, x, y, rb, conf)."""
    return [(i, 100.0 + i, 200.0 + i, 5.0, 0.9) for i in idxs]


def test_join_offsets_only_the_second_segment():
    prev, own = _track(10, 470), _track(0, 12)
    out = _join_window_track(prev, own, SEG)
    assert [t[0] for t in out] == [10, 470, 480, 492]
    # payload must ride along untouched -- only the index moves
    assert out[2][1:] == own[0][1:]


def test_join_preserves_order_and_length():
    out = _join_window_track(_track(1, 2), _track(3), SEG)
    assert len(out) == 3
    assert [t[0] for t in out] == [1, 2, 483]


def test_split_recovers_the_second_segment():
    window = _join_window_track(_track(10, 470), _track(0, 12), SEG)
    assert _split_window_track(window, SEG) == _track(0, 12)


def test_split_drops_the_first_segment_entirely():
    window = _join_window_track(_track(0, 479), [], SEG)
    assert _split_window_track(window, SEG) == []


def test_roundtrip_is_identity_for_any_window():
    """split-then-join reproduces the window a concat scan would have given.
    This is the property the cache relies on."""
    window = _join_window_track(_track(0, 200, 479), _track(0, 1, 479), SEG)
    own = _split_window_track(window, SEG)
    assert _join_window_track(window[:3], own, SEG) == window


@pytest.mark.parametrize("boundary", [SEG - 1, SEG, SEG + 1])
def test_boundary_indices_land_on_the_right_side(boundary):
    """A detection exactly at the seam must not be double-counted or dropped."""
    window = [(boundary, 1.0, 2.0, 3.0, 0.9)]
    own = _split_window_track(window, SEG)
    assert len(own) == (0 if boundary < SEG else 1)
    if own:
        assert own[0][0] == boundary - SEG


def test_empty_inputs_are_safe():
    assert _join_window_track([], [], SEG) == []
    assert _split_window_track([], SEG) == []
    assert _join_window_track(_track(5), [], SEG) == _track(5)


def test_offset_error_would_be_caught():
    """Guard the guard: a wrong offset must change the output, so that a future
    refactor cannot silently break t_shot without a test failing."""
    right = _join_window_track(_track(1), _track(0), SEG)
    wrong = _join_window_track(_track(1), _track(0), SEG + 4)
    assert right != wrong, "a 4-frame offset error must be visible here"

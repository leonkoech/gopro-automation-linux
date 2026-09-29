"""The 2K ring sits UNDER players: back half behind the holder's legs, all of it behind anyone nearer."""
import os
import sys

import numpy as np
import pytest

cv2 = pytest.importorskip("cv2")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agx_pipeline", "possession_tracker"))
import render_2k as R  # noqa: E402

W, H = 192, 108          # frame; masks at half resolution like the tracker cache (ratio kept)


def person(x0, y0, x1, y1):
    m = np.zeros((H // 2, W // 2), bool)
    m[y0 // 2:y1 // 2, x0 // 2:x1 // 2] = True
    return {"box": np.array([x0, y0, x1, y1], float), "mask": m}


def occ(players, oid, feet_y):
    R.OCC_FEATHER = 0
    return R.occluders(players, oid, feet_y, (H, W, 3), (0, 0, W, H))


def test_owner_hides_ring_only_above_his_feet():
    o = occ({1: person(80, 20, 100, 80)}, 1, 80)
    assert o[60, 90] == 1          # leg, above the feet line: back arc goes behind
    assert o[85, 90] == 0          # below the feet: front arc stays on top
    assert o[60, 40] == 0          # empty floor


def test_player_nearer_camera_covers_ring_farther_one_does_not():
    ps = {1: person(80, 20, 100, 70), 2: person(40, 30, 60, 90), 3: person(120, 10, 140, 50)}
    o = occ(ps, 1, 70)
    assert o[80, 50] == 1          # #2 feet at 90 > 70: in front of the ring
    assert o[30, 130] == 0         # #3 feet at 50 < 70: behind the ring, ring drawn over him


def test_draw_ring_under_keeps_original_pixels_where_covered():
    im = np.full((H, W, 3), 100, np.uint8)
    poly = np.array([[70, 60], [110, 60], [110, 76], [70, 76]], np.int32)
    R.OCC_FEATHER = 0
    R.draw_ring_under(im, poly, (0, 255, 255), 1.0, {1: person(84, 10, 96, 68)}, 1, 68)
    assert (im[60, 90] == 100).all()      # back edge behind the leg: untouched
    assert not (im[76, 90] == 100).all()  # front edge drawn
    assert not (im[60, 72] == 100).all()  # back edge away from the leg drawn


def test_redistort_inverts_holder_undistort():
    import holder
    cal = {"division_lambda": -0.12, "principal_point": [960.0, 540.0], "image_size": [1920, 1080]}
    pts = np.array([[464.0, 885.0], [1800.0, 100.0], [960.0, 540.0]])
    back = R.redistort(cal, holder.undistort(cal, pts))
    assert np.abs(back - pts).max() < 0.5

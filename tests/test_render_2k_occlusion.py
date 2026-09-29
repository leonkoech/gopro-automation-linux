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


class _Cam:
    """Identity floor: court cm == image px, no lens distortion."""
    H = np.eye(3)
    cal = {"division_lambda": 0}

    def to_court(self, px):
        return np.asarray(px, float)


def test_sprite_quad_is_centred_on_the_feet_and_sized_by_outer_radius():
    q = R.sprite_quad(_Cam(), [500.0, 400.0], outer_cm=R.SPRITE_R_PX)
    assert np.allclose(q.mean(0), [500, 400])
    assert np.allclose(q[1, 0] - q[0, 0], R.SPRITE_PX)          # asset px == cm when outer_cm == R px


def test_sprite_is_blended_by_alpha_and_hidden_behind_the_holder():
    R.OCC_FEATHER = 0
    rgba = np.zeros((R.SPRITE_PX, R.SPRITE_PX, 4), np.uint8)
    rgba[..., 0] = 255                                          # blue, fully opaque
    rgba[..., 3] = 255
    im = np.full((H, W, 3), 100, np.uint8)
    quad = np.array([[60, 50], [120, 50], [120, 90], [60, 90]], np.float32)
    R.draw_sprite_under(im, rgba, quad, 1.0, {1: person(84, 10, 96, 70)}, 1, 70)
    assert (im[60, 90] == 100).all()                            # behind his leg, above the feet line
    assert im[60, 70, 0] == 255 and im[60, 70, 1] == 0          # floor next to him: the sprite
    assert im[80, 90, 0] == 255                                 # in front of his feet: the sprite
    assert (im[20, 20] == 100).all()                            # outside the quad: untouched


def test_sprite_stream_reads_forward_and_loops(tmp_path):
    import subprocess
    p = tmp_path / "ring.mov"
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i",
                    "color=c=red:s=%dx%d:d=0.125:r=24,format=rgba" % (R.SPRITE_PX, R.SPRITE_PX),
                    "-c:v", "png", str(p)], check=True)
    sp = R.SpriteStream(str(p))
    try:
        f0 = sp.frame(0)
        f2 = sp.frame(2)
        assert f0.shape == (R.SPRITE_PX, R.SPRITE_PX, 4) and f2[0, 0, 2] == 255   # BGRA: red
        assert sp.frame(7) is not None                          # past the 3-frame end: loops
        assert sp.frame(1) is not None                          # going back reopens
    finally:
        sp.close()


def test_missing_assets_fall_back_to_the_classic_ring(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "ASSETS", str(tmp_path))
    monkeypatch.setattr(R, "RING_STYLE", "sprite")
    assert R.load_sprites() is None

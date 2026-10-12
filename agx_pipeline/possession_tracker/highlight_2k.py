"""Turn one highlight clip into its 2K-style version (ring on the ball-holder, ball-centred
crop, score badge). Production entry point for the post-game highlight job.

    render_highlight_2k(clip_path, out_path) -> out_path or None

The highlight clip already has the scoring moment at PRE seconds (the cut is [T-5 s, T+3 s]),
which is also where the tracker expects the rim. Anything that fails returns None, and the
caller keeps the plain clip — a 2K render is never allowed to lose a highlight.
Only FL/FR clips are rendered (the court calibration covers those cameras).
"""
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

PRE = 5.0
CAMS = ("FL", "FR")


def camera_of(path: str):
    stem = os.path.splitext(os.path.basename(path))[0]
    cam = stem.rsplit("_", 1)[-1].upper()
    return cam if cam in CAMS else None


def render_highlight_2k(clip_path: str, out_path: str, vertical: bool = False):
    cam = camera_of(clip_path)
    if cam is None or not os.path.isfile(clip_path):
        return None
    import render_2k
    with tempfile.TemporaryDirectory(prefix="h2k_") as work:
        name = "%s_H2K" % cam                       # render_2k reads the camera from the name prefix
        clip = os.path.join(work, name + ".mp4")
        shutil.copyfile(clip_path, clip)
        cp = subprocess.run(["python3", os.path.join(HERE, "perceive_fast.py"), clip, work, "%.2f" % PRE],
                            capture_output=True, text=True, timeout=300)
        if cp.returncode != 0 or not os.path.isfile(os.path.join(work, "cache", name + ".npz")):
            return None
        dst = os.path.join(work, "r2k")
        try:
            render_2k.render(work, work, name, dst, vertical, make=True)   # highlights are scores
        except Exception:  # noqa: BLE001 - keep the plain clip
            return None
        made = os.path.join(dst, name + "_2k.mp4")
        if not os.path.isfile(made) or os.path.getsize(made) == 0:
            return None
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        shutil.move(made, out_path)
        if vertical and os.path.isfile(os.path.join(dst, name + "_2k_vertical.mp4")):
            shutil.move(os.path.join(dst, name + "_2k_vertical.mp4"), out_path.replace(".mp4", "_vertical.mp4"))
    return out_path


if __name__ == "__main__":
    print(render_highlight_2k(sys.argv[1], sys.argv[2], vertical=len(sys.argv) > 3))

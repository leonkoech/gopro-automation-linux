#!/usr/bin/env python3
"""
Pre-flight for SHOT_LIVE_WINDOW_CACHE: does a split scan reproduce a concat scan?

The cache replaces "concatenate the previous segment onto this one and scan the
pair" with "scan each segment once and join their TRACKS". The unit tests prove
the join arithmetic; they cannot prove that scanning A and B separately gives
what scanning concat(A,B) gives on real footage. Only this does, and it needs no
game -- two consecutive segments and the detector.

It compares the two paths three ways, in increasing order of what actually
matters:

  1 track     same detections, same frame indices, same boxes?
  2 verdicts  same MAKE/MISS decisions out of logic.decide?
  3 t_shot    same SHOT TIMES? This is the one that cuts the clip. An index
              offset error finds the shot at the wrong moment -- the failure
              mode with no symptom except a clip of the wrong play.

Run it on REAL splitmuxsink segments, not ffmpeg-cut ones: the nominal 480-frame
offset the cache assumes is a property of how splitmuxsink cuts, and cut files
will not exercise it.

STANDALONE ON PURPOSE. It inlines the join rather than importing it from
live.py, so it runs on the AGX from a paste with nothing checked out -- that box
runs games nightly and switching branches there would swap the live detector
under the running service.

    python3 scripts/perf/window_cache_verify.py --segments /home/dev/app/recordings/<game>/shot_seg
    python3 scripts/perf/window_cache_verify.py --a seg_1_SL.mp4 --b seg_2_SL.mp4
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SEG_RE = re.compile(r"^seg_(\d{5,})_([A-Z]{2})\.mp4$")


def _join_window_track(prev_track, own_track, offset):
    """MUST stay identical to _join_window_track in
    agx_pipeline/shot_detect/live.py. Inlined, not imported, so this runs on the
    AGX without a checkout. Two lines, and the unit tests on that function are
    the guard against the two copies drifting apart."""
    return list(prev_track) + [(t[0] + offset,) + tuple(t[1:]) for t in own_track]


def concat(a: str, b: str, out: str) -> None:
    lst = out + ".txt"
    with open(lst, "w") as f:
        f.write("file '%s'\nfile '%s'\n" % (os.path.abspath(a), os.path.abspath(b)))
    subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-f", "concat",
                    "-safe", "0", "-i", lst, "-c", "copy", "-map", "0:v", out],
                   check=True, stdin=subprocess.DEVNULL)
    os.remove(lst)


def nframes(path: str) -> int:
    out = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0",
                          "-count_frames", "-show_entries", "stream=nb_read_frames",
                          "-of", "csv=p=0", path], capture_output=True, text=True).stdout
    return int("".join(c for c in out if c.isdigit()) or 0)


def verdicts(track, rim, fps):
    from agx_pipeline.shot_detect import logic
    G = logic.Geo.from_rim(rim, float(fps))
    return [(round(float(v.get("t", 0.0)), 3), v["verdict"])
            for v in logic.decide(G, track) if "verdict" in v]


def compare(a: str, b: str, det, scan, rim, fps, stride, imgsz) -> bool:
    na, nb = nframes(a), nframes(b)
    tmp = tempfile.mkdtemp(prefix="wcv_")
    ab = f"{tmp}/ab.mp4"
    try:
        concat(a, b, ab)
        w_concat, _ = scan.scan_ball_and_hoops(det.model, ab, det.device,
                                               stride=stride, imgsz=imgsz)
        ta, _ = scan.scan_ball_and_hoops(det.model, a, det.device,
                                         stride=stride, imgsz=imgsz)
        tb, _ = scan.scan_ball_and_hoops(det.model, b, det.device,
                                         stride=stride, imgsz=imgsz)
    finally:
        for p in (ab,):
            try:
                os.remove(p)
            except OSError:
                pass
        try:
            os.rmdir(tmp)
        except OSError:
            pass

    nominal = int(round(4.0 * float(fps)))
    print(f"  frames: A={na} B={nb}   offset used={nominal} "
          f"({'EXACT' if na == nominal else f'off by {na - nominal:+d} vs actual A'})")

    ok = True
    for label, off in (("nominal", nominal), ("actual A frames", na)):
        w_cache = _join_window_track(ta, tb, off)
        same_n = len(w_cache) == len(w_concat)
        idx_c = [t[0] for t in w_concat]
        idx_k = [t[0] for t in w_cache]
        max_d = max((abs(x - y) for x, y in zip(idx_c, idx_k)), default=0) if same_n else None
        v_c, v_k = verdicts(w_concat, rim, fps), verdicts(w_cache, rim, fps)
        t_d = (max((abs(x[0] - y[0]) for x, y in zip(v_c, v_k)), default=0.0)
               if len(v_c) == len(v_k) else None)
        print(f"  offset={label:<16} detections {len(w_concat)} vs {len(w_cache)}"
              f"   max index delta {max_d}   verdicts {len(v_c)} vs {len(v_k)}"
              f"   max t_shot delta {t_d if t_d is None else f'{t_d:.3f}s'}")
        if label == "nominal":
            ok = same_n and max_d == 0 and v_c == v_k
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify the window-cache equivalence")
    ap.add_argument("--segments", help="a shot_seg dir of seg_<epoch>_<ANGLE>.mp4")
    ap.add_argument("--a"); ap.add_argument("--b")
    ap.add_argument("--fps", type=float, default=119.8965)
    ap.add_argument("--pairs", type=int, default=3, help="how many consecutive pairs")
    o = ap.parse_args()

    stride = int(os.getenv("SHOT_LIVE_STRIDE", os.getenv("SHOT_AUTO_STRIDE", "4")))
    imgsz = int(os.getenv("SHOT_DET_IMGSZ", "640"))

    pairs = []
    if o.a and o.b:
        pairs = [(o.a, o.b, os.path.basename(o.a)[-6:-4])]
    elif o.segments:
        byang = {}
        for fn in sorted(os.listdir(o.segments)):
            m = SEG_RE.match(fn)
            if m:
                byang.setdefault(m.group(2), []).append(os.path.join(o.segments, fn))
        for ang, fs in byang.items():
            for i in range(min(o.pairs, len(fs) - 1)):
                pairs.append((fs[i], fs[i + 1], ang))
    if not pairs:
        raise SystemExit("no segment pairs found — pass --segments <dir> or --a/--b")

    from agx_pipeline.shot_detect import node
    from agx_pipeline.shot_detect.backtest import scan
    from agx_pipeline.shot_detect.detect import ShotDetector

    rims = json.load(open(ROOT / "agx_pipeline/shot_detect/rims.json"))
    det = ShotDetector(os.getenv("SHOT_DET_WEIGHT", node._DEFAULT_WEIGHT))
    print(f"stride {stride}  imgsz {imgsz}  fps {o.fps}  pairs {len(pairs)}\n")

    allok = True
    for a, b, ang in pairs:
        rim = rims.get(ang)
        if rim is None:
            print(f"{os.path.basename(a)} -> no rim for angle {ang}, skipped")
            continue
        print(f"{ang}  {os.path.basename(a)} + {os.path.basename(b)}")
        allok &= compare(a, b, det, scan, rim, o.fps, stride, imgsz)
        print()
    det.empty_cache()

    print("EQUIVALENT — the cache reproduces the concat window exactly."
          if allok else
          "DIFFERENCES FOUND — read the t_shot delta above. Anything non-zero there\n"
          "moves where the clip is cut, and the cache must not ship until it is zero\n"
          "or understood. Compare the 'nominal' and 'actual A frames' rows: if only\n"
          "nominal differs, the 480-frame assumption is the cause.")
    return 0 if allok else 1


if __name__ == "__main__":
    raise SystemExit(main())

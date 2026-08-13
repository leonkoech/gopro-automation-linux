"""P1.0 acceptance: does the windowed detector reproduce the full-runner verdicts?

For a game with known verdicts (makemiss_v43/<video>_full_verdicts.json from the
fusion repo), take a sample of real shots, cut a window around each shot's
cross_frame straight from the source mp4, run ShotDetector.detect on it, and
compare MAKE/MISS. High agreement => the AGX windowed port decides like the
validated 99-100% runner.

Run on a box/GPU env with the source video + the v3 weight:

  python -m agx_pipeline.shot_detect.acceptance \\
      --video   /path/2026-07-20_ba4d38fa_SL_highfps.mp4 \\
      --verdicts /path/makemiss_v43/ba4d38fa_SL_full_verdicts.json \\
      --rim-key 2026-07-20_ba4d38fa_SL_highfps.mp4 \\
      --weight  /path/ball_yolo26s_gray_hifps_v3_best.pt \\
      --fps 120 --before 1.8 --after 0.8 --sample 25
"""
from __future__ import annotations

import argparse
import json
import os

from agx_pipeline.shot_detect.detect import ShotDetector, read_window

HERE = os.path.dirname(__file__)


def _load_rim(rim_key: str) -> dict:
    with open(os.path.join(HERE, "rims.json")) as fh:
        rims = json.load(fh)
    if rim_key not in rims:
        raise SystemExit(f"rim key {rim_key!r} not in rims.json ({list(rims)})")
    return rims[rim_key]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--video", required=True)
    ap.add_argument("--verdicts", required=True, help="makemiss_v43 *_full_verdicts.json")
    ap.add_argument("--rim-key", required=True)
    ap.add_argument("--weight", required=True)
    ap.add_argument("--fps", type=float, default=120.0)
    ap.add_argument("--before", type=float, default=1.8, help="window seconds before crossing")
    ap.add_argument("--after", type=float, default=0.8, help="window seconds after crossing")
    ap.add_argument("--sample", type=int, default=25, help="max shots to test")
    a = ap.parse_args()

    rim = _load_rim(a.rim_key)
    d = json.load(open(a.verdicts))
    results = d.get("results") or (d if isinstance(d, list) else [])
    shots = [r for r in results
             if isinstance(r, dict) and r.get("verdict") in ("MAKE", "MISS")
             and r.get("cross_frame") is not None]
    if a.sample and len(shots) > a.sample:
        step = len(shots) / a.sample
        shots = [shots[int(i * step)] for i in range(a.sample)]

    det = ShotDetector(a.weight)
    nb, na = int(a.before * a.fps), int(a.after * a.fps)
    agree = tested = 0
    for r in shots:
        fcr = int(r["cross_frame"])
        lo, hi = max(0, fcr - nb), fcr + na
        frames = read_window(a.video, lo, hi)
        if not frames:
            print(f"  shot @f{fcr}: no frames (past EOF?) — skip")
            continue
        v = det.detect(frames, rim, fps=a.fps, target_idx=fcr - lo)
        got = v["verdict"] if v else "NONE"
        want = r["verdict"]
        ok = got == want
        tested += 1
        agree += ok
        flag = "" if ok else "  <-- DIFF"
        print(f"  @f{fcr} {r.get('mmss','')}: want={want:4s} got={got:4s} "
              f"(by {v['decided_by'] if v else '-'} rho={v['rho'] if v else '-'}){flag}")
    print(f"\nAGREEMENT: {agree}/{tested}"
          + (f"  ({100*agree/tested:.0f}%)" if tested else ""))


if __name__ == "__main__":
    main()

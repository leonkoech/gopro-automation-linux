"""TYPE-stage eval: score the production shot classifier against GT classes.

For every in-game manual GT shot (2/3/4PT and FREE_THROW, make+miss), cut a
12s FL/FR window around the card time (rim anchor 8.0s into the clip) and run
the EXACT production typing chain (agx_classify.py, same env recipe as
LiveTyper). Compare predicted zone points vs the GT class.

FREE_THROW rows are scored expected=1pt — the chain has no FT class yet, so
they all fail; that bucket IS the measurement (how much the FT class will
recover). Disagreement clips are kept (capped) for review.

Usage: python3 type_eval.py --label s3_xxx --uball-game-id <uuid> --rec-dir DIR
Report: /home/dev/gt_eval/{label}_type_report.json
"""
import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from glob import glob

sys.path.insert(0, "/home/dev")
sys.path.insert(0, "/home/dev/gopro-automation-linux")
from gt_eval import load_gt  # noqa: E402  (same-box module; reuses UballClient)

OUT = "/home/dev/gt_eval"
TYPING_CWD = os.getenv("SHOT_TYPING_CWD", "/home/dev/shot_typing")
PRE_S = 8.0            # rim anchor sits this far into the cut clip
POST_S = 4.0
CLASSIFY_TIMEOUT_S = 240
MAX_KEEP_CLIPS = 40

EXPECTED_PTS = {"FREE_THROW": 1, "FG": 2, "2PT": 2, "3PT": 3, "4PT": 4}
ZONE_PTS = {"2PT": 2, "3PT": 3, "4PT": 4}


def log(m):
    print(f"[type_eval {datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


def classify_env():
    """Same CUDA/knob recipe as production LiveTyper._classify_env."""
    env = os.environ.copy()
    nvlibs = ":".join(glob(
        "/home/dev/.local/lib/python3.10/site-packages/nvidia/*/lib"))
    env["LD_LIBRARY_PATH"] = (
        f"{nvlibs}:/usr/local/cuda-12.6/targets/aarch64-linux/lib:"
        f"/usr/local/cuda-12.6/lib64:" + env.get("LD_LIBRARY_PATH", ""))
    env["SHOT_ATTRIB"] = "possession"
    env["SHOT_FEET"] = "bbox"
    env["SHOT_RIM_TS"] = f"{PRE_S:.2f}"
    return env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--uball-game-id", required=True)
    ap.add_argument("--rec-dir", required=True)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()

    gt = load_gt(a.uball_game_id)
    if not gt:
        log("GT EMPTY — backend down? aborting")
        sys.exit(2)
    if a.limit:
        gt = gt[:a.limit]
    log(f"GT: {len(gt)} shots")

    clip_dir = f"{OUT}/{a.label}/type_clips"
    os.makedirs(clip_dir, exist_ok=True)
    env = classify_env()

    matrix = defaultdict(lambda: defaultdict(int))
    rows, kept = [], 0
    for i, g in enumerate(gt):
        zone_gt = g["cls"].rsplit("_", 1)[0]         # e.g. FREE_THROW / 3PT / FG
        exp = EXPECTED_PTS.get(zone_gt)
        if exp is None:
            continue
        angle = "FL" if g["side"] == "left" else "FR"
        src = f"{a.rec_dir}/{a.label}/{a.label}_{angle}.mp4"
        t0 = max(0.0, g["ts"] - PRE_S)
        clip = f"{clip_dir}/t{int(g['ts'])}_{angle}.mp4"
        cut = subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{t0:.2f}", "-i", src,
             "-t", f"{PRE_S + POST_S:.1f}", "-c:v", "libx264", "-preset",
             "veryfast", "-pix_fmt", "yuv420p", clip],
            capture_output=True, text=True)
        if cut.returncode != 0 or not os.path.isfile(clip):
            rows.append({"ts": g["ts"], "gt": g["cls"], "pred": "CUT_FAIL"})
            continue
        try:
            cp = subprocess.run(
                ["python3", "agx_classify.py", angle, clip, f"{PRE_S:.2f}",
                 f"te_{int(g['ts'])}"],
                cwd=TYPING_CWD, env=env, capture_output=True, text=True,
                timeout=CLASSIFY_TIMEOUT_S)
            m = re.search(r"ZONE_NEW=(\w+)", cp.stdout)
            zone = m.group(1) if m else None
        except subprocess.TimeoutExpired:
            zone = "TIMEOUT"
        pred_pts = ZONE_PTS.get(zone)
        agree = pred_pts == exp
        matrix[zone_gt][zone or "NONE"] += 1
        rows.append({"ts": g["ts"], "gt": g["cls"], "pred": zone,
                     "agree": agree})
        if agree or kept >= MAX_KEEP_CLIPS:
            os.unlink(clip)
        else:
            kept += 1
            os.rename(clip, f"{clip_dir}/T{len(rows):03d}_GT-{g['cls']}_CV-{zone}_t{int(g['ts'])}.mp4")
        if (i + 1) % 10 == 0:
            done = [r for r in rows if "agree" in r]
            acc = sum(1 for r in done if r["agree"]) / max(1, len(done))
            log(f"{i+1}/{len(gt)} typed — running acc {acc:.0%}")

    scored = [r for r in rows if "agree" in r]
    non_ft = [r for r in scored if not r["gt"].startswith("FREE_THROW")]
    ft = [r for r in scored if r["gt"].startswith("FREE_THROW")]
    report = {
        "label": a.label, "uball_game_id": a.uball_game_id,
        "n_scored": len(scored),
        "accuracy_all": round(sum(r["agree"] for r in scored) / max(1, len(scored)), 4),
        "accuracy_excl_ft": round(sum(r["agree"] for r in non_ft) / max(1, len(non_ft)), 4),
        "ft_rows": len(ft),
        "matrix": {k: dict(v) for k, v in matrix.items()},
        "rows": rows,
    }
    with open(f"{OUT}/{a.label}_type_report.json", "w") as f:
        json.dump(report, f, indent=1)
    log(f"===== TYPE SUMMARY {a.label} =====")
    log(f"scored {len(scored)} | acc(all) {report['accuracy_all']:.1%} | "
        f"acc(excl FT) {report['accuracy_excl_ft']:.1%} | FT rows {len(ft)}")
    for gt_z, preds in sorted(matrix.items()):
        log(f"  {gt_z}: {dict(preds)}")
    log("TYPE_EVAL_DONE")


if __name__ == "__main__":
    main()

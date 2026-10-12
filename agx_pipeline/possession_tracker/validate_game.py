"""Validate shot typing on an ANNOTATED game the tracker has never seen, driven by the
annotators' own shot list (no CV cards needed).

For every manual shot card: cut [t - 5 s, t + 3 s] from the annotation tool's own FL/FR video
(the clock the card was made on), then type it with
  prod   production's live classifier, exactly as deployed (agx_classify.py with the service's
         _classify_env), rim anchored on the tracker's rim time when it has one
  fast   the fast tracker (YOLO-seg + ByteTrack)
  sam3   SAM3, only where fast != prod (the deployed escalation rule)
  final  fast, or sam3 where it was called
and score each against the annotator's type. Feet are also measured against the court lines
(signed px to the nearest line), so a calibration problem shows up as errors packed at a line.

Usage: validate_game.py <uball_game_id> [--limit N]   (run in /home/dev/possession, CUDA env)
Writes /home/dev/validate/<uuid>/results.jsonl (resumable) and prints a summary.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")

import track_shadow as T  # noqa: E402  (cut_clip, type_with, load_arcs, recording, TYPE_OF)

VAL = "/home/dev/validate"
TYPE_OF = {"FG": "2PT", "2PT": "2PT", "3PT": "3PT", "4PT": "4PT", "FREE_THROW": "FREE_THROW"}


def gt_shots(ug):
    from uball_client import UballClient
    out = []
    for p in UballClient().list_plays(ug):
        if (p.get("source") or "").lower() == "cv" or p.get("timestamp_seconds") is None:
            continue
        cl = str(p.get("classification") or "")
        for res in ("MAKE", "MISS"):
            if cl.endswith("_" + res) and TYPE_OF.get(cl[:-5]):
                out.append({"id": p["id"], "t": float(p["timestamp_seconds"]), "type": TYPE_OF[cl[:-5]],
                            "result": res, "side": (p.get("angle") or "LEFT").upper()})
    return sorted(out, key=lambda g: g["t"])


def prod_type(clip, rim_t):
    """Production's live classifier on the same clip, same env as the service."""
    from agx_pipeline.shot_typing_live import _classify_env, decide_zone, TYPING_CWD
    env = _classify_env()
    env["SHOT_RIM_TS"] = "%.2f" % rim_t
    cp = subprocess.run(["python3", "agx_classify.py", os.path.basename(clip).split("_")[0], clip,
                         "%.2f" % rim_t, "val"], cwd=TYPING_CWD, env=env, capture_output=True, text=True,
                        timeout=300)
    zone, conf, src = decide_zone(cp.stdout, cp.returncode)
    return zone if zone in ("2PT", "3PT", "4PT", "FREE_THROW") else None


def line_dist(cam, feet):
    arcs = T.load_arcs(cam, 1920)
    pt = (float(feet[0]), float(feet[1]))
    d = [cv2.pointPolygonTest(arcs[k], pt, True) for k in ("three_pt_white", "four_pt_red") if k in arcs]
    return round(float(min(d, key=abs)), 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ug")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    d = os.path.join(VAL, a.ug)
    shots = gt_shots(a.ug)
    if a.limit:
        shots = shots[:a.limit]
    res_path = os.path.join(d, "results.jsonl")
    done = {json.loads(l)["gt_id"] for l in open(res_path)} if os.path.exists(res_path) else set()
    print("[val] %s: %d GT shots, %d done" % (a.ug[:8], len(shots), len(done)), flush=True)
    with tempfile.TemporaryDirectory(prefix="val_") as work, open(res_path, "a") as fo:
        for i, g in enumerate(shots):
            if g["id"] in done:
                continue
            while T.recording():
                time.sleep(60)
            t0 = time.time()
            cam = "FL" if g["side"] == "LEFT" else "FR"
            clip = os.path.join(work, "%s_v%04d.mp4" % (cam, i))
            rec = {"gt_id": g["id"], "t": g["t"], "cam": cam, "gt": g["type"], "result": g["result"]}
            try:
                if not T.cut_clip(os.path.join(d, cam + ".mp4"), g["t"], clip):
                    raise RuntimeError("cut failed")
                fast = T.type_with(clip, cam, work, "fast")
                rec["fast"] = fast.get("track_zone")
                rec["fast_feet"] = fast.get("feet_px")
                rec["rim_t"] = fast.get("rim_t")
                rec["prod"] = prod_type(clip, fast.get("rim_t") or T.PRE)
                rec["final"] = rec["fast"]
                if rec["fast"] != rec["prod"]:
                    sam = T.type_with(clip, cam, work, "sam3")
                    rec["sam3"] = sam.get("track_zone")
                    rec["sam3_feet"] = sam.get("feet_px")
                    if rec["sam3"]:
                        rec["final"] = rec["sam3"]
                feet = rec.get("sam3_feet") if rec.get("sam3") else rec.get("fast_feet")
                if feet:
                    rec["line_px"] = line_dist(cam, feet)
            except Exception as e:  # noqa: BLE001
                rec["error"] = "%s: %s" % (type(e).__name__, e)
            finally:
                import shutil
                for f in os.listdir(work):
                    p = os.path.join(work, f)
                    shutil.rmtree(p) if os.path.isdir(p) else os.remove(p)
            rec["secs"] = round(time.time() - t0, 1)
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
            print("[val] %d/%d %s %s %s | prod %s fast %s%s -> %s (%.0fs)%s" % (
                i + 1, len(shots), cam, rec["gt"], rec["result"], rec.get("prod"), rec.get("fast"),
                " sam3 %s" % rec.get("sam3") if "sam3" in rec else "", rec.get("final"), rec["secs"],
                " ERR " + rec["error"] if "error" in rec else ""), flush=True)
    summarize(res_path)


def summarize(path):
    rows = [json.loads(l) for l in open(path) if l.strip()]
    ok = [r for r in rows if "error" not in r]
    n = len(ok)
    if not n:
        return
    s = {k: sum(r.get(k) == r["gt"] for r in ok) for k in ("prod", "fast", "final")}
    esc = sum("sam3" in r for r in ok)
    print("[val] SUMMARY %d shots (%d errors): prod %d (%.0f%%)  fast %d (%.0f%%)  final %d (%.0f%%)  sam3 called %d"
          % (n, len(rows) - n, s["prod"], 100 * s["prod"] / n, s["fast"], 100 * s["fast"] / n,
             s["final"], 100 * s["final"] / n, esc), flush=True)
    for t in ("2PT", "3PT", "4PT", "FREE_THROW"):
        rr = [r for r in ok if r["gt"] == t]
        if rr:
            print("[val]   %-10s %3d  prod %3d  final %3d" % (t, len(rr), sum(r.get("prod") == t for r in rr),
                                                          sum(r.get("final") == t for r in rr)), flush=True)
    wrong = [r for r in ok if r.get("final") != r["gt"] and r.get("line_px") is not None]
    if wrong:
        near = sum(abs(r["line_px"]) < 40 for r in wrong)
        print("[val]   final errors with feet within 40px of a line: %d/%d (calibration-sensitive)" % (near, len(wrong)),
              flush=True)


if __name__ == "__main__":
    main()

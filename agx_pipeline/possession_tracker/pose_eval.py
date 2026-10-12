"""Pose-based shooter selection, scored against the automatic shooter labels.

The ball is often hidden against the shooter's body as he gathers, so "whose outline touches the
ball" misses him (22 of 51 wrong-shooter cases). Instead: whose WRISTS are at the ball. For each
frame in the ~3 s before the ball reaches the rim where the ball is detected, every player whose
(padded) box contains the ball gets an RTMPose pass; hold = min wrist-to-ball distance / box
height. Rules compared (shooter = ...):
  latest_hold   the player with the LATEST frame where hold < HOLD_T (the last hands on the ball)
  min_dist      the player with the smallest hold distance overall
Both fall back to the current rule when no one qualifies.

Measured 2026-09-30 on 329 auto-labelled shots (current rule 237): as a replacement every
variant is worse (best: min_dist, 22 fixed / 28 broken); as a tiebreaker only when the current
pick's hands never come near the ball, +2 at best. NOT adopted: on these far cameras the wrists
are small and noisy, and the gather that hides the ball hides the hands too.

Usage: pose_eval.py   (box, /home/dev/possession; RTMPose on CPU via rtmlib)
"""
import json
import os
import sys
from collections import Counter

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402

VAL = "/home/dev/validate"
WIN_S = 3.0
PAD = 0.3
L_WRIST, R_WRIST = 9, 10
KP_MIN = 0.3


def main():
    from rtmlib import RTMPose
    pose = RTMPose(onnx_model="https://download.openmmlab.com/mmpose/v1/projects/rtmposev1/onnx_sdk/"
                              "rtmpose-m_simcc-body7_pt-body7_420e-256x192-e48f03d0_20230504.zip",
                   model_input_size=(192, 256), backend="onnxruntime", device="cpu")
    rows = [json.loads(l) for l in open(os.path.join(HERE, "shooter_labels_auto.jsonl"))]
    out = []
    for r in rows:
        if not r.get("label_tracks"):
            continue
        d = os.path.join(VAL, r["game"])
        name = "%s_t%07.1f" % (r["cam"], r["t"])
        try:
            S = H.load(d, name)
            res = H.analyse(S, H.Camera(r["cam"]))
        except Exception:  # noqa: BLE001
            continue
        shots = [x for x in res.get("shots", []) if x.get("shooter") is not None]
        if not shots:
            continue
        sh = min(shots, key=lambda x: abs(x["rim_t"] - T.PRE))
        ri = sh["rim_i"]
        fr = res["frames"]
        t = S["times"]
        ks = [k for k in range(ri + 1) if t[ri] - t[k] <= WIN_S and "ball_px" in fr[k]]
        cap = cv2.VideoCapture(os.path.join(d, "clips", name + ".mp4"))
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        holds = []                                       # (k, oid, dist)
        for k in ks:
            u, v = fr[k]["ball_px"]
            cands = []
            for oid, p in S["players"][k].items():
                b = p["box"]
                w, h = b[2] - b[0], b[3] - b[1]
                if b[0] - PAD * w <= u <= b[2] + PAD * w and b[1] - PAD * h <= v <= b[3]:
                    cands.append((oid, b))
            if not cands:
                continue
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(round(t[k] * fps)))
            ok, im = cap.read()
            if not ok:
                continue
            kpts, ksc = pose(im, bboxes=[[float(x) for x in b] for _, b in cands])
            for (oid, b), kp, sc in zip(cands, kpts, ksc):
                h = b[3] - b[1]
                ds = [np.hypot(kp[j][0] - u, kp[j][1] - v) / h for j in (L_WRIST, R_WRIST) if sc[j] >= KP_MIN]
                if ds:
                    holds.append((k, oid, min(ds)))
        cap.release()
        out.append({"key": [r["game"], r["t"]], "labels": [str(x) for x in r["label_tracks"]],
                    "current": str(sh["shooter"]), "holds": holds})
    json.dump(out, open(os.path.join(HERE, "pose_holds.json"), "w"))
    score(out)


def score(out):
    lab = lambda o, s: s is not None and str(s) in o["labels"]
    base = sum(lab(o, o["current"]) for o in out)
    print("[pose] %d labelled shots; current rule %d (%.1f%%)" % (len(out), base, 100 * base / len(out)), flush=True)
    for thr in (0.08, 0.12, 0.18, 0.25):
        for rule in ("latest_hold", "min_dist"):
            ok = fx = br = 0
            for o in out:
                hs = [h for h in o["holds"] if h[2] < thr]
                if rule == "latest_hold" and hs:
                    pick = max(hs, key=lambda h: (h[0], -h[2]))[1]
                elif rule == "min_dist" and hs:
                    pick = min(hs, key=lambda h: h[2])[1]
                else:
                    pick = o["current"]
                good = lab(o, pick)
                ok += good
                fx += good and not lab(o, o["current"])
                br += (not good) and lab(o, o["current"])
            print("[pose] %-11s thr %.2f -> %d (%.1f%%)  fixed %d broke %d" % (rule, thr, ok, 100 * ok / len(out), fx, br),
                  flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--score":
        score(json.load(open(os.path.join(HERE, "pose_holds.json"))))
    else:
        main()

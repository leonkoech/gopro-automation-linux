"""WHO with the shooter tracked beyond the clip: same camera, back (and then forward) in time,
stopping at the first confident number.

Per annotated shot (who_gt.json) of the validation games:
  1. the clip's tracker gives the shooter's box at the start of the clip (first tracked frame)
  2. read the jersey inside the clip as who_eval does; if it speaks, stop
  3. otherwise walk BACK in STEP_S chunks (up to BACK_S): fast perception (YOLO-seg + ByteTrack)
     on the chunk run in reverse, seeded by IoU with the shooter's box at the chunk boundary,
     so the same person is followed; read his jersey; stop at the first number seen twice
     (two agreeing confident reads)
  4. then forward after the clip (up to FWD_S) the same way
Scores coverage / precision exactly like who_eval so the two are comparable.

Usage: who_extend.py [--back 15 --fwd 5 --step 2.5] (box; CUDA env + jersey stack on PYTHONPATH)
"""
import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/who_deps_staging/pysrc")

import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402
import who_eval as W  # noqa: E402

VAL = "/home/dev/validate"
FPS = 15
IOU_MIN = 0.3
AGREE = 2            # number must be read this many times (confident) to stop


def iou(a, b):
    ix = max(0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    u = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / u if u > 0 else 0.0


def frames(video, t0, t1, reverse):
    cap = cv2.VideoCapture(video)
    cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, t0) * 1000)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    step = max(1, int(round(fps / FPS)))
    out, i = [], 0
    while True:
        ok, im = cap.read()
        if not ok:
            break
        t = t0 + i / fps
        if t > t1:
            break
        if i % step == 0:
            out.append((t, im))
        i += 1
    cap.release()
    return out[::-1] if reverse else out


def follow(model, chunk, seed_box):
    """Track through the chunk (already in walking order) with ByteTrack; keep the id whose box
    overlaps the seed box on the first frame, follow it; return [(t, box, crop)] and the last box."""
    model.predictor = None                     # fresh tracker state per chunk
    target, path, last = None, [], seed_box
    for t, im in chunk:
        r = model.track(im, persist=True, tracker="bytetrack.yaml", classes=[0], imgsz=1280,
                        conf=0.25, verbose=False)[0]
        if r.boxes is None or r.boxes.id is None:
            continue
        ids = r.boxes.id.int().cpu().tolist()
        bx = r.boxes.xyxy.cpu().numpy()
        if target is None:
            best = max(range(len(ids)), key=lambda j: iou(bx[j], last), default=None)
            if best is None or iou(bx[best], last) < IOU_MIN:
                continue
            target = ids[best]
        if target not in ids:
            continue
        b = bx[ids.index(target)]
        last = b
        x0, y0, x1, y1 = [int(v) for v in b]
        x0, y0 = max(0, x0), max(0, y0)
        if x1 - x0 > 8 and y1 - y0 > 16:
            path.append((t, b, im[y0:y1, x0:x1].copy()))
    return path, last, target is not None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--back", type=float, default=15)
    ap.add_argument("--fwd", type=float, default=5)
    ap.add_argument("--step", type=float, default=2.5)
    ap.add_argument("--out", default="who_extend.jsonl")
    a = ap.parse_args()
    from ultralytics import YOLO
    from uball_cc.tracking.jersey_stack import JerseyStack
    js = JerseyStack()
    model = YOLO(os.path.join(HERE, "weights", "yolo11s-seg.pt"))
    reads_in_clip = {(j["game"], j["t"]): j for j in map(json.loads, open(os.path.join(HERE, "who_reads.jsonl")))}
    G = json.load(open(os.path.join(HERE, "who_gt.json")))
    done = set()
    if os.path.exists(a.out):
        done = {(j["game"], j["t"]) for j in map(json.loads, open(a.out))}
    with open(a.out, "a") as fo:
        for s in G["shots"]:
            key = (s["game"], s["t"])
            if key in done:
                continue
            base = reads_in_clip.get(key, {})
            rec = {"game": s["game"], "t": s["t"], "gt": str(s["jersey"]), "team": s["team"],
                   "reads": list(base.get("reads") or []), "extended_s": 0.0}
            if "reads" not in base:
                rec["skip"] = base.get("skip", "no clip reads")
                fo.write(json.dumps(rec) + "\n")
                continue
            if W.speak(rec["reads"], None) is None:
                cam = "FL" if (s["side"] or "LEFT").upper() == "LEFT" else "FR"
                d = os.path.join(VAL, s["game"])
                video = os.path.join(d, cam + ".mp4")
                try:
                    boxes, _ = W_shooter(d, cam, s["t"])
                except Exception as e:  # noqa: BLE001
                    boxes, rec["error"] = None, str(e)
                if boxes:
                    clip_t0 = s["t"] - T.PRE
                    for direction, limit, t_edge, seed in (
                            ("back", a.back, clip_t0 + boxes[0][0], boxes[0][1]),
                            ("fwd", a.fwd, clip_t0 + boxes[-1][0], boxes[-1][1])):
                        walked, edge, box = 0.0, t_edge, seed
                        while walked < limit and W.speak(rec["reads"], None) is None:
                            if direction == "back":
                                chunk = frames(video, edge - a.step, edge, reverse=True)
                                edge -= a.step
                            else:
                                chunk = frames(video, edge, edge + a.step, reverse=False)
                                edge += a.step
                            walked += a.step
                            path, box, held = follow(model, chunk, box)
                            if not held:
                                rec["lost_" + direction] = walked
                                break
                            got = js.read_crops([c for _, _, c in path]) if path else []
                            rec["reads"] += [(str(n), float(c or 0)) for n, c in got if n is not None]
                        rec["extended_s"] += walked
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
    summarize(a.out)


def W_shooter(d, cam, t):
    name = "%s_t%07.1f" % (cam, t)
    boxes, shot = W.shooter_boxes(d, name, cam)
    return boxes, shot


def summarize(path):
    G = json.load(open(os.path.join(HERE, "who_gt.json")))
    team_roster = defaultdict(set)
    for r in G["rosters"]:
        team_roster[(r["game"], r["team"])].add(str(r["jersey"]))
    rows = [json.loads(l) for l in open(path)]
    n = len(rows)
    for label, allowed in (("any", lambda r: None), ("shooting-team roster", lambda r: team_roster[(r["game"], r["team"])])):
        spoke = right = 0
        for r in rows:
            if "reads" not in r or r.get("skip"):
                continue
            ans = W.speak(r["reads"], allowed(r))
            spoke += ans is not None
            right += ans == r["gt"]
        print("[ext] %-22s spoke %3d/%d (%.0f%%)  right %3d  precision %.0f%%  right of all %.0f%%"
              % (label, spoke, n, 100 * spoke / n, right, 100 * right / max(1, spoke), 100 * right / n), flush=True)
    ext = [r for r in rows if r.get("extended_s")]
    print("[ext] extended %d shots, mean %.1f s; lost the player on %d" % (
        len(ext), np.mean([r["extended_s"] for r in ext]) if ext else 0,
        sum(1 for r in rows if r.get("lost_back") or r.get("lost_fwd"))), flush=True)


if __name__ == "__main__":
    main()

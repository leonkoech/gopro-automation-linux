"""Automatic shooter labels from jersey numbers.

For every annotated shot of the validation games we know the shooter's jersey number and team
(who_gt.json). Read the jersey on EVERY player track in that shot's clip (sampled frames); a
track whose vote is the true number, on the shooting team's roster, is the true shooter. That
turns the annotators' cards into per-shot shooter labels without anyone drawing a box, so
shooter-selection rules can be scored on hundreds of shots.

Writes shooter_labels_auto.jsonl: {game, t, cam, gt, label_tracks: [ids], track_votes: {id: [[num, w], ...]}}
Usage: shooter_autolabel.py   (box, /home/dev/possession, CUDA env + jersey stack on PYTHONPATH)
"""
import json
import os
import sys
from collections import Counter, defaultdict

import cv2

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/who_deps_staging/pysrc")

import holder as H  # noqa: E402

VAL = "/home/dev/validate"
SAMPLE = 3            # every 3rd tracked frame (15 fps -> 5 reads per second per player)
MIN_FRAMES = 8
READ_MIN = 0.5


def track_boxes(S):
    tracks = defaultdict(list)
    for k, fr in enumerate(S["players"]):
        for oid, p in fr.items():
            tracks[oid].append((k, p["box"]))
    return {o: v for o, v in tracks.items() if len(v) >= MIN_FRAMES}


def main():
    from uball_cc.tracking.jersey_stack import JerseyStack
    js = JerseyStack()
    G = json.load(open(os.path.join(HERE, "who_gt.json")))
    team_roster = defaultdict(set)
    for r in G["rosters"]:
        team_roster[(r["game"], r["team"])].add(str(r["jersey"]))
    out_path = os.path.join(HERE, "shooter_labels_auto.jsonl")
    done = set()
    if os.path.exists(out_path):
        done = {(j["game"], j["t"]) for j in map(json.loads, open(out_path))}
    with open(out_path, "a") as fo:
        for s in G["shots"]:
            if (s["game"], s["t"]) in done:
                continue
            cam = "FL" if (s["side"] or "LEFT").upper() == "LEFT" else "FR"
            d = os.path.join(VAL, s["game"])
            name = "%s_t%07.1f" % (cam, s["t"])
            rec = {"game": s["game"], "t": s["t"], "cam": cam, "gt": str(s["jersey"]), "team": s["team"]}
            try:
                S = H.load(d, name)
                tracks = track_boxes(S)
                cap = cv2.VideoCapture(os.path.join(d, "clips", name + ".mp4"))
                fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
                want = defaultdict(list)          # source frame -> [(track, box)]
                for oid, seq in tracks.items():
                    for k, b in seq[::SAMPLE]:
                        want[int(round(S["times"][k] * fps))].append((oid, b))
                crops, owners, i = [], [], 0
                last = max(want) if want else -1
                while i <= last:
                    ok, im = cap.read()
                    if not ok:
                        break
                    for oid, b in want.get(i, []):
                        x0, y0, x1, y1 = [int(v) for v in b]
                        x0, y0 = max(0, x0), max(0, y0)
                        if x1 - x0 > 8 and y1 - y0 > 16:
                            crops.append(im[y0:y1, x0:x1].copy())
                            owners.append(oid)
                    i += 1
                cap.release()
                votes = defaultdict(Counter)
                for oid, (n, c) in zip(owners, js.read_crops(crops) if crops else []):
                    if n is not None and float(c or 0) >= READ_MIN:
                        votes[oid][str(n)] += float(c)
                roster = team_roster[(s["game"], s["team"])]
                labels = [oid for oid, v in votes.items()
                          if v and v.most_common(1)[0][0] == rec["gt"] and rec["gt"] in roster]
                rec["label_tracks"] = labels
                rec["track_votes"] = {str(o): [[n, round(w, 2)] for n, w in v.most_common(3)] for o, v in votes.items()}
            except Exception as e:  # noqa: BLE001
                rec["error"] = "%s: %s" % (type(e).__name__, e)
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
    rows = [json.loads(l) for l in open(out_path)]
    lab = [r for r in rows if r.get("label_tracks")]
    print("[auto] %d shots, labelled %d (exactly one track: %d)" % (
        len(rows), len(lab), sum(1 for r in lab if len(r["label_tracks"]) == 1)), flush=True)


if __name__ == "__main__":
    main()

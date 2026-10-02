"""Label every CV highlight clip of the annotated games: a real in-game make, or not.

A clip is LIVE when an annotator logged a make at the same basket within MATCH_S of it; every
other clip is NOT IN GT (after a whistle, a stoppage, warm-ups, or a make the annotators missed).
The CV clock (epoch in the clip id) and the annotation clock (seconds into the game video) differ
by an unknown offset per game: it is the 1-second bin holding the most same-basket (clip, make)
differences, refined to the median of the pairs inside it.

For each clip the nearest annotated events of any kind are kept too (a FOUL right before a
not-in-GT clip = shot after the whistle), and the whole-game CV sequence so temporal features can
be computed without GT.

Usage: deadball_labels.py <out.jsonl>   (box, repo .env.agx sourced; reads Firebase + annotation API)
"""
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

import numpy as np

sys.path.insert(0, "/home/dev/gopro-automation-linux")

CLIPS = "/home/dev/app/recordings/highlight_clips"
MATCH_S = 4.0
MIN_PAIRS = 15
_ID = re.compile(r"^cv_(\d{9,11})_(left|right)$")


def epoch(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()


def label_of_created(created):
    return "game_" + datetime.fromtimestamp(epoch(created), timezone.utc).strftime("%Y%m%d_%H%M%S")


def offset_of(cv, makes):
    diffs = [e - m["timestamp_seconds"] for e, s in cv for m in makes if m["side"] == s]
    if not diffs:
        return None, 0
    d = np.array(diffs)
    bins = Counter(np.round(d).astype(int))
    best = max(bins, key=lambda b: bins[b - 1] + bins[b] + bins[b + 1])
    inside = d[np.abs(d - best) <= MATCH_S]
    return float(np.median(inside)), int(len(inside))


def main():
    out = sys.argv[1]
    import firebase_admin
    from firebase_admin import credentials, firestore
    from uball_client import UballClient
    firebase_admin.initialize_app(credentials.Certificate(
        "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"))
    db = firestore.client()
    uc = UballClient()
    games = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "annotated_games.json")))
    local = set(os.listdir(CLIPS))
    with open(out, "w") as fo:
        for ann, fb in games:
            d = db.collection("basketball-games").document(fb).get().to_dict() or {}
            if not d.get("createdAt"):
                continue
            label = label_of_created(d["createdAt"])
            near = [x for x in local if abs(epoch_of_label(x) - epoch_of_label(label)) <= 5]
            if not near:
                continue
            label = near[0]
            cv = sorted((int(m.group(1)), m.group(2)) for k in (d.get("highlights") or {})
                        for m in [_ID.match(k)] if m)
            plays = [p for p in uc.list_plays(ann) if (p.get("source") or "") != "cv"]
            for p in plays:
                p["side"] = str(p.get("angle") or "").lower() or None
            makes = [p for p in plays if str(p.get("classification", "")).endswith("_MAKE") and p["side"]]
            off, n_pairs = offset_of(cv, makes)
            print(label, fb, ann[:8], "cv clips", len(cv), "GT makes", len(makes), "offset", off, "pairs", n_pairs, flush=True)
            if off is None or n_pairs < MIN_PAIRS:
                continue
            ts = np.array([p["timestamp_seconds"] for p in plays], float)
            span = (float(ts.min()) + off, float(ts.max()) + off) if len(ts) else (None, None)
            for e, side in cv:
                t = e - off
                m = [p for p in makes if p["side"] == side and abs(p["timestamp_seconds"] - t) <= MATCH_S]
                prev = [p for p in plays if p["timestamp_seconds"] <= t + 1]
                nxt = [p for p in plays if p["timestamp_seconds"] > t + 1]
                row = {"game": fb, "label": label, "ann": ann, "cv": "cv_%d_%s" % (e, side), "epoch": e, "side": side,
                       "live": bool(m), "gt_class": min(m, key=lambda p: abs(p["timestamp_seconds"] - t))["classification"] if m else None,
                       "prev_event": prev[-1]["classification"] if prev else None,
                       "prev_dt": round(t - prev[-1]["timestamp_seconds"], 1) if prev else None,
                       "next_event": nxt[0]["classification"] if nxt else None,
                       "next_dt": round(nxt[0]["timestamp_seconds"] - t, 1) if nxt else None,
                       "gt_span": span, "game_start": epoch(d["createdAt"]),
                       "game_end": epoch(d["endedAt"]) if d.get("endedAt") else None, "offset": off}
                fo.write(json.dumps(row) + "\n")
            fo.flush()


def epoch_of_label(label):
    m = re.match(r"game_(\d{8})_(\d{6})$", label)
    if not m:
        return -1e18
    return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp()


if __name__ == "__main__":
    main()

"""Which TEAM took the shot, even when the player is unknown.

Signals, per annotated shot of the validation games:
  colour   median colour of the tracked shooter's torso (SAM/YOLO mask, upper-middle of the box)
           around release, matched to the game's two registered jersey colours
  side     shots at one basket within a half all belong to the attacking team, so per basket we
           take the colour majority over the game, allowing ONE switch (half-time) at the split
           point that best agrees with the colour votes
Scored against the annotators' team for every shot. GAMES maps annotation game ->
(left team id, left colour, right team id, right colour) from the game record.

Usage: team_eval.py      (box, /home/dev/possession)
"""
import json
import os
import sys
from collections import Counter, defaultdict

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402

VAL = "/home/dev/validate"
GAMES = {
    "0f58ef71-cda0-4096-a5c8-2467e0f95613": ("e55410a9-d9d8-4a23-b8a2-3cabf9feb609", "#ffffff",
                                             "ee1f2ffe-8162-48fc-8052-a99895190a3e", "#eab308"),
    "11850fc1-34c9-47ae-a6e0-6edde5ed0bc9": ("5e8c75a4-0cea-4919-89e2-49ef34733a2a", "#9ca3af",
                                             "9135fe52-d635-4dbf-9984-1be038442629", "#1a1a1a"),
    "302c9277-5fc7-4f3a-849d-daacd5ff7c76": ("c2a129bb-18e3-47ba-96b0-35b04e9eab02", "#3b82f6",
                                             "96d1674e-bb17-4a57-b26d-7a0bc973a04b", "#ef4444"),
}


def hex_lab(h):
    rgb = np.array([[[int(h[1:3], 16), int(h[3:5], 16), int(h[5:7], 16)]]], np.uint8)
    return cv2.cvtColor(rgb, cv2.COLOR_RGB2LAB)[0, 0].astype(float)


def torso_colour(S, clip, shooter, around_k):
    """Median Lab of the shooter's mask pixels in the torso band (20-55% of box height)."""
    cap = cv2.VideoCapture(clip)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    pix = []
    for k in around_k:
        p = S["players"][k].get(shooter)
        if p is None:
            continue
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(round(S["times"][k] * fps)))
        ok, im = cap.read()
        if not ok:
            continue
        Hh, W = im.shape[:2]
        m = cv2.resize(p["mask"].astype(np.uint8), (W, Hh), interpolation=cv2.INTER_NEAREST) > 0
        x0, y0, x1, y1 = [int(v) for v in p["box"]]
        h = y1 - y0
        band = np.zeros_like(m)
        band[max(0, y0 + int(0.2 * h)):max(0, y0 + int(0.55 * h)), max(0, x0):max(0, x1)] = True
        sel = m & band
        if sel.sum() < 30:
            continue
        lab = cv2.cvtColor(im, cv2.COLOR_BGR2LAB)[sel]
        pix.append(np.median(lab, axis=0))
    cap.release()
    return np.median(np.array(pix), axis=0) if pix else None


def main():
    G = json.load(open(os.path.join(HERE, "who_gt.json")))
    rows = []
    for s in G["shots"]:
        if s["game"] not in GAMES:
            continue
        cam = "FL" if (s["side"] or "LEFT").upper() == "LEFT" else "FR"
        d = os.path.join(VAL, s["game"])
        name = "%s_t%07.1f" % (cam, s["t"])
        rec = {"game": s["game"], "t": s["t"], "side": cam, "gt": s["team"]}
        try:
            S = H.load(d, name)
            res = H.analyse(S, H.Camera(cam))
            shots = [x for x in res.get("shots", []) if x.get("shooter") is not None]
            if shots:
                sh = min(shots, key=lambda x: abs(x["rim_t"] - T.PRE))
                rel = sh.get("release_i") or 0
                ks = [k for k in range(max(0, rel - 12), rel + 1, 3)]
                col = torso_colour(S, os.path.join(d, "clips", name + ".mp4"), sh["shooter"], ks)
                if col is not None:
                    rec["lab"] = [float(v) for v in col]
        except Exception as e:  # noqa: BLE001
            rec["error"] = str(e)
        rows.append(rec)
    json.dump(rows, open(os.path.join(HERE, "team_colours.json"), "w"))
    score(rows)


def team_by_structure(R, lt, lc, rt, rc):
    """Per basket, the half-time switch is where the shooters' colour changes most; the two teams
    are {left basket before, right basket after} and {left after, right before}; names go to the
    groups by the cheaper of the two colour assignments (relative, so a mis-registered colour
    still maps as long as the order holds)."""
    # ONE half-time for both baskets: team A = left basket before it + right basket after it
    times = sorted(r["t"] for r in R)
    best = (-1, None)
    for i in range(3, len(times) - 3):
        T_ = (times[i - 1] + times[i]) / 2
        A = [np.array(r["lab"]) for r in R if "lab" in r and ((r["side"] == "FL") == (r["t"] < T_))]
        B = [np.array(r["lab"]) for r in R if "lab" in r and ((r["side"] == "FL") != (r["t"] < T_))]
        if len(A) < 3 or len(B) < 3:
            continue
        score = np.linalg.norm(np.median(A, axis=0) - np.median(B, axis=0)) * np.sqrt(len(A) * len(B) / (len(A) + len(B)))
        if score > best[0]:
            best = (score, T_)
    T_ = best[1]
    grpA = [r for r in R if (r["side"] == "FL") == (r["t"] < T_)]
    grpB = [r for r in R if (r["side"] == "FL") != (r["t"] < T_)]
    med = lambda G: np.median([r["lab"] for r in G if "lab" in r], axis=0)
    mA, mB = med(grpA), med(grpB)
    L, Rr = hex_lab(lc), hex_lab(rc)
    straight = np.linalg.norm(mA - L) + np.linalg.norm(mB - Rr)
    swapped = np.linalg.norm(mA - Rr) + np.linalg.norm(mB - L)
    a_team, b_team = (lt, rt) if straight <= swapped else (rt, lt)
    for r in grpA:
        r["struct_team"] = a_team
    for r in grpB:
        r["struct_team"] = b_team


def score(rows):
    tot = Counter()
    for g, (lt, lc, rt, rc) in GAMES.items():
        R = [r for r in rows if r["game"] == g]
        L, Rr = hex_lab(lc), hex_lab(rc)
        # colour alone: nearest registered colour (L channel down-weighted: lighting varies)
        w = np.array([0.5, 1.0, 1.0])
        for r in R:
            if "lab" in r:
                a = np.array(r["lab"])
                r["col"] = lt if np.linalg.norm((a - L) * w) < np.linalg.norm((a - Rr) * w) else rt
        # side + one switch: per camera side, best split of the time-ordered shots
        for side in ("FL", "FR"):
            seq = sorted([r for r in R if r["side"] == side], key=lambda r: r["t"])
            best = None
            for cut in range(0, len(seq) + 1):
                for first in (lt, rt):
                    second = rt if first == lt else lt
                    agree = sum(1 for i, r in enumerate(seq) if r.get("col") == (first if i < cut else second))
                    if best is None or agree > best[0]:
                        best = (agree, cut, first, second)
            _, cut, first, second = best
            for i, r in enumerate(seq):
                r["side_team"] = first if i < cut else second
        team_by_structure(R, lt, lc, rt, rc)
        n = len(R)
        st = sum(1 for r in R if r.get("struct_team") == r["gt"])
        tot["st"] += st
        print("[team] %s basket structure + relative colour: %d/%d (%.0f%%)" % (g[:8], st, n, 100 * st / n), flush=True)
        c = sum(1 for r in R if r.get("col") == r["gt"])
        sdt = sum(1 for r in R if r.get("side_team") == r["gt"])
        print("[team] %s %d shots | colour alone %d (%.0f%%) | side + one switch %d (%.0f%%)"
              % (g[:8], n, c, 100 * c / n, sdt, 100 * sdt / n), flush=True)
        tot["n"] += n; tot["c"] += c; tot["s"] += sdt
    print("[team] ALL %d shots | colour alone %d (%.0f%%) | side + one switch %d (%.0f%%) | structure + relative colour %d (%.0f%%)"
          % (tot["n"], tot["c"], 100 * tot["c"] / tot["n"], tot["s"], 100 * tot["s"] / tot["n"], tot["st"], 100 * tot["st"] / tot["n"]), flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--score":
        score(json.load(open(os.path.join(HERE, "team_colours.json"))))
    else:
        main()

"""Whole-game score of the possession tracker against manual ground truth.

Shot TYPE is the measurable payoff of tracking without jersey numbers: if the tracker holds
the right player, his feet at takeoff land in the right zone. Every CV clip that pairs with a
GT shot (loop_score's rule: same side, one-to-one, within 4s of video time, same make/miss)
is typed three ways and compared with the annotator's type:

  prod        production's own answer (its shooter, its feet, golden calibration)
  prod_fit    production's feet on the per-game FITTED court lines
  track_fit   the tracker's shooter, feet at takeoff, on the same fitted lines

prod_fit vs track_fit differ only in WHICH player's feet, so that gap is the tracker.

Usage: eval_full.py <game> <types.json> <annotation_game_id> <tracker_out_dir> <ver> <fit_dir>
"""
import json
import os
import sys
from collections import Counter

import cv2
import numpy as np

sys.path.insert(0, "/home/dev/confirm_test")
import loop_score as L  # noqa: E402  (gt_shots, region; pairing rule reused as-is)

TAKEOFF_WIN = 0.8        # s before release searched for grounded feet
RELEASE_EARLY = 0.3      # 'early' feet: this long before release
FT_STILL_PX = 60         # feet moving less than this over the window = set shot
CLIP_W = 1920


def load_arcs(d, cam):
    a = json.load(open("%s/calib_arcs_%s.json" % (d, cam)))
    sc = CLIP_W / float(a.get("w") or CLIP_W)
    return {k: (np.array(a[k], np.float32) * sc) for k in ("three_pt_white", "four_pt_red", "ft_stripe") if a.get(k)}


def zone(arcs, px, still):
    r, ft = L.region(arcs, px)
    return "FREE_THROW" if ft and still else r


def feet_variants(tr, rt, still_ref=None):
    """Three ways to read grounded feet from the shooter's boxes before release:
    maxy  - the lowest box bottom in the window (a jump lifts it; a merged box drops it)
    med   - the median bottom point over the window
    early - the box RELEASE_EARLY s before release, before the jump starts"""
    win = [(t, b) for t, b in tr if rt is not None and rt - TAKEOFF_WIN <= t <= rt]
    if not win:
        return {}, None
    pts = [((b[0] + b[2]) / 2, b[3]) for _, b in win]
    xs, ys = zip(*pts)
    still = (max(xs) - min(xs)) < FT_STILL_PX and (max(ys) - min(ys)) < FT_STILL_PX
    t_m, b_m = max(win, key=lambda tb: tb[1][3])
    t_e, b_e = min(win, key=lambda tb: abs(tb[0] - (rt - RELEASE_EARLY)))
    return {"maxy": ((b_m[0] + b_m[2]) / 2, b_m[3]),
            "med": (float(np.median(xs)), float(np.median(ys))),
            "early": ((b_e[0] + b_e[2]) / 2, b_e[3])}, still


def pick_attempt(res, want_dt):
    """A clip can hold several attempts (miss + putback). Pair the one whose rim time best
    matches the GT card: want_dt = GT time - clip's nominal rim time, on the video clock."""
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    if not shots:
        return None
    return min(shots, key=lambda s: abs((s["rim_t"] - res.get("nominal_rim_t", 5.0)) - want_dt))


def takeoff_feet(res):
    """Grounded feet of the tracked shooter: the frame with the LOWEST box bottom (largest
    y2) in the TAKEOFF_WIN before release. A jump shot's release feet are airborne and
    project metres behind where he shot from."""
    rt, tr = res.get("release_t"), res.get("shooter_track") or []
    win = [(t, b) for t, b in tr if rt is not None and rt - TAKEOFF_WIN <= t <= rt]
    if not win:
        return None, None
    t, b = max(win, key=lambda tb: tb[1][3])
    bottoms = [((bb[0] + bb[2]) / 2, bb[3]) for _, bb in win]
    xs, ys = zip(*bottoms)
    still = (max(xs) - min(xs)) < FT_STILL_PX and (max(ys) - min(ys)) < FT_STILL_PX
    return ((b[0] + b[2]) / 2, b[3]), still


def main():
    game, tpath, ub, tdir, ver, fit_dir = sys.argv[1:7]
    T = json.load(open(tpath))["shots"]
    shots = [dict(v, key=k) for k, v in T.items() if "prod" in v.get("types", {})]
    gt = L.gt_shots(ub)
    side_of = {"left": "LEFT", "right": "RIGHT"}
    pairs = sorted((abs(s["video_t"] - g["t"]), i, j) for i, s in enumerate(shots) for j, g in enumerate(gt)
                   if abs(s["video_t"] - g["t"]) <= L.TOL and (not g["side"] or g["side"] == side_of[s["side"]]))
    used_s, used_g, M = set(), set(), {}
    for _, i, j in pairs:
        if i in used_s or j in used_g:
            continue
        used_s.add(i); used_g.add(j); M[j] = i
    arcs = {cam: load_arcs(fit_dir, cam) for cam in ("FL", "FR")}
    tally = {k: Counter() for k in ("prod", "prod_fit", "track_fit", "track_med", "track_early")}
    rows = []
    for j, i in sorted(M.items(), key=lambda kv: gt[kv[0]]["t"]):
        g, s = gt[j], shots[i]
        if s["verdict"] != g["result"]:
            continue
        clip = "%s_%s" % (s["angle"], s["key"])
        p = s["types"]["prod"]
        row = {"clip": clip, "gt": g["type"], "result": g["result"], "gt_player": g.get("player"),
               "prod": p.get("zone")}
        if p.get("feet_px"):
            fx = [int(v) for v in p["feet_px"].split(",")]
            row["prod_fit"] = L.region(arcs[s["angle"]], fx)[0]
            if p.get("zone") == "FREE_THROW":
                row["prod_fit"] = "FREE_THROW"      # production's FT test is kept as-is
        rp = os.path.join(tdir, ver, clip + ".json")
        if os.path.exists(rp):
            res = json.load(open(rp))
            shot = pick_attempt(res, g["t"] - s["video_t"])
            tr = res.get("tracks", {}).get(str(shot["shooter"])) if shot else None
            if shot is not None and tr is None and shot.get("release_t") == res.get("release_t"):
                tr = res.get("shooter_track")
            fv, still = feet_variants(tr or [], shot and shot.get("release_t"))
            row.update({"shooter": shot and shot.get("shooter"), "release_t": shot and shot.get("release_t"),
                        "n_shots_in_clip": len(res.get("shots", [])), "votes": shot and shot.get("votes"),
                        "track_feet": fv.get("maxy") and [round(v) for v in fv["maxy"]]})
            for name, key in (("maxy", "track_fit"), ("med", "track_med"), ("early", "track_early")):
                row[key] = zone(arcs[s["angle"]], fv[name], still) if name in fv else None
        else:
            row["track_fit"] = row["track_med"] = row["track_early"] = "NOT_RUN"
        for k in tally:
            v = row.get(k)
            tally[k]["n"] += 1
            if v in (None, "NOT_RUN", "UNKNOWN"):
                tally[k]["no_answer"] += 1
            elif v == g["type"]:
                tally[k]["right"] += 1
            else:
                tally[k]["wrong"] += 1
        rows.append(row)
    summary = {}
    for k, c in tally.items():
        ans = c["right"] + c["wrong"]
        summary[k] = {"paired": c["n"], "answered": ans, "right": c["right"],
                      "accuracy_when_answered": round(c["right"] / ans, 3) if ans else None,
                      "right_of_all": round(c["right"] / c["n"], 3) if c["n"] else None}
    ran = [r for r in rows if r.get("track_fit") != "NOT_RUN"]
    summary["same_shots_where_tracker_ran"] = {
        k: "%d/%d" % (sum(r.get(k) == r["gt"] for r in ran), len(ran)) for k in ("prod", "prod_fit", "track_fit", "track_med", "track_early")}
    by_type = {}
    for r in rows:
        for k in ("prod", "track_fit"):
            d = by_type.setdefault(r["gt"], {}).setdefault(k, [0, 0])
            d[0] += 1
            d[1] += int(r.get(k) == r["gt"])
    out = {"game": game, "ver": ver, "gt_shots": len(gt), "paired_same_result": len(rows),
           "summary": summary, "by_gt_type": {t: {k: "%d/%d" % tuple(v[::-1]) for k, v in d.items()} for t, d in by_type.items()},
           "flips": {"fixed_by_tracker": [r["clip"] for r in rows if r.get("prod") != r["gt"] and r.get("track_fit") == r["gt"]],
                     "broken_by_tracker": [r["clip"] for r in rows if r.get("prod") == r["gt"] and r.get("track_fit") not in (r["gt"], "NOT_RUN")]},
           "rows": rows}
    json.dump(out, open(os.path.join(tdir, "eval_%s_%s.json" % (game, ver)), "w"), indent=1)
    print(json.dumps({k: out[k] for k in ("game", "gt_shots", "paired_same_result", "summary", "by_gt_type")}, indent=1))
    print("fixed_by_tracker", len(out["flips"]["fixed_by_tracker"]), "broken_by_tracker", len(out["flips"]["broken_by_tracker"]))


if __name__ == "__main__":
    main()

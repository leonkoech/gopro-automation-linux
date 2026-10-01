"""Full-pipeline test of ONE annotated game, replayed with today's production code, scored
against the annotators. Every stage is resumable; run them in order.

  gt     the annotators' shots (type, make/miss, basket, jersey + team from the game's check-in
         rosters, which is what production sees) + the registered kit colours (Firebase)
  map    clock line SL/SR master -> game video, one RANSAC line per side fitted on (replayed
         shot, annotated shot) pairs. Production cuts clips by wall clock and needs no map; this
         only aligns two recordings, globally, so no single shot's answer leaks in.
  clips  a clip [t-5 s, t+3 s] from the annotation tool's FL/FR video at every replayed CV shot
         (live pass + full-rate confirm, makes AND misses), exactly what the live clip would hold
  fast   the production tracker chain per clip: fast perception -> possession -> shooter ->
         takeoff feet -> zone on the golden arcs (type); shooter kit colour (team); jersey read on
         the tracked shooter, and if silent the extended timeline (15 s back, 5 s forward);
         seconds per stage
  score  detection (vs every annotated shot), reel filter, type, team, jersey (any number / the
         shooting team's roster from OUR team call), and SAM3 when gametest/<g>/sam3.jsonl
         (cloud run on the same clips) is present

Usage: game_test.py <stage> <annotation uuid> [--fb <firebase id>] [--replay <out.json>]
Files: /home/dev/gametest/<g8>/{gt.json,map.json,shots.jsonl,clips/,fast/,results.jsonl,report.json}
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")
sys.path.insert(0, "/home/dev/who_deps_staging/pysrc")

ROOT = "/home/dev/gametest"
VAL = "/home/dev/validate"
MATCH_S = 2.0            # a CV shot and an annotated shot pair within this (after the clock line)
MAP_TOL_S = 1.0
DEDUP_S = 1.0
TYPE_OF = {"FG": "2PT", "2PT": "2PT", "3PT": "3PT", "4PT": "4PT", "FREE_THROW": "FREE_THROW"}


def gdir(g):
    d = os.path.join(ROOT, g[:8])
    os.makedirs(d, exist_ok=True)
    return d


def jl(path):
    return [json.loads(l) for l in open(path)] if os.path.exists(path) else []


# ------------------------------------------------------------------------------------------- gt
def stage_gt(g, fb):
    from uball_client import UballClient
    uc = UballClient()
    game = uc.get_game_by_firebase_id(fb) or {}
    if game.get("id") != g:
        raise SystemExit("annotation game for %s is %s, not %s" % (fb, game.get("id"), g))
    by_pid, rosters = {}, {}
    for side, key in (("left", "roster_team1"), ("right", "roster_team2")):
        rosters[side] = {}
        for p in game.get(key) or []:
            if p.get("jersey_number") is not None:
                rosters[side][str(p["jersey_number"])] = p.get("name")
            by_pid[p.get("player_id")] = (side, None if p.get("jersey_number") is None else str(p["jersey_number"]))
    shots = []
    for p in uc.list_plays(g):
        cls = str(p.get("classification") or "")
        if (p.get("source") or "") == "cv" or not re.search(r"_(MAKE|MISS)$", cls):
            continue
        side, num = by_pid.get(p.get("player_a_id"), (None, None))
        shots.append({"t": float(p["timestamp_seconds"]), "cls": cls, "made": cls.endswith("_MAKE"),
                      "type": TYPE_OF.get(cls.rsplit("_", 1)[0]), "side": str(p.get("angle") or "").lower() or None,
                      "jersey": num, "team": side})
    import firebase_admin
    from firebase_admin import credentials, firestore
    firebase_admin.initialize_app(credentials.Certificate(
        "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"))
    d = firestore.client().collection("basketball-games").document(fb).get().to_dict() or {}
    out = {"game": g, "firebase": fb, "rosters": rosters, "shots": sorted(shots, key=lambda s: s["t"]),
           "colours": {"left": (d.get("leftTeam") or {}).get("jerseyColor"),
                       "right": (d.get("rightTeam") or {}).get("jerseyColor")}}
    json.dump(out, open(os.path.join(gdir(g), "gt.json"), "w"), indent=1)
    print("gt: %d annotated shots (%d makes), %d with a jersey, rosters %s, colours %s" % (
        len(shots), sum(s["made"] for s in shots), sum(1 for s in shots if s["jersey"]),
        {k: len(v) for k, v in rosters.items()}, out["colours"]))


# ------------------------------------------------------------------------------------------ map
def replay_shots(replay):
    R = json.load(open(replay))
    out = []
    for cam, side in (("SL", "left"), ("SR", "right")):
        rs = sorted(((R.get(cam) or {}).get("base", []) + (R.get(cam) or {}).get("confirm_new", [])),
                    key=lambda s: s["t"])
        last = None
        for s in rs:
            if last is not None and s["t"] - last < DEDUP_S:
                continue
            last = s["t"]
            out.append({"side": side, "sl_t": float(s["t"]), "made": s.get("verdict") == "MAKE",
                        "src": "confirm" if s in (R.get(cam) or {}).get("confirm_new", []) else "live"})
    return out


def stage_map(g, replay):
    import sync_masters as SM
    gt = json.load(open(os.path.join(gdir(g), "gt.json")))
    shots = replay_shots(replay)
    SM.TOL_S = MAP_TOL_S
    maps = {}
    for side in ("left", "right"):
        pairs = [(s["sl_t"], a["t"]) for s in shots if s["side"] == side
                 for a in gt["shots"] if a["side"] == side and a["made"] == s["made"]]
        maps[side] = SM.ransac(pairs, iters=20000) if len(pairs) > 4 else None
        print("map %-5s %s" % (side, maps[side]))
    json.dump(maps, open(os.path.join(gdir(g), "map.json"), "w"), indent=1)


# ---------------------------------------------------------------------------------------- clips
def stage_clips(g, replay):
    import track_shadow as T
    d = gdir(g)
    maps = json.load(open(os.path.join(d, "map.json")))
    os.makedirs(os.path.join(d, "clips"), exist_ok=True)
    rows = []
    for s in replay_shots(replay):
        m = maps.get(s["side"])
        if not m:
            continue
        cam = "FL" if s["side"] == "left" else "FR"
        vt = m["a"] + m["b"] * s["sl_t"]
        name = "%s_t%07.1f" % (cam, vt)
        clip = os.path.join(d, "clips", name + ".mp4")
        if not os.path.exists(clip) and not T.cut_clip(os.path.join(VAL, g, cam + ".mp4"), vt, clip):
            continue
        rows.append(dict(s, cam=cam, video_t=round(vt, 2), name=name))
    with open(os.path.join(d, "shots.jsonl"), "w") as fo:
        for r in rows:
            fo.write(json.dumps(r) + "\n")
    print("clips: %d CV shots (%d makes; %d found only by the full-rate confirm)" % (
        len(rows), sum(r["made"] for r in rows), sum(r["src"] == "confirm" for r in rows)))


# ----------------------------------------------------------------------------------------- fast
def extend(js, model, video, t_clip0, boxes, reads):
    """The extended timeline (who_extend): follow the shooter 15 s back, then 5 s forward."""
    import who_eval as W
    import who_extend as X
    for direction, limit, t_edge, seed in (("back", 15.0, t_clip0 + boxes[0][0], boxes[0][1]),
                                           ("fwd", 5.0, t_clip0 + boxes[-1][0], boxes[-1][1])):
        walked, edge, box = 0.0, t_edge, seed
        while walked < limit and W.speak(reads, None) is None:
            if direction == "back":
                chunk = X.frames(video, edge - 2.5, edge, reverse=True)
                edge -= 2.5
            else:
                chunk = X.frames(video, edge, edge + 2.5, reverse=False)
                edge += 2.5
            walked += 2.5
            path, box, held = X.follow(model, chunk, box)
            if not held:
                break
            got = js.read_crops([c for _, _, c in path]) if path else []
            reads = reads + [(str(n), float(c or 0)) for n, c in got if n is not None]
    return reads


def stage_fast(g):
    import holder as H
    import team_eval as TE
    import track_shadow as T
    import who_eval as W
    from ultralytics import YOLO
    from uball_cc.tracking.jersey_stack import JerseyStack
    d = gdir(g)
    js = JerseyStack()
    seg = YOLO(os.path.join(HERE, "weights", "yolo11s-seg.pt"))
    arcs = {c: T.load_arcs(c, 1920) for c in ("FL", "FR")}
    out_path = os.path.join(d, "results.jsonl")
    done = {r["name"] for r in jl(out_path)}
    with open(out_path, "a") as fo:
        for s in jl(os.path.join(d, "shots.jsonl")):
            if s["name"] in done:
                continue
            if T.recording():
                raise SystemExit("the box is recording: stopping (resumable)")
            rec, clip, t0 = dict(s), os.path.join(d, "clips", s["name"] + ".mp4"), time.time()
            try:
                subprocess.run(["python3", os.path.join(HERE, "perceive_fast.py"), clip, os.path.join(d, "fast"),
                                "%.2f" % T.PRE], capture_output=True, text=True, timeout=600, check=True)
                rec["perceive_s"] = round(time.time() - t0, 1)
                S = H.load(os.path.join(d, "fast"), s["name"])
                res = H.analyse(S, H.Camera(s["cam"]))
                cand = [x for x in res.get("shots", []) if x.get("shooter") is not None]
                if cand:
                    shot = min(cand, key=lambda x: abs(x["rim_t"] - T.PRE))
                    boxes = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
                    feet, still = T.takeoff_feet(boxes, shot["release_t"])
                    rec["zone"] = T.zone_of(arcs[s["cam"]], feet, still) if feet else None
                    rel = shot.get("release_i") or 0
                    lab = TE.torso_colour(S, clip, shot["shooter"], list(range(max(0, rel - 12), rel + 1, 3)))
                    rec["kit_lab"] = None if lab is None else [round(float(v), 1) for v in lab]
                    t1 = time.time()
                    reads = W.read_track(js, clip, boxes) if boxes else []
                    rec["reads"], rec["read_s"] = reads, round(time.time() - t1, 1)
                    if boxes and W.speak(reads, None) is None:
                        t1 = time.time()
                        rec["reads_ext"] = extend(js, seg, os.path.join(VAL, g, s["cam"] + ".mp4"),
                                                  s["video_t"] - T.PRE, boxes, reads)
                        rec["extend_s"] = round(time.time() - t1, 1)
                else:
                    rec["zone"] = None
            except Exception as e:  # noqa: BLE001
                rec["error"] = "%s: %s" % (type(e).__name__, str(e)[:200])
            rec["total_s"] = round(time.time() - t0, 1)
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
    print("fast: %d clips processed" % len(jl(out_path)))


# ---------------------------------------------------------------------------------------- score
def pair(cv, gt):
    """One-to-one, same basket, nearest first, within MATCH_S."""
    cand = sorted((abs(c["video_t"] - a["t"]), i, j) for i, c in enumerate(cv) for j, a in enumerate(gt)
                  if c["side"] == a["side"] and abs(c["video_t"] - a["t"]) <= MATCH_S)
    used_c, used_a, out = set(), set(), {}
    for _, i, j in cand:
        if i in used_c or j in used_a:
            continue
        used_c.add(i)
        used_a.add(j)
        out[i] = j
    return out


def pct(a, b):
    return "%d/%d (%.0f%%)" % (a, b, 100.0 * a / b) if b else "0/0"


def stage_score(g):
    import who_eval as W
    from agx_pipeline.deadball import dense_shooting
    from agx_pipeline.team_assign import solve, team_for_shot
    d = gdir(g)
    gt = json.load(open(os.path.join(d, "gt.json")))
    A = gt["shots"]
    R = jl(os.path.join(d, "results.jsonl"))
    sam = {r["name"]: r for r in jl(os.path.join(d, "sam3.jsonl"))}
    P = pair(R, A)
    rep = {"game": g[:8]}
    # detection
    found = set(P.values())
    gm = [j for j, a in enumerate(A) if a["made"]]
    rep["detect_makes_found"] = pct(sum(j in found for j in gm), len(gm))
    rep["detect_misses_found"] = pct(sum(j in found for j, a in enumerate(A) if not a["made"]), len(A) - len(gm))
    pts = {"2PT": 2, "3PT": 3, "4PT": 4, "FREE_THROW": 1}
    rep["points_in_found_makes"] = pct(sum(pts.get(A[j]["type"], 0) for j in gm if j in found),
                                       sum(pts.get(A[j]["type"], 0) for j in gm))
    rep["detect_makes_by_type"] = {t: pct(sum(j in found for j in gm if A[j]["type"] == t),
                                          sum(1 for j in gm if A[j]["type"] == t)) for t in pts}
    cvm = [i for i, r in enumerate(R) if r["made"]]
    rep["cv_makes_that_are_annotated_shots"] = pct(sum(i in P for i in cvm), len(cvm))
    rep["make_miss_agreement"] = pct(sum(R[i]["made"] == A[j]["made"] for i, j in P.items()), len(P))
    rep["found_only_by_full_rate_confirm"] = sum(1 for i in P if R[i]["src"] == "confirm")
    # reel filter (CV makes)
    ids = {i: "cv_%d_%s" % (int(R[i]["video_t"]), R[i]["side"]) for i in cvm}
    dense = dense_shooting(ids.values())
    real = [i for i in cvm if i in P and A[P[i]]["made"]]
    junk = [i for i in cvm if not (i in P and A[P[i]]["made"])]
    rep["reel_filter"] = {"junk_removed": pct(sum(ids[i] in dense for i in junk), len(junk)),
                          "real_makes_removed": pct(sum(ids[i] in dense for i in real), len(real))}
    # type (paired, any make/miss)
    typed = [(R[i], A[j]) for i, j in P.items() if A[j]["type"]]
    rep["type_fast"] = pct(sum(r.get("zone") == a["type"] for r, a in typed), len(typed))
    rep["type_fast_by_type"] = {t: pct(sum(r.get("zone") == t for r, a in typed if a["type"] == t),
                                       sum(1 for r, a in typed if a["type"] == t)) for t in pts}
    if sam:
        st = [(sam.get(r["name"], {}), a) for r, a in typed]
        rep["type_sam3"] = pct(sum(s.get("zone") == a["type"] for s, a in st), len(st))
    # team: one half-time switch from the shooters' kit colours, named by the registered colours
    tt = solve([(R[i]["video_t"], R[i]["side"], R[i]["kit_lab"]) for i in range(len(R)) if R[i].get("kit_lab")],
               gt["colours"]["left"], gt["colours"]["right"])
    teamed = [(team_for_shot(tt, R[i]["side"], R[i]["video_t"]), A[j]["team"]) for i, j in P.items() if A[j]["team"]]
    rep["team"] = pct(sum(x == y for x, y in teamed), len(teamed))
    # who: our team call picks the roster
    rows = []
    for i, j in P.items():
        a = A[j]
        if not a["jersey"]:
            continue
        r = R[i]
        team = team_for_shot(tt, r["side"], r["video_t"])
        roster = set(gt["rosters"].get(team, {})) if team else None
        fast_clip = W.speak(r.get("reads") or [], None)
        fast_live = W.speak(r.get("reads_ext") or r.get("reads") or [], None)
        fast_ros = W.speak(r.get("reads_ext") or r.get("reads") or [], roster)
        s3 = sam.get(r["name"])
        s3_ros = W.speak((s3 or {}).get("reads") or [], roster) if s3 else None
        rows.append({"gt": a["jersey"], "fast clip": fast_clip, "fast + timeline": fast_live,
                     "fast + timeline + roster": fast_ros,
                     **({"SAM3 + roster": s3_ros, "fast, then SAM3 if silent (roster)":
                         fast_ros if fast_ros is not None else s3_ros} if sam else {})})
    n = len(rows)
    rep["who_shots_with_annotated_jersey"] = n
    for k in [k for k in (rows[0] if rows else {}) if k != "gt"]:
        spoke = sum(r[k] is not None for r in rows)
        right = sum(r[k] == r["gt"] for r in rows)
        rep["who " + k] = "names %s, right when named %s, right of all %s" % (
            pct(spoke, n), pct(right, spoke), pct(right, n))
    # time
    for k in ("perceive_s", "read_s", "extend_s", "total_s"):
        v = [r[k] for r in R if r.get(k) is not None]
        if v:
            rep["sec_" + k] = "median %.1f, total %.0f min over %d clips" % (np.median(v), sum(v) / 60, len(v))
    if sam:
        v = [s["sam3_s"] for s in sam.values() if s.get("sam3_s")]
        rep["sec_sam3_cloud"] = "median %.1f over %d clips" % (np.median(v), len(v)) if v else None
    rep["errors"] = Counter(str(r.get("error", ""))[:40] for r in R if r.get("error"))
    json.dump(rep, open(os.path.join(d, "report.json"), "w"), indent=1, default=str)
    for k, v in rep.items():
        print("%-40s %s" % (k, v))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["gt", "map", "clips", "fast", "score"])
    ap.add_argument("game")
    ap.add_argument("--fb")
    ap.add_argument("--replay")
    a = ap.parse_args()
    if a.stage == "gt":
        stage_gt(a.game, a.fb)
    elif a.stage == "map":
        stage_map(a.game, a.replay)
    elif a.stage == "clips":
        stage_clips(a.game, a.replay)
    elif a.stage == "fast":
        stage_fast(a.game)
    else:
        stage_score(a.game)


if __name__ == "__main__":
    main()

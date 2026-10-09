"""A TEST annotation game showing the green / yellow / red CV card flags on a finished test game.

Copies the real annotation game's teams, rosters and FL/FR videos into a NEW game named
"[TEST — CV flags — DO NOT ANNOTATE] ..." (fake Firebase id, so nothing ever looks it up as the
real game), then writes its CV cards with the PRODUCTION card code (plays_sync.
create_plays_from_shot_live), fed with the pipeline-test results for that game (game_test.py):
every replayed CV shot, the tracker's type, line distance, jersey reads (clip, then follow-back)
and the team from the half-time rule. The real game and its cards are not touched.

Usage: make_flag_test_game.py <g8> [--into <test game id>]   (box; repo .env.agx sourced)
  --into rewrites that TEST game's own cards in place (matched by basket + time) instead of
  creating a new game. --create-in <game id> adds the CV cards to that EXISTING game (production card
  code: the game's own CV cards are skipped if it already has some, and CV cards never change its
  score). FLAGTEST_REPO = the repo copy holding the branch's plays_sync.
"""
import json
import os
import sys

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import game_test as GT  # noqa: E402
import night_report as NR  # noqa: E402
import track_shadow as T  # noqa: E402
import who_eval as W  # noqa: E402

EPOCH0 = 1_700_000_000           # synthetic wall clock: card time = EPOCH0 + video time


def signed_line(arcs, feet):
    pt = (float(feet[0]), float(feet[1]))
    return float(min((cv2.pointPolygonTest(arcs[k], pt, True) for k in ("three_pt_white", "four_pt_red")), key=abs))


def main():
    g8 = sys.argv[1]
    into = sys.argv[3] if len(sys.argv) > 3 and sys.argv[2] == "--into" else None
    create_in = sys.argv[3] if len(sys.argv) > 3 and sys.argv[2] == "--create-in" else None
    # the branch's card code must win over the box repo that the imports above put on the path
    sys.path.insert(0, os.environ.get("FLAGTEST_REPO", "/home/dev/gopro-automation-linux"))
    from uball_client import UballClient
    from plays_sync import _cv_card, create_plays_from_shot_live, rosters_from_annotation_game
    import plays_sync
    print("card code:", plays_sync.__file__, flush=True)
    from agx_pipeline.team_assign import solve
    d = os.path.join(GT.ROOT, g8)
    gt = json.load(open(os.path.join(d, "gt.json")))
    R = GT.jl(os.path.join(d, "results.jsonl"))
    uc = UballClient()
    orig = uc.get_game_by_firebase_id(gt["firebase"])
    if not orig or orig.get("id") != gt["game"]:
        raise SystemExit("original annotation game not found")
    if into or create_in:
        gid, name = into or create_in, None
    else:
        # the test game: same teams, rosters, colours, videos
        name = "[TEST — CV flags — DO NOT ANNOTATE] %s" % (orig.get("video_name") or g8)
        new = uc.create_game({"firebase_game_id": "TEST-cvflags-%s" % g8, "date": orig.get("date"),
                              "team1_id": orig.get("team1_id"), "team2_id": orig.get("team2_id"),
                              "video_name": name, "team1_color": orig.get("team1_color"),
                              "team2_color": orig.get("team2_color"), "roster_team1": orig.get("roster_team1"),
                              "roster_team2": orig.get("roster_team2"), "source": "test"})
        if not new or not new.get("id"):
            raise SystemExit("create_game failed")
        gid = new["id"]
        vids = uc.get_videos_for_game(orig["id"])
        if not vids and os.environ.get("VIDEOS_JSON"):        # rows of video_metadata, when the API lists none
            vids = json.load(open(os.environ["VIDEOS_JSON"]))
        for v in vids:
            if v.get("angle") and v.get("s3_key"):
                ok = uc.register_video(gid, v["s3_key"], v["angle"], v.get("filename") or os.path.basename(v["s3_key"]),
                                       duration=v.get("duration"), file_size=v.get("file_size"))
                print("video", v["angle"], "registered" if ok else "NOT registered", flush=True)

    # the tracker's results as the worker would have stored them in cv_points
    arcs = {c: T.load_arcs(c, 1920) for c in ("FL", "FR")}
    shots, cv_points, kits = [], {}, []
    for r in R:
        ep = EPOCH0 + float(r["video_t"])
        wc = __import__("datetime").datetime.fromtimestamp(ep, __import__("datetime").timezone.utc).isoformat()
        shots.append({"made": r["made"], "side": r["side"], "cam": "SL" if r["side"] == "left" else "SR",
                      "wallclock": wc, "video_ts": float(r["video_t"])})
        clip_reads = r.get("reads") or []
        clip_num = W.speak(clip_reads, None)
        reads = clip_reads if clip_num is not None else (r.get("reads_ext") or clip_reads)
        votes = {}
        for n, c in reads:
            if c >= W.READ_MIN:
                votes[n] = round(votes.get(n, 0) + c, 2)
        num = W.speak(reads, None)
        v = {"zone": r.get("zone"), "who": num, "who_votes": dict(sorted(votes.items(), key=lambda kv: -kv[1])[:3]),
             "who_from": ("clip" if clip_num is not None else ("timeline" if num is not None else None)),
             "kit_lab": r.get("kit_lab")}
        if r.get("zone"):
            feet = NR.feet_of(d, r["name"], r["cam"])
            if feet is not None:
                v["line_px"] = round(signed_line(arcs[r["cam"]], feet), 1)
        cv_points["cv_%d_%s" % (int(ep), r["side"])] = v
        if r.get("kit_lab"):
            kits.append((ep, r["side"], r["kit_lab"]))
    game = {"createdAt": __import__("datetime").datetime.fromtimestamp(EPOCH0, __import__("datetime").timezone.utc).isoformat(),
            "shot_live": {"shots": shots}, "cv_points": cv_points,
            "tracker_teams": solve(kits, gt["colours"]["left"], gt["colours"]["right"])}
    rosters = rosters_from_annotation_game(orig)
    if into:
        from datetime import datetime, timezone
        start = datetime.fromtimestamp(EPOCH0, timezone.utc)
        fresh = {}
        for sh in shots:
            c = _cv_card(sh, game, cv_points, start, gid, rosters)
            c.pop("_typed", None)
            fresh[(c.get("angle"), round(float(c["timestamp_seconds"]), 1))] = c
        done = 0
        for p in uc.list_plays(gid):
            c = fresh.get((p.get("angle"), round(float(p.get("timestamp_seconds") or 0), 1)))
            if c:
                uc.update_play(p["id"], {k: c[k] for k in ("classification", "note", "confidence", "events")})
                done += 1
        print(json.dumps({"test_game_id": gid, "cards_rewritten": done}))
        return
    summary = {}
    n = create_plays_from_shot_live(uc, gid, game, summary=summary, rosters=rosters)
    print(json.dumps({"test_game_id": gid, "name": name, "cards": n, "summary": summary}, default=str))


if __name__ == "__main__":
    main()

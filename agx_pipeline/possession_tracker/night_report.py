"""Client report on the pipeline-test games (game_test.py output): accuracy, flagged cards and a
simulated night.

Per game, on the CV shots that pair with an annotated shot:
  make/miss   CV verdict vs the annotator (and how many annotated makes/misses the CV found at all)
  type        tracker zone vs the annotated type
  who         production jersey suggestion: in-clip read, else the follow-back read, over the
              shooting team's roster (our team call); coverage = shown, accuracy = right when shown
  flags       GREEN  type found with the feet >= LINE_PX from any line AND a jersey from the clip
              YELLOW answers present but shaky: feet within LINE_PX of a line, or the jersey
                     found only by following the player back
              RED    type or jersey missing (or the shot was during a paused clock)
CV shots that pair with no annotated shot are counted separately (no game timer was logged for
these games, so they all count; the timer, once logged, removes the paused ones).

Night simulation: the games back to back (1 min apart, shots at their real times), ONE low-priority
worker taking clips first-in-first-out. Per clip AGX time = measured tracking + jersey read, + the
follow-back search where the clip read is silent (measured, or 95 s, game 1's median, where it was
not run), + RENDER_S for a 2K render of each make. Ingestion starts INGEST_WAIT_MIN after the last
game and runs the games in turn at their measured durations.

Usage: night_report.py <g8>:<ingest min> [...]  (box, /home/dev/possession) -> JSON on stdout
"""
import json
import os
import sys

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")
import game_test as GT  # noqa: E402
import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402
import who_eval as W  # noqa: E402

LINE_PX = 25
EXTEND_DEFAULT_S = 95.0
RENDER_S = 25.0
INGEST_WAIT_MIN = 30
TURNOVER_S = 60


def line_dist(arcs, feet):
    pt = (float(feet[0]), float(feet[1]))
    return min(abs(cv2.pointPolygonTest(arcs[k], pt, True)) for k in ("three_pt_white", "four_pt_red"))


def feet_of(d, name, cam):
    try:
        S = H.load(os.path.join(d, "fast"), name)
        res = H.analyse(S, H.Camera(cam))
        cand = [x for x in res.get("shots", []) if x.get("shooter") is not None]
        if not cand:
            return None
        shot = min(cand, key=lambda x: abs(x["rim_t"] - T.PRE))
        feet, _ = T.takeoff_feet((res.get("tracks") or {}).get(str(shot["shooter"])) or [], shot["release_t"])
        return feet
    except Exception:  # noqa: BLE001
        return None


def game(g8):
    from agx_pipeline.team_assign import solve, team_for_shot
    d = os.path.join(GT.ROOT, g8)
    gt = json.load(open(os.path.join(d, "gt.json")))
    A, R = gt["shots"], GT.jl(os.path.join(d, "results.jsonl"))
    P = GT.pair(R, A)
    arcs = {c: T.load_arcs(c, 1920) for c in ("FL", "FR")}
    tt = solve([(r["video_t"], r["side"], r["kit_lab"]) for r in R if r.get("kit_lab")],
               gt["colours"]["left"], gt["colours"]["right"])
    out = {"game": g8, "annotated_makes": sum(a["made"] for a in A), "annotated_misses": sum(not a["made"] for a in A)}
    found = {j for j in P.values()}
    out["makes_found"] = sum(1 for j, a in enumerate(A) if a["made"] and j in found)
    out["misses_found"] = sum(1 for j, a in enumerate(A) if not a["made"] and j in found)
    out["make_miss_right"] = sum(R[i]["made"] == A[j]["made"] for i, j in P.items())
    out["paired"] = len(P)
    out["cv_shots"] = len(R)
    out["cv_not_annotated"] = len(R) - len(P)
    typed = [(R[i], A[j]) for i, j in P.items() if A[j]["type"]]
    out["type_right"], out["type_n"] = sum(r.get("zone") == a["type"] for r, a in typed), len(typed)
    flags = {"GREEN": [0, 0, 0], "YELLOW": [0, 0, 0], "RED": [0, 0, 0]}   # cards, type right, who right
    who_shown = who_right = who_n = 0
    for i, j in P.items():
        r, a = R[i], A[j]
        team = team_for_shot(tt, r["side"], r["video_t"])
        roster = set(gt["rosters"].get(team, {})) if team else None
        clip_who = W.speak(r.get("reads") or [], roster)
        ext_who = None if clip_who is not None else W.speak(r.get("reads_ext") or [], roster)
        who = clip_who if clip_who is not None else ext_who
        if a["jersey"]:
            who_n += 1
            who_shown += who is not None
            who_right += who is not None and who == a["jersey"]
        feet = feet_of(d, r["name"], r["cam"]) if r.get("zone") else None
        near = feet is not None and line_dist(arcs[r["cam"]], feet) < LINE_PX and r.get("zone") != "FREE_THROW"
        if not r.get("zone") or who is None:
            f = "RED"
        elif near or clip_who is None:
            f = "YELLOW"
        else:
            f = "GREEN"
        flags[f][0] += 1
        flags[f][1] += r.get("zone") == a["type"]
        flags[f][2] += who is not None and who == a["jersey"]
    out.update(who_n=who_n, who_shown=who_shown, who_right=who_right, flags=flags)
    # per-clip AGX seconds, as production would spend them
    jobs = []
    for r in R:
        s = (r.get("perceive_s") or 20.0) + (r.get("read_s") or 0.0)
        silent = r.get("reads") is not None and W.speak(r.get("reads") or [], None) is None
        if silent:
            s += r.get("extend_s") or EXTEND_DEFAULT_S
        if r["made"]:
            s += RENDER_S
        jobs.append((r["video_t"], s))
    out["jobs"] = sorted(jobs)
    out["agx_minutes"] = round(sum(x for _, x in jobs) / 60)
    out["span"] = [min(a["t"] for a in A) - 600, max(max(a["t"] for a in A), max(r["video_t"] for r in R)) + 5]
    return out


def simulate(games, ingest_min):
    """Back-to-back games, one FIFO worker; returns per game times (minutes from the first start)."""
    t0, timeline, queue = 0.0, [], []
    for g in games:
        start, end = g["span"]
        for vt, s in g["jobs"]:
            queue.append((t0 + (vt - start) + 15.0, s, g["game"]))    # clip ready ~15 s after the shot
        timeline.append({"game": g["game"], "start": t0, "end": t0 + (end - start)})
        t0 += (end - start) + TURNOVER_S
    free, done = 0.0, {}
    for arrive, s, gg in sorted(queue):
        free = max(free, arrive) + s
        done[gg] = free
    last_end = timeline[-1]["end"]
    ing = last_end + INGEST_WAIT_MIN * 60
    for row, m in zip(timeline, ingest_min):
        row["worker_done"] = done.get(row["game"])
        row["ingest_start"] = ing
        ing += m * 60
        row["ingest_end"] = ing
    return [{k: (round(v / 60, 1) if isinstance(v, float) else v) for k, v in row.items()} for row in timeline]


def main():
    specs = [a.split(":") for a in sys.argv[1:]]
    games = [game(g8) for g8, _ in specs]
    night = simulate(games, [float(m) for _, m in specs])
    for g in games:
        g.pop("jobs")
    print(json.dumps({"games": games, "night": night}, indent=1))


if __name__ == "__main__":
    main()

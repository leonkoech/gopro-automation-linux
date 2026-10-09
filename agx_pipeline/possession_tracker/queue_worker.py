"""Possession-tracker queue worker: one low-priority process that works through the highlight
clips of live games, one clip at a time.

Per clip (job files written by agx_pipeline/tracker_queue.py):
  1. fast perception (YOLO-seg + ByteTrack + ball/hoop) -> possession logic -> the shooter
  2. shot type from the shooter's median takeoff feet on production's court lines
  3. optional SAM3 re-check, only when the feet are near a line, capped per game
  4. write the type to basketball-games/{game}.cv_points.{logId} (what the scorecard shows and
     what ingest turns into the annotation card's type)
  5. makes: render the 2K highlight (floor ring on the ball-holder, ball-centred crop, score
     badge), upload it next to the plain clip, record highlights.{logId}.url_2k
When a game has ended and its fast jobs are done, publish its reel to Core; publish again as
later SAM3 corrections land (Core upserts on the game, so it just updates). SAM3 never delays
the first publish: in the full-game replay it cost ~6 min per shot and changed no answer.

Load: runs at nice 19 / idle IO, one clip at a time, and pauses while the live shot detector is
behind (shot_live.backlog). Recording is a separate process and is never touched.

Env (all optional):
  TRACKER_QUEUE_DIR         /home/dev/possession/queue
  TRACKER_SAM3              false  | true: re-check near-line shots with SAM3 (slow, optional)
  TRACKER_SAM3_MAX_PER_GAME 6      SAM3 re-checks per game (4 min each)
  TRACKER_SAM3_NEAR_PX      40     feet this close to a court line -> SAM3 re-check
  TRACKER_2K                true   render + upload 2K highlights for makes
  TRACKER_TV_2K             false  also point highlights.{id}.url (what the TV plays) at the 2K clip
                                   (the plain clip stays at url_plain)
  TRACKER_PUBLISH_CORE      true   publish the reel to Core when a game's queue is done
  TRACKER_WHO               false  read the jersey number on the tracked shooter -> cv_points.who
  TRACKER_WHO_EXTEND        false  if the clip gives no number, follow the shooter up to 15 s back /
                                   5 s forward in the live buffer (67% named at 78% vs 60% at 83%)
                                   (validated on 395 annotated shots: speaks on 72%, right 83%)
  TRACKER_BACKLOG_MAX       6      pause while the live detector is this many segments behind

Run:  python3 queue_worker.py            (forever)      --once   (drain, then exit)
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")


def _flag(name, default):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


QUEUE = os.environ.get("TRACKER_QUEUE_DIR", "/home/dev/possession/queue")
USE_SAM3 = _flag("TRACKER_SAM3", "false")
SAM3_MAX = int(os.environ.get("TRACKER_SAM3_MAX_PER_GAME", "6"))
REFRESH_CARDS = os.environ.get("TRACKER_REFRESH_CARDS", "true").lower() in ("1", "true", "yes")
SAM3_NEAR_PX = float(os.environ.get("TRACKER_SAM3_NEAR_PX", "40"))
DO_2K = _flag("TRACKER_2K", "true")
TV_2K = _flag("TRACKER_TV_2K", "false")
PUBLISH = _flag("TRACKER_PUBLISH_CORE", "true")
WHO = _flag("TRACKER_WHO", "false")
WHO_EXTEND = _flag("TRACKER_WHO_EXTEND", "false")
WHO_PYSRC = os.environ.get("TRACKER_WHO_PYSRC", "/home/dev/who_deps_staging/pysrc")
BACKLOG_MAX = int(os.environ.get("TRACKER_BACKLOG_MAX", "6"))
BUCKET = os.environ.get("UPLOAD_BUCKET", "uball-videos-production")
CDN = os.environ.get("HIGHLIGHT_CDN_DOMAIN", "d22gul8sdref0l.cloudfront.net")
POINTS = {"2PT": 2, "3PT": 3, "4PT": 4, "FREE_THROW": 1}
GAMES = "basketball-games"
STATES = ("pending", "sam3", "done", "failed", "published")


def log(msg):
    print("%s [tracker] %s" % (datetime.now().strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------- firebase / s3
_DB = None


def db():
    global _DB
    if _DB is None:
        import firebase_admin
        from firebase_admin import credentials, firestore
        try:
            firebase_admin.get_app()
        except ValueError:
            firebase_admin.initialize_app(credentials.Certificate(
                "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"))
        _DB = firestore.client()
    return _DB


def game_doc(game_id):
    return db().collection(GAMES).document(game_id)


def upload(path, key):
    import boto3
    boto3.client("s3").upload_file(path, BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})
    return "https://%s/%s" % (CDN, key)


# ---------------------------------------------------------------- queue files
def qpath(state, name=""):
    return os.path.join(QUEUE, state, name)


def ensure_dirs():
    for s in STATES:
        os.makedirs(qpath(s), exist_ok=True)


def move(name, src, dst, job=None):
    if job is not None:
        with open(qpath(src, name), "w") as f:
            json.dump(job, f)
    os.replace(qpath(src, name), qpath(dst, name))


def jobs(state, game_id=None):
    out = []
    for f in os.listdir(qpath(state)):
        if not f.endswith(".json") or (game_id and not f.startswith(game_id + "__")):
            continue
        try:
            j = json.load(open(qpath(state, f)))
        except Exception:  # noqa: BLE001 - a half-written file: next pass
            continue
        out.append((j.get("queued_at", 0), f, j))
    return [(f, j) for _, f, j in sorted(out)]


# ---------------------------------------------------------------- load guard
def detector_behind(game_id):
    """True while the live shot detector for this game is behind: the tracker waits."""
    try:
        d = game_doc(game_id).get().to_dict() or {}
        live = d.get("shot_live") or {}
        return live.get("status") == "running" and int(live.get("backlog") or 0) > BACKLOG_MAX
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------- tracking
def arcs(cam):
    import track_shadow as T
    return T.load_arcs(cam, 1920)


def line_px(cam, feet):
    a = arcs(cam)
    pt = (float(feet[0]), float(feet[1]))
    return float(min((cv2.pointPolygonTest(a[k], pt, True) for k in ("three_pt_white", "four_pt_red") if k in a),
                     key=abs))


def track(clip, cam, name, work, pre, how):
    """Perception + possession logic on one clip. Returns (S, res, shot, zone, feet)."""
    import holder as H
    import track_shadow as T
    script = "perceive_fast.py" if how == "fast" else "perceive.py"
    root = os.path.join(work, how)
    subprocess.run(["nice", "-n", "19", "ionice", "-c3", "python3", os.path.join(HERE, script), clip, root,
                    "%.2f" % pre], capture_output=True, text=True, timeout=1500, check=True)
    S = H.load(root, name)
    res = H.analyse(S, H.Camera(cam))
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    if not shots:
        return S, res, None, None, None
    shot = min(shots, key=lambda s: abs(s["rim_t"] - pre))
    feet, still = T.takeoff_feet((res.get("tracks") or {}).get(str(shot["shooter"]), []), shot["release_t"])
    zone = T.zone_of(arcs(cam), feet, still) if feet else None
    return S, res, shot, zone, feet


def stage_clip(job, work):
    """Copy the clip into the work dir under a name that carries the camera (FL_/FR_)."""
    cam = job["angle"]
    name = "%s_%s" % (cam, job["log_id"])
    dst = os.path.join(work, name + ".mp4")
    shutil.copyfile(job["clip"], dst)
    return name, dst


def write_type(job, zone, source, extra):
    rec = {"zone": zone, "points": POINTS[zone], "who": None, "angle": job["angle"],  # extra may set who
           "typed_at": datetime.now(timezone.utc).isoformat(), "confidence": 0.9 if source == "sam3" else 0.8,
           "zone_source": "tracker_" + source}
    rec.update(extra)
    game_doc(job["game_id"]).set({"cv_points": {job["log_id"]: rec}}, merge=True)


def render_2k(job, work, name, S, res, zone):
    import render_2k as R
    dst = os.path.join(work, "r2k")
    R.render(os.path.join(work, "fast"), work, name, dst, False, make=True, badge_zone=zone, S=S, res=res)
    made = os.path.join(dst, name + "_2k.mp4")
    if not os.path.isfile(made) or os.path.getsize(made) == 0:
        return None
    key = (job.get("s3_key") or "highlights/tracker/%s/%s_%s.mp4" % (job["game_id"], job["log_id"], job["angle"]))
    # a new name per render: a re-render must never be hidden behind the CDN's cached copy
    tag = "_2k_%d.mp4" % int(time.time())
    key = key[:-4] + tag if key.endswith(".mp4") else key + tag
    url = upload(made, key)
    local = os.path.join(os.path.dirname(job["clip"]), "2k", os.path.basename(key))
    os.makedirs(os.path.dirname(local), exist_ok=True)
    shutil.copyfile(made, local)
    patch = {"highlights.%s.url_2k" % job["log_id"]: url, "highlights.%s.s3_key_2k" % job["log_id"]: key}
    if TV_2K:
        h = ((game_doc(job["game_id"]).get().to_dict() or {}).get("highlights") or {}).get(job["log_id"]) or {}
        if h.get("url") and not h.get("url_plain"):
            patch["highlights.%s.url_plain" % job["log_id"]] = h["url"]
        patch["highlights.%s.url" % job["log_id"]] = url
    game_doc(job["game_id"]).update(patch)
    return url


_JS = None


def jersey_stack():
    global _JS
    if _JS is None:
        if WHO_PYSRC not in sys.path:
            sys.path.insert(0, WHO_PYSRC)
        from uball_cc.tracking.jersey_stack import JerseyStack
        _JS = JerseyStack()
    return _JS


def read_who(clip, res, shot, job=None):
    """Jersey number of the tracked shooter: read on every frame he is tracked, vote; if the
    clip gives none and TRACKER_WHO_EXTEND is on, follow him beyond the clip."""
    import who_eval as W
    boxes = (res.get("tracks") or {}).get(str(shot["shooter"]), [])
    reads = W.read_track(jersey_stack(), clip, boxes)
    who_from = "clip" if W.speak(reads, None) is not None else None
    if WHO_EXTEND and job is not None and who_from is None:
        try:
            import who_extend_live as XL
            n0 = len(reads)
            reads = XL.extend_reads(jersey_stack(), job, boxes, reads)
            job["who_extended_reads"] = len(reads) - n0
        except Exception as e:  # noqa: BLE001 - the clip-only answer stands
            job["who_extend_error"] = "%s: %s" % (type(e).__name__, e)
    num = W.speak(reads, None)
    if num is not None and who_from is None:
        who_from = "timeline"          # found only after following the player back
    votes = {}
    for n, c in reads:
        if c >= W.READ_MIN:
            votes[n] = round(votes.get(n, 0) + c, 2)
    top = sorted(votes.items(), key=lambda kv: -kv[1])[:3]
    return num, top, who_from


def process_fast(name, job):
    t0 = time.time()
    if not os.path.isfile(job["clip"]):
        raise RuntimeError("clip missing: %s" % job["clip"])
    with tempfile.TemporaryDirectory(prefix="tq_") as work:
        cname, clip = stage_clip(job, work)
        S, res, shot, zone, feet = track(clip, job["angle"], cname, work, job["pre"], "fast")
        job["fast_zone"] = zone
        job["feet_px"] = feet
        extra = {}
        if shot is not None:
            try:
                import team_eval as TE
                rel = shot.get("release_i") or 0
                lab = TE.torso_colour(S, clip, shot["shooter"], list(range(max(0, rel - 12), rel + 1, 3)))
                if lab is not None:
                    extra["kit_lab"] = [round(float(v), 1) for v in lab]
            except Exception as e:  # noqa: BLE001 - team falls back to basket side alone
                job["kit_error"] = "%s: %s" % (type(e).__name__, e)
        if WHO and shot is not None:
            try:
                num, top, who_from = read_who(clip, res, shot, job)
                job["who"] = num
                extra.update({"who": num, "who_votes": dict(top), "who_source": "tracker", "who_from": who_from})
            except Exception as e:  # noqa: BLE001 - WHO never blocks the type
                job["who_error"] = "%s: %s" % (type(e).__name__, e)
        if zone:
            job["line_px"] = round(line_px(job["angle"], feet), 1)
            write_type(job, zone, "fast", dict(extra, feet_px=[round(v) for v in feet], line_px=job["line_px"]))
        near = zone is None or abs(job.get("line_px") or 0) < SAM3_NEAR_PX
        if DO_2K and job.get("made") is not False:
            try:
                job["url_2k"] = render_2k(job, work, cname, S, res, zone)
            except Exception as e:  # noqa: BLE001 - the plain clip stays
                job["render_error"] = "%s: %s" % (type(e).__name__, e)
    job["fast_s"] = round(time.time() - t0, 1)
    sam3_done = len(jobs("sam3", job["game_id"])) + sum(1 for _, j in jobs("done", job["game_id"]) if j.get("sam3_ran"))
    if USE_SAM3 and near and sam3_done < SAM3_MAX:
        move(name, "pending", "sam3", job)
        log("%s fast=%s line=%s -> SAM3 queue (%.0fs)" % (job["log_id"], zone, job.get("line_px"), job["fast_s"]))
    else:
        move(name, "pending", "done", job)
        log("%s fast=%s line=%s 2k=%s who=%s (%.0fs)" % (job["log_id"], zone, job.get("line_px"),
                                                         "yes" if job.get("url_2k") else "no", job.get("who"), job["fast_s"]))


def process_sam3(name, job):
    t0 = time.time()
    with tempfile.TemporaryDirectory(prefix="tq3_") as work:
        cname, clip = stage_clip(job, work)
        _, _, _, zone, feet = track(clip, job["angle"], cname, work, job["pre"], "sam3")
    job["sam3_zone"], job["sam3_ran"], job["sam3_s"] = zone, True, round(time.time() - t0, 1)
    if zone:
        write_type(job, zone, "sam3", {"feet_px": [round(v) for v in feet], "fast_zone": job.get("fast_zone")})
    move(name, "sam3", "done", job)
    log("%s SAM3=%s (fast %s) (%.0fs)" % (job["log_id"], zone, job.get("fast_zone"), job["sam3_s"]))


# ---------------------------------------------------------------- publish
def maybe_publish():
    """Publish each ended game whose fast work is done; republish when more jobs finish."""
    if not PUBLISH:
        return
    games = {f.split("__")[0] for st in ("done", "failed") for f in os.listdir(qpath(st)) if f.endswith(".json")}
    for g in games:
        if jobs("pending", g):
            continue
        n_done = len(jobs("done", g))
        if os.path.exists(qpath("published", g)):
            try:
                if json.load(open(qpath("published", g))).get("done", 0) >= n_done:
                    continue            # nothing new since the last publish
            except Exception:  # noqa: BLE001
                continue
        d = game_doc(g).get().to_dict() or {}
        if d.get("status") != "completed" and not d.get("endedAt"):
            continue
        # team for every shot: one half-time switch found from the shooters' kit colours
        try:
            from agx_pipeline.team_assign import assign_game, team_for_shot
            tt = assign_game(d)
            if tt:
                patch = {"tracker_teams": tt}
                for lid in (d.get("cv_points") or {}):
                    m = lid.split("_")
                    if len(m) == 3 and m[0] == "cv":
                        team = team_for_shot(tt, m[2], float(m[1]))
                        if team:
                            patch["cv_points.%s.team" % lid] = team
                game_doc(g).update(patch)
                d = game_doc(g).get().to_dict() or {}
                log("game %s teams: half-time %s, left basket first: %s" % (g, tt.get("switch_epoch"), tt.get("left_basket_first")))
        except Exception as e:  # noqa: BLE001
            log("team pass failed for %s: %s" % (g, e))
        # the game's annotation cards, if ingest has already made them: bring them up to date with
        # this game's tracker results (type, jersey, green/yellow/red flag); cards an annotator
        # changed are never touched. If ingest comes later it builds the cards from these results.
        if REFRESH_CARDS:
            try:
                sys.path.insert(0, "/home/dev/gopro-automation-linux")
                from uball_client import UballClient
                from plays_sync import refresh_cv_cards, rosters_from_annotation_game
                uc = UballClient()
                ag = uc.get_game_by_firebase_id(g)
                if ag and ag.get("id"):
                    st = refresh_cv_cards(uc, ag["id"], d, rosters_from_annotation_game(ag))
                    log("game %s cards refreshed: %s" % (g, st))
            except Exception as e:  # noqa: BLE001
                log("card refresh failed for %s: %s" % (g, e))
        from agx_pipeline.core_highlight import publish_core_highlight
        date = None
        for h in (d.get("highlights") or {}).values():
            k = h.get("s3_key") or ""
            if k.count("/") >= 2:
                date = k.split("/")[1]
                break
        n = publish_core_highlight(g, d, date)
        open(qpath("published", g), "w").write(json.dumps({"at": time.time(), "clips": n, "done": n_done}))
        log("game %s published to Core: %d clips" % (g, n))


# ---------------------------------------------------------------- main loop
def step():
    """Do one unit of work. Returns True if something was processed."""
    pend = jobs("pending")
    for name, job in pend:
        if detector_behind(job["game_id"]):
            continue
        try:
            process_fast(name, job)
        except Exception as e:  # noqa: BLE001
            job["error"] = "%s: %s" % (type(e).__name__, e)
            job["trace"] = traceback.format_exc()[-1500:]
            move(name, "pending", "failed", job)
            log("%s FAILED %s" % (job.get("log_id"), job["error"]))
        return True
    for name, job in jobs("sam3"):          # SAM3 only when no fast work is waiting
        if detector_behind(job["game_id"]):
            continue
        try:
            process_sam3(name, job)
        except Exception as e:  # noqa: BLE001 - the fast type stands
            job["sam3_error"] = "%s: %s" % (type(e).__name__, e)
            move(name, "sam3", "done", job)
            log("%s SAM3 failed, fast type stands: %s" % (job.get("log_id"), job["sam3_error"]))
        return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="drain the queue, publish, exit")
    a = ap.parse_args()
    ensure_dirs()
    try:
        os.nice(19)
    except OSError:
        pass
    log("worker up: queue=%s sam3=%s (max %d/game, near %gpx) 2k=%s tv_2k=%s publish=%s who=%s who_extend=%s"
        % (QUEUE, USE_SAM3, SAM3_MAX, SAM3_NEAR_PX, DO_2K, TV_2K, PUBLISH, WHO, WHO_EXTEND))
    idle = 0
    while True:
        worked = step()
        if not worked:
            try:
                maybe_publish()
            except Exception as e:  # noqa: BLE001
                log("publish check failed: %s" % e)
            if a.once:
                break
            idle += 1
            time.sleep(min(30, 2 + idle))
        else:
            idle = 0


if __name__ == "__main__":
    main()

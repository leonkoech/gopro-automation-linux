"""SHADOW shot typing from the possession tracker. Writes nothing to any card.

For every CV card of one game (same selection as the nightly shot_typing.py), cut the
FL/FR master around the card time, run the FAST tracker (YOLO-seg + ByteTrack perception,
then holder.py's possession logic) and type the shot from the tracked shooter's median
takeoff feet on the SAME calibration arcs production uses. Where the fast answer disagrees
with the card's current type (or is missing), SAM3 re-checks that card and its answer wins. The answer lands next to
production's in <out>/<label>.jsonl so the two can be compared against annotator GT before
anything is switched over.

Run on the box, in this directory, with the SAM3/CUDA env (box/sam3env.sh) and .env.agx:
  python3 track_shadow.py --label game_20260914_... --uball-game-id <uuid> [--limit N]

Never runs while the box is recording (checked before every card).
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")

REC = os.environ.get("SHADOW_REC_DIR", "/home/dev/app/recordings")
TYPING_CWD = os.environ.get("SHOT_TYPING_CWD", "/home/dev/shot_typing")
OUT = os.environ.get("SHADOW_OUT", os.path.join(HERE, "shadow"))
HEALTH = "http://localhost:5000/health"
TRACK_CAM = {"LEFT": "FL", "RIGHT": "FR"}
PRE, POST = 5.0, 3.0              # clip = [card_ts - PRE, card_ts + POST] at 1920x1080 (the
                                  # camera pose's resolution); rim nominal at PRE
TAKEOFF_WIN = 0.8                 # s before release: grounded feet (median of box bottoms)
FT_STILL_PX = 60                  # feet moving less than this over the window = set shot


def log(msg):
    print("[shadow] %s" % msg, flush=True)


def recording() -> bool:
    try:
        with urllib.request.urlopen(HEALTH, timeout=5) as r:
            return bool(json.loads(r.read().decode()).get("recording"))
    except Exception:  # noqa: BLE001 - unknown state counts as busy
        return True


def cut_clip(master: str, ts: float, dst: str) -> bool:
    start = max(0.0, ts - PRE)
    cp = subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-y", "-ss", "%.2f" % start, "-i", master,
                         "-t", "%.2f" % (PRE + POST), "-vf", "scale=1920:1080",
                         "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                         "-r", "30", "-an", dst], capture_output=True, text=True)
    return cp.returncode == 0 and os.path.isfile(dst) and os.path.getsize(dst) > 0


def load_arcs(cam: str, width: int):
    a = json.load(open(os.path.join(TYPING_CWD, "calib_arcs_%s.json" % cam)))
    sc = width / float(a.get("w") or width)
    return {k: np.array(a[k], np.float32) * sc for k in ("three_pt_white", "four_pt_red", "ft_stripe") if a.get(k)}


def zone_of(arcs, px, still):
    pt = (float(px[0]), float(px[1]))
    if still and "ft_stripe" in arcs and cv2.pointPolygonTest(arcs["ft_stripe"], pt, False) >= 0:
        return "FREE_THROW"
    if cv2.pointPolygonTest(arcs["three_pt_white"], pt, False) >= 0:
        return "2PT"
    if cv2.pointPolygonTest(arcs["four_pt_red"], pt, False) >= 0:
        return "3PT"
    return "4PT"


def takeoff_feet(track, release_t):
    """Median bottom-centre of the shooter's box over the TAKEOFF_WIN before release; the
    lowest-box rule was fooled when the box swallowed a defender's legs."""
    win = [b for t, b in track if release_t - TAKEOFF_WIN <= t <= release_t]
    if not win:
        return None, False
    pts = np.array([((b[0] + b[2]) / 2, b[3]) for b in win], float)
    still = bool(np.ptp(pts[:, 0]) < FT_STILL_PX and np.ptp(pts[:, 1]) < FT_STILL_PX)
    return [float(v) for v in np.median(pts, axis=0)], still


PERCEIVERS = {"fast": ("perceive_fast.py", 300), "sam3": ("perceive.py", 1200)}


def track_card(clip: str, cam: str, work: str, how: str = "fast"):
    """Perception (fast YOLO-seg + ByteTrack, or SAM3) into its own cache dir under `work`,
    then the possession logic. Returns (chosen shot, holder result) or None."""
    import holder
    script, timeout = PERCEIVERS[how]
    cache_root = os.path.join(work, how)
    subprocess.run(["python3", os.path.join(HERE, script), clip, cache_root, "%.2f" % PRE],
                   capture_output=True, text=True, timeout=timeout, check=True)
    name = os.path.splitext(os.path.basename(clip))[0]
    S = holder.load(cache_root, name)
    res = holder.analyse(S, holder.Camera(cam))
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    if not shots:
        return None
    # the attempt whose rim moment is nearest the card time (the card IS a rim event)
    return min(shots, key=lambda s: abs(s["rim_t"] - PRE)), res


TYPE_OF = {"FG": "2PT", "2PT": "2PT", "3PT": "3PT", "4PT": "4PT", "FREE_THROW": "FREE_THROW"}


def type_with(clip: str, cam: str, work: str, how: str) -> dict:
    """One perception route -> shooter -> median takeoff feet -> zone on production's arcs."""
    got = track_card(clip, cam, work, how)
    if got is None:
        return {"track_zone": None}
    shot, res = got
    track = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
    feet, still = takeoff_feet(track, shot["release_t"])
    return {"track_zone": zone_of(load_arcs(cam, 1920), feet, still) if feet else None,  # cut is 1920x1080
            "release_t": shot["release_t"], "rim_t": shot["rim_t"], "votes": shot.get("votes"),
            "feet_px": feet, "still": still, "n_attempts": len(res.get("shots", []))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--uball-game-id", required=True)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--ts-offset", type=float, default=0.0)
    # where the FL/FR masters are; {label} and {cam} are filled in
    ap.add_argument("--master-pattern", default=REC + "/{label}/{label}_{cam}.mp4")
    ap.add_argument("--no-sam3-escalation", dest="sam3_escalation", action="store_false",
                    help="fast tracker only (default: SAM3 re-checks cards where fast disagrees)")
    a = ap.parse_args()

    from uball_client import UballClient
    client = UballClient()
    cards = [p for p in client.list_plays(a.uball_game_id)
             if p.get("source") == "cv" and re.match(r"(FG|2PT|3PT|4PT|FREE_THROW)_(MAKE|MISS)",
                                                    str(p.get("classification") or ""))]
    cards.sort(key=lambda p: p.get("timestamp_seconds") or 0)
    if a.limit:
        cards = cards[:a.limit]
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "%s.jsonl" % a.label)
    done = set()
    if os.path.exists(out_path):
        done = {json.loads(l)["card_id"] for l in open(out_path) if l.strip()}
    log("%d CV cards, %d already shadowed -> %s" % (len(cards), len(done), out_path))

    n_ok = n_none = 0
    with tempfile.TemporaryDirectory(prefix="shadow_") as work, open(out_path, "a") as fo:
        for i, card in enumerate(cards):
            if card["id"] in done:
                continue
            while recording():
                log("box is recording a game — waiting")
                time.sleep(60)
            t0 = time.time()
            ts = float(card.get("timestamp_seconds") or 0) + a.ts_offset
            cam = TRACK_CAM.get(card.get("angle") or "LEFT", "FL")
            master = a.master_pattern.format(label=a.label, cam=cam)
            clip = os.path.join(work, "%s_c%04d.mp4" % (cam, i))
            rec = {"card_id": card["id"], "ts": ts, "angle": cam,
                   "prod_classification": card.get("classification")}
            try:
                if not cut_clip(master, ts, clip):
                    raise RuntimeError("clip cut failed (%s)" % master)
                rec.update(type_with(clip, cam, work, "fast"))
                rec["fast_zone"] = rec["track_zone"]
                # SAM3 only where it can matter: the fast tracker disagrees with the card's
                # current type, or has no answer. Measured on 271 GT shots: escalates 19% and
                # scores 251 vs 252 for SAM3 on every shot (fast alone 245, production 220).
                prod_type = TYPE_OF.get(str(rec["prod_classification"]).rsplit("_", 1)[0])
                if a.sam3_escalation and rec["track_zone"] != prod_type:
                    sam = type_with(clip, cam, work, "sam3")
                    rec["sam3"] = sam
                    rec["escalated"] = True
                    if sam.get("track_zone"):
                        rec["track_zone"] = sam["track_zone"]
                n_ok += rec["track_zone"] is not None
                n_none += rec["track_zone"] is None
            except Exception as e:  # noqa: BLE001 - one card never stops the game
                rec["error"] = "%s: %s" % (type(e).__name__, e)
                n_none += 1
            finally:
                import shutil
                for f in os.listdir(work):
                    p = os.path.join(work, f)
                    shutil.rmtree(p) if os.path.isdir(p) else os.remove(p)
            rec["secs"] = round(time.time() - t0, 1)
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
            log("[%d/%d] %s %.1f prod=%s fast=%s%s final=%s (%.0fs)%s" % (
                i + 1, len(cards), cam, ts, rec["prod_classification"], rec.get("fast_zone"),
                " sam3=%s" % rec["sam3"].get("track_zone") if rec.get("sam3") else "", rec.get("track_zone"),
                rec["secs"], " ERR " + rec["error"] if "error" in rec else ""))
    log("SUMMARY typed=%d untyped=%d -> %s" % (n_ok, n_none, out_path))
    print("SHADOW_DONE", flush=True)


if __name__ == "__main__":
    main()

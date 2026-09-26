"""Possession logic over a perceive.py cache: who has the ball on each frame, and who
released the shot.

  1. ball: tracked BACKWARDS from the rim. At the rim moment the ball nearest the hoop is
     the shot ball beyond doubt; each earlier frame takes the candidate closest to where
     the ball was a frame later, so parked balls and false positives elsewhere never enter.
  2. ball in 3D: pixel size -> distance along the ray (a ball is 24cm), size smoothed over
     5 frames because the raw size jitters ~10% (= +-150cm of depth at 20m).
  3. contact: the ball centre touches a player's SAM3 mask (dilated by ~a ball radius).
     Depth is the tiebreak between overlapping players, and a veto when the ball is
     clearly in front of / behind the player it overlaps.
  4. release = last contact frame before the ball's free flight to the rim; shooter = the
     player with the most contact frames in the RELEASE_WIN seconds up to the release.

Usage: holder.py <out_dir> <clip_dir> <name> [<name> ...]      (angle from the name: FL_/FR_ or L_FL_/L_FR_)
Writes <out_dir>/v1/<name>.json, _sheet.jpg, .mp4
"""
import json
import os
import sys

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
POSE = os.path.join(HERE, "calib", "camera_pose.json")
CALIB = os.path.join(HERE, "calib", "calib_staging")
VER = os.environ.get("HOLDER_VER", "v1")
# Cheaper-perception simulations, run on the SAM3 caches:
#   CONTACT_MODE=box  touching = ball inside the player's box (padded), no mask: what a
#                     plain detector + tracker could give at a fraction of SAM3's cost
#   FPS_DIV=2         keep every 2nd frame (7.5 fps): half the perception cost
CONTACT_MODE = os.environ.get("CONTACT_MODE", "mask")
FPS_DIV = int(os.environ.get("FPS_DIV", "1"))

BALL_CM = 24.0
MAX_W, MAX_H, MIN_H = 400, 750, 40     # SAM3 sometimes returns a merged box spanning half the court
BALL_MIN_CONF = 0.10
BALL_JUMP_PX = 160                     # per frame at 15 fps, backwards
DEPTH_VETO_CM = 200                    # overlap in the image but this far apart on the floor: not holding
HOLD_MAX_H_CM = 300                    # a ball above this is in flight, whoever it overlaps
RELEASE_WIN = 0.6
MIN_RELEASE_VOTES = max(1, 2 // FPS_DIV)
JUMP_MIN_CONF = 0.3
ON_COURT_MIN_FRAC = 0.3               # share of a track's frames that must be on court
OFF_COURT_CM = 150                     # a player's feet may be this far out of bounds
MAX_RISE_CM_S = 900                    # release height -> rim height, average climb
NEAR_PX = 40 * FPS_DIV
PARKED_PX = 12                         # a still ball: same spot within this many px
PARKED_FRAC = 0.5                      # ... on at least half of the clip's frames
SINGLE_VETO_CM = 450                   # one toucher: jumping feet project far back
RIM_CM = 305.0
RIM_SIZE_LO, RIM_SIZE_HI = 0.7, 1.35
EVENT_GAP = max(2, 4 // FPS_DIV)                          # frames between two separate arrivals at the rim
MIN_HISTORY = 1.0                      # an arrival needs this much clip before it
LOCK = 3
if FPS_DIV > 1:
    BALL_JUMP_PX *= FPS_DIV
    LOCK = max(2, LOCK // FPS_DIV)


def court_box():
    hull = np.vstack([np.array(json.load(open(os.path.join(CALIB, "%s.json" % c)))["calib_hull"], float)
                      for c in ("FL", "FR")])
    return hull.min(0), hull.max(0)


def undistort(cal, pts):
    lam = cal.get("division_lambda")
    pts = np.asarray(pts, float).reshape(-1, 2)
    if not lam:
        return pts
    cx, cy = cal.get("principal_point") or [960.0, 540.0]
    d = float(np.hypot(cx, cy))
    q = pts - [cx, cy]
    r2 = (q ** 2).sum(1) / d ** 2
    return q / (1 + lam * r2)[:, None] + [cx, cy]


COURT_BOX = None


class Camera:
    def __init__(self, angle):
        p = json.load(open(POSE))[angle]
        self.K, self.R, self.t = (np.array(p[k], float) for k in ("K", "R", "t"))
        self.t = self.t.reshape(3)
        self.f = float(p.get("f") or self.K[0, 0])
        self.cal = json.load(open(os.path.join(CALIB, "%s.json" % angle)))
        self.H = np.array(self.cal["homography_matrix"], float)
        global COURT_BOX
        if COURT_BOX is None:
            COURT_BOX = court_box()

    def on_court(self, box):
        """Feet within the court plus OFF_COURT_CM. SAM3's "basketball player" prompt also
        segments people seated courtside; one of them was once credited with a free throw."""
        x, y = self.to_court([[(box[0] + box[2]) / 2, box[3]]])[0]
        (x0, y0), (x1, y1) = COURT_BOX
        return x0 - OFF_COURT_CM <= x <= x1 + OFF_COURT_CM and y0 - OFF_COURT_CM <= y <= y1 + OFF_COURT_CM

    def to_court(self, px):
        return cv2.perspectiveTransform(undistort(self.cal, px).reshape(-1, 1, 2), self.H).reshape(-1, 2)

    def rim_ball_px(self, u, v):
        """Pixel diameter a ball would have AT the rim seen at hoop pixel (u, v): the ray's
        depth where it crosses rim height (world z = -RIM_CM, z points down)."""
        uu, vv = undistort(self.cal, [[u, v]])[0]
        C = -self.R.T @ self.t
        ray = np.linalg.inv(self.K) @ np.array([uu, vv, 1.0])
        s = (-RIM_CM - C[2]) / (self.R.T @ ray)[2]
        return self.f * BALL_CM / s

    def ball_3d(self, u, v, d_px):
        """-> (x, y, height) cm. This pose's world z points DOWN, hence the sign flip."""
        uu, vv = undistort(self.cal, [[u, v]])[0]
        z = self.f * BALL_CM / max(d_px, 1.0)
        X = self.R.T @ (z * np.linalg.inv(self.K) @ np.array([uu, vv, 1.0]) - self.t)
        return np.array([X[0], X[1], -X[2]])


def load(out, name):
    c = np.load(os.path.join(out, "cache", name + ".npz"))
    n = len(c["times"])
    mh, mw = [int(v) for v in c["mask_shape"]]
    H, W = [int(v) for v in c["frame_shape"]]
    players = [dict() for _ in range(n)]
    for row, m in zip(c["players"], c["masks"]):
        i, oid = int(row[0]), int(row[1])
        x1, y1, x2, y2 = row[3:7]
        if x2 - x1 > MAX_W or y2 - y1 > MAX_H or y2 - y1 < MIN_H:
            continue
        players[i][oid] = {"box": row[3:7], "mask": np.unpackbits(m)[:mh * mw].reshape(mh, mw).astype(bool)}
    balls = [[] for _ in range(n)]
    hoops = [[] for _ in range(n)]
    for d in c["dets"]:
        (balls if int(d[1]) == 0 else hoops)[int(d[0])].append(d[2:7])
    balls = drop_parked(balls)
    S = {"times": c["times"], "rim_t": float(c["rim_t"]), "players": players, "balls": balls,
         "hoops": hoops, "scale": mw / float(W), "HW": (H, W)}
    if FPS_DIV > 1:
        keep = list(range(0, n, FPS_DIV))
        S.update({"times": c["times"][keep], "players": [players[k] for k in keep],
                  "balls": [balls[k] for k in keep], "hoops": [hoops[k] for k in keep]})
    return S


def drop_parked(balls):
    """A ball resting by the bench or in a spectator's lap sits in the same pixel spot for
    most of the clip, and the backward track can jump onto it (a free throw was credited to
    someone seated behind the court). Drop candidates that have a neighbour within PARKED_PX
    on at least PARKED_FRAC of all frames."""
    n = len(balls)
    pts = [np.array([[(b[1] + b[3]) / 2, (b[2] + b[4]) / 2] for b in fr]) for fr in balls]
    out = []
    for k, fr in enumerate(balls):
        keep = []
        for j, b in enumerate(fr):
            p = pts[k][j]
            hits = sum(1 for q in pts if len(q) and np.min(np.linalg.norm(q - p, axis=1)) <= PARKED_PX)
            if hits < PARKED_FRAC * n:
                keep.append(b)
        out.append(keep)
    return out


def hoop_of(S):
    """The hoop does not move: median of the best hoop box per frame -> (centre, radius)."""
    hb = [max(h, key=lambda d: d[0]) for h in S["hoops"] if h]
    if not hb:
        return None, None
    hb = np.median(np.array(hb), axis=0)
    return np.array([(hb[1] + hb[3]) / 2, (hb[2] + hb[4]) / 2]), max(60.0, 1.5 * (hb[3] - hb[1]))


def ctr(b):
    return np.array([(b[1] + b[3]) / 2, (b[2] + b[4]) / 2])


def rim_events(S, hoop, near_r, rim_d):
    """Every arrival of a ball at the hoop: runs of frames with a ball inside near_r, split
    by gaps of more than EVENT_GAP frames. A missed shot and its putback are two events."""
    t, hits = S["times"], []
    for k in range(len(t)):
        if t[k] < t[0] + MIN_HISTORY:
            continue
        c = [(np.linalg.norm(ctr(b) - hoop), b) for b in S["balls"][k] if b[0] >= BALL_MIN_CONF]
        # near the hoop in the image AND rim-sized: a ball held high between the camera and
        # the hoop overlaps it in the image but is much bigger
        c = [x for x in c if x[0] <= near_r
             and RIM_SIZE_LO <= ((x[1][3] - x[1][1]) + (x[1][4] - x[1][2])) / 2 / rim_d <= RIM_SIZE_HI]
        # arriving means falling: a ball rising past the hoop just after release overlaps it too
        prev = [ctr(b) for b in S["balls"][k - 1] if b[0] >= BALL_MIN_CONF] if k > 0 else []
        falling = []
        for d, bb in c:
            p = min(prev, key=lambda q: np.linalg.norm(q - ctr(bb)), default=None)
            if p is None or np.linalg.norm(p - ctr(bb)) > BALL_JUMP_PX or ctr(bb)[1] >= p[1] - 2:
                falling.append((d, bb))
        if falling:
            d, bb = min(falling, key=lambda x: x[0])
            hits.append((k, d, bb))
    events, cur = [], []
    for h in hits:
        if cur and h[0] - cur[-1][0] > EVENT_GAP:
            events.append(cur)
            cur = []
        cur.append(h)
    if cur:
        events.append(cur)
    return [min(e, key=lambda h: h[1]) for e in events]


def track_from(S, start, b0):
    """Ball track anchored at the rim frame: backwards (the shot) and forwards (the picture)."""
    n = len(S["times"])
    track = [None] * n
    track[start] = b0
    prev, vel, gap = b0, np.zeros(2), 0
    for k in range(start - 1, -1, -1):
        pred = ctr(prev) + vel * (gap + 1)
        cands = [b for b in S["balls"][k] if b[0] >= BALL_MIN_CONF]
        # a long jump needs a confident detection: faint false positives (a spectator's lap,
        # a shoe) are only taken when they sit right where the ball is expected
        cands = [b for b in cands if b[0] >= JUMP_MIN_CONF or np.linalg.norm(ctr(b) - pred) <= NEAR_PX]
        best = min(cands, key=lambda b: np.linalg.norm(ctr(b) - pred), default=None)
        if best is not None and np.linalg.norm(ctr(best) - pred) <= BALL_JUMP_PX * (1 + 0.5 * gap):
            vel = 0.5 * vel + 0.5 * (ctr(best) - ctr(prev)) / (gap + 1)
            track[k], prev, gap = best, best, 0
        else:
            gap += 1
    prev = b0
    for k in range(start + 1, n):
        cands = [b for b in S["balls"][k] if b[0] >= BALL_MIN_CONF]
        best = min(cands, key=lambda b: np.linalg.norm(ctr(b) - ctr(prev)), default=None)
        if best is not None and np.linalg.norm(ctr(best) - ctr(prev)) <= BALL_JUMP_PX:
            track[k], prev = best, best
    return track


def pick_near(contact):
    """Nearest touching player on the floor. Depth is noisy and a jumping player's feet
    project too far back, so one toucher is trusted up to SINGLE_VETO_CM; the strict
    DEPTH_VETO_CM only applies when several players touch the ball."""
    if not contact:
        return None
    oid = min(contact, key=contact.get)
    lim = SINGLE_VETO_CM if len(contact) == 1 else DEPTH_VETO_CM
    return oid if contact[oid] <= lim else None


def off_court_tracks(S, cam):
    """Tracks whose feet are on court in under ON_COURT_MIN_FRAC of their frames: people
    seated courtside. Judged over the whole clip because a player in the air projects metres
    behind the baseline for a few frames (a layup was once filtered out mid-jump)."""
    seen, on = {}, {}
    for fr in S["players"]:
        for oid, p in fr.items():
            seen[oid] = seen.get(oid, 0) + 1
            on[oid] = on.get(oid, 0) + int(cam.on_court(p["box"]))
    return {o for o in seen if on[o] < ON_COURT_MIN_FRAC * seen[o]}


def frame_states(S, cam, track):
    n = len(S["times"])
    off_court = off_court_tracks(S, cam)
    sizes = np.array([np.nan if b is None else ((b[3] - b[1]) + (b[4] - b[2])) / 2 for b in track])
    sm = np.array([np.nanmedian(sizes[max(0, k - 2):k + 3]) if not np.isnan(sizes[k]) else np.nan
                   for k in range(n)])
    frames = []
    for k in range(n):
        st = {"t": round(float(S["times"][k]), 3), "players": len(S["players"][k]), "contact": {}}
        b = track[k]
        if b is not None:
            u, v = ctr(b)
            X = cam.ball_3d(u, v, sm[k])
            st.update({"ball_px": [round(u), round(v)], "ball_d": round(float(sm[k]), 1),
                       "ball_xyh": [round(float(c)) for c in X]})
            r = max(2, int(round(0.75 * sm[k] * S["scale"])))
            mu, mv = int(round(u * S["scale"])), int(round(v * S["scale"]))
            for oid, p in S["players"][k].items():
                if oid in off_court:
                    continue
                m = p["mask"] if CONTACT_MODE == "mask" else np.zeros((1, 1), bool)
                y0, y1_, x0, x1_ = max(0, mv - r), mv + r + 1, max(0, mu - r), mu + r + 1
                touch = m[y0:y1_, x0:x1_].any() if m.any() else (
                    p["box"][0] - r / S["scale"] <= u <= p["box"][2] + r / S["scale"]
                    and p["box"][1] <= v <= p["box"][3])
                if not touch:
                    continue
                feet = cam.to_court([[(p["box"][0] + p["box"][2]) / 2, p["box"][3]]])[0]
                st["contact"][oid] = round(float(np.linalg.norm(feet - X[:2])))
        low = "ball_xyh" in st and st["ball_xyh"][2] <= HOLD_MAX_H_CM
        st["near"] = pick_near(st["contact"]) if low else None
        frames.append(st)
    holder, run_id, run_n = None, None, 0
    for st in frames:
        if st["near"] is not None and st["near"] == run_id:
            run_n += 1
        else:
            run_id, run_n = st["near"], 1 if st["near"] is not None else 0
        if run_id is not None and run_n >= LOCK:
            holder = run_id
        st["holder"] = holder
    return frames


def attribute(S, frames, lo, ri):
    """Release = last contact frame in (lo, ri]; shooter = most contact in RELEASE_WIN before it."""
    rel = max((k for k in range(lo + 1, ri + 1) if frames[k]["near"] is not None), default=None)
    if rel is None:
        return {"rim_i": ri, "rim_t": frames[ri]["t"]}
    t_rel = frames[rel]["t"]
    votes = {}
    for st in frames[lo + 1:rel + 1]:
        if st["t"] >= t_rel - RELEASE_WIN and st["near"] is not None:
            votes[st["near"]] = votes.get(st["near"], 0) + 1
    # the player touching the ball AT release wins if the contact is not a one-frame fluke:
    # a closely guarded dribbler's mask overlaps his defender's, so a plain majority over the
    # window can hand the shot to the defender
    at_rel = frames[rel]["near"]
    sid = at_rel if votes.get(at_rel, 0) >= MIN_RELEASE_VOTES else max(votes, key=votes.get)
    return {"rim_i": ri, "rim_t": frames[ri]["t"], "release_i": rel, "release_t": t_rel,
            "shooter": sid, "votes": votes,
            "shooter_track": [[round(float(S["times"][k]), 3), [round(float(v)) for v in S["players"][k][sid]["box"]]]
                              for k in range(len(frames)) if sid in S["players"][k]]}


def analyse(S, cam):
    n = len(S["times"])
    hoop, near_r = hoop_of(S)
    res = {"hoop_px": None if hoop is None else [round(float(c)) for c in hoop], "shots": []}
    rim_d = cam.rim_ball_px(*hoop) if hoop is not None else None
    res["rim_ball_px"] = None if rim_d is None else round(float(rim_d), 1)
    events = rim_events(S, hoop, near_r, rim_d) if hoop is not None else []
    lo = -1
    for k, _, b in events:
        track = track_from(S, k, b)
        frames = frame_states(S, cam, track)
        shot = attribute(S, frames, lo, k)
        if "shooter" not in shot:
            continue            # nobody touched it since the last arrival: a rim bounce, or a shot from before the clip
        h_rel = (frames[shot["release_i"]].get("ball_xyh") or [0, 0, RIM_CM])[2]
        dt = max(frames[k]["t"] - shot["release_t"], FPS_DIV / 15.0)
        if (RIM_CM - h_rel) / dt > MAX_RISE_CM_S:
            continue            # the ball cannot climb that fast: two balls stitched together (dead-ball warm-ups)
        shot["ball_frames"] = int(sum(x is not None for x in track[:k + 1]))
        shot["_frames"] = frames
        res["shots"].append(shot)
        lo = k
    if not res["shots"]:
        res.update({"rim_i": int(np.argmin(np.abs(S["times"] - S["rim_t"]))), "ball_frames": 0,
                    "frames": frame_states(S, cam, [None] * n)})
        return res
    # the primary shot = the arrival nearest the clip's nominal rim time
    prim = min(res["shots"], key=lambda s: abs(s["rim_t"] - S["rim_t"]))
    if os.environ.get("SHOT_IDX"):
        prim = res["shots"][min(int(os.environ["SHOT_IDX"]), len(res["shots"]) - 1)]
    res.update({k: v for k, v in prim.items() if not k.startswith("_")})
    res["frames"] = prim["_frames"]
    res["nominal_rim_t"] = float(S["rim_t"])
    res["tracks"] = {str(sh["shooter"]): [[round(float(S["times"][k]), 3), [round(float(v)) for v in S["players"][k][sh["shooter"]]["box"]]]
                                          for k in range(n) if sh["shooter"] in S["players"][k]]
                     for sh in res["shots"] if sh.get("shooter") is not None}
    for shot in res["shots"]:
        shot.pop("_frames")
    return res


def read_frames(clip, times):
    cap = cv2.VideoCapture(clip)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    want = {int(round(t * fps)) for t in times}
    out, i = {}, 0
    while True:
        ok, f = cap.read()
        if not ok or i > max(want):
            break
        if i in want:
            out[i] = f
        i += 1
    cap.release()
    return [out.get(int(round(t * fps))) for t in times]


CAPTIONS = json.load(open(os.environ["CAPTIONS"])) if os.environ.get("CAPTIONS") else {}


def render(S, res, clip, base):
    imgs = read_frames(clip, S["times"])
    H, W = S["HW"]
    sid = res.get("shooter")
    vw = cv2.VideoWriter(base + ".mp4", cv2.VideoWriter_fourcc(*"mp4v"), 15, (W // 2, H // 2))
    pts = [s["ball_px"] for s in res["frames"] if "ball_px" in s]
    shown = []
    for k, (im, st) in enumerate(zip(imgs, res["frames"])):
        if im is None:
            continue
        im = im.copy()
        for oid, p in S["players"][k].items():
            x1, y1, x2, y2 = [int(v) for v in p["box"]]
            col = (70, 70, 70)
            fill = None
            if oid == sid:
                col, fill = (60, 220, 60), (60, 220, 60)
            elif oid == st["near"]:
                col, fill = (0, 220, 255), (0, 220, 255)
            elif oid in st["contact"]:
                col = (0, 140, 255)
            if fill is not None:
                mk = cv2.resize(p["mask"].astype(np.uint8), (W, H), interpolation=cv2.INTER_NEAREST) > 0
                im[mk] = (0.5 * im[mk] + 0.5 * np.array(fill)).astype(np.uint8)
            cv2.rectangle(im, (x1, y1), (x2, y2), col, 2)
            lab = str(oid) + ("  %dcm" % st["contact"][oid] if oid in st["contact"] else "")
            cv2.putText(im, lab, (x1, y1 - 6), cv2.FONT_HERSHEY_SIMPLEX, 0.8, col, 2)
        for a, b in zip(pts, pts[1:]):
            cv2.line(im, tuple(a), tuple(b), (0, 165, 255), 1)
        if "ball_px" in st:
            u, v = st["ball_px"]
            r = int(st["ball_d"] / 2) + 4
            cv2.circle(im, (u, v), r, (0, 0, 255), 3)
            cv2.putText(im, "h=%dcm" % st["ball_xyh"][2], (u + r + 4, v), cv2.FONT_HERSHEY_SIMPLEX, 0.9,
                        (0, 0, 255), 2)
        tag = "t=%.2f rim %.1f  near=%s holder=%s" % (st["t"], S["times"][res["rim_i"]], st["near"], st["holder"])
        for shot in res["shots"]:
            if k == shot.get("release_i"):
                tag += "   RELEASE -> shooter %s" % shot["shooter"]
        cv2.putText(im, tag, (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 0, 0), 6)
        cv2.putText(im, tag, (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (255, 255, 255), 2)
        cap = CAPTIONS.get(os.path.basename(base))
        if cap:
            cv2.putText(im, cap, (20, 95), cv2.FONT_HERSHEY_SIMPLEX, 1.1, (0, 0, 0), 6)
            cv2.putText(im, cap, (20, 95), cv2.FONT_HERSHEY_SIMPLEX, 1.1, (0, 255, 255), 2)
        small = cv2.resize(im, (W // 2, H // 2))
        vw.write(small)
        shown.append((k, small))
    vw.release()
    ri, rel = res["rim_i"], res.get("release_i", res["rim_i"])
    pick = sorted({max(0, rel - 9), max(0, rel - 5), max(0, rel - 2), rel, min(ri, rel + 3), ri})
    tiles = [s for k, s in shown if k in pick]
    if len(tiles) % 2:
        tiles.append(np.zeros_like(tiles[0]))
    cv2.imwrite(base + "_sheet.jpg", np.vstack([np.hstack(tiles[i:i + 2]) for i in range(0, len(tiles), 2)]),
                [cv2.IMWRITE_JPEG_QUALITY, 80])


def main():
    out, clip_dir, names = sys.argv[1], sys.argv[2], sys.argv[3:]
    vd = os.path.join(out, VER)
    os.makedirs(vd, exist_ok=True)
    no_render = os.environ.get("NO_RENDER") == "1"
    for name in names:
        angle = name.split("_")[1] if name.startswith("L_") else name.split("_")[0]
        try:
            S = load(out, name)
            res = analyse(S, Camera(angle))
        except Exception as e:  # noqa: BLE001 - one bad clip must not stop a whole-game run
            print(name, "FAILED", type(e).__name__, e, flush=True)
            continue
        res.update({"clip": name, "angle": angle})
        json.dump(res, open(os.path.join(vd, name + ".json"), "w"), indent=1)
        if not no_render:
            clip = os.path.join(clip_dir if not name.startswith("L_") else os.path.join(HERE, "lclips"), name + ".mp4")
            render(S, res, clip, os.path.join(vd, name))
        print(name, " | ".join("rim %.2f rel %s shooter %s %s" % (x["rim_t"], x.get("release_t"), x.get("shooter"),
              x.get("votes")) for x in res["shots"]) or "NO RIM EVENT", flush=True)


if __name__ == "__main__":
    main()

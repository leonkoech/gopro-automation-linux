"""2K-style highlight render: a ring on the court under the ball-holder, and a crop that keeps
the ball in the centre of the frame, following it through dribbles, passes and the flight to
the rim. Built on the possession tracker's output (perceive cache + holder.analyse), so the
ring sits on the player the tracker says has the ball, and on the shooter through the shot.

The ring is a real circle on the floor (RING_CM radius around the holder's feet) projected into
the image through the court homography, so it lies in perspective like the in-game marker. It is
drawn UNDER the players, cut out by their tracker masks: the back half of the ring passes behind
the holder's legs (so he stands in it), and anyone nearer the camera covers it entirely.

The ring is the client's animated blue "2K player circle" (a looping top-down RGBA animation,
laid flat on the floor in perspective) under the ball-holder; from the release it grows a little
(SHOT_GROW) on the shooter. That circle is the ONLY overlay by default (client, 2026-10-01): the
head marker, the ball trail and the score badge are off (R2K_MARKER / R2K_TRAIL / R2K_BADGE=1 to
show them). R2K_SHOT_SPRITE=<file in R2K_ASSETS> swaps the circle's colour while shooting (for
approval reels; production keeps the blue). Assets: ring_blue_512.mov (the 1080px original scaled
to 512, PNG codec); without it, or with R2K_RING_STYLE=classic, the drawn cyan/gold ring is used.

Usage: render_2k.py <out_dir> <clip_dir> <name> [<name> ...]
Env:   R2K_OUT (default <out_dir>/r2k), R2K_ZOOM (1.6), R2K_VERTICAL=1 to add a 9:16 cut,
       R2K_RING_STYLE (sprite|classic), R2K_ASSETS (default <this dir>/assets)
Writes <R2K_OUT>/<name>_2k.mp4 (1920x1080, 30 fps) and, if asked, <name>_2k_vertical.mp4.
"""
import os
import subprocess
import sys

import cv2
import numpy as np

import holder as H

ZOOM = float(os.environ.get("R2K_ZOOM", "1.6"))
RING_CM = 70.0
RING_COL = (255, 229, 0)          # BGR electric cyan
SHOT_COL = (40, 200, 255)         # BGR gold: the shooter after release
RING_STYLE = os.environ.get("R2K_RING_STYLE", "sprite")
ASSETS = os.environ.get("R2K_ASSETS", os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets"))
SPRITE_FILES = {"hold": "ring_blue_512.mov"}
SHOT_SPRITE = os.environ.get("R2K_SHOT_SPRITE", "")     # e.g. ring_yellow_512.mov; "" = same blue
SHOT_GROW = float(os.environ.get("R2K_SHOT_GROW", "1.2"))  # circle size on the shooter, x the holder's
GROW_S = 0.3                      # seconds to grow after the release
SHOW_MARKER = os.environ.get("R2K_MARKER") == "1"
SHOW_TRAIL = os.environ.get("R2K_TRAIL") == "1"
SHOW_BADGE = os.environ.get("R2K_BADGE") == "1"
# R2K_SHOT_FRAMING=1: around the release, frame the ball AND the shooter's feet (the shooting
# circle stays in view) instead of following the ball alone; off by default (production framing)
SHOT_FRAMING = os.environ.get("R2K_SHOT_FRAMING") == "1"
SPRITE_PX = 512                   # asset side
SPRITE_FPS = 24
SPRITE_N = 240                    # frames in one loop (10 s)
SPRITE_R_PX = 166.7               # the ring's outer edge in the asset (measured on the alpha)
SPRITE_OUTER_CM = 80.0            # ... laid on the floor at this radius (hole ~44 cm for the feet)
SPRITE_COL = (249, 161, 93)       # BGR of the asset's blue, for the head marker
TRAIL_LEN = 10                    # output frames of ball trail
SMOOTH_S = 0.45                   # crop-centre smoothing window
FPS = 30


def interp_track(times, pts, out_t):
    """Linear interpolation of an (n, 2) track with NaN gaps onto out_t."""
    pts = np.asarray(pts, float)
    ok = ~np.isnan(pts[:, 0])
    if ok.sum() < 2:
        return None
    return np.stack([np.interp(out_t, times[ok], pts[ok, k]) for k in range(2)], 1)


def smooth(x, win):
    if win < 2:
        return x
    k = np.ones(win) / win
    pad = np.pad(x, ((win // 2, win - 1 - win // 2), (0, 0)), mode="edge")
    return np.stack([np.convolve(pad[:, j], k, mode="valid") for j in range(x.shape[1])], 1)


def redistort(cal, pts):
    """Inverse of holder.undistort (division model), by fixed-point iteration."""
    lam = cal.get("division_lambda")
    pts = np.asarray(pts, float).reshape(-1, 2)
    if not lam:
        return pts
    cx, cy = cal.get("principal_point") or [cal["image_size"][0] / 2, cal["image_size"][1] / 2]
    diag = float(np.hypot(cx, cy))
    qu = pts - [cx, cy]
    qd = qu.copy()
    for _ in range(30):
        qd = qu * (1 + lam * (qd ** 2).sum(1) / diag ** 2)[:, None]
    return qd + [cx, cy]


def ring_poly(cam, feet_px):
    """Circle of RING_CM around the feet on the court plane, projected back into the image
    (homography inverse, then the lens distortion put back so the ring is centred on the feet)."""
    c = cam.to_court([feet_px])[0]
    a = np.linspace(0, 2 * np.pi, 48, endpoint=False)
    court = np.stack([c[0] + RING_CM * np.cos(a), c[1] + RING_CM * np.sin(a)], 1).astype(np.float32)
    img = cv2.perspectiveTransform(court.reshape(-1, 1, 2), np.linalg.inv(cam.H)).reshape(-1, 2)
    return np.round(redistort(cam.cal, img)).astype(np.int32)


def draw_ring(im, poly, col, alpha):
    over = im.copy()
    cv2.fillPoly(over, [poly], col)
    cv2.addWeighted(over, 0.18 * alpha, im, 1 - 0.18 * alpha, 0, im)
    glow = im.copy()
    cv2.polylines(glow, [poly], True, col, 14, cv2.LINE_AA)
    cv2.addWeighted(glow, 0.35 * alpha, im, 1 - 0.35 * alpha, 0, im)
    cv2.polylines(im, [poly], True, col, 4, cv2.LINE_AA)


OCC_FEATHER = 5                   # px blur on the occlusion edge (masks are 512x288, upscaled)


def occluders(players, oid, feet_y, shape, roi):
    """Float mask (roi-sized) of where the ring is hidden behind someone: the ring owner's body
    ABOVE his feet line (the back half of the ring runs behind his legs; the front half stays in
    front of his feet) and the whole body of anyone standing nearer the camera (feet lower)."""
    x0, y0, x1, y1 = roi
    H, W = shape[:2]
    occ = np.zeros((y1 - y0, x1 - x0), np.float32)
    for pid, p in players.items():
        b = p["box"]
        if b[2] < x0 or b[0] > x1 or b[3] < y0 or b[1] > y1:
            continue
        own = pid == oid
        if not own and b[3] <= feet_y:
            continue
        m = cv2.resize(p["mask"].astype(np.float32), (W, H), interpolation=cv2.INTER_LINEAR)[y0:y1, x0:x1]
        if own:
            m[max(0, int(feet_y) - y0):, :] = 0
        occ = np.maximum(occ, m)
    if OCC_FEATHER and occ.any():
        k = 2 * OCC_FEATHER + 1
        occ = cv2.GaussianBlur(occ, (k, k), 0)
    return np.clip(occ, 0, 1)


def draw_ring_under(im, poly, col, alpha, players, oid, feet_y):
    """draw_ring, then put back the original pixels wherever a player covers the ring."""
    layer = im.copy()
    draw_ring(layer, poly, col, alpha)
    x, y, w, h = cv2.boundingRect(poly)
    pad = 12
    x0, y0 = max(0, x - pad), max(0, y - pad)
    x1, y1 = min(im.shape[1], x + w + pad), min(im.shape[0], y + h + pad)
    if x1 <= x0 or y1 <= y0:
        return
    occ = occluders(players or {}, oid, feet_y, im.shape, (x0, y0, x1, y1))[..., None]
    im[y0:y1, x0:x1] = (layer[y0:y1, x0:x1] * (1 - occ) + im[y0:y1, x0:x1] * occ).astype(np.uint8)


class SpriteStream:
    """One animated RGBA ring, decoded on demand at SPRITE_PX. Frames are asked for in time
    order within a clip, so a single ffmpeg pipe is read forward (reopened if asked to go back)."""

    def __init__(self, path):
        self.path, self.proc, self.i, self.cur = path, None, -1, None

    def _open(self):
        self.close()
        self.proc = subprocess.Popen(["ffmpeg", "-nostdin", "-v", "error", "-i", self.path, "-f", "rawvideo",
                                      "-pix_fmt", "bgra", "-"], stdout=subprocess.PIPE, stdin=subprocess.DEVNULL)
        self.i = -1

    def frame(self, n):
        n = int(n) % SPRITE_N
        if self.proc is None or n < self.i:
            self._open()
        size = SPRITE_PX * SPRITE_PX * 4
        while self.i < n:
            raw = self.proc.stdout.read(size)
            if len(raw) < size:                       # shorter than expected: loop from the top
                if self.i < 0:
                    raise RuntimeError("ring asset unreadable: %s" % self.path)
                n %= self.i + 1
                self._open()
                continue
            self.cur = np.frombuffer(raw, np.uint8).reshape(SPRITE_PX, SPRITE_PX, 4)
            self.i += 1
        return self.cur

    def close(self):
        if self.proc is not None:
            self.proc.stdout.close()
            self.proc.kill()
            self.proc.wait()
            self.proc = None


def load_sprites():
    """{"hold", "shot"} SpriteStreams, or None (classic ring) when switched off or missing."""
    if RING_STYLE != "sprite":
        return None
    paths = {k: os.path.join(ASSETS, f) for k, f in SPRITE_FILES.items()}
    if SHOT_SPRITE:
        paths["shot"] = os.path.join(ASSETS, SHOT_SPRITE)
    missing = [p for p in paths.values() if not os.path.isfile(p)]
    if missing:
        print("ring assets missing (%s) - classic ring" % ", ".join(missing), flush=True)
        return None
    return {k: SpriteStream(p) for k, p in paths.items()}


def sprite_quad(cam, feet_px, outer_cm=SPRITE_OUTER_CM):
    """Image positions of the asset's four corners when its ring (outer edge SPRITE_R_PX) is
    laid on the floor around the feet with an outer radius of outer_cm."""
    c = cam.to_court([feet_px])[0]
    h = outer_cm * (SPRITE_PX / 2) / SPRITE_R_PX
    court = np.array([[c[0] - h, c[1] - h], [c[0] + h, c[1] - h], [c[0] + h, c[1] + h], [c[0] - h, c[1] + h]],
                     np.float32)
    img = cv2.perspectiveTransform(court.reshape(-1, 1, 2), np.linalg.inv(cam.H)).reshape(-1, 2)
    return redistort(cam.cal, img).astype(np.float32)


def draw_sprite_under(im, rgba, quad, alpha, players, oid, feet_y):
    """Warp the RGBA sprite onto the floor quad and blend it in, cut out where players cover it."""
    x, y, w, h = cv2.boundingRect(np.round(quad).astype(np.int32))
    x0, y0 = max(0, x), max(0, y)
    x1, y1 = min(im.shape[1], x + w), min(im.shape[0], y + h)
    if x1 <= x0 or y1 <= y0:
        return
    src = np.array([[0, 0], [SPRITE_PX, 0], [SPRITE_PX, SPRITE_PX], [0, SPRITE_PX]], np.float32)
    M = cv2.getPerspectiveTransform(src, quad - np.array([x0, y0], np.float32))
    warped = cv2.warpPerspective(rgba, M, (x1 - x0, y1 - y0), flags=cv2.INTER_LINEAR,
                                 borderMode=cv2.BORDER_CONSTANT, borderValue=0)
    occ = occluders(players or {}, oid, feet_y, im.shape, (x0, y0, x1, y1))
    a = (warped[..., 3].astype(np.float32) / 255.0 * alpha * (1 - occ))[..., None]
    roi = im[y0:y1, x0:x1].astype(np.float32)
    im[y0:y1, x0:x1] = (roi * (1 - a) + warped[..., :3].astype(np.float32) * a).astype(np.uint8)


def shot_scale(t_shot):
    """Circle size on the shooter: grows from 1 to SHOT_GROW over GROW_S after the release."""
    return 1.0 + (SHOT_GROW - 1.0) * min(1.0, max(0.0, t_shot) / GROW_S)


def draw_owner_ring(im, cam, feet, is_shot, alpha, players, oid, t_hold, t_shot, sprites):
    """The ring under its owner: the animated sprite when loaded (one continuous loop, grown on
    the shooter, in the shooting colour only when a shot sprite is configured), else the classic
    drawn ring. Returns the marker colour."""
    if sprites:
        sp = sprites["shot"] if is_shot and "shot" in sprites else sprites["hold"]
        scale = shot_scale(t_shot) if is_shot else 1.0
        draw_sprite_under(im, sp.frame(max(0, t_hold * SPRITE_FPS)), sprite_quad(cam, feet, SPRITE_OUTER_CM * scale),
                          alpha, players, oid, feet[1])
        return SPRITE_COL
    col = SHOT_COL if is_shot else RING_COL
    draw_ring_under(im, ring_poly(cam, feet), col, alpha, players, oid, feet[1])
    return col


BALL_CONF = 0.3                   # a raw ball detection must be at least this confident
AWAY_W = 1.5                      # ... a ball further than this many box widths is not his
GAP_S = 0.5                       # the shooter walk-back bridges tracking gaps up to this long
HOLD_EXPIRE_S = 2.0               # the ring leaves a player not seen with the ball for this long


def raw_contacts(S, cam):
    """Per cache frame: the player holding ANY detected ball (not only the shot's ball traced back
    from the rim, which the shot logic uses and which starts shortly before the shot): the ball
    centre on his (dilated) mask, the ball low enough to be in hands, the nearest player if several."""
    off = H.off_court_tracks(S, cam)
    out = []
    for k, balls in enumerate(S["balls"]):
        best = None
        for d in balls:
            conf, x1, y1, x2, y2 = (float(v) for v in d[:5])
            if conf < BALL_CONF:
                continue
            u, v, dia = (x1 + x2) / 2, (y1 + y2) / 2, ((x2 - x1) + (y2 - y1)) / 2
            X = cam.ball_3d(u, v, dia)
            if X[2] > H.HOLD_MAX_H_CM:
                continue
            r = max(2, int(round(0.75 * dia * S["scale"])))
            mu, mv = int(round(u * S["scale"])), int(round(v * S["scale"]))
            contact = {}
            for oid, p in S["players"][k].items():
                if oid in off:
                    continue
                m = p["mask"]
                if m[max(0, mv - r):mv + r + 1, max(0, mu - r):mu + r + 1].any():
                    feet = cam.to_court([[(p["box"][0] + p["box"][2]) / 2, p["box"][3]]])[0]
                    contact[oid] = float(np.linalg.norm(feet - X[:2]))
            pick = H.pick_near(contact, {o: S["players"][k][o]["box"] for o in contact})
            if pick is not None and (best is None or conf > best[1]):
                best = (pick, conf)
        out.append(best[0] if best else None)
    return out


def owner_timeline(S, res, cam, shot):
    """Per cache frame: who wears the ring before the release (None = nobody).

    1. the shot logic's holder where it has one (near the shot);
    2. elsewhere, whoever holds any detected ball for LOCK frames in a row, kept until another
       player locks or HOLD_EXPIRE_S passes without him being seen with it;
    3. the shooter, walking back from the release for as long as nobody else held the ball, no
       confident ball is seen away from him and he is tracked: a free-throw shooter at the line
       keeps the ring although the far camera rarely sees the ball in his hands."""
    t = np.asarray(S["times"], float)
    n = len(t)
    raw = raw_contacts(S, cam)
    frames = res.get("frames") or [{}] * n
    own, cur, seen, run_id, run_n = [None] * n, None, -1e9, None, 0
    for k in range(n):
        r = raw[k]
        run_id, run_n = (r, run_n + 1) if (r is not None and r == run_id) else (r, 1 if r is not None else 0)
        if run_id is not None and run_n >= H.LOCK:
            cur = run_id
        if cur is not None and (r == cur or frames[k].get("near") == cur):
            seen = t[k]
        if cur is not None and t[k] - seen > HOLD_EXPIRE_S:
            cur = None
        h = frames[k].get("holder")
        own[k] = h if h is not None else cur
    if shot and shot.get("shooter") is not None and shot.get("release_t") is not None:
        sid = shot["shooter"]
        ri = int(np.argmin(np.abs(t - shot["release_t"])))
        last_seen = t[ri]
        for k in range(ri, -1, -1):
            if own[k] is not None and own[k] != sid:
                break                          # someone else had the ball: his possession ends here
            if sid not in S["players"][k]:
                if last_seen - t[k] > GAP_S:
                    break                      # lost him for too long
                continue
            if ball_away(S["balls"][k], S["players"][k][sid]["box"]):
                break                          # the ball is clearly elsewhere
            last_seen = t[k]
            own[k] = sid                       # e.g. a free throw: the ball hidden in his hands
    return own


def ball_away(balls, box):
    """A confident ball detection clearly away from this player (beyond AWAY_W box widths)."""
    w = box[2] - box[0]
    cx, cy = (box[0] + box[2]) / 2, (box[1] + box[3]) / 2
    for d in balls:
        conf, x1, y1, x2, y2 = (float(v) for v in d[:5])
        if conf >= BALL_CONF and np.hypot((x1 + x2) / 2 - cx, (y1 + y2) / 2 - cy) > AWAY_W * w:
            return True
    return False


def draw_marker(im, box, col):
    """Downward chevron well above the holder's head (clear of whoever stands behind him)."""
    x = int((box[0] + box[2]) / 2)
    h = box[3] - box[1]
    y = int(box[1] - 0.18 * h)
    s = max(12, int(0.07 * h))
    pts = np.array([[x - s, y - s], [x + s, y - s], [x, y + s // 3]], np.int32)
    cv2.fillPoly(im, [pts], (0, 0, 0), cv2.LINE_AA)
    cv2.fillPoly(im, [pts - [0, 3]], col, cv2.LINE_AA)


def draw_trail(im, trail, col):
    """Comet: circles that grow and brighten towards the ball."""
    n = len(trail)
    for i, p in enumerate(trail):
        a = (i + 1) / n
        over = im.copy()
        cv2.circle(over, p, int(4 + 10 * a), col, -1, cv2.LINE_AA)
        cv2.addWeighted(over, 0.15 + 0.45 * a, im, 0.85 - 0.45 * a, 0, im)


def draw_badge(im, text, anchor, alpha):
    """Score badge ("+3") next to the rim as the make drops through."""
    if alpha <= 0:
        return
    over = im.copy()
    font, sc, th = cv2.FONT_HERSHEY_DUPLEX, 3.2, 8
    (tw, tht), _ = cv2.getTextSize(text, font, sc, th)
    x, y = int(anchor[0] + 70), int(anchor[1] + tht / 2)
    cv2.putText(over, text, (x + 4, y + 4), font, sc, (0, 0, 0), th + 4, cv2.LINE_AA)
    cv2.putText(over, text, (x, y), font, sc, SHOT_COL, th, cv2.LINE_AA)
    cv2.addWeighted(over, alpha, im, 1 - alpha, 0, im)


ARCS_DIR = os.environ.get("R2K_ARCS", "/home/dev/shot_typing")


def zone_of(cam, px, still):
    """Production's zone test (golden arcs) on the feet pixel."""
    import json
    a = json.load(open(os.path.join(ARCS_DIR, "calib_arcs_%s.json" % cam)))
    sc = 1920 / float(a.get("w") or 1920)
    arc = {k: np.array(a[k], np.float32) * sc for k in ("three_pt_white", "four_pt_red", "ft_stripe") if a.get(k)}
    pt = (float(px[0]), float(px[1]))
    if still and "ft_stripe" in arc and cv2.pointPolygonTest(arc["ft_stripe"], pt, False) >= 0:
        return "FREE_THROW"
    if cv2.pointPolygonTest(arc["three_pt_white"], pt, False) >= 0:
        return "2PT"
    return "3PT" if cv2.pointPolygonTest(arc["four_pt_red"], pt, False) >= 0 else "4PT"


def render(out, clip_dir, name, dst_dir, vertical, make=None, badge_zone=None, S=None, res=None):
    angle = name.split("_")[1] if name.startswith("L_") else name.split("_")[0]
    cam = H.Camera(angle)
    if S is None:
        S = H.load(out, name)
    if res is None:
        res = H.analyse(S, cam)
    frames = res["frames"]
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    shot = min(shots, key=lambda s: abs(s["rim_t"] - S["rim_t"])) if shots else None
    t = np.asarray(S["times"], float)

    cap = cv2.VideoCapture(os.path.join(clip_dir, name + ".mp4"))
    src_fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    W = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    Hh = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    out_t = np.arange(t[0], t[-1], 1.0 / FPS)

    ball = np.array([st.get("ball_px", [np.nan, np.nan]) for st in frames], float)
    ball_o = interp_track(t, ball, out_t)
    if ball_o is None:
        print(name, "no ball track — skipped")
        return
    centre = smooth(ball_o, max(1, int(SMOOTH_S * FPS)))
    cw, ch = int(W / ZOOM), int(Hh / ZOOM)

    # who wears the ring on each output frame: the holder (sticky), the shooter from release
    # until the ball reaches the rim, then a fade over 0.5 s
    idx = np.clip(np.searchsorted(t, out_t) - 1, 0, len(t) - 1)
    rel_t = shot["release_t"] if shot else None
    rim_t = shot["rim_t"] if shot else None

    own = owner_timeline(S, res, cam, shot)

    def ring_owner(k):
        tt = out_t[k]
        if shot and rel_t <= tt:
            fade = 1.0 if tt <= rim_t else max(0.0, 1 - (tt - rim_t) / 0.5)
            return shot["shooter"], SHOT_COL, fade
        return own[idx[k]], RING_COL, 1.0

    def box_at(oid, k):
        """Holder's box interpolated between the two cache frames around out_t[k]."""
        i = idx[k]
        j = min(i + 1, len(t) - 1)
        bi, bj = S["players"][i].get(oid), S["players"][j].get(oid)
        if bi is None and bj is None:
            return None
        if bi is None or bj is None or j == i:
            return (bi or bj)["box"]
        w = (out_t[k] - t[i]) / max(t[j] - t[i], 1e-6)
        return (1 - w) * bi["box"] + w * bj["box"]

    if SHOT_FRAMING and shot:
        aim = ball_o.copy()
        for k, tt in enumerate(out_t):
            if rel_t - 0.3 <= tt <= rel_t + 0.9:
                b = box_at(shot["shooter"], k)
                if b is not None:            # midway between the ball and his feet: both in frame
                    aim[k] = [(ball_o[k][0] + (b[0] + b[2]) / 2) / 2, (ball_o[k][1] + b[3]) / 2]
        centre = smooth(aim, max(1, int(SMOOTH_S * FPS)))

    # score badge for makes: the tracker's own type from the shooter's median takeoff feet
    badge, hoop = None, res.get("hoop_px")
    if make is None:
        make = "_MAKE_" in name
    if shot and make and hoop:
        track = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
        win = [b for tq, b in track if shot["release_t"] - 0.8 <= tq <= shot["release_t"]]
        if win:
            pts = np.array([((b[0] + b[2]) / 2, b[3]) for b in win], float)
            still = bool(np.ptp(pts[:, 0]) < 60 and np.ptp(pts[:, 1]) < 60)
            zone = badge_zone or zone_of(angle, np.median(pts, axis=0), still)
            badge = {"2PT": "+2", "3PT": "+3", "4PT": "+4", "FREE_THROW": "+1"}.get(zone)
    os.makedirs(dst_dir, exist_ok=True)
    targets = [("", 1920, 1080)] + ([("_vertical", 1080, 1920)] if vertical else [])
    pipes = {}
    for suf, ow, oh in targets:
        path = os.path.join(dst_dir, "%s_2k%s.mp4" % (name, suf))
        pipes[suf] = (subprocess.Popen(
            ["ffmpeg", "-nostdin", "-v", "error", "-y", "-f", "rawvideo", "-pix_fmt", "bgr24",
             "-s", "%dx%d" % (ow, oh), "-r", str(FPS), "-i", "-", "-c:v", "libx264", "-preset", "slow",
             "-crf", "18", "-pix_fmt", "yuv420p", "-movflags", "+faststart", path], stdin=subprocess.PIPE),
            ow, oh)

    src_i, frame = -1, None
    trail = []
    sprites = load_sprites()
    for k, tt in enumerate(out_t):
        want = int(round(tt * src_fps))
        while src_i < want:
            ok, f = cap.read()
            if not ok:
                break
            src_i += 1
            frame = f
        if frame is None:
            break
        im = frame.copy()
        oid, col, alpha = ring_owner(k)
        if oid is not None and alpha > 0:
            b = box_at(oid, k)
            if b is not None:
                feet = [(b[0] + b[2]) / 2, b[3]]
                near = int(np.argmin(np.abs(t - tt)))
                is_shot = shot is not None and rel_t <= tt
                col = draw_owner_ring(im, cam, feet, is_shot, alpha, S["players"][near], oid,
                                      tt - out_t[0], tt - rel_t if is_shot else 0.0, sprites)
                if alpha >= 1 and SHOW_MARKER:
                    draw_marker(im, b, col)
        bx, by = ball_o[k]
        airborne = shot is not None and rel_t <= tt <= rim_t + 0.3
        trail = (trail + [(int(bx), int(by))])[-TRAIL_LEN:] if airborne else []
        if trail and SHOW_TRAIL:
            draw_trail(im, trail, SHOT_COL)
        if badge and shot and tt >= rim_t and SHOW_BADGE:
            draw_badge(im, badge, hoop, min(1.0, (tt - rim_t) / 0.25) * max(0.0, 1 - max(0.0, tt - rim_t - 1.2) / 0.4))
        cx, cy = centre[k]
        for suf, (proc, ow, oh) in pipes.items():
            if suf == "":
                x0 = int(np.clip(cx - cw / 2, 0, W - cw))
                y0 = int(np.clip(cy - ch / 2, 0, Hh - ch))
                crop = im[y0:y0 + ch, x0:x0 + cw]
            else:
                vh = int(Hh / (ZOOM * 0.75))        # vertical: a little wider view, full-height crop
                vw = int(vh * 9 / 16)
                x0 = int(np.clip(cx - vw / 2, 0, W - vw))
                y0 = int(np.clip(cy - vh / 2, 0, Hh - vh))
                crop = im[y0:y0 + vh, x0:x0 + vw]
            proc.stdin.write(cv2.resize(crop, (ow, oh), interpolation=cv2.INTER_CUBIC).tobytes())
    cap.release()
    for sp in (sprites or {}).values():
        sp.close()
    for proc, _, _ in pipes.values():
        proc.stdin.close()
        proc.wait()
    print(name, "rendered", len(out_t), "frames; shooter", shot and shot["shooter"],
          "release", rel_t, "->", dst_dir, flush=True)


def main():
    out, clip_dir, names = sys.argv[1], sys.argv[2], sys.argv[3:]
    dst = os.environ.get("R2K_OUT", os.path.join(out, "r2k"))
    vertical = os.environ.get("R2K_VERTICAL") == "1"
    for n in names:
        try:
            render(out, clip_dir, n, dst, vertical)
        except Exception as e:  # noqa: BLE001
            print(n, "FAILED", type(e).__name__, e, flush=True)


if __name__ == "__main__":
    main()

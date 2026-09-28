"""2K-style highlight render: a ring on the court under the ball-holder, and a crop that keeps
the ball in the centre of the frame, following it through dribbles, passes and the flight to
the rim. Built on the possession tracker's output (perceive cache + holder.analyse), so the
ring sits on the player the tracker says has the ball, and on the shooter through the shot.

The ring is a real circle on the floor (RING_CM radius around the holder's feet) projected into
the image through the court homography, so it lies in perspective like the in-game marker.

Usage: render_2k.py <out_dir> <clip_dir> <name> [<name> ...]
Env:   R2K_OUT (default <out_dir>/r2k), R2K_ZOOM (1.6), R2K_VERTICAL=1 to add a 9:16 cut
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


def ring_poly(cam, feet_px):
    """Circle of RING_CM around the feet on the court plane, projected back into the image."""
    c = cam.to_court([feet_px])[0]
    a = np.linspace(0, 2 * np.pi, 48, endpoint=False)
    court = np.stack([c[0] + RING_CM * np.cos(a), c[1] + RING_CM * np.sin(a)], 1).astype(np.float32)
    img = cv2.perspectiveTransform(court.reshape(-1, 1, 2), np.linalg.inv(cam.H)).reshape(-1, 2)
    return img.astype(np.int32)


def draw_ring(im, poly, col, alpha):
    over = im.copy()
    cv2.fillPoly(over, [poly], col)
    cv2.addWeighted(over, 0.18 * alpha, im, 1 - 0.18 * alpha, 0, im)
    glow = im.copy()
    cv2.polylines(glow, [poly], True, col, 14, cv2.LINE_AA)
    cv2.addWeighted(glow, 0.35 * alpha, im, 1 - 0.35 * alpha, 0, im)
    cv2.polylines(im, [poly], True, col, 4, cv2.LINE_AA)


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

    def ring_owner(k):
        tt = out_t[k]
        if shot and rel_t <= tt:
            fade = 1.0 if tt <= rim_t else max(0.0, 1 - (tt - rim_t) / 0.5)
            return shot["shooter"], SHOT_COL, fade
        h = frames[idx[k]].get("holder")
        return h, RING_COL, 1.0

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
                draw_ring(im, ring_poly(cam, feet), col, alpha)
                if alpha >= 1:
                    draw_marker(im, b, col)
        bx, by = ball_o[k]
        airborne = shot is not None and rel_t <= tt <= rim_t + 0.3
        trail = (trail + [(int(bx), int(by))])[-TRAIL_LEN:] if airborne else []
        if trail:
            draw_trail(im, trail, SHOT_COL)
        if badge and shot and tt >= rim_t:
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

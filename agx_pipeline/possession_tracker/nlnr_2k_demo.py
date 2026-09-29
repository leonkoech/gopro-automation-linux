"""2K-style clips from the NEAR cameras (NL/NR, mounted on the backboards looking down).

The ball detector barely fires from above the rim, so the ball-holder is handed over from the
far camera: the far clip's tracker gives the holder (then the shooter from release), his feet go
to court coordinates, and the NL/NR player standing nearest that court point wears the ring. The
near clips are the rim-synced cuts from confirm_test (clips/<game>_near/<far key>.mp4); the far
and near clocks are aligned per clip on the rim moment each camera saw.

The crop follows the ring-wearer (no usable ball track here); the badge is the annotated type.
Demo only: production renders stay far-camera (render_2k.render). Stops if the box starts
recording.

Usage: nlnr_2k_demo.py <game8> <per_side> <work_dir>   (box, CUDA env; see xr2k/ symlinks)
"""
import json
import os
import shutil
import subprocess
import sys
import urllib.request

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import holder as H  # noqa: E402
import render_2k as R  # noqa: E402
import track_shadow as T  # noqa: E402

CONFIRM = "/home/dev/confirm_test"
EVAL = os.path.join(HERE, "out_fast", "eval_%s_vfast.json")
FAR_CACHE = os.path.join(HERE, "out_fast")
NEAR_BIAS = {"NL": np.array([-40.1, -1.9]), "NR": np.array([40.1, 5.2])}   # near_bias_from_cb9.json
MATCH_CM = 250.0
KEEP_RATIO = 1.5          # stay on the current near player unless another is this much closer
ZOOM_NEAR = 1.3
FPS = 30
BADGE = {"2PT": "+2", "3PT": "+3", "4PT": "+4", "FREE_THROW": "+1"}


def recording():
    try:
        return json.load(urllib.request.urlopen("http://localhost:5000/health", timeout=5)).get("recording")
    except Exception:  # noqa: BLE001
        return True                      # can't tell: assume busy


def picks(game, per_side):
    rows = json.load(open(EVAL % game))["rows"]
    makes = [r for r in rows if r["result"] == "MAKE" and r.get("track_fit") == r["gt"]]
    out = []
    for far in ("FL", "FR"):
        by_type = {}
        for r in makes:
            if r["clip"].startswith(far + "_"):
                by_type.setdefault(r["gt"], []).append(r)
        chosen = []
        while len(chosen) < per_side and any(by_type.values()):
            for k in sorted(by_type):         # round-robin over types so the reel is varied
                if by_type[k] and len(chosen) < per_side:
                    chosen.append(by_type[k].pop(len(by_type[k]) // 2))
        out += chosen
    return out


def far_owners(key, cam_f):
    """Per far cache frame: (t, owner id, is_shooter, court xy or None), plus rim/release times."""
    S = H.load(FAR_CACHE, key)
    res = H.analyse(S, cam_f)
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    shot = min(shots, key=lambda s: abs(s["rim_t"] - S["rim_t"])) if shots else None
    rows = []
    for i, tt in enumerate(S["times"]):
        is_shot = bool(shot and tt >= shot["release_t"])
        oid = shot["shooter"] if is_shot else res["frames"][i].get("holder")
        p = S["players"][i].get(oid) if oid is not None else None
        xy = cam_f.to_court([[(p["box"][0] + p["box"][2]) / 2, p["box"][3]]])[0] if p else None
        rows.append((float(tt), oid, is_shot, xy))
    return rows, shot


def near_owner_seq(S_n, cam_n, near, far_rows, offset):
    """Near cache frame -> (near player id or None, is_shooter): nearest on the court to the far
    owner at the same instant, sticky so the ring does not hop between two close players."""
    ft = np.array([r[0] for r in far_rows])
    seq, cur = [], None
    for j, tn in enumerate(S_n["times"]):
        _, oid, is_shot, xy = far_rows[int(np.argmin(np.abs(ft - (tn - offset))))]
        if xy is None:
            seq.append((None, is_shot))
            continue
        d = {}
        for pid, p in S_n["players"][j].items():
            c = cam_n.to_court([[(p["box"][0] + p["box"][2]) / 2, p["box"][3]]])[0] - NEAR_BIAS[near]
            d[pid] = float(np.linalg.norm(c - xy))
        best = min(d, key=d.get) if d else None
        if best is None or d[best] > MATCH_CM:
            seq.append((None, is_shot))
            continue
        if cur in d and d[cur] <= MATCH_CM and d[cur] <= KEEP_RATIO * d[best]:
            best = cur
        cur = best
        seq.append((best, is_shot))
    return seq


def render_near(S, cam, clip, seq, rim_t, release_t, badge, dst_dir, name, vertical):
    t = np.asarray(S["times"], float)
    cap = cv2.VideoCapture(clip)
    src_fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    W, Hh = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    out_t = np.arange(t[0], t[-1], 1.0 / FPS)
    near = [int(np.argmin(np.abs(t - tt))) for tt in out_t]

    def box(k):
        oid = seq[near[k]][0]
        p = S["players"][near[k]].get(oid) if oid is not None else None
        return (oid, p["box"]) if p else (None, None)

    cent = np.array([[(b[0] + b[2]) / 2, (b[1] + b[3]) / 2] if b is not None else [np.nan, np.nan]
                     for _, b in (box(k) for k in range(len(out_t)))], float)
    ok = ~np.isnan(cent[:, 0])
    if ok.sum() < 2:
        print(name, "no ring owner in the near view - skipped", flush=True)
        return False
    cent = np.stack([np.interp(out_t, out_t[ok], cent[ok, i]) for i in range(2)], 1)
    cent = R.smooth(cent, int(0.6 * FPS))
    hoop, _ = H.hoop_of(S)

    os.makedirs(dst_dir, exist_ok=True)
    targets = [("", 1920, 1080)] + ([("_vertical", 1080, 1920)] if vertical else [])
    pipes = {}
    for suf, ow, oh in targets:
        path = os.path.join(dst_dir, "%s_2k%s.mp4" % (name, suf))
        pipes[suf] = (subprocess.Popen(
            ["ffmpeg", "-nostdin", "-v", "error", "-y", "-f", "rawvideo", "-pix_fmt", "bgr24",
             "-s", "%dx%d" % (ow, oh), "-r", str(FPS), "-i", "-", "-c:v", "libx264", "-preset", "slow",
             "-crf", "18", "-pix_fmt", "yuv420p", "-movflags", "+faststart", path], stdin=subprocess.PIPE), ow, oh)
    src_i, frame = -1, None
    sprites = R.load_sprites()
    for k, tt in enumerate(out_t):
        want = int(round(tt * src_fps))
        while src_i < want:
            got, f = cap.read()
            if not got:
                break
            src_i += 1
            frame = f
        if frame is None:
            break
        im = frame.copy()
        oid, b = box(k)
        is_shot = seq[near[k]][1]
        alpha = 1.0 if tt <= rim_t else max(0.0, 1 - (tt - rim_t) / 0.5)
        if b is not None and alpha > 0:
            feet = [(b[0] + b[2]) / 2, b[3]]
            col = R.draw_owner_ring(im, cam, feet, is_shot, alpha, S["players"][near[k]], oid,
                                    tt - out_t[0], max(0.0, tt - release_t), sprites)
            if alpha >= 1:
                R.draw_marker(im, b, col)
        if badge and hoop is not None and tt >= rim_t:
            R.draw_badge(im, badge, hoop, min(1.0, (tt - rim_t) / 0.25) * max(0.0, 1 - max(0.0, tt - rim_t - 1.2) / 0.4))
        cx, cy = cent[k]
        for suf, (proc, ow, oh) in pipes.items():
            vh = int(Hh / ZOOM_NEAR) if suf == "" else Hh
            vw = int(vh * ow / oh)
            x0 = int(np.clip(cx - vw / 2, 0, W - vw))
            y0 = int(np.clip(cy - vh / 2, 0, Hh - vh))
            proc.stdin.write(cv2.resize(im[y0:y0 + vh, x0:x0 + vw], (ow, oh), interpolation=cv2.INTER_CUBIC).tobytes())
    cap.release()
    for sp in (sprites or {}).values():
        sp.close()
    for proc, _, _ in pipes.values():
        proc.stdin.close()
        proc.wait()
    return True


def one(game, r, work, sync):
    key = r["clip"]                                    # e.g. FL_SL_MAKE_280.35
    far = key.split("_")[0]
    near = "NL" if far == "FL" else "NR"
    t = float(key.rsplit("_", 1)[1])
    name = "%s_%s_MAKE_%.2f" % (near, r["gt"], t)
    if os.path.exists(os.path.join(work, "r2k", name + "_2k.mp4")):
        return
    src = os.path.join(CONFIRM, "clips", "%s_near" % game, key + ".mp4")
    s = sync.get(key.split("_", 1)[1]) or {}
    if not os.path.exists(src) or s.get("rim_in_clip") is None:
        print(name, "no synced near clip", flush=True)
        return
    clips = os.path.join(work, "clips")
    os.makedirs(clips, exist_ok=True)
    clip = os.path.join(clips, name + ".mp4")
    shutil.copy(src, clip)
    subprocess.run(["python3", os.path.join(HERE, "perceive_fast.py"), clip, work, "%.2f" % T.PRE],
                   capture_output=True, text=True, timeout=600, check=True)
    cam_f, cam_n = H.Camera(far), H.Camera(near)
    far_rows, shot = far_owners(key, cam_f)
    if shot is None:
        print(name, "far tracker has no shooter", flush=True)
        return
    offset = float(s["rim_in_clip"]) - float(shot["rim_t"])      # near clock - far clock
    S_n = H.load(work, name)
    seq = near_owner_seq(S_n, cam_n, near, far_rows, offset)
    held = sum(1 for o, _ in seq if o is not None)
    done = render_near(S_n, cam_n, clip, seq, float(shot["rim_t"]) + offset, float(shot["release_t"]) + offset,
                       BADGE.get(r["gt"]),
                       os.path.join(work, "r2k"), name, True)
    print(name, "rendered" if done else "skipped", "| ring on %d/%d frames | offset %.2fs" % (held, len(seq), offset), flush=True)


def main():
    game, per_side, work = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    sync = {x["key"]: x for x in json.load(open(os.path.join(CONFIRM, "rimsync_%s_near.json" % game)))["rows"]}
    for r in picks(game, per_side):
        if recording():
            print("recording started - stopping", flush=True)
            return
        try:
            one(game, r, work, sync)
        except Exception as e:  # noqa: BLE001
            print(r["clip"], "FAILED", type(e).__name__, str(e)[-300:], flush=True)


if __name__ == "__main__":
    main()

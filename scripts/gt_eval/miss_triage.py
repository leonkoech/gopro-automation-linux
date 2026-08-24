"""Miss triage — the recall loop's diagnostic stage.

For every GT shot the model missed (from a gt_eval report), re-examine its
±3s window at FULL 120fps with relaxed confidence and bucket the failure:

  coarse-miss   full-fps windowed detector DOES find the shot
                -> the 30fps coarse pass skipped it (stride/sampling knob)
  logic-reject  ball IS detected near the rim but no crossing verdict
                -> geometry/decision rules (tap-ins, shallow arcs, banks)
  no-ball       detector never sees the ball near the rim
                -> model failure (occlusion/blur/edge) -> fine-tune data

Also cuts an FL/FR review clip per miss. Downloads SL/SR from S3 when local
masters are gone (batch games). Appends a `triage` section to the report.

Usage: python3 miss_triage.py --report /home/dev/gt_eval/<label>_report.json
         [--rec-dir DIR] [--s3-prefix court-a/DATE/PREFIX --s3-date DATE]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import boto3

sys.path.insert(0, "/home/dev/gopro-automation-linux")
from agx_pipeline.shot_detect.detect import ShotDetector, iter_frames  # noqa: E402
from agx_pipeline.shot_detect.backtest import scan  # noqa: E402

BUCKET = "uball-videos-production"
WEIGHTS = ("/home/dev/gopro-automation-linux/agx_pipeline/shot_detect/weights/"
           "ball_yolo26s_gray_hifps_v3_best.pt")
RIMS = "/home/dev/gopro-automation-linux/agx_pipeline/shot_detect/rims.json"
CAM_OF = {"left": "SL", "right": "SR"}
TRACK_OF = {"left": "FL", "right": "FR"}
HALF = 3.0
RELAX_CONF = 0.12


def log(m):
    print(f"[triage] {m}", flush=True)


def ensure_video(rec, label, cam, s3_prefix):
    path = f"{rec}/{label}/{label}_{cam}.mp4"
    if os.path.isfile(path):
        return path
    if not s3_prefix:
        return None
    os.makedirs(os.path.dirname(path), exist_ok=True)
    key = f"{s3_prefix}_{cam}.mp4"
    log(f"downloading {key}")
    boto3.client("s3").download_file(BUCKET, key, path)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--rec-dir", default="/home/dev/app/recordings")
    ap.add_argument("--s3-prefix", default=None,
                    help="court-a/<date>/<p4>/<date>_<p4> (no _CAM.mp4)")
    a = ap.parse_args()
    rep = json.load(open(a.report))
    label = rep["label"]
    lin_a = rep.get("sync_linear_a", rep.get("sync_offset_s", 0.0))
    lin_b = rep.get("sync_linear_b", 1.0)
    misses = rep.get("model_missed", [])
    log(f"{label}: {len(misses)} missed shots to triage "
        f"(map: cv=(gt-{lin_a:.2f})/{lin_b:.5f})")
    if not misses:
        return

    det = ShotDetector(WEIGHTS)
    rims_all = json.load(open(RIMS))
    rims_cache = {}
    out_dir = f"/home/dev/gt_eval/{label}/miss_clips"
    os.makedirs(out_dir, exist_ok=True)
    s3c = boto3.client("s3")
    results = []
    for i, m in enumerate(misses):
        side = m["side"]
        cam = CAM_OF[side]
        video = ensure_video(a.rec_dir, label, cam, a.s3_prefix)
        if not video:
            results.append({**m, "bucket": "no-footage"})
            continue
        cv_t = (m["ts"] - lin_a) / lin_b       # GT video clock -> shot-cam clock
        if cam not in rims_cache:
            rims_cache[cam] = (scan.estimate_rim(det.model, video, det.device,
                                                 fps=119.9)
                               or (rims_all or {}).get(cam))
        rim = rims_cache[cam]
        frames = [fr for _, fr in iter_frames(
            video, ss=max(0.0, cv_t - HALF), max_frames=int(2 * HALF * 120))]
        v = det.detect(frames, rim, fps=119.9) if frames else None
        if v and v.get("verdict"):
            bucket = "coarse-miss"
        else:
            # relaxed per-frame ball presence near rim
            n_ball = 0
            if frames and rim:
                cx, cy = rim["center"]
                r = max(rim["semi_axes"]) * 2.5
                step = max(1, len(frames) // 90)
                for fr in frames[::step]:
                    rres = det.model.predict(fr, imgsz=960, conf=RELAX_CONF,
                                             verbose=False, device=det.device)[0]
                    for b in rres.boxes:
                        if int(b.cls.item()) == 0:
                            x1, y1, x2, y2 = (float(x) for x in b.xyxy[0])
                            bx, by = (x1 + x2) / 2, (y1 + y2) / 2
                            if abs(bx - cx) < r and abs(by - cy) < r:
                                n_ball += 1
                                break
            bucket = "logic-reject" if n_ball >= 3 else "no-ball"
        # FL/FR review clip on the GT/video clock
        name = f"M{i+1:02d}_{bucket}_{side}_t{int(m['ts'])}"
        clip = f"{out_dir}/{name}.mp4"
        tr = ensure_video(a.rec_dir, label, TRACK_OF[side], a.s3_prefix)
        url = None
        if tr and not os.path.exists(clip):
            try:
                subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                                "-ss", f"{max(0.0, m['ts']-4):.1f}", "-i", tr,
                                "-t", "8", "-vf",
                                f"scale=1280:720,drawtext=text='{name} GT-{m['cls']}'"
                                ":x=20:y=20:fontsize=32:fontcolor=white:box=1:"
                                "boxcolor=black@0.6:boxborderw=8",
                                "-c:v", "libx264", "-preset", "veryfast",
                                "-an", clip], check=True, timeout=300)
                key = f"review/miss_triage/{label}/{name}.mp4"
                s3c.upload_file(clip, BUCKET, key,
                                ExtraArgs={"ContentType": "video/mp4"})
                url = s3c.generate_presigned_url(
                    "get_object", Params={"Bucket": BUCKET, "Key": key},
                    ExpiresIn=604800)
            except Exception as e:  # noqa: BLE001
                log(f"{name} clip failed: {e}")
        results.append({**m, "bucket": bucket, "cv_t": round(cv_t, 1),
                        "clip": url})
        log(f"[{i+1}/{len(misses)}] {name}")

    counts = {}
    for r in results:
        counts[r["bucket"]] = counts.get(r["bucket"], 0) + 1
    rep["triage"] = {"buckets": counts, "misses": results}
    with open(a.report, "w") as f:
        json.dump(rep, f, indent=2)
    log(f"TAXONOMY {label}: {counts}")
    print("TRIAGE_DONE", flush=True)


if __name__ == "__main__":
    main()

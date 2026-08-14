"""GT evaluation harness — CV shot detection vs manual annotation (full game).

Per game (needs SL/SR/FL/FR masters on disk + manual GT in the annotation tool):
  1. GT           manual shot cards (FG|2PT|3PT|4PT)_(MAKE|MISS) via uball API
  2. CV shots     coarse scan of BOTH SL/SR masters -> each candidate confirmed
                  with the full-fps windowed detector (the validated make/miss)
  3. SYNC         FL<->SL offset: sidecar spawned_at prior, refined empirically
                  (mode of GT-vs-CV make deltas) — reported per game
  4. MATCH        greedy nearest within +/-4s on the same hoop side ->
                    matched (make/miss agreement matrix)
                    GT-only  = OUR MODEL MISSED
                    CV-only  = GT (annotators) MISSED  [in-game vs warmup flag]
  5. CLIPS        every make/miss disagreement -> SL|FL side-by-side 8s clip,
                  uploaded to S3 with 7-day presigned links
  6. REPORT       /home/dev/gt_eval/<label>_report.json + printed summary

Usage: python3 gt_eval.py --label game_X --uball-game-id U [--limit-s N]
Run only while nothing is recording (GPU + correctness).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime

import boto3

sys.path.insert(0, "/home/dev/gopro-automation-linux")
from agx_pipeline.shot_detect import logic  # noqa: E402
from agx_pipeline.shot_detect.backtest import scan  # noqa: E402
from agx_pipeline.shot_detect.detect import ShotDetector, iter_frames  # noqa: E402
from uball_client import UballClient  # noqa: E402

REC = "/home/dev/app/recordings"
OUT = "/home/dev/gt_eval"
BUCKET = "uball-videos-production"
WEIGHTS = ("/home/dev/gopro-automation-linux/agx_pipeline/shot_detect/weights/"
           "ball_yolo26s_gray_hifps_v3_best.pt")
RIMS = "/home/dev/gopro-automation-linux/agx_pipeline/shot_detect/rims.json"
SIDE = {"SL": "left", "SR": "right"}
GT_SIDE = {"LEFT": "left", "RIGHT": "right"}
MATCH_WIN = 4.0          # s, after offset correction
CONFIRM_HALF = 2.0       # s of full-fps window each side of a candidate
WARMUP_PAD = 90.0        # s outside [first_gt, last_gt] counts as out-of-game


def log(msg: str) -> None:
    print(f"[gt_eval {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_gt(uball_id: str):
    client = UballClient()
    rows = []
    for p in client.list_plays(uball_id):
        cls = str(p.get("classification") or "")
        if p.get("source") == "manual" and any(
                cls == f"{z}_{m}" for z in ("FG", "2PT", "3PT", "4PT")
                for m in ("MAKE", "MISS")):
            rows.append({"ts": float(p.get("timestamp_seconds") or 0),
                         "side": GT_SIDE.get(str(p.get("angle") or "").upper()),
                         "made": cls.endswith("MAKE"), "cls": cls,
                         "player": p.get("player_a")})
    rows.sort(key=lambda r: r["ts"])
    return [r for r in rows if r["side"]]


def sidecar_offsets(label: str):
    """Per shot-cam offset prior: t_FL ~= t_cam + (cam.spawned_at - FL_start).
    FL start ~= the label's UTC time (recorders spawn with the start command)."""
    try:
        sc = json.load(open(f"{REC}/{label}/{label}_shot_timing.json"))
        t_label = datetime.strptime(label, "game_%Y%m%d_%H%M%S")
        out = {}
        for c in sc.get("cameras", []):
            sp = datetime.fromisoformat(
                str(c.get("spawned_at")).replace("Z", "+00:00")).replace(tzinfo=None)
            out[c["angle"]] = (sp - t_label).total_seconds()
        return out
    except Exception as e:  # noqa: BLE001
        log(f"sidecar offsets unavailable ({e}) — using 0")
        return {}


def cv_shots(label: str, det, rims_all, limit_s):
    """Coarse scan of both shot cams + full-fps confirm per candidate."""
    shots = []
    for cam in ("SL", "SR"):
        video = f"{REC}/{label}/{label}_{cam}.mp4"
        if not os.path.isfile(video):
            log(f"{cam}: master missing — skipped")
            continue
        t0 = time.time()
        rim = (scan.estimate_rim(det.model, video, det.device, fps=119.9)
               or (rims_all or {}).get(cam))
        log(f"{cam}: rim={rim and 'ok'} ({time.time()-t0:.0f}s)")
        t0 = time.time()
        cand = scan.scan_shots(det.model, video, det.device, 119.9, rim,
                               stride=4, imgsz=640, progress_every=0,
                               limit_s=limit_s)
        log(f"{cam}: coarse scan -> {len(cand)} candidates "
            f"({time.time()-t0:.0f}s)")
        for c in cand:
            t = c["t_shot"]
            frames = [fr for _, fr in iter_frames(
                video, ss=max(0.0, t - CONFIRM_HALF),
                max_frames=int(2 * CONFIRM_HALF * 120))]
            v = det.detect(frames, rim, fps=119.9) if frames else None
            shots.append({"cam": cam, "side": SIDE[cam], "t": round(t, 2),
                          "made_coarse": c["made"],
                          "made": bool(v and v.get("made")) if v else c["made"],
                          "confirmed": bool(v)})
        log(f"{cam}: confirmed {sum(1 for s in shots if s['cam']==cam)} shots")
    shots.sort(key=lambda s: s["t"])
    return shots


def refine_offset(gt, cv, prior: float):
    """Mode of (gt_ts - cv_t) over same-side makes within +/-12s of prior."""
    deltas = []
    for g in gt:
        if not g["made"]:
            continue
        for s in cv:
            if s["side"] != g["side"] or not s["made"]:
                continue
            d = g["ts"] - s["t"]
            if abs(d - prior) <= 12.0:
                deltas.append(round(d * 2) / 2)
    if not deltas:
        return prior, 0
    off, n = Counter(deltas).most_common(1)[0]
    return off, n


def match(gt, cv, off):
    used = set()
    pairs, gt_only = [], []
    for g in gt:
        best, best_d = None, MATCH_WIN + 1
        for i, s in enumerate(cv):
            if i in used or s["side"] != g["side"]:
                continue
            d = abs(g["ts"] - (s["t"] + off))
            if d < best_d:
                best, best_d = i, d
        if best is not None and best_d <= MATCH_WIN:
            used.add(best)
            pairs.append((g, cv[best], round(best_d, 2)))
        else:
            gt_only.append(g)
    cv_only = [s for i, s in enumerate(cv) if i not in used]
    return pairs, gt_only, cv_only


def cut_clip(label, cam, t_cam, t_fl, name):
    os.makedirs(f"{OUT}/{label}/clips", exist_ok=True)
    out = f"{OUT}/{label}/clips/{name}.mp4"
    if os.path.exists(out):
        return out
    sl = f"{REC}/{label}/{label}_{cam}.mp4"
    fl = f"{REC}/{label}/{label}_{'FL' if cam == 'SL' else 'FR'}.mp4"
    cmd = ["ffmpeg", "-y", "-loglevel", "error",
           "-ss", f"{max(0.0, t_cam-4):.1f}", "-i", sl,
           "-ss", f"{max(0.0, t_fl-4):.1f}", "-i", fl,
           "-t", "8", "-filter_complex",
           "[0:v]scale=720:540[a];[1:v]scale=960:540[b];[a][b]hstack",
           "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p", out]
    subprocess.run(cmd, check=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--uball-game-id", required=True)
    ap.add_argument("--limit-s", type=float, default=None)
    a = ap.parse_args()
    os.makedirs(f"{OUT}/{a.label}", exist_ok=True)

    gt = load_gt(a.uball_game_id)
    log(f"GT: {len(gt)} manual shots ({sum(1 for g in gt if g['made'])} makes)")
    rims_all = json.load(open(RIMS))
    det = ShotDetector(WEIGHTS)
    cv = cv_shots(a.label, det, rims_all, a.limit_s)
    log(f"CV: {len(cv)} confirmed shots ({sum(1 for s in cv if s['made'])} makes)")

    prior_all = sidecar_offsets(a.label)
    prior = -(prior_all.get("SL", 0.0))          # t_fl = t_sl + (SL_spawn-FL) -> gt-cv
    off, votes = refine_offset(gt, cv, prior)
    log(f"SYNC: prior={prior:+.1f}s empirical={off:+.1f}s (votes={votes})")

    pairs, gt_only, cv_only = match(gt, cv, off)
    first_gt = gt[0]["ts"] if gt else 0
    last_gt = gt[-1]["ts"] if gt else 0
    for s in cv_only:
        t_fl = s["t"] + off
        s["in_game"] = (first_gt - WARMUP_PAD) <= t_fl <= (last_gt + WARMUP_PAD)

    agree = sum(1 for g, s, _ in pairs if g["made"] == s["made"])
    dis = [(g, s) for g, s, _ in pairs if g["made"] != s["made"]]
    s3c = boto3.client("s3")
    links = []
    for i, (g, s) in enumerate(dis):
        name = (f"D{i+1:02d}_{s['cam']}_GT-{'MAKE' if g['made'] else 'MISS'}"
                f"_CV-{'MAKE' if s['made'] else 'MISS'}_t{int(g['ts'])}")
        try:
            path = cut_clip(a.label, s["cam"], s["t"], g["ts"], name)
            key = f"review/gt_eval/{a.label}/{name}.mp4"
            s3c.upload_file(path, BUCKET, key,
                            ExtraArgs={"ContentType": "video/mp4"})
            url = s3c.generate_presigned_url(
                "get_object", Params={"Bucket": BUCKET, "Key": key},
                ExpiresIn=604800)
            links.append({"name": name, "url": url})
            log(f"clip {name} uploaded")
        except Exception as e:  # noqa: BLE001
            log(f"clip {name} FAILED: {e}")

    in_game_cv_only = [s for s in cv_only if s.get("in_game")]
    report = {
        "label": a.label, "uball_game_id": a.uball_game_id,
        "gt_shots": len(gt), "cv_shots": len(cv),
        "sync_offset_s": off, "sync_prior_s": prior, "sync_votes": votes,
        "matched": len(pairs),
        "model_missed": [{"ts": g["ts"], "side": g["side"], "cls": g["cls"]}
                         for g in gt_only],
        "gt_missed_in_game": [{"t_fl": round(s["t"] + off, 1), "side": s["side"],
                               "made": s["made"]} for s in in_game_cv_only],
        "gt_missed_warmup_or_ft": len(cv_only) - len(in_game_cv_only),
        "makemiss_agree": agree,
        "makemiss_accuracy": round(agree / len(pairs), 3) if pairs else None,
        "disagreements": [{"gt_ts": g["ts"], "gt": g["cls"],
                           "cv_made": s["made"], "cam": s["cam"]}
                          for g, s in dis],
        "clips": links,
    }
    with open(f"{OUT}/{a.label}_report.json", "w") as f:
        json.dump(report, f, indent=2)

    log("===== SUMMARY =====")
    log(f"GT {len(gt)} | CV {len(cv)} | matched {len(pairs)}")
    log(f"MODEL MISSED {len(gt_only)}/{len(gt)} "
        f"(recall {1 - len(gt_only)/len(gt):.0%})" if gt else "no GT")
    log(f"GT MISSED (in-game) {len(in_game_cv_only)} | warmup/FT {report['gt_missed_warmup_or_ft']}")
    log(f"MAKE/MISS accuracy {agree}/{len(pairs)}"
        f" = {report['makemiss_accuracy']}")
    log(f"disagreement clips: {len(links)}")
    print("GT_EVAL_DONE", flush=True)


if __name__ == "__main__":
    main()

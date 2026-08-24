"""Backtest orchestrator — runs the whole thing on the box GPU.

  1. load frozen GT                     (gt.load_gt)
  2. stage SL/SR footage from S3        (aws s3 cp if not local)
  3. per cam: estimate rim, coarse scan (scan.*)  -> setup-2 candidates
  4. per cam: calibrate δ               (calibrate.calibrate_cam)
  5. setup-1 manual/trigger: window each GT shot at t_track+δ -> detect -> verdict
  6. (optional) confirm setup-2 candidates at full fps
  7. score both vs GT + write results JSON (+ Firebase for the frontend card)

Runs OFFLINE; does not touch SHOT_VALIDATION_ENABLED or the live service.

Example (box):
  python3 -m agx_pipeline.shot_detect.backtest.run \
    --game fdcd9bd4-3615-4b4a-911f-3a5242c561ac \
    --weight agx_pipeline/shot_detect/weights/ball_yolo26s_gray_hifps_v3_best.pt \
    --footage-dir /home/dev/backtest/fdcd9bd4 --out /home/dev/backtest/out \
    --s3-prefix court-a/2026-07-28/fdcd9bd4-3615-4b4a-911f --subset 20
"""
from __future__ import annotations

import argparse
import bisect
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from agx_pipeline.shot_detect.detect import ShotDetector
from agx_pipeline.shot_detect.backtest import calibrate, gt, report, scan

RIMS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rims.json")


def probe_fps(video_path: str, default: float = 119.9) -> float:
    """True frame rate of the shot-cam clip.

    The FLIR/record container reports r_frame_rate as its timebase (e.g. 12000/1),
    NOT the ~120fps we need — so prefer avg_frame_rate, then nb_frames/duration,
    and only fall back to r_frame_rate if it is physically sane (1<fps<1000)."""
    def _q(entry: str, fmt: bool = False) -> str:
        scope = "format=" if fmt else "stream="
        sel = [] if fmt else ["-select_streams", "v:0"]
        return subprocess.check_output(
            ["ffprobe", "-v", "error", *sel, "-show_entries", scope + entry,
             "-of", "default=nokey=1:noprint_wrappers=1", video_path],
            text=True).strip()

    def _ratio(s: str) -> Optional[float]:
        try:
            num, den = (s.split("/") + ["1"])[:2]
            f = float(num) / float(den) if float(den) else 0.0
            return f if 1.0 < f < 1000.0 else None
        except Exception:  # noqa: BLE001
            return None

    for entry in ("avg_frame_rate", "r_frame_rate"):
        try:
            f = _ratio(_q(entry))
            if f:
                return f
        except Exception:  # noqa: BLE001
            pass
    try:
        nb, dur = float(_q("nb_frames")), float(_q("duration", fmt=True))
        if nb > 0 and dur > 0 and 1.0 < nb / dur < 1000.0:
            return nb / dur
    except Exception:  # noqa: BLE001
        pass
    return default


BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")


def stage_footage(cam: str, footage_dir: str, s3_prefix: Optional[str],
                  date: str, folder: str) -> str:
    """Return a local path to the cam's mp4, downloading from S3 if absent.

    Uses boto3 (the box has ~/.aws/credentials; no aws CLI); falls back to the
    aws CLI if boto3 is unavailable."""
    os.makedirs(footage_dir, exist_ok=True)
    fn = f"{date}_{folder}_{cam}.mp4"
    local = os.path.join(footage_dir, fn)
    if os.path.exists(local) and os.path.getsize(local) > 0:
        return local
    if not s3_prefix:
        raise FileNotFoundError(f"{local} missing and no --s3-prefix to fetch it")
    key = f"{s3_prefix.rstrip('/')}/{fn}"
    print(f"[stage] downloading s3://{BUCKET}/{key} -> {local}", flush=True)
    try:
        import boto3
        boto3.client("s3").download_file(BUCKET, key, local)
    except ImportError:
        subprocess.check_call(["aws", "s3", "cp", f"s3://{BUCKET}/{key}", local])
    return local


def run(game_id: str, weight: str, footage_dir: str, out_dir: str,
        s3_prefix: Optional[str], stride: int = 4, n_before: float = 2.5,
        m_after: float = 2.5, subset: Optional[int] = None,
        confirm_setup2: bool = False, do_setup1: bool = True,
        no_firebase: bool = False, scan_limit_s: Optional[float] = None,
        matchup: Optional[str] = None, skip_scan: bool = False,
        deltas_override: Optional[Dict[str, float]] = None,
        rescan: bool = False, confirm_setup1: bool = False) -> Dict:
    t_start = time.time()
    os.makedirs(out_dir, exist_ok=True)
    stages: Dict[str, Dict] = {}

    gt_doc = gt.load_gt(game_id)
    stages["gt"] = {"state": "done", "n_shots": gt_doc["n_shots"]}
    shots: List[Dict] = gt_doc["shots"]

    # derive S3 date/folder from the prefix (court-a/<date>/<folder>)
    date, folder = "", ""
    if s3_prefix:
        parts = s3_prefix.strip("/").split("/")
        if len(parts) >= 3:
            date, folder = parts[1], parts[2]

    canon = json.load(open(RIMS_PATH))
    det = ShotDetector(weight)
    print(f"[run] detector device={det.device}", flush=True)

    rims: Dict[str, Dict] = {}
    scans: Dict[str, List[Dict]] = {}
    calib: Dict[str, Dict] = {}
    videos: Dict[str, str] = {}
    fps = None

    # The full-video scan is the expensive part (~1h/cam) and its candidates +
    # δ + rim are all setup-1/2 need (no per-window re-decode). Checkpoint them so
    # a re-run skips scanning entirely.
    ckpt_path = os.path.join(out_dir, f"{game_id.split('-')[0]}_scan_ckpt.json")
    if not skip_scan and not rescan and os.path.exists(ckpt_path):
        ck = json.load(open(ckpt_path))
        fps = ck["fps"]
        for cam in ("SL", "SR"):
            rims[cam] = ck["cams"][cam]["rim"]
            scans[cam] = ck["cams"][cam]["candidates"]
            calib[cam] = ck["cams"][cam]["calib"]
        stages["scan"] = {"state": "done", "from_checkpoint": True,
                          "per_cam": {c: {"candidates": len(scans[c])} for c in ("SL", "SR")}}
        print(f"[run] loaded scan checkpoint {ckpt_path} — SL {len(scans['SL'])} / "
              f"SR {len(scans['SR'])} candidates; skipping scan", flush=True)
    else:
        for cam in ("SL", "SR"):
            video = stage_footage(cam, footage_dir, s3_prefix, date, folder)
            videos[cam] = video
            if fps is None:
                fps = probe_fps(video)
                print(f"[run] fps={fps:.4f}", flush=True)
            rim = scan.estimate_rim(det.model, video, det.device, fps=fps)
            if rim is None:
                rim = canon.get(cam)
                print(f"[run] {cam}: auto-rim failed, using canonical rims.json", flush=True)
            rims[cam] = rim
            print(f"[run] {cam} rim={rim}", flush=True)

            if skip_scan:
                scans[cam] = []
                d = float((deltas_override or {}).get(cam, 0.0))
                calib[cam] = {"cam": cam, "delta": d, "matched": None, "n_gt": None,
                              "frac": None, "peakness": None, "tol": None,
                              "median_residual": None, "calibrated_on": "override"}
                print(f"[run] {cam} δ={d}s (override; scan skipped)", flush=True)
            else:
                t0 = time.time()
                sc = scan.scan_shots(det.model, video, det.device, fps, rim,
                                     stride=stride, limit_s=scan_limit_s)
                scans[cam] = sc
                stages.setdefault("scan", {"state": "done", "per_cam": {}})
                stages["scan"]["per_cam"][cam] = {"candidates": len(sc), "secs": round(time.time() - t0, 1),
                                                  "limit_s": scan_limit_s}
                print(f"[run] {cam}: {len(sc)} coarse candidates in {time.time()-t0:.0f}s", flush=True)
                calib[cam] = calibrate.calibrate_cam(shots, sc, cam)
                print(f"[run] {cam} δ={calib[cam]['delta']}s matched={calib[cam]['matched']}/"
                      f"{calib[cam]['n_gt']} peakness={calib[cam]['peakness']}", flush=True)
            det.empty_cache()

        if not skip_scan:  # persist the real scan for cheap re-runs
            with open(ckpt_path, "w") as fh:
                json.dump({"game_id": game_id, "fps": fps,
                           "cams": {c: {"rim": rims[c], "calib": calib[c],
                                        "candidates": scans[c]} for c in ("SL", "SR")}}, fh)
            print(f"[run] saved scan checkpoint -> {ckpt_path}", flush=True)

    stages["calibrate"] = {"state": "done", "SL": calib["SL"], "SR": calib["SR"]}
    deltas = {c: calib[c]["delta"] for c in ("SL", "SR")}

    # ---- setup 1: manual/trigger — for each GT shot, the CV verdict at t_track+δ ----
    # Default (fast): look the verdict up in the whole-game scan track — the detector
    # already ran over every frame, so re-decoding a per-shot window is pure waste
    # (and ~5min/shot on this GPU). `--confirm-setup1` re-decodes each window at full
    # fps (slow; only for a deep spot-check).
    setup1_results: List[Dict] = []
    if do_setup1:
        pool = [s for s in shots if (scan_limit_s is None or s["t_track"] <= scan_limit_s)]
        work = pool if not subset else _subset_spread(pool, subset)
        if confirm_setup1:
            stages["setup1"] = {"state": "running", "n": len(work), "mode": "confirm"}
            for i, s in enumerate(work):
                cam = s["cam"]
                center = s["t_track"] + deltas[cam]
                t_lo = max(0.0, center - n_before)
                dur = (center + m_after) - t_lo
                target_idx = int((center - t_lo) * fps)
                frame_iter = scan.iter_frames(videos[cam], ss=t_lo, t=dur)
                v = det.detect_stream(frame_iter, rims[cam], fps, target_idx=target_idx)
                setup1_results.append({
                    "id": s["id"], "gt_made": s["made"], "cam": cam, "kind": s["kind"],
                    "cv_made": (None if v is None else bool(v["made"])),
                    "verdict": (None if v is None else v.get("verdict")),
                    "rho": (None if v is None else v.get("rho")), "t_track": s["t_track"],
                })
                if (i + 1) % 5 == 0:
                    det.empty_cache()
                    print(f"[setup1] {i+1}/{len(work)}", flush=True)
            stages["setup1"] = {"state": "done", "n": len(work), "mode": "confirm"}
            det.empty_cache()
        else:
            setup1_results = _setup1_lookup(work, scans, deltas)
            stages["setup1"] = {"state": "done", "n": len(work), "mode": "lookup"}

    # ---- setup 2: automated detected list (coarse, optionally confirmed) ----
    detected: List[Dict] = []
    for cam in ("SL", "SR"):
        for c in scans[cam]:
            made = c["made"]
            if confirm_setup2:
                t_lo = max(0.0, c["t_shot"] - 1.5)
                frame_iter = scan.iter_frames(videos[cam], ss=t_lo, t=3.0)
                v = det.detect_stream(frame_iter, rims[cam], fps)
                if v is not None:
                    made = bool(v["made"])
            detected.append({"t_shot": c["t_shot"], "made": made, "cam": cam})
    stages["setup2"] = {"state": "done", "n_detected": len(detected),
                        "confirmed": confirm_setup2}

    # ---- score ----
    setup1_score = report.score_setup1(shots, setup1_results) if do_setup1 else None
    setup2_score = None if skip_scan else report.score_setup2(shots, detected, deltas)
    meta = {
        "matchup": matchup, "weight": os.path.basename(weight), "fps": fps,
        "stride": stride, "device": det.device, "n_before": n_before, "m_after": m_after,
        "subset": subset, "confirm_setup2": confirm_setup2, "scan_limit_s": scan_limit_s,
        "stages": stages, "secs_total": round(time.time() - t_start, 1),
    }
    rep = report.build_report(gt_doc, calib, setup1_score, setup2_score, meta)
    rep["matchup"] = matchup
    rep["updated_at"] = datetime.now(timezone.utc).isoformat()
    rep["setup1_results"] = setup1_results
    rep["setup2_detected"] = detected

    out_path = os.path.join(out_dir, f"{game_id.split('-')[0]}_backtest.json")
    with open(out_path, "w") as fh:
        json.dump(rep, fh, indent=1)
    print(f"[run] wrote {out_path} in {meta['secs_total']}s", flush=True)
    if not no_firebase:
        ok = report.write_firebase(rep)
        print(f"[run] firebase shot-backtests/{game_id}: {'written' if ok else 'skipped'}", flush=True)
    return rep


def _subset_spread(shots: List[Dict], n: int) -> List[Dict]:
    """Evenly spread subset across the game (smoke without full cost)."""
    if n >= len(shots):
        return shots
    step = len(shots) / n
    return [shots[int(i * step)] for i in range(n)]


def _setup1_lookup(work: List[Dict], scans: Dict[str, List[Dict]],
                   deltas: Dict[str, float], tol: float = 1.5) -> List[Dict]:
    """Setup-1 by lookup: for each GT shot, the nearest scan candidate to
    t_track+δ (same cam, within tol) gives the CV verdict. No re-decode."""
    by_cam = {c: sorted((x["t_shot"], x) for x in scans.get(c, [])) for c in ("SL", "SR")}
    out: List[Dict] = []
    for s in work:
        cam = s["cam"]
        target = s["t_track"] + deltas.get(cam, 0.0)
        arr = by_cam.get(cam, [])
        times = [t for t, _ in arr]
        j = bisect.bisect_left(times, target)
        best = None
        for k in (j - 1, j, j + 1):
            if 0 <= k < len(arr):
                t, cand = arr[k]
                if abs(t - target) <= tol and (best is None or abs(t - target) < best[0]):
                    best = (abs(t - target), cand)
        c = None if best is None else best[1]
        out.append({
            "id": s["id"], "gt_made": s["made"], "cam": cam, "kind": s["kind"],
            "cv_made": (None if c is None else bool(c["made"])),
            "verdict": (None if c is None else c.get("verdict")),
            "rho": (None if c is None else c.get("rho")), "t_track": s["t_track"],
        })
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--game", required=True)
    ap.add_argument("--weight", required=True)
    ap.add_argument("--footage-dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--s3-prefix", default=None,
                    help="court-a/<date>/<folder> to fetch footage if not local")
    ap.add_argument("--stride", type=int, default=4, help="spot-pass frame stride (4≈30fps)")
    ap.add_argument("--scan-limit-s", type=float, default=None,
                    help="cap the scan to the first N seconds (bounded smoke)")
    ap.add_argument("--subset", type=int, default=None, help="setup-1 on N spread shots (smoke)")
    ap.add_argument("--confirm-setup2", action="store_true")
    ap.add_argument("--no-setup1", action="store_true")
    ap.add_argument("--no-firebase", action="store_true")
    ap.add_argument("--matchup", default=None, help="display name, e.g. 'Hustle vs Akatsuki'")
    ap.add_argument("--skip-scan", action="store_true",
                    help="skip scan+calibrate+setup2; inject --delta-sl/--delta-sr (fast setup-1 test)")
    ap.add_argument("--delta-sl", type=float, default=0.0)
    ap.add_argument("--delta-sr", type=float, default=0.0)
    ap.add_argument("--rescan", action="store_true", help="ignore any scan checkpoint; scan fresh")
    ap.add_argument("--confirm-setup1", action="store_true",
                    help="re-decode each setup-1 window at full fps (slow; default is lookup)")
    a = ap.parse_args()
    run(a.game, a.weight, a.footage_dir, a.out, a.s3_prefix, stride=a.stride,
        subset=a.subset, confirm_setup2=a.confirm_setup2,
        do_setup1=not a.no_setup1, no_firebase=a.no_firebase,
        scan_limit_s=a.scan_limit_s, matchup=a.matchup, skip_scan=a.skip_scan,
        deltas_override={"SL": a.delta_sl, "SR": a.delta_sr},
        rescan=a.rescan, confirm_setup1=a.confirm_setup1)

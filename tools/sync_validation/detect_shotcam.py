"""Find every rim crossing in an SL/SR master, and timestamp it with real seconds.

This is the pipeline's own detector run over the whole-game master instead of
the live 4s segments. That single change removes the arithmetic that broke the
clips: on a segment, `cross_frame` is an index inside a short window and the
live code reconstructs a game time as segment_index * nominal_seconds. On the
master, `cross_frame` IS the game position, so the timestamp is

    real_seconds = cross_frame / measured_fps

with `measured_fps` taken from the file (nb_frames / duration), never from the
120 lock -- the cameras deliver 118.5-119.9 and the value moves per game with
exposure, so the lock alone puts full time up to 37 s out.

Because every camera's frames are converted with its own measured rate, and the
four cameras cover the same wall-clock window to under a second, a crossing at
real time T on SL is real time T on FL. That is what makes the clip cut and the
SL->FL pairing checkable rather than assumed.

Scanning is windowed with overlap so a shot straddling a boundary is whole in
one window, and crossings are de-duplicated on real time afterwards.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = "/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux"
sys.path.insert(0, REPO)

WIN_S = 8.0          # window length in real seconds
OVERLAP_S = 2.0      # carried between windows so a shot cannot fall in a crack
DEDUP_S = 1.5


def measured_fps(path: Path) -> tuple:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=nb_frames,duration", "-of", "json", str(path)],
        capture_output=True, text=True)
    s = json.loads(r.stdout)["streams"][0]
    nb, dur = float(s["nb_frames"]), float(s["duration"])
    return nb / dur, nb, dur


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--video", required=True)
    ap.add_argument("--angle", required=True, choices=["SL", "SR"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", type=float, default=0.0)
    ap.add_argument("--end", type=float, default=None)
    ap.add_argument("--stride", type=int, default=4)
    ap.add_argument("--imgsz", type=int, default=960)
    a = ap.parse_args()

    from agx_pipeline.shot_detect import logic
    from agx_pipeline.shot_detect.detect import ShotDetector
    from agx_pipeline.shot_detect.backtest import scan as scan_mod

    video = Path(a.video)
    fps, nframes, dur = measured_fps(video)
    print(f"{a.angle}: {nframes:.0f} frames / {dur:.2f}s -> MEASURED {fps:.3f} fps "
          f"(lock would be 120 -> {nframes/120.0:.1f}s, i.e. {dur - nframes/120.0:+.1f}s out)",
          flush=True)

    rims = json.loads((Path(REPO) / "agx_pipeline/shot_detect/rims.json").read_text())
    rim = rims[a.angle]
    det = ShotDetector(str(HERE / "weights" / "ball_yolo26s_gray_hifps_v3_best.pt"),
                       device="mps")
    # Geo is built with the MEASURED rate so its internal frame->second maths and
    # any speed gating match reality rather than the lock.
    G = logic.Geo.from_rim(rim, float(fps))

    end = a.end if a.end is not None else dur
    t = a.start
    found, nwin = [], 0
    tmp = HERE / f"_w_{a.angle}.mp4"
    while t < end:
        seg_dur = min(WIN_S, end - t)
        if seg_dur < 1.0:
            break
        cut = subprocess.run(
            ["ffmpeg", "-nostdin", "-y", "-loglevel", "error", "-ss", f"{t:.3f}",
             "-i", str(video), "-t", f"{seg_dur:.3f}", "-c", "copy", str(tmp)],
            capture_output=True, text=True)
        if cut.returncode != 0 or not tmp.exists():
            t += WIN_S - OVERLAP_S
            continue
        try:
            track, _hoops = scan_mod.scan_ball_and_hoops(
                det.model, str(tmp), det.device, stride=a.stride, imgsz=a.imgsz)
            for v in logic.decide(G, track):
                if "verdict" not in v:
                    continue
                # v["t"] is seconds INSIDE this window, already using measured fps
                real_t = t + float(v.get("t", 0.0))
                found.append({"angle": a.angle, "real_t": round(real_t, 3),
                              "verdict": v["verdict"], "geo": v.get("geo"),
                              "cross_frame_in_win": v.get("cross_frame"),
                              "rho": v.get("rho")})
        except Exception as e:  # noqa: BLE001 - one bad window must not stop the sweep
            print(f"  window {t:.1f}s failed: {type(e).__name__}: {e}", flush=True)
        nwin += 1
        if nwin % 25 == 0:
            print(f"  {t:7.1f}s / {end:.0f}s   crossings so far: {len(found)}", flush=True)
        t += WIN_S - OVERLAP_S
    tmp.unlink(missing_ok=True)

    found.sort(key=lambda r: r["real_t"])
    dedup = []
    for r in found:
        if dedup and r["real_t"] - dedup[-1]["real_t"] < DEDUP_S:
            continue          # same shot seen in two overlapping windows
        dedup.append(r)
    Path(a.out).write_text(json.dumps(
        {"angle": a.angle, "video": str(video), "measured_fps": fps,
         "duration": dur, "n_raw": len(found), "shots": dedup}, indent=1))
    makes = sum(1 for r in dedup if r["verdict"] == "MAKE")
    print(f"\n{a.angle}: {len(dedup)} rim crossings after dedup "
          f"({makes} MAKE, {len(dedup)-makes} MISS) -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

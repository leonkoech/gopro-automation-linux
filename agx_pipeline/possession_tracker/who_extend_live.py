"""Extended-timeline jersey reading for live games (TRACKER_WHO_EXTEND).

When the highlight clip itself gives no confident number, rebuild a longer window around the
shot from the side's rolling highlight buffer (recordings/highlight_buf/<label>/<side>/, 4 s
raw segments kept ~10 min), transcode it to 1080p on the hardware path the service already uses,
and follow the shooter BACK from the clip start and FORWARD from the clip end, reading his
jersey until a number is confident (who_extend.follow — the method measured on 395 annotated
shots: names 67% of shots at 78% vs 60% at 83% from the clip alone).

Best-effort: if the buffer no longer holds the window (worker more than ~10 min behind) or
anything fails, the clip-only answer (usually none) stands.
"""
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")

BACK_S = float(os.environ.get("TRACKER_WHO_BACK_S", "15"))
FWD_S = float(os.environ.get("TRACKER_WHO_FWD_S", "5"))
STEP_S = 2.5
BUF_ROOT = os.environ.get("HIGHLIGHT_BUF_DIR", "/home/dev/app/recordings/highlight_buf")

_CFG = None
_MODEL = None


def _cfg():
    global _CFG
    if _CFG is None:
        from agx_pipeline.recording import load_config
        _CFG = load_config()
    return _CFG


def _model():
    global _MODEL
    if _MODEL is None:
        from ultralytics import YOLO
        _MODEL = YOLO(os.path.join(HERE, "weights", "yolo11s-seg.pt"))
    return _MODEL


def build_window(label, side, angle, t0, t1, work):
    """1080p mp4 covering [t0, t1] (epoch s) from the buffer; returns (path, start_epoch) or None."""
    from agx_pipeline import highlight as HL
    d = os.path.join(BUF_ROOT, label, side)
    try:
        segs = []
        for fn in os.listdir(d):
            m = HL._SEG_RE.match(fn)
            if m and m.group(2) == angle:
                segs.append((int(m.group(1)), m.group(2), os.path.join(d, fn)))
    except OSError:
        return None
    picked = HL._select_segments(sorted(segs), t0, t1)
    if not picked or picked[0][0] > t0 + STEP_S:
        return None
    lst = os.path.join(work, "list.txt")
    with open(lst, "w") as f:
        f.writelines("file '%s'\n" % p for _s, _a, p in picked)
    merged = os.path.join(work, "merged.mp4")
    if subprocess.run(["nice", "-n", "19", "ffmpeg", "-nostdin", "-v", "error", "-y", "-f", "concat", "-safe", "0",
                       "-i", lst, "-c", "copy", merged], capture_output=True).returncode != 0:
        return None
    hd = os.path.join(work, "window_1080p.mp4")
    from agx_pipeline.ingest import _transcode_1080p
    if not _transcode_1080p(merged, hd, _cfg()):
        return None
    return hd, float(picked[0][0])


def extend_reads(js, job, boxes, reads):
    """Append jersey reads from beyond the clip; returns the extended reads (or the input)."""
    import who_eval as W
    import who_extend as X
    if not boxes or W.speak(reads, None) is not None:
        return reads
    clip_dir = os.path.dirname(job["clip"])
    label = os.path.basename(clip_dir.rstrip("/"))
    side = job["log_id"].rsplit("_", 1)[-1]
    try:
        epoch = float(job["log_id"].split("_")[1])
    except (IndexError, ValueError):
        return reads
    clip_start = epoch - job["pre"]                     # epoch of clip t = 0
    t_first, t_last = clip_start + boxes[0][0], clip_start + boxes[-1][0]
    with tempfile.TemporaryDirectory(prefix="whox_") as work:
        got = build_window(label, side, job["angle"], t_first - BACK_S, t_last + FWD_S, work)
        if not got:
            return reads
        video, w0 = got
        reads = list(reads)
        for direction, limit, edge, box in (("back", BACK_S, t_first - w0, boxes[0][1]),
                                            ("fwd", FWD_S, t_last - w0, boxes[-1][1])):
            walked = 0.0
            while walked < limit and W.speak(reads, None) is None:
                if direction == "back":
                    chunk = X.frames(video, edge - STEP_S, edge, reverse=True)
                    edge -= STEP_S
                else:
                    chunk = X.frames(video, edge, edge + STEP_S, reverse=False)
                    edge += STEP_S
                walked += STEP_S
                if not chunk:
                    break
                path, box, held = X.follow(_model(), chunk, box)
                if not held:
                    break
                got_reads = js.read_crops([c for _, _, c in path]) if path else []
                reads += [(str(n), float(c or 0)) for n, c in got_reads if n is not None]
    return reads

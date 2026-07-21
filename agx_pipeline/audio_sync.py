#!/usr/bin/env python3
"""Audio cross-correlation sync for the Zowietek tracking cameras.

The tracking cameras (FL/FR/NL) all pick up the same court audio, so the fixed
per-game frame offset between any two of them can be recovered by
cross-correlating their audio tracks — no manual marking, no shared clock.

Ported from the CV repo's proven pipeline/audio_sync_detect.py (validated to
~1 frame vs manual marks, 2026-06-10). Adapted for ingest: it works directly on
the per-camera audio side-files captured alongside the video, auto-picks
windows across the whole recording (so it also samples late-game and catches
slow clock drift), and returns a single densest-cluster offset estimate.

Requires numpy + scipy (present in the AGX service env).

CLI (for validation):
    python3 -m agx_pipeline.audio_sync A.m4a B.m4a [--fps 30] [--windows 6]
"""
from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
from scipy.io import wavfile

SR = 16000            # mono resample rate for cross-correlation
WIN_LEN = 40.0        # seconds of audio per correlation window
MAX_LAG_S = 5.0       # search ±5 s (RTSP connect-time skew can exceed the CV repo's ±2 s)
CLUSTER_TOL = 1.25    # frames — densest-cluster agreement tolerance
MIN_PEAK = 6.0        # minimum peak prominence to trust a window


def _probe_dur(path: str) -> Optional[float]:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nokey=1:noprint_wrappers=1", path],
        capture_output=True, text=True, stdin=subprocess.DEVNULL)
    try:
        return float(r.stdout.strip())
    except ValueError:
        return None


def _extract_wav(src: str, t0: float, dur: float, out: Path) -> bool:
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-ss", f"{t0:.2f}", "-i", src, "-t", f"{dur:.2f}",
           "-vn", "-ac", "1", "-ar", str(SR), str(out)]
    r = subprocess.run(cmd, capture_output=True, stdin=subprocess.DEVNULL, timeout=300)
    return r.returncode == 0 and out.exists() and out.stat().st_size > 1000


def _load(p: Path) -> np.ndarray:
    _, x = wavfile.read(p)
    x = x.astype(np.float64)
    x -= x.mean()
    x = np.diff(x)                       # first difference emphasizes transients
    s = x.std()
    return x / s if s > 0 else x


def _xcorr_lag(a: np.ndarray, b: np.ndarray) -> tuple:
    """Lag (seconds) of b relative to a via FFT cross-correlation, plus peak
    prominence. Positive lag = the same event appears LATER in b."""
    n = len(a) + len(b)
    nfft = 1 << (n - 1).bit_length()
    A = np.fft.rfft(a, nfft)
    B = np.fft.rfft(b, nfft)
    corr = np.fft.irfft(B * np.conj(A), nfft)
    corr = np.concatenate([corr[-len(a) + 1:], corr[:len(b)]])
    lags = np.arange(-len(a) + 1, len(b))
    keep = np.abs(lags) <= int(MAX_LAG_S * SR)
    corr, lags = corr[keep], lags[keep]
    i = int(np.argmax(corr))
    peak = float(corr[i] / (np.median(np.abs(corr)) + 1e-9))
    return lags[i] / SR, peak


def _cluster(frames: List[float], tol: float = CLUSTER_TOL) -> tuple:
    """Densest-cluster estimator: music/noise scatters false peaks, but
    true-offset windows agree to ~±1 frame; return the largest cluster's mean."""
    arr = np.asarray(frames, dtype=float)
    best_n, best_mean = -1, 0.0
    for c in arr:
        sel = arr[np.abs(arr - c) <= tol]
        if len(sel) > best_n:
            best_n, best_mean = len(sel), float(sel.mean())
    return best_mean, best_n


def measure_offset(audio_a: str, audio_b: str, *, fps: float = 30.0,
                   n_windows: int = 6, win_len: float = WIN_LEN,
                   min_peak: float = MIN_PEAK) -> Dict:
    """Frame offset of b relative to a (positive = b lags a).

    Samples up to n_windows evenly across the shorter track and cross-correlates
    each; returns the densest-cluster estimate with per-window detail. ok=False
    if fewer than 2 windows agreed confidently. offset_sec is the authoritative,
    fps-independent value; offset_frames is offset_sec * fps for convenience.
    """
    da, db = _probe_dur(audio_a), _probe_dur(audio_b)
    if not da or not db:
        return {"ok": False, "reason": "cannot probe audio duration"}
    dur = min(da, db)
    if dur < win_len:
        return {"ok": False, "reason": f"audio too short ({dur:.1f}s < {win_len}s)"}
    span = dur - win_len
    starts = [round(span * (i + 0.5) / n_windows, 1) for i in range(n_windows)]
    rows: List[Dict] = []
    good: List[float] = []
    with tempfile.TemporaryDirectory() as td:
        for t0 in starts:
            wa, wb = Path(td) / "a.wav", Path(td) / "b.wav"
            if not (_extract_wav(audio_a, t0, win_len, wa)
                    and _extract_wav(audio_b, t0, win_len, wb)):
                rows.append({"t0": t0, "ok": False})
                continue
            lag_s, peak = _xcorr_lag(_load(wa), _load(wb))
            rows.append({"t0": t0, "ok": True, "lag_s": round(lag_s, 4),
                         "frames": round(lag_s * fps, 2), "peak": round(peak, 1)})
            if peak >= min_peak:
                good.append(lag_s * fps)
    if len(good) < 2:
        return {"ok": False, "reason": "not enough confident windows",
                "duration": round(dur, 1), "windows": rows}
    est, support = _cluster(good)
    drift = round(max(good) - min(good), 2)
    return {"ok": True, "offset_frames": round(est, 2),
            "offset_sec": round(est / fps, 3), "fps": fps,
            "support": support, "n_confident": len(good),
            "median_frames": round(float(np.median(good)), 2),
            "drift_frames": drift, "duration": round(dur, 1), "windows": rows}


def main() -> int:
    import argparse
    import json
    ap = argparse.ArgumentParser(description="audio cross-correlation sync offset")
    ap.add_argument("audio_a", help="reference track (e.g. FL)")
    ap.add_argument("audio_b", help="other track (e.g. FR)")
    ap.add_argument("--fps", type=float, default=30.0)
    ap.add_argument("--windows", type=int, default=6)
    ap.add_argument("--win-len", type=float, default=WIN_LEN)
    a = ap.parse_args()
    res = measure_offset(a.audio_a, a.audio_b, fps=a.fps,
                         n_windows=a.windows, win_len=a.win_len)
    print(json.dumps(res, indent=2))
    return 0 if res.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

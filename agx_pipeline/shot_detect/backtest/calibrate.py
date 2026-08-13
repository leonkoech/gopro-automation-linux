"""Clock-offset (δ) calibration for the backtest.

Annotation times are on the tracking (FL/FR) clock; the SL/SR FLIR cams started
at a different moment. We recover one constant offset per cam:

    shot_cam_time = t_track + δ

by aligning the GT shot times to the automated scan's detected crossings: for a
grid of δ, count how many GT shots land within `tol` of a detected crossing, and
take the δ that maximizes the count. With ~90 events/cam the peak is sharp; a
flat/weak peak means the footage and GT don't line up and we must NOT trust any
downstream accuracy number (surfaced as low `peakness`).
"""
from __future__ import annotations

import bisect
from typing import Dict, List


def _nearest_gap(sorted_times: List[float], x: float) -> float:
    """Distance from x to the closest value in a sorted list (inf if empty)."""
    if not sorted_times:
        return float("inf")
    i = bisect.bisect_left(sorted_times, x)
    best = float("inf")
    if i < len(sorted_times):
        best = min(best, abs(sorted_times[i] - x))
    if i > 0:
        best = min(best, abs(x - sorted_times[i - 1]))
    return best


def calibrate_delta(gt_times: List[float], detected_times: List[float],
                    lo: float = -60.0, hi: float = 60.0, step: float = 0.1,
                    tol: float = 1.5) -> Dict:
    """Grid-search δ that best aligns gt_times+δ onto detected_times.

    Returns {delta, matched, n_gt, frac, peakness, tol, median_residual}. `peakness`
    = 1 - (second-best count far from the peak)/(best count): ~1 is a clean unique
    peak, ~0 is ambiguous. `median_residual` is the median |gt+δ - nearest| at the
    winning δ (a fine-alignment quality check)."""
    det = sorted(detected_times)
    gts = list(gt_times)
    if not gts or not det:
        return {"delta": 0.0, "matched": 0, "n_gt": len(gts), "frac": 0.0,
                "peakness": 0.0, "tol": tol, "median_residual": None}

    n_steps = int(round((hi - lo) / step)) + 1
    counts: List[tuple] = []  # (delta, matched, matched_residual_sum)
    for k in range(n_steps):
        d = lo + k * step
        gaps = [_nearest_gap(det, t + d) for t in gts]
        m = sum(1 for g in gaps if g <= tol)
        res = sum(g for g in gaps if g <= tol)
        counts.append((d, m, res))

    best_m = max(m for _, m, _ in counts)
    # A wide tol makes many δ tie at best_m (a plateau); pick the δ that also
    # minimizes the fine residual so we land on the plateau's CENTER, not an edge.
    best_delta = min((c for c in counts if c[1] == best_m), key=lambda c: c[2])[0]

    # peakness: best vs the best count OUTSIDE a small neighborhood of best_delta
    guard = 3.0  # seconds
    outside = [m for (d, m, _r) in counts if abs(d - best_delta) > guard]
    second = max(outside) if outside else 0
    peakness = 0.0 if best_m == 0 else round(1.0 - second / best_m, 3)

    residuals = sorted(_nearest_gap(det, t + best_delta) for t in gts
                       if _nearest_gap(det, t + best_delta) <= tol)
    med_res = residuals[len(residuals) // 2] if residuals else None

    return {
        "delta": round(best_delta, 3),
        "matched": best_m,
        "n_gt": len(gts),
        "frac": round(best_m / len(gts), 3),
        "peakness": peakness,
        "tol": tol,
        "median_residual": None if med_res is None else round(med_res, 3),
    }


def calibrate_cam(gt_shots: List[Dict], scan_shots: List[Dict], cam: str,
                  use_makes_only: bool = True, **kw) -> Dict:
    """Calibrate δ for one cam using its GT shots vs its automated scan crossings.

    Makes give the cleanest rim crossings, so calibrate on makes by default, then
    δ applies to all shots on that cam."""
    gt = [s["t_track"] for s in gt_shots
          if s["cam"] == cam and (s["made"] or not use_makes_only)]
    det = [s["t_shot"] for s in scan_shots]
    out = calibrate_delta(gt, det, **kw)
    out["cam"] = cam
    out["calibrated_on"] = "makes" if use_makes_only else "all"
    return out

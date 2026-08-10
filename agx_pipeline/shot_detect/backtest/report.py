"""Backtest metrics + results doc.

Scores the two setups against ground truth and assembles the doc the frontend
"Shot-Detection Backtest" card renders:

  setup 1 (manual/trigger): among GT shots that produced a CV verdict, does
    cv_made match gt_made?  -> coverage + make/miss agreement + confusion.
  setup 2 (automated):      match detected crossings to GT (per cam, via δ, within
    tol)  -> detection precision/recall + make/miss accuracy on matched pairs.
"""
from __future__ import annotations

import bisect
from typing import Dict, List, Optional


def _confusion(pairs: List[tuple]) -> Dict:
    """pairs of (gt_made, cv_made) -> confusion + accuracy."""
    tp = sum(1 for g, c in pairs if g and c)       # both make
    tn = sum(1 for g, c in pairs if not g and not c)  # both miss
    fp = sum(1 for g, c in pairs if not g and c)   # cv says make, gt miss
    fn = sum(1 for g, c in pairs if g and not c)   # cv says miss, gt make
    n = len(pairs)
    return {"tp": tp, "tn": tn, "fp": fp, "fn": fn, "n": n,
            "accuracy": round((tp + tn) / n, 4) if n else None}


def score_setup1(gt_shots: List[Dict], results: List[Dict]) -> Dict:
    """results: [{id, gt_made, cv_made(None if no verdict), cam, kind}].

    coverage = fraction of GT shots with any verdict; accuracy over those."""
    with_verdict = [r for r in results if r.get("cv_made") is not None]
    pairs = [(bool(r["gt_made"]), bool(r["cv_made"])) for r in with_verdict]
    out = {
        "n_gt": len(gt_shots),
        "n_scored": len(results),
        "coverage": round(len(with_verdict) / len(gt_shots), 4) if gt_shots else None,
        "overall": _confusion(pairs),
        "by_cam": {}, "by_kind": {},
    }
    for cam in ("SL", "SR"):
        p = [(bool(r["gt_made"]), bool(r["cv_made"])) for r in with_verdict if r.get("cam") == cam]
        out["by_cam"][cam] = _confusion(p)
    for kind in ("FG", "3PT", "4PT", "FREE"):
        p = [(bool(r["gt_made"]), bool(r["cv_made"])) for r in with_verdict if r.get("kind") == kind]
        out["by_kind"][kind] = _confusion(p)
    return out


def score_setup2(gt_shots: List[Dict], detected: List[Dict], deltas: Dict[str, float],
                 tol: float = 1.5) -> Dict:
    """Match automated detections to GT per cam (apply δ to GT), then score.

    detected: [{t_shot, made, cam}]  (shot-cam clock).
    A GT shot at t_track maps to cam clock t_track + δ[cam]; match to the nearest
    unused detection within tol. Matched -> TP; unmatched GT -> FN (missed);
    unmatched detection -> FP (phantom). Make/miss accuracy is over matched pairs.
    """
    matched_pairs: List[tuple] = []   # (gt_made, det_made)
    n_fn = 0
    used = set()
    det_by_cam = {c: sorted([(d["t_shot"], i) for i, d in enumerate(detected) if d["cam"] == c])
                  for c in ("SL", "SR")}
    for g in gt_shots:
        cam = g["cam"]
        target = g["t_track"] + deltas.get(cam, 0.0)
        arr = det_by_cam.get(cam, [])
        times = [t for t, _ in arr]
        j = bisect.bisect_left(times, target)
        best = None
        for cand in (j - 1, j, j + 1):
            if 0 <= cand < len(arr):
                t, di = arr[cand]
                if di in used:
                    continue
                if abs(t - target) <= tol and (best is None or abs(t - target) < best[0]):
                    best = (abs(t - target), di)
        if best is None:
            n_fn += 1
        else:
            used.add(best[1])
            matched_pairs.append((bool(g["made"]), bool(detected[best[1]]["made"])))
    n_fp = len(detected) - len(used)
    n_tp = len(matched_pairs)
    prec = round(n_tp / len(detected), 4) if detected else None
    rec = round(n_tp / len(gt_shots), 4) if gt_shots else None
    return {
        "n_gt": len(gt_shots), "n_detected": len(detected),
        "matched": n_tp, "missed_fn": n_fn, "phantom_fp": n_fp,
        "detect_precision": prec, "detect_recall": rec,
        "makemiss_on_matched": _confusion(matched_pairs),
    }


def build_report(gt_doc: Dict, calib: Dict, setup1: Optional[Dict],
                 setup2: Optional[Dict], meta: Dict) -> Dict:
    """Assemble the full results doc (also the frontend node payload)."""
    return {
        "game_id": gt_doc["game_id"],
        "matchup": meta.get("matchup"),
        "gt": {"n_shots": gt_doc["n_shots"], "n_make": gt_doc["n_make"],
               "n_miss": gt_doc["n_miss"], "n_sl": gt_doc["n_sl"], "n_sr": gt_doc["n_sr"]},
        "calibration": calib,       # {SL:{delta,peakness,...}, SR:{...}}
        "setup1_manual": setup1,
        "setup2_automated": setup2,
        "meta": meta,               # stage states, timings, weight, stride, etc.
    }


def write_firebase(report: Dict, fb=None) -> bool:
    """Write the report to shot-backtests/{game_id} for the frontend card.

    `fb` is a FirebaseService (has `.db`, the Firestore client); built via
    get_firebase_service() if not passed. The big per-shot arrays are stripped —
    the card doesn't use them and they'd bloat the doc. Best-effort; never raises."""
    try:
        if fb is None:
            from firebase_service import get_firebase_service  # type: ignore
            fb = get_firebase_service()
        db = getattr(fb, "db", None)
        if db is None:
            return False
        doc = {k: v for k, v in report.items()
               if k not in ("setup1_results", "setup2_detected")}
        db.collection("shot-backtests").document(report["game_id"]).set(doc)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[report] firebase write skipped: {str(e)[:150]}", flush=True)
        return False

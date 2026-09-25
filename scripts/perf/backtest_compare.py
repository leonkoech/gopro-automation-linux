#!/usr/bin/env python3
"""
Read one backtest report, or compare two — the TensorRT adoption decision.

Speed was settled separately (trt_kickstart.py: ~2.0x, which takes the live
loop from 1.0x realtime to ~0.5x and so drains the backlog). This answers the
other half: does the engine still find the same shots and call them the same
way? FP16 changes numerics, so it genuinely might not.

    python3 scripts/perf/backtest_compare.py out-trt/fdcd9bd4_backtest.json
    python3 scripts/perf/backtest_compare.py out-pt/..._backtest.json out-trt/..._backtest.json

With two files the second is treated as the candidate and the first as the
baseline. Schema is whatever backtest/report.py:build_report emits.
"""
from __future__ import annotations

import json
import sys


def g(d, *path, default=None):
    for k in path:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def rows(r):
    """(label, value, higher_is_better) for the numbers that decide adoption."""
    s1, s2 = r.get("setup1_manual") or {}, r.get("setup2_automated") or {}
    return [
        ("weight",                g(r, "meta", "weight"),                None),
        ("stride",                g(r, "meta", "stride"),                None),
        ("GT shots",              g(r, "gt", "n_shots"),                 None),
        ("cal SL delta",          g(r, "calibration", "SL", "delta"),    None),
        ("cal SL peakness",       g(r, "calibration", "SL", "peakness"), True),
        ("cal SR delta",          g(r, "calibration", "SR", "delta"),    None),
        ("cal SR peakness",       g(r, "calibration", "SR", "peakness"), True),
        ("--- setup2 (auto) ---", None,                                  None),
        ("detected",              s2.get("n_detected"),                  None),
        ("matched",               s2.get("matched"),                     True),
        ("missed (FN)",           s2.get("missed_fn"),                   False),
        ("phantom (FP)",          s2.get("phantom_fp"),                  False),
        ("detect precision",      s2.get("detect_precision"),            True),
        ("detect recall",         s2.get("detect_recall"),               True),
        ("make/miss accuracy",    g(s2, "makemiss_on_matched", "accuracy"), True),
        ("--- setup1 (manual) ---", None,                                None),
        ("coverage",              s1.get("coverage"),                    True),
        ("accuracy",              g(s1, "overall", "accuracy"),          True),
    ]


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.4f}".rstrip("0").rstrip(".")
    t = str(v)
    return t if len(t) <= 16 else "~" + t[-15:]   # keep the distinctive tail


def main() -> int:
    paths = sys.argv[1:]
    if not paths or len(paths) > 2:
        raise SystemExit(__doc__)
    reps = []
    for p in paths:
        with open(p) as fh:
            reps.append((p.split("/")[-2] if "/" in p else p, json.load(fh)))

    print(f"game {reps[0][1].get('game_id')}  {reps[0][1].get('matchup') or ''}\n")

    if len(reps) == 1:
        name, r = reps[0]
        for label, val, _ in rows(r):
            print(f"  {label:<24}{fmt(val)}" if val is not None else f"\n  {label}")
        print("\n  One run only — re-run with the other weight and pass both files "
              "to compare.\n  The absolute numbers mean little; the DELTA against the "
              "same game with .pt is the decision.")
        return 0

    (n0, r0), (n1, r1) = reps
    a, b = rows(r0), rows(r1)
    print(f"  {'':<24}{n0[:16]:>16}{n1[:16]:>18}   change")
    print("  " + "-" * 62)
    for (label, v0, better), (_, v1, _) in zip(a, b):
        if v0 is None and v1 is None:
            print(f"\n  {label}")
            continue
        chg = ""
        if isinstance(v0, (int, float)) and isinstance(v1, (int, float)) and better is not None:
            d = v1 - v0
            if abs(d) > 1e-9:
                good = (d > 0) == better
                chg = f"{d:+.4f}".rstrip("0").rstrip(".")
                chg += "  ok" if good else "  <-- WORSE"
        print(f"  {label:<24}{fmt(v0):>16}{fmt(v1):>18}   {chg}")

    n = g(r0, "gt", "n_shots") or 0
    print(f"""
  Reading it. With {n} ground-truth shots, one shot is about {100.0/n if n else 0:.1f}
  percentage points — so a swing of one or two is noise, not a regression.
  What matters is detect_recall and make/miss accuracy: if those hold within a
  couple of shots, FP16 did not move the verdicts and the ~2x speedup is free.
  If recall drops materially, re-export with --fp32 and compare again before
  concluding TensorRT is at fault.""")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

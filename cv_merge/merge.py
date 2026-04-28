"""Cross-side merge algorithm for CV shot detection.

Side A (FR+NR) and Side B (FL+NL) watch different hoops, so they should
almost never see the same shot. Cross-side overlap is a safety net for
mid-court / fast-break edge cases.

Pure functions only — unit-testable without Firebase or AWS.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List


@dataclass
class MergedShot:
    """A shot after cross-side merging — the shape the emitter consumes."""

    timestamp_seconds: float
    outcome: str           # "made" | "missed"
    confidence: float      # [0.0, 1.0]
    side: str              # "A" | "B"
    source_shot: Dict[str, Any]  # original per-side shot dict (for debugging)


def _to_merged(shots: Iterable[Dict[str, Any]], side: str) -> List[MergedShot]:
    out: List[MergedShot] = []
    for s in shots:
        outcome = s.get("outcome")
        if outcome not in ("made", "missed"):
            # "undetermined" and anything else is dropped — V1 only emits made/missed
            continue
        try:
            ts = float(s["timestamp_seconds"])
        except (KeyError, TypeError, ValueError):
            continue
        conf = float(s.get("fusion_confidence", 0.0) or 0.0)
        out.append(MergedShot(
            timestamp_seconds=ts,
            outcome=outcome,
            confidence=conf,
            side=side,
            source_shot=s,
        ))
    return out


def merge(
    side_a: Iterable[Dict[str, Any]],
    side_b: Iterable[Dict[str, Any]],
    *,
    temporal_window: float = 1.0,
) -> List[MergedShot]:
    """Merge per-side shots into a single ordered timeline.

    Dedup rule: if two consecutive shots come from DIFFERENT sides, are within
    `temporal_window` seconds of each other, AND agree on outcome, keep only
    the higher-confidence one (discard the lower). Same-side shots are never
    deduplicated — the fusion script has already handled intra-side overlap.
    """
    tagged: List[MergedShot] = _to_merged(side_a, "A") + _to_merged(side_b, "B")
    tagged.sort(key=lambda x: (x.timestamp_seconds, x.side))

    merged: List[MergedShot] = []
    for shot in tagged:
        if merged:
            last = merged[-1]
            cross_side = last.side != shot.side
            close_in_time = abs(last.timestamp_seconds - shot.timestamp_seconds) < temporal_window
            same_outcome = last.outcome == shot.outcome
            if cross_side and close_in_time and same_outcome:
                if shot.confidence > last.confidence:
                    merged[-1] = shot
                continue
        merged.append(shot)
    return merged

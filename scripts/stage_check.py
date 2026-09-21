#!/usr/bin/env python3
"""What can be tested right now, and what is blocking the rest.

Written after live typing sat broken for fifteen days behind a dangling symlink
while every dashboard said the box was healthy. The lesson was not "add a check
for symlinks" -- it was that a stage can be configured ON and be incapable of
running, and nothing says so until someone reads a log at the right moment.

So this answers one question per stage: if a game started now, would this stage
produce anything? It reports a blocked stage with the reason and the path, never
a bare boolean. Read-only -- it starts nothing and changes nothing.

    python3 scripts/stage_check.py

Exit code is 0 when every ENABLED stage is ready, 1 otherwise, so it can gate a
deploy. A disabled stage is reported but never fails the run: off is a valid
state, and only silence about being broken is not.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "agx_pipeline"))

TYPING_CWD = Path(os.getenv("SHOT_TYPING_CWD", "/home/dev/shot_typing"))

OK, BLOCKED, OFF = "READY", "BLOCKED", "off"


def _env_on(name: str) -> bool:
    return os.getenv(name, "false").strip().lower() in ("1", "true", "yes", "on")


def _missing(paths: List[Path]) -> List[Path]:
    """Absent OR a dangling symlink -- isfile() is False for both, which is the
    case that broke typing."""
    return [p for p in paths if not p.is_file()]


def check_typing() -> Tuple[str, str]:
    """Reuses the service's own preflight, so this can never disagree with it."""
    if not _env_on("SHOT_LIVE_TYPING"):
        return OFF, "SHOT_LIVE_TYPING is not set"
    try:
        from agx_pipeline.shot_typing_live import preflight
    except Exception as exc:  # noqa: BLE001
        return BLOCKED, f"cannot import the typing service: {exc}"
    # Pass our own resolved dir: the service module read SHOT_TYPING_CWD when it
    # was imported, and a checker reporting on a different directory than the one
    # it names is exactly the confusion this script exists to remove.
    broken = preflight(str(TYPING_CWD))
    return (BLOCKED, broken) if broken else (OK, f"{TYPING_CWD}/agx_classify.py")


def check_who() -> Tuple[str, str]:
    """WHO needs more than its gate: the scan script and its models must sit in
    the classify working dir, because that is the cwd it is spawned with."""
    if not _env_on("SHOT_LIVE_WHO_SCAN"):
        return OFF, "SHOT_LIVE_WHO_SCAN is not set"
    missing = _missing([
        TYPING_CWD / "who_scan_live.py",
        TYPING_CWD / "unified_yolo26s.pt",
        TYPING_CWD / "runs" / "jersey" / "legibility_resnet18.pt",
        TYPING_CWD / "runs" / "jersey" / "number_localizer_yolo11n.pt",
        TYPING_CWD / "runs" / "jersey" / "parseq_jersey.pt",
    ])
    if missing:
        return BLOCKED, "missing/dangling: " + ", ".join(str(p) for p in missing)
    if not (TYPING_CWD / "uball_cc").is_dir():
        return BLOCKED, f"uball_cc not importable from {TYPING_CWD}"
    # Not a file check: the roster filter is the largest measured accuracy lever
    # (56% -> 90%), and an empty SHOT_WHO_ROSTER disables it silently -- the scan
    # still answers, just far less reliably. That is worth refusing to call ready.
    if not os.getenv("SHOT_WHO_ROSTER", "").strip():
        return BLOCKED, ("SHOT_WHO_ROSTER is empty — the per-team filter is off, "
                         "which costs roughly 56% vs 90%; wire the check-in roster "
                         "before trusting WHO")
    return OK, "scan script, models and roster all present"


def check_eval() -> Tuple[str, str]:
    """The eval is only useful if it scores what production runs."""
    sys.path.insert(0, str(REPO / "scripts" / "gt_eval"))
    try:
        from agx_pipeline.shot_typing_live import _classify_env
        import type_eval  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        return BLOCKED, f"type_eval not importable here: {exc}"
    prod, ev = _classify_env(), type_eval.classify_env()
    drift = [k for k, v in prod.items()
             if k.startswith("SHOT_") and k != "SHOT_RIM_TS" and ev.get(k) != v]
    if drift:
        return BLOCKED, f"eval knobs differ from production: {', '.join(drift)}"
    return OK, f"matches production ({prod.get('SHOT_ATTRIB')})"


def main() -> int:
    checks = (("TYPE (live typing)", check_typing),
              ("WHO (jersey scan)", check_who),
              ("TYPE eval harness", check_eval))
    print(f"stage check — SHOT_TYPING_CWD={TYPING_CWD}\n")
    failed = False
    for name, fn in checks:
        try:
            status, detail = fn()
        except Exception as exc:  # noqa: BLE001 — a check must never crash the report
            status, detail = BLOCKED, f"check itself failed: {exc}"
        mark = {OK: "  ok     ", BLOCKED: "  BLOCKED", OFF: "  off    "}[status]
        print(f"{mark} {name}\n           {detail}")
        if status == BLOCKED:
            failed = True
    print("\n" + ("something enabled cannot run — see BLOCKED above"
                  if failed else "every enabled stage is ready"))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

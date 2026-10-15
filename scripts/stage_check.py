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


def _detector_weight() -> str:
    """The .pt the detector will open, resolved the way node.py resolves it --
    imported rather than re-derived, so this can never name a different file
    than production loads."""
    from agx_pipeline.shot_detect import node  # noqa: PLC0415
    return os.getenv("SHOT_DET_WEIGHT", node._DEFAULT_WEIGHT)


def check_detector(load: bool = False) -> Tuple[str, str]:
    """Is the detector going to run the engine it was configured with?

    The detector runs a TensorRT engine when SHOT_DET_ENGINE points at one, and
    the .pt whenever it cannot. The fallback is deliberate and it is also the
    thing worth checking: a box configured for the engine and quietly running
    the .pt works, at roughly half the speed, and says so only in a log line at
    startup.

    Three ways that happens, all checked here: the engine file is gone, a GPU
    fault disabled it and left the marker behind, or it loads and fails its
    probe. The .pt is checked too, because it is now the fallback rather than
    just the default -- if it is missing there is nothing to fall back to.

    `load` runs the detector's own probe in a subprocess. It costs seconds and
    a CUDA context, so it stays opt-in and belongs on a deploy gate.
    """
    try:
        weight = Path(_detector_weight())
    except Exception as exc:  # noqa: BLE001
        return BLOCKED, f"cannot resolve the detector weight: {exc}"
    if weight.is_symlink() and not weight.exists():
        return BLOCKED, (f"the .pt is a DANGLING SYMLINK: {weight} -> "
                         f"{os.readlink(weight)}")
    if not weight.is_file():
        return BLOCKED, f"the .pt is missing, so there is no fallback: {weight}"

    engine = os.getenv("SHOT_DET_ENGINE", "").strip()
    if not engine:
        return OK, (f"PyTorch .pt: {weight} — SHOT_DET_ENGINE is not set, so the "
                    f"engine is not in use")

    # Reuse the detector's own marker path so the two cannot disagree about
    # which file means "disabled".
    try:
        from agx_pipeline.shot_detect.detect import ENGINE_DISABLED_MARK  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        return BLOCKED, f"cannot import the detector: {exc}"
    if os.path.exists(ENGINE_DISABLED_MARK):
        return BLOCKED, (f"the engine was disabled after a GPU fault and the box "
                         f"is on the .pt: delete {ENGINE_DISABLED_MARK} to "
                         f"re-enable it")
    if not Path(engine).is_file():
        return BLOCKED, (f"SHOT_DET_ENGINE is set but the file is gone, so the "
                         f"detector will run the .pt at about half speed: {engine}")

    if not load:
        return OK, f"TensorRT engine: {engine} (run with --load to prove it runs)"

    try:
        from agx_pipeline.shot_detect.detect import _probe_in_subprocess  # noqa: PLC0415
        why = _probe_in_subprocess(engine)
    except Exception as exc:  # noqa: BLE001
        return BLOCKED, f"the probe itself failed: {exc}"
    if why:
        return BLOCKED, (f"the engine fails the detector's own probe ({why}), so "
                         f"the box will run the .pt. An engine only runs on the "
                         f"device and TensorRT version that built it — rebuild "
                         f"with scripts/build_shot_engine.py")
    return OK, f"TensorRT engine passed the detector's probe: {engine}"


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


def main(argv: Optional[List[str]] = ()) -> int:
    # Defaults to no flags rather than sys.argv so that callers (and the tests)
    # can invoke main() bare without inheriting whatever ran the process; the
    # CLI passes sys.argv[1:] explicitly below.
    import argparse  # noqa: PLC0415
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--load", action="store_true",
                    help="run the detector's own probe against the TensorRT "
                         "engine (needs the GPU; catches an engine built for "
                         "another box or another TensorRT version)")
    args = ap.parse_args(argv)

    checks = (("TYPE (live typing)", check_typing),
              ("WHO (jersey scan)", check_who),
              ("DETECTOR engine + .pt", lambda: check_detector(load=args.load)),
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
    raise SystemExit(main(sys.argv[1:]))

"""Live shot-value typing for the CV auto-scorecard.

Every CV auto-highlight (a detected make) already produces a trimmed 1080p clip
in cut_highlight — the calibration's native coordinate space (calib_arcs_*.json
is 1920x1080), verified consistent with master-based classification. This
module queues that clip through the FL/FR classifier so the live scorecard
shows REAL 2/3/4-point values (user directive: no makes-x2 — each make may
take time, but it gets its true shot type).

One serialized worker thread: ~20s per make idle, ~25-40s under game load; the
queue drains through timeouts/halftime and finishes by the end of the game.
The verdict lands on the game doc as `cv_points.{logId}` — a field the live
detector's shot_live rewrites never touch, and NEVER logs[] (the official
score stays the scorekeeper's).

Env: SHOT_LIVE_TYPING=true enables; SHOT_TYPING_CWD points at the proven
classify working dir (agx_classify.py + calib + weights + .env)."""
from __future__ import annotations

import glob
import os
import queue
import re
import subprocess
import threading
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from logging_service import get_logger

logger = get_logger("agx.shot_typing_live")

TYPING_CWD = os.getenv("SHOT_TYPING_CWD", "/home/dev/shot_typing")
CLASSIFY_TIMEOUT_S = int(os.getenv("SHOT_TYPING_TIMEOUT_S", "240"))
# The classifier emits FREE_THROW as a zone (agx_classify: zone_new = "FREE_THROW").
# It was missing here, so every correctly-typed free throw fell through the
# "zone not in _POINTS" branch and was discarded as if the chain had failed —
# 4 of 8 matched free throws on cb9e1294 were typed right and thrown away.
_POINTS = {"2PT": 2, "3PT": 3, "4PT": 4, "FREE_THROW": 1}

# Confidence carried onto the annotator's card. Two levels, not a scale: the
# chain either committed to a zone or fell back to geometry, and the card shows
# a green/red flag off exactly this number.
CONF_COMMITTED = 0.9     # STRICT produced a zone
CONF_FALLBACK = 0.4      # STRICT declined (degenerate pose); geometric zone only


def typing_enabled() -> bool:
    return os.getenv("SHOT_LIVE_TYPING", "false").strip().lower() in ("1", "true", "yes", "on")


def preflight(cwd: Optional[str] = None) -> Optional[str]:
    """Why typing cannot run, or None when it can.

    `cwd` defaults to this module's TYPING_CWD. It is a parameter so a caller
    that resolved the working dir itself -- scripts/stage_check.py does -- can
    check the directory it means rather than the one this module happened to
    read at import time. Two components disagreeing about which directory they
    are talking about is how this class of fault hides.

    This exists because typing failed silently for fifteen days. agx_classify.py
    in TYPING_CWD was a symlink into a scratch directory that a disk cleanup had
    removed, so every spawn died instantly on python's own "can't open file"
    (rc=2) and the queue logged nothing but that number. isfile() is False for a
    dangling symlink, which is exactly the case that got us.
    """
    base = cwd or TYPING_CWD
    script = os.path.join(base, "agx_classify.py")
    if not os.path.isdir(base):
        return f"SHOT_TYPING_CWD does not exist: {base}"
    if os.path.islink(script) and not os.path.exists(script):
        return f"classifier is a DANGLING SYMLINK: {script} -> {os.readlink(script)}"
    if not os.path.isfile(script):
        return f"classifier not found: {script}"
    if not os.access(script, os.R_OK):
        return f"classifier not readable: {script}"
    return None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify_env() -> Dict[str, str]:
    """The CUDA env agx_classify needs when spawned from the service (which
    itself runs without LD_LIBRARY_PATH — same recipe as the nightly typing)."""
    env = os.environ.copy()
    nvlibs = ":".join(glob.glob(
        "/home/dev/.local/lib/python3.10/site-packages/nvidia/*/lib"))
    env["LD_LIBRARY_PATH"] = (
        f"{nvlibs}:/usr/local/cuda-12.6/targets/aarch64-linux/lib:"
        f"/usr/local/cuda-12.6/lib64:" + env.get("LD_LIBRARY_PATH", ""))
    # v2 stack — every element fleet-validated on the 505-shot benchmark
    # (2026-09-05, 86.9%): release-moment attribution, hybrid bbox+ankle feet,
    # parked-ball filter, catch-and-shoot receiver rescue, fine-tuned ball
    # detector, and STRICT mode so a degenerate call emits UNKNOWN instead of
    # a coin flip (the scoreboard never shows a guess).
    env["SHOT_ATTRIB"] = "release_pose"
    env["SHOT_FEET"] = "bbox"
    env["SHOT_RP_FEET"] = "ankle+mix"
    env["SHOT_BALL_PARKFILTER"] = "1"
    env["SHOT_CS_FIX"] = "1"
    env["SHOT_TYPE_STRICT"] = "1"
    # Two-game loop, 2026-09-24 (cb9e1294 + 7cef734e, every shot scored against
    # manual GT). Both broke 0 shots on either game.
    # FLIGHT GATE: a release-pose "release" at or after the rim moment is
    # impossible for a shot from >=1.5m; hand it to the possession release
    # instead (cb9 +3/-0, 7cef 0/0). Free-throw feet are never gated.
    env["SHOT_RP_FLIGHT_GATE"] = "1"
    env["SHOT_RP_FLIGHT_A"] = "0.05"
    env["SHOT_RP_FLIGHT_B"] = "0"
    env["SHOT_RP_FLIGHT_MIN_D_CM"] = "150"
    env["SHOT_RP_FLIGHT_SKIP_FT"] = "1"
    # KEEP FT: STRICT discarded FREE_THROW calls whenever the release pose was
    # unclear, but an FT does not depend on the release (7cef +4/-0, cb9 0/0).
    env["SHOT_STRICT_KEEP_FT"] = "1"
    ball_w = os.getenv("SHOT_BALL_WEIGHTS_PATH",
                       os.path.join(TYPING_CWD, "yolo26s_ball_hoop_ft_evalweek_v1.pt"))
    if os.path.isfile(ball_w):
        env["SHOT_BALL_WEIGHTS"] = ball_w
    return env


def decide_zone(stdout: str, returncode: int) -> Tuple[Optional[str], float, str]:
    """The zone, its confidence, and where it came from.

    Pulled out of _type() so an evaluation harness can score the SAME decision
    production makes rather than reimplementing it. scripts/gt_eval/type_eval.py
    kept its own copy of the classify knobs and silently fell a whole stack
    behind; this is the same trap one level down.

    Returns (None, 0.0, "none") when no zone is defensible.
    """
    m_zone = re.search(r"ZONE_NEW=(\w+)", stdout)
    zone = m_zone.group(1) if m_zone else None
    if zone in _POINTS:
        return zone, CONF_COMMITTED, "strict"
    # STRICT declined (degenerate pose). Falling back to the geometric zone
    # looks attractive — ZONE_OLD was right on 6 of the 9 declined field goals
    # on cb9e1294 — and it is OFF because measuring it at the level that
    # matters showed it buys nothing.
    #
    # An unanswered shot does not produce a blank card: plays_sync labels it
    # FG_MAKE, i.e. it already assumes 2PT. Over the 12 shots the fallback fires
    # on, it predicts 2PT for 10 of them, so it agrees with that assumption
    # almost everywhere. Card-level accuracy measured 37/45 with it and 37/45
    # without — identical — and both land red-flagged (0.4 vs the 0.5
    # placeholder, both under the editor's 0.7), so the annotator sees no
    # difference either. All it adds is complexity and seven verdicts that look
    # wrong in the metrics.
    #
    # Kept behind a flag rather than deleted because the measurement is
    # game-specific and the code is cheap to re-test: SHOT_TYPE_FALLBACK=1.
    if os.getenv("SHOT_TYPE_FALLBACK") == "1":
        m_old = re.search(r"ZONE_OLD=(\w+)", stdout)
        if (m_old and m_old.group(1) in _POINTS and "trust=True" in stdout
                and returncode == 0):
            return m_old.group(1), CONF_FALLBACK, "geometric_fallback"
    return None, 0.0, "none"


class LiveTyper:
    """Serialized clip->shot-type queue. enqueue() never blocks the highlight
    path: a full queue drops the item with a warning (that make just stays
    'pending' on the scorecard — the nightly job still types its card)."""

    def __init__(self, fb):
        self.fb = fb
        broken = preflight()
        if broken:
            logger.error("TYPING IS ENABLED BUT CANNOT RUN — %s. Every shot will "
                         "stay pending until this is fixed.", broken)
        else:
            logger.info("typing preflight ok — %s/agx_classify.py", TYPING_CWD)
        self._q: "queue.Queue[Dict]" = queue.Queue(maxsize=64)
        self._thread = threading.Thread(target=self._run, name="shot-typing-live",
                                        daemon=True)
        self._thread.start()

    def enqueue(self, game_id: str, log_id: str, angle: str,
                clip_path: str, pre_s: float) -> None:
        item = {"game_id": game_id, "log_id": log_id, "angle": angle,
                "clip": clip_path, "pre": float(pre_s)}
        try:
            self._q.put_nowait(item)
            logger.info("typing queued %s (%s, depth=%d)", log_id, angle,
                        self._q.qsize())
        except queue.Full:
            logger.warning("typing queue FULL — dropped %s (stays pending; "
                           "nightly typing still covers its card)", log_id)

    # ---- worker ------------------------------------------------------------ #
    def _run(self) -> None:
        while True:
            item = self._q.get()
            try:
                self._type(item)
            except Exception as e:  # noqa: BLE001 — one bad clip never kills the queue
                logger.warning("typing failed for %s: %s", item.get("log_id"), e)

    def _type(self, item: Dict) -> None:
        log_id, angle, clip = item["log_id"], item["angle"], item["clip"]
        if not os.path.isfile(clip):
            logger.warning("typing skipped %s — clip missing (%s)", log_id, clip)
            return
        # The rim moment sits `pre` seconds into the trimmed clip; it is both
        # the shot ts and the precise rim anchor (no rim search needed — the
        # live detector's verdict IS the rim time).
        env = _classify_env()
        env["SHOT_RIM_TS"] = f"{item['pre']:.2f}"
        cp = subprocess.run(
            ["python3", "agx_classify.py", angle, clip, f"{item['pre']:.2f}", log_id],
            cwd=TYPING_CWD, env=env, capture_output=True, text=True,
            timeout=CLASSIFY_TIMEOUT_S)
        # HEALTH-GATED RESCUE (fleet-validated pattern): when the first pass
        # admits confusion (degenerate scores / no release), one retry with the
        # rim-anchored ball-path solver — its answer is adopted only when that
        # pass is itself healthy. Healthy first passes are never touched.
        deg = re.search(r"pose_degenerate=True", cp.stdout) or             re.search(r"release_f=None", cp.stdout)
        if deg:
            env2 = dict(env, SHOT_BALL_SOLVER="1")
            cp2 = subprocess.run(
                ["python3", "agx_classify.py", angle, clip,
                 f"{item['pre']:.2f}", log_id],
                cwd=TYPING_CWD, env=env2, capture_output=True, text=True,
                timeout=CLASSIFY_TIMEOUT_S)
            healthy2 = ("pose_degenerate=False" in cp2.stdout
                        and "release_f=None" not in cp2.stdout
                        and "rim_end=no" not in cp2.stdout)
            if healthy2 and re.search(r"ZONE_NEW=(\w+)", cp2.stdout):
                cp = cp2
                logger.info("typing rescue adopted for %s", log_id)
        m_who = re.search(r"WHO=#(\w+)", cp.stdout)
        m_proc = re.search(r"([\d.]+)s proc", cp.stdout)
        degenerate = "pose_degenerate=True" in cp.stdout
        zone, confidence, zone_source = decide_zone(cp.stdout, cp.returncode)
        if zone_source == "geometric_fallback":
            logger.info("typing fallback for %s -> %s (strict declined, "
                        "degenerate=%s)", log_id, zone, degenerate)
        if zone not in _POINTS:
            # A non-zero rc means the classifier never reached a verdict, and its
            # stderr says why. Logging only the number is what hid a dead symlink
            # for fifteen days — the answer was in cp.stderr the whole time.
            if cp.returncode != 0:
                tail = (cp.stderr or "").strip().splitlines()[-3:]
                logger.error("typing FAILED for %s (rc=%d): %s", log_id,
                             cp.returncode, " | ".join(tail) or "<no stderr>")
            else:
                logger.warning("typing no-zone for %s (zone=%s rc=0) — stays pending",
                               log_id, zone)
            return
        who = m_who.group(1) if m_who and m_who.group(1) != "None" else None
        rec = {"zone": zone, "points": _POINTS[zone], "who": who,
               "angle": angle, "typed_at": _utcnow_iso(),
               "proc_s": float(m_proc.group(1)) if m_proc else None,
               # Carried onto the annotation card by plays_sync: what the chain
               # decided, and how much it is worth.
               "confidence": confidence, "zone_source": zone_source,
               "pose_degenerate": degenerate}
        # WHO scan (eval-validated 2026-09-08: ~80% correct-when-spoken, every
        # game >=75%): seed from this pass's release feet, track that one
        # player +/-2.5s in the same clip, jersey-vote, speak only on a
        # dominant vote. Runs on the already-cut clip — no extra I/O.
        m_feet = re.search(r"feet_px=\((\d+), (\d+)\)", cp.stdout)
        # Seed the scan at the frame those feet were MEASURED at, not at
        # rim-1.0s. The two instants differ by up to ~1.8s, which is enough for
        # the nearest-box seed to land on a neighbour and track him instead.
        m_seed = re.search(r"feet_s=([\d.]+)", cp.stdout)
        if (os.getenv("SHOT_LIVE_WHO_SCAN", "false").strip().lower()
                in ("1", "true", "yes", "on") and m_feet):
            try:
                # OFF by default. Two-game measurement (2026-09-17):
                #   cb9e1294  +4 / -0  coverage 49.0->56.9%, precision 82.8%
                #   7cef734e  +2 / -3  coverage 50.0->48.0%, precision 75.0%
                # Pooled +6/-3, but it LOWERS precision on the game it was not
                # developed against, and this pipeline is precision-first. The
                # residual failure is seed-box QUALITY, not identity: at the
                # takeoff frame the detector often returns a PARTIAL box (418.5:
                # 108px wide vs 167px a second later, both the correct player),
                # and the IoU chain breaks on the next step. Revisit with that
                # fixed. SHOT_WHO_SEED_ALIGN=1 to enable.
                scan_env = dict(env)
                if m_seed and os.getenv("SHOT_WHO_SEED_ALIGN", "").strip().lower() \
                        in ("1", "true", "yes", "on"):
                    scan_env["SHOT_WHO_SEED_S"] = m_seed.group(1)
                sp = subprocess.run(
                    ["python3", "who_scan_live.py", clip, "-",
                     f"{item['pre']:.2f}", m_feet.group(1), m_feet.group(2),
                     log_id],
                    cwd=TYPING_CWD, env=scan_env, capture_output=True, text=True,
                    timeout=int(os.getenv("SHOT_WHO_SCAN_TIMEOUT_S", "150")))
                m_scan = re.search(r"WHO_SCAN=#(\w+) conf=([\d.]+)", sp.stdout)
                if m_scan and m_scan.group(1) != "None":
                    rec["who_scan"] = {"number": m_scan.group(1),
                                       "conf": float(m_scan.group(2))}
                    rec["who"] = m_scan.group(1)
            except Exception as e:  # noqa: BLE001 — scan never blocks typing
                logger.warning("who-scan failed for %s: %s", log_id, e)
        try:
            self.fb.db.collection("basketball-games").document(item["game_id"]).set(
                {"cv_points": {log_id: rec}}, merge=True)
            logger.info("typing DONE %s -> %s (%dpt, who=%s, %.0fs)", log_id,
                        zone, _POINTS[zone], who, rec["proc_s"] or -1)
        except Exception as e:  # noqa: BLE001
            logger.warning("typing write failed for %s: %s", log_id, e)


_TYPER: Optional[LiveTyper] = None
_TYPER_LOCK = threading.Lock()


def get_typer(fb) -> LiveTyper:
    global _TYPER
    with _TYPER_LOCK:
        if _TYPER is None:
            _TYPER = LiveTyper(fb)
        return _TYPER

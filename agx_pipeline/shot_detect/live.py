"""Real-time shot detection on the LIVE SL/SR segments (docs/REALTIME_SCOREBOARD_VISION.md).

Where Phase 1 QA validates the scorekeeper's makes *after* the game and Phase 2
auto-detect scans the whole master *after* ingest, this reads the closed
`splitmuxsink` segments produced *while recording* (SHOT_SEGMENT_ENABLED) and
finds makes within ~a segment of them happening — the basis for a live-updating
scoreboard.

Shadow-first (mirrors the P1.2 env-gated shadow node): with SHOT_LIVE_ENABLED on
it only records detections to a shadow field `basketball-games/{game}.shot_live`
— it does NOT touch the score. Wiring the verdict to the real scoreboard is
Phase C (SHOT_AUTOSCORE_ENABLED, still a marked seam here) and only after the
shadow is proven on a test game.

GPU discipline: the recording (NVENC) and highlight transcode always win. The
loop processes only short (~4s) closed segments, empties the CUDA cache between
them, and pauses entirely while an ingest transcode is in flight (`should_pause`).
It NEVER blocks on "a game is recording" — running during the game is the point.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

from logging_service import get_logger

from agx_pipeline.shot_recording import SHOT_SEGMENT_SEC, shot_seg_dir

logger = get_logger("agx.shot_live")

_SEG_RE = re.compile(r"^seg_(\d{5,})_([A-Z]{2})\.mp4$")
_HOOP_SIDE = {"SL": "left", "SR": "right"}      # near-rim cam -> which hoop it films

POLL_SEC = float(os.getenv("SHOT_LIVE_POLL_SEC", "2"))
DEDUP_SEC = float(os.getenv("SHOT_LIVE_DEDUP_SEC", "2.5"))   # same-side makes within this window = one shot
SHADOW_CAP = int(os.getenv("SHOT_LIVE_SHADOW_CAP", "500"))   # max shots kept in the shadow doc


def live_enabled() -> bool:
    return os.getenv("SHOT_LIVE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def autoscore_enabled() -> bool:
    return os.getenv("SHOT_AUTOSCORE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LiveShotScorer:
    """Per-game live reader over the SL/SR segments. start()/stop() mirror the
    HighlightBuffer so service.py drives it exactly alongside the recorders."""

    def __init__(self, cfg, fb, should_pause: Optional[Callable[[], bool]] = None):
        self.cfg = cfg
        self.fb = fb
        self._should_pause = should_pause or (lambda: False)
        self._lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._label: Optional[str] = None

    # ---- lifecycle (mirrors HighlightBuffer) ------------------------------- #
    def start(self, label: str, game_id: str, starting_side: Optional[str] = None) -> None:
        if not live_enabled():
            return
        with self._lock:
            self._stop_prev_locked()
            self._label = label
            self._stop_evt = threading.Event()
            self._thread = threading.Thread(
                target=self._run, name="shot-live",
                args=(label, game_id, starting_side, self._stop_evt), daemon=True)
            self._thread.start()
        logger.info("shot-live started label=%s game=%s autoscore=%s",
                    label, game_id, autoscore_enabled())

    def stop(self) -> None:
        with self._lock:
            self._stop_prev_locked()
            self._label = None

    def _stop_prev_locked(self) -> None:
        self._stop_evt.set()
        t = self._thread
        self._thread = None
        if t and t.is_alive():
            t.join(timeout=2)   # best-effort; daemon dies with the process anyway

    def status(self) -> Dict:
        with self._lock:
            alive = self._thread is not None and self._thread.is_alive()
            return {"enabled": live_enabled(), "autoscore": autoscore_enabled(),
                    "label": self._label if alive else None, "alive": alive}

    # ---- worker ------------------------------------------------------------ #
    def _run(self, label: str, game_id: str, starting_side: Optional[str],
             stop_evt: threading.Event) -> None:
        try:
            from agx_pipeline.shot_detect.backtest import scan
            from agx_pipeline.shot_detect.node import _VALIDATOR
        except Exception as e:  # noqa: BLE001 — runtime missing => stay dark
            logger.warning("shot-live runtime unavailable (%s) — not starting", e)
            return
        seg_dir = shot_seg_dir(self.cfg.output_dir, label)
        sidecar = self._read_sidecar(label)
        fps = float((sidecar or {}).get("fps_lock") or 119.9)
        imgsz = int(os.getenv("SHOT_DET_IMGSZ", "640"))
        stride = int(os.getenv("SHOT_LIVE_STRIDE", os.getenv("SHOT_AUTO_STRIDE", "4")))
        spawned = {c.get("angle"): c.get("spawned_at")
                   for c in (sidecar or {}).get("cameras", [])}

        detector = None
        rims: Dict = {}
        rim_cache: Dict[str, Optional[Dict]] = {}
        processed: set = set()
        scored: List[Dict] = []         # {wallclock_epoch, side} — dedup ledger
        shadow: List[Dict] = []         # detections for the shadow field
        n_seg = 0
        t0 = time.time()
        self._write_shadow(game_id, shadow, n_seg, t0, status="running")

        while not stop_evt.wait(POLL_SEC):
            if self._should_pause():
                continue                # yield the GPU to an ingest transcode
            try:
                closed = self._closed_segments(seg_dir, processed)
                if not closed:
                    continue
                if detector is None:
                    detector, rims = _VALIDATOR.get()   # lazy load once (2.5s)
                for idx, angle, path in closed:
                    if stop_evt.is_set() or self._should_pause():
                        break
                    processed.add(os.path.basename(path))
                    n_seg += 1
                    self._process_segment(scan, detector, rims, rim_cache, path,
                                          idx, angle, fps, imgsz, stride,
                                          spawned.get(angle), starting_side,
                                          scored, shadow, game_id)
                    if hasattr(detector, "empty_cache"):
                        detector.empty_cache()
                self._write_shadow(game_id, shadow, n_seg, t0, status="running")
            except Exception as e:  # noqa: BLE001 — a bad segment must not kill the loop
                logger.warning("shot-live segment pass failed: %s", e)

        self._write_shadow(game_id, shadow, n_seg, t0, status="stopped")
        logger.info("shot-live stopped game=%s segments=%d shots=%d",
                    game_id, n_seg, len(shadow))

    # ---- per-segment ------------------------------------------------------- #
    def _process_segment(self, scan, detector, rims, rim_cache, path, idx, angle,
                         fps, imgsz, stride, spawned_iso, starting_side,
                         scored, shadow, game_id) -> None:
        if angle not in rim_cache:
            rim_cache[angle] = (
                scan.estimate_rim(detector.model, path, detector.device,
                                  fps=fps, imgsz=imgsz)
                or (rims or {}).get(angle))
        rim = rim_cache[angle]
        if rim is None:
            return
        found = scan.scan_shots(detector.model, path, detector.device, fps, rim,
                                stride=stride, imgsz=imgsz, progress_every=0)
        # Each segment's footage starts ~idx*SHOT_SEGMENT_SEC after the camera
        # spawned; t_shot is seconds into THIS segment. (±~1.5s spawn/negotiation
        # slack — fine for a ~15-25s-latency live scoreboard.)
        seg_base = idx * SHOT_SEGMENT_SEC
        side = _HOOP_SIDE.get(angle)
        for s in found:
            wc_iso = self._wallclock(spawned_iso, seg_base + s["t_shot"])
            wc_epoch = self._epoch(wc_iso)
            if self._is_dup(scored, wc_epoch, side):
                continue
            scored.append({"wc": wc_epoch, "side": side})
            rec = {"cam": angle, "side": side, "seg": idx,
                   "t_shot": s["t_shot"], "made": s["made"], "verdict": s["verdict"],
                   "rho": s.get("rho"), "wallclock": wc_iso,
                   "detected_at": _utcnow_iso()}
            shadow.append(rec)
            logger.info("shot-live %s %s seg=%d t=%.2f -> %s", game_id, angle, idx,
                        s["t_shot"], s["verdict"])
            if s["made"] and autoscore_enabled():
                self._maybe_autoscore(game_id, rec, starting_side)

    # ---- Phase C seam (NOT yet wired to the visible scoreboard) ------------- #
    def _maybe_autoscore(self, game_id: str, shot: Dict, starting_side: Optional[str]) -> None:
        """Phase C: turn a CV make into +2 on the live scoreboard. Deliberately a
        no-op write for now — it needs team attribution (side+period -> team) and
        a race-free score channel (cv_events / cv_score, NEVER logs[]) plus the wb
        frontend merge, all to be added after the shadow is proven on a test game.
        Logs intent so we can see what it *would* score."""
        logger.info("shot-live AUTOSCORE (pending Phase C): would +2 side=%s game=%s wc=%s",
                    shot.get("side"), game_id, shot.get("wallclock"))

    # ---- helpers ----------------------------------------------------------- #
    def _closed_segments(self, seg_dir: str, processed: set) -> List:
        """Finalized segments not yet processed, oldest first. A segment is closed
        once a higher-index segment for the SAME angle exists (splitmuxsink writes
        the moov when it rolls to the next file)."""
        by_angle: Dict[str, List] = {}
        try:
            for fn in os.listdir(seg_dir):
                m = _SEG_RE.match(fn)
                if m:
                    by_angle.setdefault(m.group(2), []).append(
                        (int(m.group(1)), m.group(2), os.path.join(seg_dir, fn)))
        except OSError:
            return []
        out = []
        for segs in by_angle.values():
            segs.sort()
            for s in segs[:-1]:     # all but the currently-open (highest index)
                if os.path.basename(s[2]) not in processed:
                    out.append(s)
        out.sort()
        return out

    @staticmethod
    def _is_dup(scored: List[Dict], wc_epoch: Optional[float], side: Optional[str]) -> bool:
        if wc_epoch is None:
            return False
        return any(s["side"] == side and s["wc"] is not None
                   and abs(s["wc"] - wc_epoch) <= DEDUP_SEC for s in scored)

    @staticmethod
    def _wallclock(spawned_iso: Optional[str], t: float) -> Optional[str]:
        try:
            return (datetime.fromisoformat(spawned_iso) + timedelta(seconds=float(t))).isoformat()
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _epoch(iso: Optional[str]) -> Optional[float]:
        try:
            return datetime.fromisoformat(iso).timestamp()
        except Exception:  # noqa: BLE001
            return None

    def _read_sidecar(self, label: str) -> Dict:
        path = os.path.join(self.cfg.output_dir, label, f"{label}_shot_timing.json")
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return {}

    def _write_shadow(self, game_id: str, shadow: List[Dict], n_seg: int,
                      t0: float, status: str) -> None:
        if not self.fb:
            return
        n_make = sum(1 for s in shadow if s["made"])
        doc = {
            "status": status, "autoscore": autoscore_enabled(),
            "n_segments": n_seg, "n_shots": len(shadow), "n_make": n_make,
            "n_miss": len(shadow) - n_make,
            "n_sl": sum(1 for s in shadow if s["cam"] == "SL"),
            "n_sr": sum(1 for s in shadow if s["cam"] == "SR"),
            "shots": shadow[-SHADOW_CAP:], "secs": round(time.time() - t0, 1),
            "updated_at": _utcnow_iso(),
        }
        try:
            # set(merge=True), not update(): create-or-merge so a not-yet-created
            # game doc never throws NOT_FOUND; only the shot_live key is replaced.
            self.fb.db.collection("basketball-games").document(game_id).set(
                {"shot_live": doc}, merge=True)
        except Exception as e:  # noqa: BLE001
            logger.warning("shot-live shadow write failed (%s): %s", game_id, e)

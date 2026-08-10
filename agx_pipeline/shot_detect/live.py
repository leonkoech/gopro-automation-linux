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
import shutil
import subprocess
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
# Recall fixes (raise live recall from the fragmented-segment baseline):
#  - WINDOW: scan [prev_segment + current] so a shot straddling a 4s boundary is
#    whole in one window (dedup collapses the overlap). Off => per-segment only.
#  - RIM accumulation: grow hoop samples across segments and take a running median
#    (whole-game-quality rim) instead of trusting one 4s segment's estimate;
#    canonical rims.json is the fallback until MIN samples are seen.
SHOT_LIVE_WINDOW = os.getenv("SHOT_LIVE_WINDOW", "true").strip().lower() in ("1", "true", "yes", "on")
RIM_MIN_SAMPLES = int(os.getenv("SHOT_LIVE_RIM_MIN", "8"))
HOOP_ACC_CAP = int(os.getenv("SHOT_LIVE_HOOP_CAP", "5000"))


def live_enabled() -> bool:
    return os.getenv("SHOT_LIVE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def autoscore_enabled() -> bool:
    return os.getenv("SHOT_AUTOSCORE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rm(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


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
        hoop_acc: Dict[str, Dict] = {}   # angle -> {cx,cy,w,h} accumulated hoop samples (rim)
        prev_seg: Dict[str, tuple] = {}  # angle -> (idx, path) kept for the sliding window
        processed: set = set()
        scored: List[Dict] = []          # {wallclock_epoch, side} — dedup ledger
        shadow: List[Dict] = []          # detections for the shadow field
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
                    self._process_window(scan, detector, rims, path, idx, angle,
                                         fps, imgsz, stride, spawned.get(angle),
                                         starting_side, scored, shadow, game_id,
                                         prev_seg, hoop_acc)
                    if hasattr(detector, "empty_cache"):
                        detector.empty_cache()
                    # publish after EACH segment so a fresh make reaches the shadow
                    # (and, in Phase C, the scoreboard) within a segment of the
                    # shot — not in a burst after a whole backlog drains.
                    self._write_shadow(game_id, shadow, n_seg, t0, status="running")
            except Exception as e:  # noqa: BLE001 — a bad segment must not kill the loop
                logger.warning("shot-live segment pass failed: %s", e)

        self._write_shadow(game_id, shadow, n_seg, t0, status="stopped")
        shutil.rmtree(seg_dir, ignore_errors=True)   # segments are ephemeral; master is durable
        logger.info("shot-live stopped game=%s segments=%d shots=%d",
                    game_id, n_seg, len(shadow))

    # ---- per-window (recall fixes: sliding window + rim accumulation) ------- #
    def _process_window(self, scan, detector, rims, path, idx, angle, fps, imgsz,
                        stride, spawned_iso, starting_side, scored, shadow, game_id,
                        prev_seg, hoop_acc) -> None:
        from agx_pipeline.shot_detect import logic
        # 1. Build the scan window. With SHOT_LIVE_WINDOW, prepend the previous
        #    segment so a shot straddling the 4s boundary is whole in one window;
        #    else scan just this segment. `base_idx` = footage-offset of the
        #    window's first segment.
        prev = prev_seg.get(angle)
        window, base_idx, tmp = path, idx, None
        if SHOT_LIVE_WINDOW and prev and os.path.exists(prev[1]):
            tmp = self._concat(prev[1], path)
            if tmp:
                window, base_idx = tmp, prev[0]
        # 2. One pass: coarse ball track + hoop samples.
        try:
            track, hoops = scan.scan_ball_and_hoops(
                detector.model, window, detector.device, stride=stride, imgsz=imgsz)
        finally:
            if tmp:
                _rm(tmp)
        # 3. Accumulate hoop samples -> running-median rim (whole-game quality),
        #    canonical rims.json until we have enough.
        acc = hoop_acc.setdefault(angle, {"cx": [], "cy": [], "w": [], "h": []})
        for (cx, cy, w, h) in hoops:
            acc["cx"].append(cx); acc["cy"].append(cy); acc["w"].append(w); acc["h"].append(h)
        if len(acc["cx"]) > HOOP_ACC_CAP:
            for k in acc:
                acc[k] = acc[k][-HOOP_ACC_CAP:]
        rim = None
        if len(acc["cx"]) >= RIM_MIN_SAMPLES:
            rim = scan.rim_from_hoops(acc["cx"], acc["cy"], acc["w"], acc["h"])
        if rim is None:
            rim = (rims or {}).get(angle)
        if rim is None:
            self._advance_prev(prev_seg, angle, idx, path)
            return
        # 4. Decide crossings on the (windowed) track; map window-relative t to
        #    footage seconds via the window's first-segment offset.
        G = logic.Geo.from_rim(rim, float(fps))
        window_base = base_idx * SHOT_SEGMENT_SEC
        side = _HOOP_SIDE.get(angle)
        for v in logic.decide(G, track):
            if "verdict" not in v:
                continue
            t_shot = float(v.get("t", 0.0))
            wc_iso = self._wallclock(spawned_iso, window_base + t_shot)
            wc_epoch = self._epoch(wc_iso)
            if self._is_dup(scored, wc_epoch, side):
                continue   # overlap between consecutive windows -> one shot
            scored.append({"wc": wc_epoch, "side": side})
            rec = {"cam": angle, "side": side, "seg": base_idx,
                   "t_shot": round(t_shot, 3), "made": v["verdict"] == "MAKE",
                   "verdict": v["verdict"], "rho": v.get("rho"),
                   "wallclock": wc_iso, "detected_at": _utcnow_iso()}
            shadow.append(rec)
            logger.info("shot-live %s %s win@%d t=%.2f -> %s", game_id, angle,
                        base_idx, t_shot, v["verdict"])
            if rec["made"] and autoscore_enabled():
                self._maybe_autoscore(game_id, rec, starting_side)
        self._advance_prev(prev_seg, angle, idx, path)

    def _concat(self, a: str, b: str) -> Optional[str]:
        """Stream-copy concat [a, b] -> temp mp4 for one scan window. None on error
        (caller falls back to scanning the current segment alone)."""
        tmp = a[:-4] + "_win.mp4"
        lst = a + ".txt"
        try:
            with open(lst, "w") as f:
                f.write("file '%s'\nfile '%s'\n" % (a, b))
            subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y",
                            "-f", "concat", "-safe", "0", "-i", lst, "-c", "copy",
                            "-map", "0:v", tmp], check=True, stdin=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return tmp
        except Exception:  # noqa: BLE001
            _rm(tmp)
            return None
        finally:
            _rm(lst)

    @staticmethod
    def _advance_prev(prev_seg: Dict, angle: str, idx: int, path: str) -> None:
        """Delete the segment that just aged out of the window (fully consumed —
        it led one window; the master is the durable copy) and keep the current
        one as the next window's lead."""
        old = prev_seg.get(angle)
        if old and old[1] != path:
            _rm(old[1])
        prev_seg[angle] = (idx, path)

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

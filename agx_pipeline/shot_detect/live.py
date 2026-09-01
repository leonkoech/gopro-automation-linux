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


def _seg_real_time(path: str, prev: Optional[tuple]) -> tuple:
    """Real wall-clock anchor for a shot segment: (close_epoch, real_seconds).

    The segment index cannot be trusted to carry wall-clock, because segments are
    split on media time at a nominal fps the cameras do not actually deliver. The
    files themselves do carry it: each one's mtime is the instant it was closed,
    and the gap between consecutive closes is that segment's true duration.

    Returns (None, None) when there is no usable previous segment to measure
    against, or when the measured gap is implausible -- a stalled or restarted
    recorder can leave mtimes that would place a shot wildly wrong, and a wrong
    anchor is worse than the nominal one it replaces.
    """
    try:
        close = os.path.getmtime(path)
    except OSError:
        return None, None
    if not (prev and prev[1] and os.path.exists(prev[1])):
        return close, None
    try:
        gap = close - os.path.getmtime(prev[1])
    except OSError:
        return close, None
    # A real segment sits near its nominal size, stretched by however far the
    # capture rate falls short. Half to triple covers every rate we have seen and
    # still rejects the multi-second gaps a stall leaves behind.
    if not (0.5 * SHOT_SEGMENT_SEC <= gap <= 3.0 * SHOT_SEGMENT_SEC):
        return close, None
    return close, gap

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
# Freshness guard: skip (never scan) closed segments older than this — a verdict
# that old can't cut a clip anyway (the highlight buffer holds ~10 min), and
# scanning a stale backlog keeps the loop stale forever. Night 1: a 54-min
# starvation produced an hour-deep backlog the loop dutifully chewed through,
# staying ~45 min behind all game. Fresh-first beats complete-but-late live;
# the nightly typing still covers the skipped stretch from the masters.
MAX_AGE_S = float(os.getenv("SHOT_LIVE_MAX_AGE_S", "480"))


def live_enabled() -> bool:
    return os.getenv("SHOT_LIVE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def autohighlight_enabled() -> bool:
    """CV make -> auto highlight clip (same pipeline as scorekeeper clips).
    Default OFF; flip SHOT_AUTO_HIGHLIGHT=true on the box when ready."""
    return os.getenv("SHOT_AUTO_HIGHLIGHT", "false").lower() in ("1", "true", "yes", "on")


# Clip window around the trigger, which is the ball AT THE RIM -- not the release.
# A make and a miss want different framing: on a make the interesting part is the
# build-up and the ball going in, so the clip runs long before and keeps a short
# tail; on a miss the rebound matters as much as the shot, so it runs longer
# after. Reviewed on real clips 2026-08-24 and lengthened by 1s on both tails.
def clip_window(made: bool) -> tuple:
    """(pre_s, post_s) for a CV-triggered cut. Rim-anchored.

    The live path only cuts makes, so the miss branch is unreachable from there.
    It is kept because offline re-cuts (a backfill, a review reel) still ask for
    a miss window, and because the two windows differ for a reason: on a make the
    interest is the build-up and the ball dropping, on a miss it is the rebound.
    """
    if made:
        return (float(os.getenv("CV_CLIP_PRE_MAKE_S", "5")),
                float(os.getenv("CV_CLIP_POST_MAKE_S", "2")))
    return (float(os.getenv("CV_CLIP_PRE_MISS_S", "3")),
            float(os.getenv("CV_CLIP_POST_MISS_S", "4")))


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

    def __init__(self, cfg, fb, should_pause: Optional[Callable[[], bool]] = None,
                 on_make: Optional[Callable[[Dict], object]] = None):
        self.cfg = cfg
        self.fb = fb
        self._should_pause = should_pause or (lambda: False)
        # Called with a highlight-cut cmd for every CV MAKE (service wires this to
        # the same handler the scorekeeper's relay command uses).
        self._on_make = on_make
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
        backlog = {"now": 0, "max": 0}   # unprocessed-closed-segments queue depth
        t0 = time.time()
        self._write_shadow(game_id, shadow, n_seg, t0, status="running", backlog=backlog)

        pause_since: Optional[float] = None
        while not stop_evt.wait(POLL_SEC):
            if self._should_pause():
                # visible pause: night 1's 54-min silent pause hid the outage
                if pause_since is None:
                    pause_since = time.time()
                elif time.time() - pause_since > 60:
                    logger.warning("shot-live paused %ds (ingest transcode)",
                                   int(time.time() - pause_since))
                    pause_since = time.time() - 0.001  # re-log every ~60s
                continue                # yield the GPU to an ingest transcode
            pause_since = None
            try:
                closed = self._closed_segments(seg_dir, processed)
                if not closed:
                    backlog["now"] = 0
                    continue
                backlog["now"] = len(closed)
                backlog["max"] = max(backlog["max"], len(closed))
                if len(closed) > 2:     # falling behind real time — visible in logs
                    logger.warning("shot-live backlog=%d segments", len(closed))
                if detector is None:
                    detector, rims = _VALIDATOR.get()   # lazy load once (2.5s)
                n_stale = 0
                for idx, angle, path in closed:
                    if stop_evt.is_set() or self._should_pause():
                        break
                    processed.add(os.path.basename(path))
                    # Freshness guard: drop segments too old to matter live
                    # (mtime = splitmuxsink close time). See MAX_AGE_S above.
                    try:
                        stale = time.time() - os.path.getmtime(path) > MAX_AGE_S
                    except OSError:
                        stale = True
                    if stale:
                        n_stale += 1
                        self._advance_prev(prev_seg, angle, idx, path)
                        continue
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
                    self._write_shadow(game_id, shadow, n_seg, t0, status="running",
                                       backlog=backlog)
                if n_stale:
                    logger.warning("shot-live skipped %d stale segments (>%.0fs old)",
                                   n_stale, MAX_AGE_S)
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
        t_sc = time.time()
        try:
            track, hoops = scan.scan_ball_and_hoops(
                detector.model, window, detector.device, stride=stride, imgsz=imgsz)
        finally:
            if tmp:
                _rm(tmp)
        scan_s = round(time.time() - t_sc, 2)
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
        # splitmuxsink cuts on MEDIA time -- SHOT_SEGMENT_SEC worth of frames at
        # the NOMINAL fps lock. The FLIRs deliver fewer frames than that lock
        # (measured 105.6 and 106.0 fps on 2026-08-24 against a lock of 120), so
        # a "4s" segment really spans ~4.54s of wall-clock. Deriving the shot's
        # wall-clock as spawn + base_idx*SHOT_SEGMENT_SEC therefore loses a fixed
        # PERCENTAGE of elapsed time -- 13% on those games, which is four minutes
        # of error by the end of a game and cuts the clip nowhere near the shot.
        # Anchoring on the segment file's own close time fixes it without any fps
        # constant, and keeps working when the cameras slow further under
        # different lighting (exposure drives their real frame rate).
        seg_close, seg_real = _seg_real_time(path, prev)
        n_segs = (idx - base_idx) + 1
        side = _HOOP_SIDE.get(angle)
        for v in logic.decide(G, track):
            if "verdict" not in v:
                continue
            t_shot = float(v.get("t", 0.0))
            if seg_close is not None and seg_real is not None:
                # t_shot is media seconds inside a nominal window; stretch it by
                # the same ratio the segments themselves are stretched.
                win_start = seg_close - n_segs * seg_real
                wc_epoch = win_start + t_shot * (seg_real / SHOT_SEGMENT_SEC)
                wc_iso = datetime.fromtimestamp(wc_epoch, timezone.utc).isoformat()
            else:
                # mtimes unusable (first segment of a run, or a filesystem that
                # lost them) -- fall back to the nominal math rather than drop
                # the shot, and let the drift show up in latency_s as before.
                wc_iso = self._wallclock(spawned_iso, window_base + t_shot)
                wc_epoch = self._epoch(wc_iso)
            if self._is_dup(scored, wc_epoch, side):
                continue   # overlap between consecutive windows -> one shot
            scored.append({"wc": wc_epoch, "side": side})
            # latency_s: shot happened (wallclock) -> verdict now. THE number the
            # green-button target is tuned on; scan_s isolates the decode+infer
            # share of it per window.
            rec = {"cam": angle, "side": side, "seg": base_idx,
                   "t_shot": round(t_shot, 3), "made": v["verdict"] == "MAKE",
                   "verdict": v["verdict"], "rho": v.get("rho"),
                   "wallclock": wc_iso, "detected_at": _utcnow_iso(),
                   "latency_s": (round(time.time() - wc_epoch, 1)
                                 if wc_epoch else None),
                   "scan_s": scan_s}
            shadow.append(rec)
            logger.info("shot-live %s %s win@%d t=%.2f -> %s (latency=%ss scan=%ss)",
                        game_id, angle, base_idx, t_shot, v["verdict"],
                        rec["latency_s"], scan_s)
            if rec["made"] and autoscore_enabled():
                self._maybe_autoscore(game_id, rec, starting_side)
            # MAKES ONLY. A reel of missed shots is not something anyone wants to
            # watch, and cutting them roughly doubled clip volume -- misses
            # outnumber makes -- for no viewer value, while doubling the work the
            # live loop has to finish before the next segment lands.
            #
            # Misses are NOT discarded: the record above (with its verdict and
            # time) is what plays_sync turns into FG_MISS annotation cards. A
            # miss needs a timestamp, not a clip.
            if rec["made"] and autohighlight_enabled():
                self._maybe_highlight(game_id, rec)
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
    def _maybe_highlight(self, game_id: str, shot: Dict) -> None:
        """Cut a highlight clip for a CV-detected shot — the SAME pipeline as a
        scorekeeper score row (buffer -> transcode -> S3 -> highlights.{id} on the
        game doc), keyed `cv_<epoch>_<side>` so the frontend's CV timeline rows can
        grow the green play button. Best-effort: a failed cut never affects
        detection. Side comes straight from the shot cam (SL=left / SR=right)."""
        if self._on_make is None:
            return
        wc = shot.get("wallclock")
        side = shot.get("side")
        if not wc or side not in ("left", "right"):
            logger.info("shot-live highlight skipped (wc=%s side=%s)", wc, side)
            return
        epoch = self._epoch(wc)
        pre_s, post_s = clip_window(bool(shot.get("made")))
        cmd = {"logId": f"cv_{int(epoch)}_{side}" if epoch else f"cv_{wc}_{side}",
               "ts": wc, "side": side, "firebase_game_id": game_id,
               "pre": pre_s, "post": post_s}
        try:
            resp = self._on_make(cmd)
            # detect_latency + this log's timestamp vs the highlight doc's ready
            # time = the full basket->green-button chain, measured per make.
            logger.info("shot-live AUTO-HIGHLIGHT %s %s pre=%.1f post=%.1f (detect_latency=%ss) -> %s",
                        cmd["logId"], shot.get("verdict"), pre_s, post_s,
                        shot.get("latency_s"), resp)
        except Exception as e:  # noqa: BLE001 — never break the detector
            logger.warning("shot-live auto-highlight failed for %s: %s", cmd["logId"], e)

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
            # The sidecar's spawned_at ends in "Z"; py3.10 fromisoformat can't
            # parse that suffix. Without this, wallclock is None for EVERY shot
            # and _maybe_highlight silently never fires (the 2026-08-12 games'
            # shadow docs all show wallclock: null).
            return (datetime.fromisoformat(spawned_iso.replace("Z", "+00:00"))
                    + timedelta(seconds=float(t))).isoformat()
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
                      t0: float, status: str,
                      backlog: Optional[Dict] = None) -> None:
        if not self.fb:
            return
        n_make = sum(1 for s in shadow if s["made"])
        doc = {
            "status": status, "autoscore": autoscore_enabled(),
            "n_segments": n_seg, "n_shots": len(shadow), "n_make": n_make,
            "n_miss": len(shadow) - n_make,
            "n_sl": sum(1 for s in shadow if s["cam"] == "SL"),
            "n_sr": sum(1 for s in shadow if s["cam"] == "SR"),
            # per-hoop-side make counts: the CV auto-scorecard's source numbers
            # (team attribution + halftime flip happen in the frontend, which
            # owns the period state; this stays a pure count).
            "n_make_left": sum(1 for s in shadow
                               if s["made"] and s.get("side") == "left"),
            "n_make_right": sum(1 for s in shadow
                                if s["made"] and s.get("side") == "right"),
            "backlog": (backlog or {}).get("now", 0),
            "max_backlog": (backlog or {}).get("max", 0),
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

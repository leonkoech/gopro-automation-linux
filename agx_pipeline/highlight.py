"""
Highlight-clip sidecar (Stage 1 of docs/HIGHLIGHT_CLIP_INTEGRATION_PLAN.md).

Two halves:

- HighlightRecorder — a host-side ffmpeg pulling ONE watchable tracking angle
  (FL, falling back to FR on stall) into short stream-copy MP4 segments,
  alongside the main per-camera 4K docker recorders. The raw masters are
  unreadable mid-game (plain mp4mux writes moov only at EOS), so these closed
  segments are the only live-cuttable source. Segment filenames carry the
  segment-start epoch (ffmpeg -strftime %s) — that is the wall-clock → footage
  bridge for cuts, immune to the started_at preroll slack.

- cut_highlight() — run on a daemon thread per `highlight_clip` command: waits
  for the segments covering [T-pre, T+post] to close, concat+trims them,
  transcodes to 1080p H.264 (ingest's HW NVDEC→NVENC path, SW fallback),
  uploads to S3 and writes a presigned URL to
  basketball-games/{game}.highlights.{logId} (dot-path update — never logs[],
  which the frontend rewrites whole in transactions).

Both halves are best-effort and must never disturb the main recording.
Clip artifacts live OUTSIDE recordings/{label}/ — ingest rmtree's that dir
after upload (see plan §11.4).
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("agx.highlight")

GAMES = "basketball-games"

SEG_SEC = int(os.getenv("HIGHLIGHT_SEGMENT_SEC", "4"))
BUFFER_MIN = int(os.getenv("HIGHLIGHT_BUFFER_MIN", "10"))       # ring size (janitor)
PRE_SEC = float(os.getenv("HIGHLIGHT_PRE_SEC", "5"))            # window before T (5s pre-roll, per 2026-08-03 sync)
POST_SEC = float(os.getenv("HIGHLIGHT_POST_SEC", "3"))          # window after T
STALL_SEC = float(os.getenv("HIGHLIGHT_STALL_SEC", "25"))       # no fresh segment -> rotate angle
ANGLES = [a.strip() for a in os.getenv("HIGHLIGHT_ANGLES", "FL,FR").split(",") if a.strip()]
# Side-aware buffering (issue #4): a left- and a right-hoop buffer so a clip can
# be cut from the camera that filmed the scoring team's basket. Each side fails
# over within itself (FL→NL, FR→NR) so a stall never crosses to the wrong hoop.
LEFT_ANGLES = [a.strip() for a in os.getenv("HIGHLIGHT_LEFT_ANGLES", "FL,NL").split(",") if a.strip()]
RIGHT_ANGLES = [a.strip() for a in os.getenv("HIGHLIGHT_RIGHT_ANGLES", "FR,NR").split(",") if a.strip()]
S3_BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")
S3_PREFIX = os.getenv("HIGHLIGHT_S3_PREFIX", "highlights")
AWS_REGION = os.getenv("UPLOAD_REGION", "us-east-1")
# CloudFront CDN in front of the S3 bucket — serve clips via the CDN (fast, cached,
# HTTPS, reliable) instead of a raw/presigned S3 URL. Empty => presigned S3 fallback.
CLOUDFRONT_DOMAIN = os.getenv("HIGHLIGHT_CDN_DOMAIN", "d22gul8sdref0l.cloudfront.net")
# SigV4 presigned URLs cap at 7 days.
URL_TTL = min(int(os.getenv("HIGHLIGHT_URL_TTL", str(7 * 24 * 3600))), 604800)

_SEG_RE = re.compile(r"^seg_(\d{10,})_([A-Z]{2})\.mp4$")


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class HighlightRecorder:
    """Segment sidecar for one tracking angle, with FL→FR stall failover.

    One ffmpeg, RTSP video-only stream copy (no decode — negligible CPU),
    closing a seekable MP4 every SEG_SEC seconds. A watchdog restarts it on the
    next preferred angle when segments stop appearing, and ages out segments
    older than BUFFER_MIN so the buffer never grows past a few hundred MB.
    """

    def __init__(self, cfg, angles: Optional[List[str]] = None,
                 side: Optional[str] = None):
        self.cfg = cfg
        self.side = side  # "left"/"right"/None — segregates this buffer's dir
        # Preferred-order camera rotation within this side (e.g. FL then NL).
        self._cams = [c for a in (angles or ANGLES) for c in cfg.cameras if c.angle == a]
        self._lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._cam_idx = 0
        self._label: Optional[str] = None
        self._spawn_ts = 0.0
        self._stop_evt = threading.Event()

    # ---- paths -------------------------------------------------------------
    def buf_dir(self, label: str) -> str:
        # Sibling of recordings/{label} — survives ingest's session rmtree.
        # Per-side subdir so the left/right buffers never conflate segments.
        base = os.path.join(self.cfg.output_dir, "highlight_buf", label)
        return os.path.join(base, self.side) if self.side else base

    # ---- lifecycle ---------------------------------------------------------
    def start(self, label: str) -> None:
        if not self._cams:
            logger.warning("no cameras matching HIGHLIGHT_ANGLES=%s — recorder off", ANGLES)
            return
        with self._lock:
            self._teardown_locked()
            # Wipe only THIS side's buffer; HighlightBuffer clears stale labels.
            shutil.rmtree(self.buf_dir(label), ignore_errors=True)
            self._label = label
            self._cam_idx = 0
            self._stop_evt = threading.Event()
            self._spawn_locked()
            threading.Thread(target=self._watchdog, name="highlight-watchdog",
                             daemon=True).start()
        logger.info("highlight recorder started label=%s angle=%s", label, self.angle())

    def stop(self) -> None:
        with self._lock:
            self._stop_evt.set()
            self._teardown_locked()
            # Segments stay on disk for cuts requested near the final horn;
            # the next start() wipes them.
            self._label = None

    def angle(self) -> Optional[str]:
        return self._cams[self._cam_idx].angle if self._cams else None

    def status(self) -> Dict:
        with self._lock:
            active = self._proc is not None and self._proc.poll() is None
            return {"active": active, "angle": self.angle() if active else None,
                    "label": self._label}

    # ---- internals ---------------------------------------------------------
    def _cmd(self, cam, out_dir: str) -> List[str]:
        url = f"rtsp://{cam.ip}:{self.cfg.rtsp_port}{self.cfg.rtsp_path}"
        # %s in the pattern = segment-start epoch (second resolution) — the
        # timing anchor the cutter maps wall-clock T onto.
        pattern = os.path.join(out_dir, f"seg_%s_{cam.angle}.mp4")
        return ["ffmpeg", "-nostdin", "-loglevel", "error",
                "-rtsp_transport", "tcp", "-i", url,
                "-map", "0:v", "-c", "copy", "-an",
                "-f", "segment", "-segment_time", str(SEG_SEC),
                "-reset_timestamps", "1", "-strftime", "1", pattern]

    def _spawn_locked(self) -> None:
        cam = self._cams[self._cam_idx]
        out_dir = self.buf_dir(self._label)
        os.makedirs(out_dir, exist_ok=True)
        self._spawn_ts = time.time()
        try:
            self._proc = subprocess.Popen(self._cmd(cam, out_dir),
                                          stdin=subprocess.DEVNULL,
                                          stdout=subprocess.DEVNULL,
                                          stderr=subprocess.DEVNULL)
        except OSError as e:
            logger.error("highlight ffmpeg failed to spawn (%s): %s", cam.angle, e)
            self._proc = None

    def _teardown_locked(self) -> None:
        p, self._proc = self._proc, None
        if p is None or p.poll() is not None:
            return
        try:
            p.send_signal(signal.SIGINT)
            p.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                p.kill()
            except OSError:
                pass

    def _watchdog(self) -> None:
        """Restart on the next angle when segments stall; age out old segments."""
        evt = self._stop_evt
        while not evt.wait(5):
            with self._lock:
                if evt.is_set() or not self._label:
                    return
                segs = self.segments()
                # Grace anchor: freshest of newest-segment start and spawn time,
                # so a just-spawned ffmpeg gets STALL_SEC to connect RTSP before
                # the watchdog rotates it to the next angle.
                newest = max([self._spawn_ts] + [s[0] for s in segs[-1:]])
                dead = self._proc is None or self._proc.poll() is not None
                stalled = time.time() - newest > STALL_SEC
                if dead or stalled:
                    nxt = (self._cam_idx + 1) % len(self._cams)
                    logger.warning("highlight recorder %s (%s) — switching to %s",
                                   "died" if dead else "stalled",
                                   self._cams[self._cam_idx].angle,
                                   self._cams[nxt].angle)
                    self._teardown_locked()
                    self._cam_idx = nxt
                    self._spawn_locked()
                # Ring janitor: drop segments older than the buffer horizon.
                cutoff = time.time() - BUFFER_MIN * 60
                for start, _angle, path in segs:
                    if start < cutoff:
                        try:
                            os.unlink(path)
                        except OSError:
                            pass

    def segments(self, label: Optional[str] = None) -> List[Tuple[int, str, str]]:
        """Sorted [(start_epoch, angle, path)] currently in the buffer."""
        lab = label or self._label
        if not lab:
            return []
        out_dir = self.buf_dir(lab)
        found = []
        try:
            for fn in os.listdir(out_dir):
                m = _SEG_RE.match(fn)
                if m:
                    found.append((int(m.group(1)), m.group(2),
                                  os.path.join(out_dir, fn)))
        except OSError:
            return []
        return sorted(found)


class HighlightBuffer:
    """A left- and a right-hoop HighlightRecorder, so a clip can be cut from the
    camera that filmed the scoring team's basket (issue #4). This class just
    keeps both sides buffered; the scoring side is resolved per-cut by the
    caller (team + period + startingSideTeam1) and passed to ``recorder_for``.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.left = HighlightRecorder(cfg, angles=LEFT_ANGLES, side="left")
        self.right = HighlightRecorder(cfg, angles=RIGHT_ANGLES, side="right")

    def start(self, label: str) -> None:
        # Clear all stale buffers (old labels/sides) once, then start both.
        shutil.rmtree(os.path.join(self.cfg.output_dir, "highlight_buf"),
                      ignore_errors=True)
        self.left.start(label)
        self.right.start(label)

    def stop(self) -> None:
        self.left.stop()
        self.right.stop()

    def recorder_for(self, side: Optional[str]) -> HighlightRecorder:
        """The recorder for ``side`` ("left"/"right"; None → left), falling back
        to the other side if the preferred one isn't producing segments — a clip
        from the wrong angle beats no clip."""
        want = self.right if side == "right" else self.left
        if want.status()["active"]:
            return want
        other = self.left if want is self.right else self.right
        if other.status()["active"]:
            logger.warning("highlight %s side inactive — falling back to %s",
                           want.side, other.side)
            return other
        return want

    def active(self) -> bool:
        return self.left.status()["active"] or self.right.status()["active"]

    def status(self) -> Dict:
        return {"active": self.active(),
                "left": self.left.status(), "right": self.right.status()}


# --------------------------------------------------------------------------- #
# Cutter (runs on a daemon thread per highlight_clip command)
# --------------------------------------------------------------------------- #
def _mark(fb, game_id: str, log_id: str, patch: Dict) -> None:
    """Progress writeback: dot-path merge onto the game doc's highlights map."""
    if not fb:
        return
    try:
        fb.db.collection(GAMES).document(game_id).update(
            {f"highlights.{log_id}": {**patch, "updatedAt": _utcnow()}})
    except Exception as e:  # noqa: BLE001
        logger.error("highlight status write failed (%s/%s): %s", game_id, log_id, e)


def _select_segments(segs: List[Tuple[int, str, str]], t0: float, t1: float
                     ) -> List[Tuple[int, str, str]]:
    """Contiguous same-angle run covering [t0, t1] (mixed angles can coexist in
    the buffer after a stall failover; concat across cameras is invalid)."""
    if not segs:
        return []
    anchor = next((s for s in reversed(segs) if s[0] <= t1), segs[0])
    same = [s for s in segs if s[1] == anchor[1]]
    # Segment i covers [start_i, next same-angle start] (last: start + 2×SEG_SEC
    # slack — segments split on the first keyframe after SEG_SEC).
    picked = []
    for i, (start, angle, path) in enumerate(same):
        end = same[i + 1][0] if i + 1 < len(same) else start + 2 * SEG_SEC
        if start < t1 and end > t0:
            picked.append((start, angle, path))
    # Trim to the contiguous run nearest the marked moment (a long RTSP outage
    # between selected segments would otherwise concat a time-jump).
    runs, cur = [], []
    for s in picked:
        if cur and s[0] - cur[-1][0] > 3 * SEG_SEC:
            runs.append(cur)
            cur = []
        cur.append(s)
    if cur:
        runs.append(cur)
    if not runs:
        return []
    # Prefer the run containing the anchor (the segment covering the window
    # end); a longer-but-earlier run would miss the play itself.
    for r in runs:
        if any(p == anchor[2] for _s, _a, p in r):
            return r
    return runs[-1]


def _run(cmd: List[str], timeout: int = 600) -> bool:
    cp = subprocess.run(cmd, capture_output=True, text=True,
                        stdin=subprocess.DEVNULL, timeout=timeout)
    if cp.returncode != 0:
        logger.error("cmd failed (%s): %s", cmd[0], cp.stderr.strip()[-300:])
    return cp.returncode == 0


def cut_highlight(fb, cfg, recorder: HighlightRecorder, req: Dict) -> None:
    """req: {game_id, log_id, ts_epoch, label, pre?, post?}. Never raises."""
    game_id, log_id = req["game_id"], req["log_id"]
    t = float(req["ts_epoch"])
    pre = float(req.get("pre") or PRE_SEC)
    post = float(req.get("post") or POST_SEC)
    label = req.get("label") or recorder.status().get("label")
    t0, t1 = t - pre, t + post
    _mark(fb, game_id, log_id, {"status": "processing",
                                "requestedAt": _utcnow(),
                                "angle": recorder.angle()})
    try:
        # 1. Wait for the covering segments to close: either a segment starting
        #    after the window end exists, or enough wall-clock has passed.
        deadline = t1 + 2 * SEG_SEC + 20
        while time.time() < deadline:
            segs = recorder.segments(label)
            if segs and segs[-1][0] >= t1:
                break
            time.sleep(2)
        segs = recorder.segments(label)
        picked = _select_segments(segs, t0, t1)
        if not picked:
            raise RuntimeError(
                f"window not in highlight buffer ({len(segs)} segments)")

        angle = picked[0][1]
        work = os.path.join(cfg.output_dir, "highlight_clips", label or "unlabeled")
        os.makedirs(work, exist_ok=True)
        merged = os.path.join(work, f"{log_id}_merged.mp4")
        hd = os.path.join(work, f"{log_id}_1080p.mp4")
        final = os.path.join(work, f"{log_id}_{angle}.mp4")

        # 2. Concat the raw H.265 segments (stream copy — instant).
        lst = os.path.join(work, f"{log_id}_list.txt")
        with open(lst, "w") as f:
            f.writelines(f"file '{p}'\n" for _s, _a, p in picked)
        if not _run(["ffmpeg", "-nostdin", "-y", "-f", "concat", "-safe", "0",
                     "-i", lst, "-c", "copy", merged]):
            raise RuntimeError("segment concat failed")

        # 3. 4K H.265 → 1080p H.264, IDR every 30 frames (ingest's HW path with
        #    SW fallback) so the trim below and browser seeking are accurate.
        from agx_pipeline.ingest import _transcode_1080p
        if not _transcode_1080p(merged, hd, cfg):
            raise RuntimeError("transcode failed")

        # 4. Precise trim to [T-pre, T+post] (stream copy, snaps to ≤1s-early IDR).
        offset = max(0.0, t0 - picked[0][0])
        if not _run(["ffmpeg", "-nostdin", "-y", "-ss", f"{offset:.2f}",
                     "-i", hd, "-t", f"{pre + post:.2f}", "-c", "copy",
                     "-movflags", "+faststart", final]):
            raise RuntimeError("trim failed")
        if not (os.path.isfile(final) and os.path.getsize(final) > 0):
            raise RuntimeError("empty clip")

        # 5. Upload + presign.
        import boto3
        date = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
        key = f"{S3_PREFIX}/{date}/{game_id}/{log_id}_{angle}.mp4"
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.upload_file(final, S3_BUCKET, key,
                       ExtraArgs={"ContentType": "video/mp4"})
        url = (f"https://{CLOUDFRONT_DOMAIN}/{key}" if CLOUDFRONT_DOMAIN
               else s3.generate_presigned_url(
                   "get_object", Params={"Bucket": S3_BUCKET, "Key": key},
                   ExpiresIn=URL_TTL))

        _mark(fb, game_id, log_id, {"status": "ready", "url": url,
                                    "s3_key": key, "angle": angle,
                                    "duration": pre + post})
        logger.info("highlight ready %s/%s angle=%s key=%s", game_id, log_id, angle, key)
        for p in (merged, hd, lst):
            try:
                os.unlink(p)
            except OSError:
                pass
    except Exception as e:  # noqa: BLE001
        logger.error("highlight cut failed (%s/%s): %s", game_id, log_id, e)
        _mark(fb, game_id, log_id, {"status": "error", "error": str(e)[:300]})

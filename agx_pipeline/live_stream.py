"""Live HLS publisher — FL and FR to the annotation tool while the game runs.

Lifecycle: a live stream exists **if and only if a recording is in progress**.
`service.py` calls `start()` from `_do_start` and `stop()` from `_do_stop`.
Nothing publishes between games.

Shape (measured on this box 2026-09-24, see scripts/live_poc/PROOF_OF_CONCEPT.md):

  gst: rtspsrc -> nvv4l2decoder -> nvvidconv -> [clockoverlay] -> nvv4l2h264enc
       -> mpegtsmux -> stdout
  ffmpeg: -use_wallclock_as_timestamps 1 -c copy -f hls   (muxing ONLY)

Three things about that shape are not negotiable and each was learned the hard
way:

- **The cameras stream HEVC 4K.** Chrome cannot play HEVC in HLS, so the picture
  must be transcoded; a stream copy is not an option.
- **ffmpeg on this box cannot reach the encoder.** There is no `h264_nvenc`, and
  `h264_v4l2m2m` fails with "Could not find a valid device". GStreamer does the
  hardware work; ffmpeg is here only because it writes PROGRAM-DATE-TIME.
- **`-use_wallclock_as_timestamps 1`, never `-fflags +genpts`.** genpts measured
  -11.8s of drift and worsening. Wallclock stamping measured +0.003% rate error,
  i.e. zero seconds across a 2h game.

Everything here is best-effort. A publisher failure must never disturb
recording: a missed game cannot be recovered, a missed live stream is an
inconvenience.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger("agx.live")

ENABLED = os.getenv("LIVE_STREAM_ENABLED", "false").lower() in ("1", "true", "yes")
# Angles the annotation player understands today. FL -> LEFT, FR -> RIGHT.
ANGLE_MAP = {"FL": "LEFT", "FR": "RIGHT"}
SEG_SEC = int(os.getenv("LIVE_SEGMENT_SEC", "4"))
WIDTH = int(os.getenv("LIVE_WIDTH", "1280"))
HEIGHT = int(os.getenv("LIVE_HEIGHT", "720"))
BITRATE = int(os.getenv("LIVE_BITRATE", "2500000"))
FPS = int(os.getenv("LIVE_FPS", "30"))
OUT_ROOT = os.getenv("LIVE_OUT_ROOT", "/home/dev/live")
BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")
REGION = os.getenv("UPLOAD_REGION", "us-east-1")
CDN = os.getenv("HIGHLIGHT_CDN_DOMAIN", "d22gul8sdref0l.cloudfront.net")
S3_PREFIX = os.getenv("LIVE_S3_PREFIX", "live")
UPLOAD_POLL = float(os.getenv("LIVE_UPLOAD_POLL_SEC", "1.0"))

# Burned-in wall-clock, top right. ON during bring-up so a frame can be compared
# against the timestamp the system recorded for it; turn OFF for real games.
OVERLAY = os.getenv("LIVE_CLOCK_OVERLAY", "true").lower() in ("1", "true", "yes")
OVERLAY_FONT = os.getenv("LIVE_CLOCK_FONT", "Monospace Bold 13")
# strftime. Seconds resolution — that is what a frame-vs-record comparison needs.
OVERLAY_FORMAT = os.getenv("LIVE_CLOCK_FORMAT", "%H:%M:%S")

# A forgotten publisher must not run forever. We have filled a disk once with a
# 12-hour runaway recording; this is the same class of mistake.
MAX_MIN = int(os.getenv("LIVE_MAX_MIN", "180"))
# Refuse to start if the disk is already tight. HLS is small (~4.5 GB/angle for
# a 2h game) but recording's needs come first.
MIN_FREE_GB = float(os.getenv("LIVE_MIN_FREE_GB", "40"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _free_gb(path: str) -> float:
    try:
        return shutil.disk_usage(path).free / 1e9
    except OSError:
        return 0.0


class _AnglePublisher:
    """One camera -> one HLS playlist on disk, plus an uploader to S3."""

    def __init__(self, cam, session_dir: str, s3_prefix: str):
        self.cam = cam
        self.angle = ANGLE_MAP[cam.angle]        # LEFT / RIGHT
        self.source_angle = cam.angle            # FL / FR
        self.dir = os.path.join(session_dir, self.angle)
        self.s3_prefix = f"{s3_prefix}/{self.angle}"
        self.proc: Optional[subprocess.Popen] = None
        self.playlist = os.path.join(self.dir, "live.m3u8")
        self.playlist_url = f"https://{CDN}/{self.s3_prefix}/live.m3u8"
        self._sent: set[str] = set()
        self._stop = threading.Event()
        self._uploader: Optional[threading.Thread] = None

    # ---- pipeline ---------------------------------------------------------
    def _gst_cmd(self) -> str:
        url = f"rtsp://{self.cam.ip}:554/main/av"
        # Keyframe cadence MUST match the segment length in frames or the muxer
        # cannot cut on an IDR and segment durations drift off target.
        gop = SEG_SEC * FPS
        if OVERLAY:
            # clockoverlay needs CPU-side raw video, so drop out of NVMM for the
            # draw and go straight back in. One 720p round trip; measured cheap.
            convert = (
                f'! nvvidconv ! "video/x-raw,width={WIDTH},height={HEIGHT},format=I420" '
                f'! clockoverlay time-format="{OVERLAY_FORMAT}" halignment=right '
                f'valignment=top shaded-background=true font-desc="{OVERLAY_FONT}" '
                f'! nvvidconv ! "video/x-raw(memory:NVMM)" '
            )
        else:
            convert = f'! nvvidconv ! "video/x-raw(memory:NVMM),width={WIDTH},height={HEIGHT}" '

        return (
            f'gst-launch-1.0 -q rtspsrc location={url} protocols=tcp latency=200 '
            f'! rtph265depay ! h265parse ! nvv4l2decoder '
            f'{convert}'
            f'! nvv4l2h264enc bitrate={BITRATE} iframeinterval={gop} idrinterval={gop} '
            f'insert-sps-pps=true '
            f'! h264parse ! mpegtsmux ! fdsink fd=1 sync=false'
        )

    def _ffmpeg_cmd(self) -> str:
        return (
            f'ffmpeg -hide_banner -nostdin -loglevel warning '
            f'-use_wallclock_as_timestamps 1 -f mpegts -i - '
            f'-map 0:v -c copy '
            f'-f hls -hls_time {SEG_SEC} -hls_playlist_type event '
            f'-hls_flags append_list+program_date_time+independent_segments '
            f'-hls_segment_filename "{self.dir}/seg_%05d.ts" "{self.playlist}"'
        )

    def start(self) -> bool:
        os.makedirs(self.dir, exist_ok=True)
        cmd = f"{self._gst_cmd()} | {self._ffmpeg_cmd()}"
        try:
            self.proc = subprocess.Popen(
                ["bash", "-c", cmd],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid,
            )
        except OSError as e:
            logger.error("[LIVE] %s publisher failed to spawn: %s", self.angle, e)
            self.proc = None
            return False
        self._uploader = threading.Thread(
            target=self._upload_loop, name=f"live-upload-{self.angle}", daemon=True
        )
        self._uploader.start()
        logger.info("[LIVE] %s <- %s publishing to %s", self.angle, self.source_angle, self.playlist_url)
        return True

    def stop(self) -> None:
        """SIGINT ffmpeg so it finalises the playlist with EXT-X-ENDLIST.

        That matters: with ENDLIST the annotator's player turns cleanly into a
        recording at the final horn instead of hanging on a stream that stopped
        advancing. SIGKILL would leave the playlist open forever.
        """
        p, self.proc = self.proc, None
        if p is not None and p.poll() is None:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGINT)
                p.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except OSError:
                    pass
        # Let the uploader make one final pass so the last segment and the
        # finalised playlist both reach S3, then stop it.
        time.sleep(UPLOAD_POLL * 2)
        self._upload_once()
        self._stop.set()

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    # ---- upload -----------------------------------------------------------
    def _s3(self):
        import boto3

        return boto3.client("s3", region_name=REGION)

    def _upload_once(self) -> None:
        try:
            names = sorted(f for f in os.listdir(self.dir) if f.endswith(".ts"))
        except OSError:
            return
        # While live, the newest .ts is still being written — uploading it gives
        # the player a file it can fetch but not decode. Once EXT-X-ENDLIST is
        # present nothing is open, and the newest file MUST go up or the last
        # segment of every game is missing and the player 404s on the final
        # seconds. (Measured: 29 of 30 before this was handled.)
        finished = False
        try:
            with open(self.playlist) as fh:
                finished = "#EXT-X-ENDLIST" in fh.read()
        except OSError:
            pass

        s3 = self._s3()
        for fn in (names if finished else names[:-1]):
            if fn in self._sent:
                continue
            try:
                s3.upload_file(
                    os.path.join(self.dir, fn), BUCKET, f"{self.s3_prefix}/{fn}",
                    ExtraArgs={"ContentType": "video/mp2t",
                               "CacheControl": "public, max-age=31536000, immutable"},
                )
                self._sent.add(fn)
            except Exception as e:  # noqa: BLE001
                logger.warning("[LIVE] %s segment %s upload failed: %s", self.angle, fn, e)

        # Playlist last, so it never advertises a segment that is not up yet.
        if os.path.exists(self.playlist):
            try:
                s3.upload_file(
                    self.playlist, BUCKET, f"{self.s3_prefix}/live.m3u8",
                    ExtraArgs={"ContentType": "application/vnd.apple.mpegurl",
                               "CacheControl": "no-cache, max-age=0"},
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("[LIVE] %s playlist upload failed: %s", self.angle, e)

    def _upload_loop(self) -> None:
        while not self._stop.wait(UPLOAD_POLL):
            try:
                self._upload_once()
            except Exception as e:  # noqa: BLE001
                logger.warning("[LIVE] %s upload loop: %s", self.angle, e)

    # ---- timing anchor ----------------------------------------------------
    def first_pdt(self) -> Optional[str]:
        """EXT-X-PROGRAM-DATE-TIME of the first segment.

        This is the anchor every live card's timestamp is rebased against, so it
        is reported to the annotation API as soon as it exists.
        """
        try:
            with open(self.playlist) as fh:
                for line in fh:
                    if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
        return None

    def segment_count(self) -> int:
        try:
            return len([f for f in os.listdir(self.dir) if f.endswith(".ts")])
        except OSError:
            return 0


class LivePublisher:
    """Owns the per-angle publishers and the annotation-tool session."""

    def __init__(self, cfg):
        self.cfg = cfg
        self._lock = threading.Lock()
        self._angles: List[_AnglePublisher] = []
        self._session_id: Optional[str] = None
        self._label: Optional[str] = None
        self._started_at: Optional[float] = None
        self._watchdog: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

    # ---- guards -----------------------------------------------------------
    def _refuse(self, game_id) -> Optional[str]:
        """Why we must not publish. None means go ahead."""
        if not ENABLED:
            return "LIVE_STREAM_ENABLED is not set"
        if not game_id:
            # An unattached recording has no game in the annotation tool, so
            # there is nothing for an annotator to open and nothing to rebase
            # against later.
            return "no game id (unattached recording)"
        free = _free_gb(OUT_ROOT if os.path.isdir(OUT_ROOT) else "/")
        if free < MIN_FREE_GB:
            return f"only {free:.0f} GB free, need {MIN_FREE_GB:.0f}"
        cams = [c for c in self.cfg.cameras if c.angle in ANGLE_MAP]
        if not cams:
            return "no FL/FR cameras configured"
        return None

    # ---- lifecycle --------------------------------------------------------
    def start(self, label: str, game_id: Optional[str]) -> Dict:
        """Best-effort. Returns a status dict and never raises into _do_start."""
        with self._lock:
            try:
                refusal = self._refuse(game_id)
                if refusal:
                    logger.info("[LIVE] not publishing: %s", refusal)
                    return {"publishing": False, "reason": refusal}

                self._stop_evt = threading.Event()
                self._label = label
                self._started_at = time.time()
                session_dir = os.path.join(OUT_ROOT, label)
                shutil.rmtree(session_dir, ignore_errors=True)
                os.makedirs(session_dir, exist_ok=True)
                s3_prefix = f"{S3_PREFIX}/{label}"

                self._session_id = self._open_session(game_id, label)

                self._angles = []
                for cam in self.cfg.cameras:
                    if cam.angle not in ANGLE_MAP:
                        continue
                    pub = _AnglePublisher(cam, session_dir, s3_prefix)
                    if pub.start():
                        self._angles.append(pub)

                if not self._angles:
                    logger.warning("[LIVE] no angle started")
                    return {"publishing": False, "reason": "no angle started"}

                self._watchdog = threading.Thread(
                    target=self._watch, name="live-watchdog", daemon=True
                )
                self._watchdog.start()
                return {
                    "publishing": True,
                    "session_id": self._session_id,
                    "angles": [a.angle for a in self._angles],
                    "overlay": OVERLAY,
                }
            except Exception as e:  # noqa: BLE001
                logger.error("[LIVE] start failed (recording unaffected): %s", e)
                return {"publishing": False, "reason": str(e)}

    def stop(self) -> None:
        with self._lock:
            try:
                self._stop_evt.set()
                for a in self._angles:
                    try:
                        a.stop()
                    except Exception as e:  # noqa: BLE001
                        logger.warning("[LIVE] %s stop: %s", a.angle, e)
                if self._session_id:
                    self._end_session(self._session_id)
                logger.info("[LIVE] stopped (%d angles)", len(self._angles))
            except Exception as e:  # noqa: BLE001
                logger.error("[LIVE] stop failed: %s", e)
            finally:
                self._angles = []
                self._session_id = None
                self._label = None
                self._started_at = None

    def status(self) -> Dict:
        return {
            "enabled": ENABLED,
            "publishing": bool(self._angles),
            "label": self._label,
            "session_id": self._session_id,
            "overlay": OVERLAY,
            "angles": [
                {"angle": a.angle, "source": a.source_angle, "alive": a.alive(),
                 "segments": a.segment_count(), "url": a.playlist_url}
                for a in self._angles
            ],
        }

    # ---- watchdog ---------------------------------------------------------
    def _watch(self) -> None:
        """Report each angle's PDT anchor once it exists, and enforce the cap."""
        reported: set[str] = set()
        while not self._stop_evt.wait(5):
            try:
                if self._started_at and (time.time() - self._started_at) > MAX_MIN * 60:
                    logger.warning("[LIVE] max duration %d min reached — stopping", MAX_MIN)
                    threading.Thread(target=self.stop, daemon=True).start()
                    return
                for a in self._angles:
                    if a.angle in reported:
                        continue
                    pdt = a.first_pdt()
                    if pdt and self._session_id:
                        self._register_angle(self._session_id, a, pdt)
                        reported.add(a.angle)
            except Exception as e:  # noqa: BLE001
                logger.warning("[LIVE] watchdog: %s", e)

    # ---- annotation API ---------------------------------------------------
    def _client(self):
        try:
            from uball_client import get_uball_client

            return get_uball_client()
        except Exception as e:  # noqa: BLE001
            logger.warning("[LIVE] annotation client unavailable: %s", e)
            return None

    @staticmethod
    def _post(client, path: str, payload: Dict) -> Optional[Dict]:
        """POST to the annotation API using the client's auth and base URL.

        UballClient exposes one method per endpoint rather than a generic
        request helper, so rather than reach into it we borrow only its token
        and base URL — which keeps this module from depending on its internals
        beyond what is already public-ish.
        """
        import requests

        if not client._ensure_authenticated():
            logger.warning("[LIVE] annotation API auth failed")
            return None
        try:
            r = requests.post(f"{client.backend_url}{path}",
                              headers=client._get_headers(), json=payload, timeout=10)
            if r.status_code >= 400:
                logger.warning("[LIVE] %s -> %s %s", path, r.status_code, r.text[:200])
                return None
            return r.json() if r.content else {}
        except Exception as e:  # noqa: BLE001
            logger.warning("[LIVE] %s failed: %s", path, e)
            return None

    def _game_uuid(self, firebase_game_id: str) -> Optional[str]:
        client = self._client()
        if not client:
            return None
        try:
            game = client.get_game_by_firebase_id(firebase_game_id)
            return game.get("id") if game else None
        except Exception as e:  # noqa: BLE001
            logger.warning("[LIVE] game lookup failed: %s", e)
            return None

    def _open_session(self, firebase_game_id: str, label: str) -> Optional[str]:
        """Open the session the annotator's Live tab reads.

        If this fails we still publish: the segments keep landing in S3, so the
        feed can be attached by hand. It must never stop the recording.
        """
        game_uuid = self._game_uuid(firebase_game_id)
        if not game_uuid:
            logger.warning("[LIVE] no annotation game for firebase id %s", firebase_game_id)
            return None
        client = self._client()
        try:
            res = self._post(client, "/api/live/sessions",
                             {"game_id": game_uuid, "jetson_id": self.cfg.jetson_id,
                              "label": label, "started_at": _now_iso()})
            sid = (res or {}).get("id")
            logger.info("[LIVE] session %s opened for game %s", str(sid)[:8], game_uuid[:8])
            return sid
        except Exception as e:  # noqa: BLE001
            logger.warning("[LIVE] could not open session: %s", e)
            return None

    def _register_angle(self, session_id: str, pub: _AnglePublisher, pdt: str) -> None:
        client = self._client()
        if not client:
            return
        try:
            self._post(client, f"/api/live/sessions/{session_id}/angles",
                       {"angle": pub.angle, "source_angle": pub.source_angle,
                        "playlist_url": pub.playlist_url, "first_segment_pdt": pdt})
            logger.info("[LIVE] %s anchored at %s", pub.angle, pdt)
        except Exception as e:  # noqa: BLE001
            logger.warning("[LIVE] could not register %s: %s", pub.angle, e)

    def _end_session(self, session_id: str) -> None:
        client = self._client()
        if not client:
            return
        try:
            self._post(client, f"/api/live/sessions/{session_id}/end", {})
        except Exception as e:  # noqa: BLE001
            logger.warning("[LIVE] could not end session: %s", e)

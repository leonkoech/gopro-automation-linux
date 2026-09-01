#!/usr/bin/env python3
"""
AGX recording controller (P0).

Drives GStreamer inside the Zowietek NDI/RTSP Docker container to record all
4 cameras to per-angle MP4s, with a clean stop (SIGINT -> EOS -> finalized
moov). One container per recording session; one gst-launch with N parallel
rtspsrc->mp4mux->filesink branches (the record_zowie.sh model), so a single
SIGINT finalizes every angle at once.

Proven mechanism (2026-07-15): `docker kill --signal=INT <container>` on a
`gst-launch-1.0 -e ...` pipeline finalizes the MP4 cleanly.

Runs ON the AGX host. Requires docker access — either add the user to the
`docker` group or allow `sudo docker` (set "docker_cmd" in cameras.json).

CLI (for testing):
    python3 recording.py dry-run --label test
    python3 recording.py start   --label test_20260715 --cameras 35353,35347
    python3 recording.py status  --label test_20260715
    python3 recording.py stop    --label test_20260715
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("agx.recording")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "cameras.json")
CONTAINER_PREFIX = "agxrec_"
# Broadcast game angles (RTSP/Zowietek, player+ball tracking) and the near-rim
# high-fps shot-detection angles (Aravis/FLIR). Kept as distinct code sets so a
# shot camera can never be mistaken for a tracking angle downstream.
TRACKING_ANGLES = ("FL", "FR", "NL", "NR")
SHOT_ANGLES = ("SL", "SR")
VALID_ANGLES = TRACKING_ANGLES + SHOT_ANGLES

# Court audio for cross-correlation sync. The stripped record container can't
# mux AAC, so audio is captured host-side (one audio-only ffmpeg per camera),
# journaled to the session dir, SIGINT-finalized on stop. RECORD_AUDIO=false
# skips it. Best-effort — it never affects the per-camera video recording.
RECORD_AUDIO = os.getenv("RECORD_AUDIO", "true").lower() in ("1", "true", "yes")
AUDIO_JOURNAL = "audio_capture.json"

# Watchdog stall gaps. A restart costs real game time; that hole is re-inserted
# into the master so every angle stays on one timeline (see _concat_segments).
# Below MIN it is muxer jitter, not a gap; above MAX it is a clock glitch we
# refuse to trust rather than punch an absurd hole into a game master.
MIN_STALL_GAP_SEC = float(os.getenv("REC_MIN_STALL_GAP_SEC", "0.25"))
MAX_STALL_GAP_SEC = float(os.getenv("REC_MAX_STALL_GAP_SEC", "900"))
# Post-stop alarm: the tracking angles record the same wall-clock window, so
# their durations must agree. Divergence beyond this means one angle lost time
# and the pair can NOT be aligned by a constant per-angle offset — exactly the
# failure the annotators hit on 2026-08-17 and 2026-08-19, unnoticed for days.
ANGLE_SKEW_ALARM_SEC = float(os.getenv("REC_ANGLE_SKEW_ALARM_SEC", "2.0"))


@dataclass(frozen=True)
class Camera:
    id: str
    ip: str
    angle: str
    type: str = "rtsp"          # "rtsp" (Zowietek NDI) | "aravis" (FLIR GigE Vision)
    cv_role: str = "tracking"   # "tracking" | "shot_detection"
    camera_name: str = ""       # aravissrc camera-name (device id/IP); defaults to ip
    basket_side: str = ""       # "L" | "R" — which rim a shot camera faces


@dataclass(frozen=True)
class Config:
    location: str
    jetson_id: str
    rtsp_port: int
    rtsp_path: str
    docker_image: str
    app_mount: str
    output_dir: str
    docker_cmd: List[str]
    cameras: List[Camera]
    shot_cameras: List[Camera] = field(default_factory=list)

    def camera_by_id(self, cam_id: str) -> Optional[Camera]:
        return next((c for c in self.cameras if c.id == cam_id), None)

    def shot_camera_by_id(self, cam_id: str) -> Optional[Camera]:
        return next((c for c in self.shot_cameras if c.id == cam_id), None)


def _parse_camera(c: dict, *, aravis: bool) -> Camera:
    """Build a Camera from a cameras.json entry. Tracking cams are RTSP; shot
    cams are Aravis (FLIR GigE) and default camera_name to their IP."""
    ip = c["ip"]
    return Camera(
        id=str(c["id"]),
        ip=ip,
        angle=c["angle"],
        type="aravis" if aravis else c.get("type", "rtsp"),
        cv_role=c.get("cv_role", "shot_detection" if aravis else "tracking"),
        camera_name=c.get("camera_name") or (ip if aravis else ""),
        basket_side=c.get("basket_side", ""),
    )


def load_config(path: str = CONFIG_PATH) -> Config:
    with open(path) as f:
        d = json.load(f)
    cams = [_parse_camera(c, aravis=False) for c in d["cameras"]]
    shot_cams = [_parse_camera(c, aravis=True) for c in d.get("shot_cameras", [])]
    for c in cams + shot_cams:
        if c.angle not in VALID_ANGLES:
            raise ValueError(f"camera {c.id}: invalid angle {c.angle}")
    return Config(
        location=d.get("location", "court-a"),
        jetson_id=d.get("jetson_id", "agx-1"),
        rtsp_port=int(d.get("rtsp_port", 554)),
        rtsp_path=d.get("rtsp_path", "/main/av"),
        docker_image=d["docker_image"],
        app_mount=d["app_mount"],
        output_dir=d.get("output_dir", os.path.join(d["app_mount"], "recordings")),
        docker_cmd=shlex.split(d.get("docker_cmd", "docker")),
        cameras=cams,
        shot_cameras=shot_cams,
    )


# --------------------------------------------------------------------------- #
# GStreamer pipeline
# --------------------------------------------------------------------------- #
def _single_cam_gst(cam: Camera, cfg: Config, out_path: str) -> List[str]:
    """A complete ONE-camera gst-launch: rtspsrc->depay->parse->mp4mux->filesink.

    Each camera records in its own container (see RecordingController), so one
    camera stalling at RTSP connect can't stall the others — the failure mode of
    the old single shared gst-launch, where any branch failing to preroll left
    ALL cameras writing 0 bytes.
    """
    url = f"rtsp://{cam.ip}:{cfg.rtsp_port}{cfg.rtsp_path}"
    return [
        "gst-launch-1.0", "-e",
        "rtspsrc", f"location={url}", "protocols=tcp", "name=r0",
        "r0.", "!", "application/x-rtp,media=video",
        "!", "queue", "max-size-buffers=30",
        "!", "rtph265depay", "!", "h265parse", "!", "mp4mux",
        "!", "filesink", f"location={out_path}",
    ]


_RTSP_TIMEOUT_FLAG: Optional[str] = None


def _rtsp_timeout_flag() -> str:
    """ffmpeg 4.x uses `-stimeout` for the RTSP socket I/O timeout; ffmpeg 5+ renamed
    it to `-timeout` (and `-timeout` means LISTEN mode on 4.x). Detect once. This is
    lifted from Geoff's camrec `rtsp_timeout_flag()`."""
    global _RTSP_TIMEOUT_FLAG
    if _RTSP_TIMEOUT_FLAG is None:
        try:
            out = subprocess.run(["ffmpeg", "-hide_banner", "-h", "demuxer=rtsp"],
                                 capture_output=True, text=True, timeout=3).stdout
            _RTSP_TIMEOUT_FLAG = "-stimeout" if "stimeout" in out else "-timeout"
        except Exception:  # noqa: BLE001
            _RTSP_TIMEOUT_FLAG = "-stimeout"   # safe on 4.x; harmless err on 5+
    return _RTSP_TIMEOUT_FLAG


def _single_cam_ffmpeg(cam: Camera, cfg: Config, out_path: str, stimeout_us: int) -> List[str]:
    """Host ffmpeg RTSP recorder (bitstream copy) — the proven camrec engine.

    Unlike gst `mp4mux`, ffmpeg `-c copy` SURFACES a stall instead of absorbing it:
    frozen/duplicate PTS are non-monotonic DTS -> ffmpeg drops them and its `-progress`
    `out_time` stops advancing (which the timeline watchdog catches); `-stimeout` exits
    ffmpeg if the socket goes quiet. Video-only — audio is captured separately."""
    url = f"rtsp://{cam.ip}:{cfg.rtsp_port}{cfg.rtsp_path}"
    return ["ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
            "-progress", "pipe:1",
            "-rtsp_transport", "udp",
            _rtsp_timeout_flag(), str(stimeout_us),
            "-i", url, "-map", "0:v", "-c", "copy", out_path]


def _to_container_path(host_path: str, cfg: Config) -> str:
    """Map a host path under app_mount to the container's /app/data mount."""
    rel = os.path.relpath(host_path, cfg.app_mount)
    return os.path.join("/app/data", rel)


def _audio_cmd(cam: Camera, cfg: Config, out_path: str) -> List[str]:
    """Host ffmpeg pulling ONLY the RTSP AAC audio (no decode/re-encode) to m4a.
    -allowed_media_types audio means the video RTP is never even set up."""
    url = f"rtsp://{cam.ip}:{cfg.rtsp_port}{cfg.rtsp_path}"
    return ["ffmpeg", "-nostdin", "-loglevel", "error",
            "-allowed_media_types", "audio", "-rtsp_transport", "tcp",
            "-i", url, "-vn", "-c:a", "copy", out_path]


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class RecordingController:
    """Records each camera in its OWN container (isolated failures) with a
    post-start data-flow check: a camera whose container is up but whose file is
    still empty is stuck in RTSP preroll (the intermittent all-zero failure) —
    it is killed and relaunched rather than silently recording nothing."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.verify_settle = float(os.getenv("REC_VERIFY_SETTLE_SEC", "4"))
        self.verify_retries = int(os.getenv("REC_VERIFY_RETRIES", "2"))
        self.verify_min_bytes = int(os.getenv("REC_VERIFY_MIN_BYTES", str(64 * 1024)))
        # Mid-recording watchdog: a per-session thread that restarts a camera whose
        # file stops growing (RTSP dropped, container alive-but-stalled) or whose
        # container died — the self-healing our own backend lacked (camrec has it).
        # Gated OFF by default so deploying the code changes nothing until enabled
        # and tested on-box. Restart writes a new `.rN` segment; stop() concats.
        self.watchdog_on = os.getenv("REC_WATCHDOG", "false").lower() in ("1", "true", "yes", "on")
        self.wd_poll = float(os.getenv("REC_WATCHDOG_POLL_SEC", "20"))
        self.wd_stall = float(os.getenv("REC_WATCHDOG_STALL_SEC", "45"))
        self.wd_max_restarts = int(os.getenv("REC_WATCHDOG_MAX_RESTARTS", "40"))
        self._sessions: Dict[str, Dict] = {}   # label -> {runs, stop_evt, thread}
        self._wd_lock = threading.Lock()
        # ffmpeg engine (adopts Geoff's camrec RTSP recorder): host ffmpeg -c copy +
        # -stimeout + a TIMELINE heartbeat (out_time_s must ADVANCE, not just bytes),
        # replacing the gst+byte-growth path that missed the frozen-timeline stall that
        # lost two games. REC_ENGINE=gst (default) = unchanged; =ffmpeg opts in. See
        # docs/RECORDER_FFMPEG_WATCHDOG_FIX.md.
        self.engine = os.getenv("REC_ENGINE", "gst").strip().lower()
        self.ff_stimeout_us = int(os.getenv("REC_FFMPEG_STIMEOUT_US", "5000000"))   # 5s socket
        self.ff_stall = float(os.getenv("REC_FFMPEG_STALL_SEC", "15"))              # timeline stall
        self.ff_poll = float(os.getenv("REC_FFMPEG_POLL_SEC", "2"))
        self.ff_stop_grace = float(os.getenv("REC_FFMPEG_STOP_GRACE_SEC", "15"))
        # exponential restart backoff (camrec model): first retry immediate, then space
        # repeated failures 5->10->20->40->60s so a permanently-broken cam doesn't hammer;
        # reset once a run has recorded healthily (out_time past ff_healthy_s).
        self.ff_healthy_s = float(os.getenv("REC_FFMPEG_HEALTHY_SEC", "30"))
        self.ff_backoff_base = float(os.getenv("REC_FFMPEG_BACKOFF_BASE_SEC", "5"))
        self.ff_backoff_max = float(os.getenv("REC_FFMPEG_BACKOFF_MAX_SEC", "60"))

    def _docker(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(list(self.cfg.docker_cmd) + list(args),
                              capture_output=True, text=True, stdin=subprocess.DEVNULL)

    def _container_name(self, label: str, angle: str) -> str:
        return f"{CONTAINER_PREFIX}{label}_{angle}"

    def _running(self) -> List[str]:
        return self._docker("ps", "--format", "{{.Names}}").stdout.split()

    def _session_containers(self, label: str) -> List[str]:
        prefix = f"{CONTAINER_PREFIX}{label}_"
        return [n for n in self._running() if n.startswith(prefix)]

    def is_recording(self, label: str) -> bool:
        if self.engine == "ffmpeg":
            return self._ff_is_recording(label)
        return bool(self._session_containers(label))

    def _ff_is_recording(self, label: str) -> bool:
        """Any of this session's ffmpegs still alive — checked via the in-memory session
        (current process) or, after a service restart, the pid journal."""
        with self._wd_lock:
            sess = self._sessions.get(label)
        if sess:
            return any(r.get("pid") and _pid_alive(r["pid"]) for r in sess["runs"])
        try:
            with open(self._ff_journal_path(label)) as f:
                procs = json.load(f)
            return any(pr.get("pid") and _pid_alive(pr["pid"]) for pr in procs)
        except (OSError, ValueError):
            return False

    def _docker_run_cmd(self, name: str, cam: Camera, host_path: str) -> List[str]:
        """The `docker run` for one camera writing to host_path."""
        container_path = _to_container_path(host_path, self.cfg)
        return list(self.cfg.docker_cmd) + [
            "run", "-d", "--name", name, "--rm",
            "--privileged", "--runtime", "nvidia", "--net=host",
            "-v", f"{self.cfg.app_mount}:/app/data", "--workdir", "/app/data",
            self.cfg.docker_image,
        ] + _single_cam_gst(cam, self.cfg, container_path)

    def _run_cmd(self, cam: Camera, session_dir: str, label: str) -> Dict[str, object]:
        """One camera's run dict. `path` = its current segment file; `segments` =
        every segment written so far (the watchdog appends `.rN` files on restart,
        stop() concats them back to one master); `cam`/`session_dir`/`label` let the
        watchdog relaunch it."""
        host_path = os.path.join(session_dir, f"{label}_{cam.angle}.mp4")
        run: Dict[str, object] = {"angle": cam.angle, "id": cam.id, "path": host_path,
                                  "cam": cam, "session_dir": session_dir, "label": label,
                                  "restart": 0, "segments": [host_path],
                                  # Wall-clock start of each segment, index-aligned with
                                  # `segments`. This is what lets _concat_segments put the
                                  # stall gap BACK into the master instead of butting the
                                  # segments together and silently deleting that time.
                                  "seg_starts": [time.time()]}
        if self.engine == "ffmpeg":
            # host ffmpeg subprocess: proc/pid + timeline heartbeat (out_time advancing)
            run.update({"proc": None, "pid": None, "out_time": 0.0, "last_advance": 0.0,
                        "consec_fail": 0, "retry_after": 0.0})
        else:
            name = self._container_name(label, cam.angle)
            run.update({"name": name, "cmd": self._docker_run_cmd(name, cam, host_path)})
        return run

    def plan(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        cams = self.cfg.cameras if not camera_ids else \
            [c for cid in camera_ids for c in [self.cfg.camera_by_id(cid)] if c]
        if not cams:
            raise ValueError("no valid cameras selected")
        session_dir = os.path.join(self.cfg.output_dir, label)
        return {"session_dir": session_dir,
                "runs": [self._run_cmd(c, session_dir, label) for c in cams]}

    def _launch(self, run: Dict) -> bool:
        if self.engine == "ffmpeg":
            return self._launch_ffmpeg(run)
        cp = subprocess.run(run["cmd"], capture_output=True, text=True, stdin=subprocess.DEVNULL)
        if cp.returncode != 0:
            logger.error("docker run failed for %s: %s", run["angle"], cp.stderr.strip()[-300:])
            return False
        return True

    # ---- ffmpeg host-subprocess engine ---------------------------------------
    def _launch_ffmpeg(self, run: Dict) -> bool:
        """Spawn a host ffmpeg (RTSP -c copy) in its own session so it survives a
        service restart (orphaned, keeps writing — matches the audio path), and a
        reader thread parses `-progress` to track the encoded timeline."""
        try:
            p = subprocess.Popen(
                _single_cam_ffmpeg(run["cam"], self.cfg, run["path"], self.ff_stimeout_us),
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, start_new_session=True)   # stderr->DEVNULL: no pipe-fill deadlock
        except OSError as e:  # noqa: BLE001
            logger.error("ffmpeg launch failed for %s: %s", run["angle"], e)
            return False
        run["proc"] = p
        run["pid"] = p.pid
        run["out_time"] = 0.0
        run["last_advance"] = time.time()
        threading.Thread(target=self._ff_read_progress, args=(run,),
                         name=f"ffprog-{run['label']}-{run['angle']}", daemon=True).start()
        return True

    def _ff_read_progress(self, run: Dict) -> None:
        """Drain ffmpeg `-progress`; advance the heartbeat ONLY when the encoded time
        (out_time) INCREASES — not merely when a progress block is emitted. This is the
        one place we harden beyond camrec: a frozen timeline no longer looks alive."""
        p = run.get("proc")
        if not p or not p.stdout:
            return
        try:
            for line in p.stdout:
                if run.get("proc") is not p:   # a watchdog restart replaced us — stop updating shared state
                    break
                if "=" not in line:
                    continue
                k, v = line.strip().split("=", 1)
                if k in ("out_time_us", "out_time_ms"):
                    try:
                        t = int(v) / 1_000_000.0
                    except ValueError:
                        continue
                    if t > run["out_time"]:
                        run["out_time"] = t
                        run["last_advance"] = time.time()
        except (ValueError, OSError):
            pass

    def _ff_finalize(self, run: Dict) -> None:
        """SIGINT the ffmpeg so it writes the moov (clean, playable mp4); force-kill if
        it lingers past the grace window."""
        p = run.get("proc")
        if not p:
            return
        if p.poll() is None:
            try:
                p.send_signal(signal.SIGINT)
                p.wait(timeout=self.ff_stop_grace)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    p.kill()
                    p.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    pass

    def _ff_journal_path(self, label: str) -> str:
        return os.path.join(self.cfg.output_dir, label, "video_capture.json")

    def _ff_write_journal(self, label: str, runs: List[Dict]) -> None:
        """Journal current per-camera pids so stop()/is_recording work even after a
        service restart (the in-memory session is gone but the orphaned ffmpegs run on).
        Rewritten after start and on every watchdog restart so it holds the live pids."""
        try:
            with open(self._ff_journal_path(label), "w") as f:
                json.dump([{"angle": r["angle"], "pid": r.get("pid")} for r in runs], f)
        except OSError:
            pass

    def _ff_stop(self, label: str, finalize_timeout: float) -> None:
        """SIGINT each ffmpeg for a clean moov, reap, force-kill stragglers. Uses the
        in-memory session (current process) or the pid journal (after a service restart)."""
        with self._wd_lock:
            sess = self._sessions.get(label)
        if sess:
            for r in sess["runs"]:
                self._ff_finalize(r)
            return
        try:
            with open(self._ff_journal_path(label)) as f:
                pids = [pr["pid"] for pr in json.load(f) if pr.get("pid")]
        except (OSError, ValueError):
            return
        for pid in pids:
            try:
                os.kill(pid, signal.SIGINT)
            except OSError:
                pass
        deadline = time.time() + finalize_timeout
        while time.time() < deadline and any(_pid_alive(pid) for pid in pids):
            time.sleep(0.5)
        for pid in pids:
            if _pid_alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass

    def start(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        if self.is_recording(label):
            raise RuntimeError(f"session {label} already recording")
        p = self.plan(label, camera_ids)
        os.makedirs(p["session_dir"], exist_ok=True)  # host side (best-effort)
        live = [r for r in p["runs"] if self._launch(r)]
        if not live:
            raise RuntimeError("no camera recorders started")
        if self.engine != "ffmpeg":
            self._verify_and_retry(live)   # docker preroll check; ffmpeg's watchdog+stimeout cover it
        self._start_watchdog(label, live)
        if self.engine == "ffmpeg":
            self._ff_write_journal(label, live)
        audio = self._start_audio(label, camera_ids, p["session_dir"])
        outputs = [{"angle": r["angle"], "id": r["id"], "path": r["path"]} for r in live]
        logger.info("recording started (per-camera) label=%s angles=%s",
                    label, [r["angle"] for r in live])
        return {"label": label, "container": f"{CONTAINER_PREFIX}{label}",
                "outputs": outputs, "audio": audio, "started_at": _utcnow()}

    def _flowing(self, run: Dict) -> bool:
        """Container up AND file grown past the empty-preroll threshold."""
        if run["name"] not in self._running():
            return False
        try:
            return os.path.getsize(run["path"]) >= self.verify_min_bytes
        except OSError:
            return False

    def _verify_and_retry(self, runs: List[Dict]) -> None:
        """Confirm each camera is actually writing data; kill+relaunch any that
        came up but stalled in RTSP preroll (0-byte), up to verify_retries."""
        for attempt in range(self.verify_retries + 1):
            time.sleep(self.verify_settle)
            stalled = [r for r in runs if not self._flowing(r)]
            if not stalled:
                return
            if attempt == self.verify_retries:
                logger.warning("cameras not producing data after %d retries: %s",
                               self.verify_retries, [r["angle"] for r in stalled])
                return
            for r in stalled:
                logger.warning("camera %s not writing data — restarting (attempt %d)",
                               r["angle"], attempt + 1)
                self._docker("kill", r["name"])   # SIGKILL the stalled one; --rm cleans up
                time.sleep(1)
                self._launch(r)

    # ---- mid-recording watchdog (self-healing) -------------------------------
    def _start_watchdog(self, label: str, runs: List[Dict]) -> None:
        ff = self.engine == "ffmpeg"
        if not ff and not self.watchdog_on:   # ffmpeg's timeline watchdog is mandatory (the fix)
            return
        stop_evt = threading.Event()
        t = threading.Thread(target=self._watchdog, name=f"rec-wd-{label}",
                             args=(label, runs, stop_evt), daemon=True)
        with self._wd_lock:
            self._sessions[label] = {"runs": runs, "stop_evt": stop_evt, "thread": t}
        t.start()
        logger.info("recording watchdog on label=%s engine=%s (poll=%.0fs stall=%.0fs)",
                    label, self.engine, self.ff_poll if ff else self.wd_poll,
                    self.ff_stall if ff else self.wd_stall)

    def _seg_path(self, run: Dict, n: int) -> str:
        """Nth restart segment, under a `_wd` subdir so _scan_outputs ignores it."""
        wd = os.path.join(run["session_dir"], "_wd")
        os.makedirs(wd, exist_ok=True)
        return os.path.join(wd, f"{run['label']}_{run['angle']}.r{n}.mp4")

    def _watchdog(self, label: str, runs: List[Dict], stop_evt: threading.Event) -> None:
        """Restart any camera whose file stops growing (RTSP dropped, container
        alive-but-stalled) or whose container died — the others keep rolling."""
        poll = self.ff_poll if self.engine == "ffmpeg" else self.wd_poll
        seen = {r["angle"]: (-1, time.time()) for r in runs}   # gst only: angle -> (last_size, last_grow_t)
        while not stop_evt.wait(poll):
            for r in runs:
                try:
                    self._watch_one(r, seen)
                except Exception as e:  # noqa: BLE001 — a watchdog hiccup must never kill recording
                    logger.warning("watchdog error on %s: %s", r.get("angle"), e)

    def _watch_one_ffmpeg(self, run: Dict) -> None:
        """Restart a camera whose ffmpeg died OR whose encoded timeline stopped
        advancing (the frozen-timeline stall the byte-growth watchdog missed)."""
        angle = run["angle"]
        now = time.time()
        if now < run.get("retry_after", 0.0):      # in backoff after a recent restart — let it settle
            return
        p = run.get("proc")
        alive = bool(p and p.poll() is None)
        stalled = alive and (now - run["last_advance"]) > self.ff_stall
        if not (stalled or not alive) or run["restart"] >= self.wd_max_restarts:
            return
        # exponential backoff: reset the failure streak once a run recorded healthily
        # (out_time past ff_healthy_s), else grow it so a broken cam doesn't hammer.
        healthy = run.get("out_time", 0.0) >= self.ff_healthy_s
        run["consec_fail"] = 1 if healthy else run.get("consec_fail", 0) + 1
        delay = min(self.ff_backoff_base * (2 ** (run["consec_fail"] - 1)), self.ff_backoff_max)
        n = run["restart"] + 1
        logger.warning("watchdog: camera %s %s (out_time=%.1fs, idle=%.0fs) — restart r%d, "
                       "next retry backoff %.0fs (fail#%d)",
                       angle, "died" if not alive else "timeline-stalled",
                       run.get("out_time", 0.0), now - run["last_advance"], n, delay, run["consec_fail"])
        self._ff_finalize(run)                     # SIGINT -> clean moov on the pre-stall file
        run["path"] = self._seg_path(run, n)
        run["restart"] = n
        run["segments"].append(run["path"])
        run.setdefault("seg_starts", []).append(time.time())
        run["retry_after"] = time.time() + delay   # gate the NEXT restart, not this respawn
        if not self._launch_ffmpeg(run):
            logger.error("watchdog relaunch (ffmpeg) failed for %s", angle)
            return
        with self._wd_lock:
            sess = self._sessions.get(run["label"])
        self._ff_write_journal(run["label"], sess["runs"] if sess else [run])

    def _watch_one(self, run: Dict, seen: Dict) -> None:
        if self.engine == "ffmpeg":
            self._watch_one_ffmpeg(run)
            return
        angle = run["angle"]
        alive = run["name"] in self._running()
        try:
            size = os.path.getsize(run["path"])
        except OSError:
            size = -1
        last_size, last_grow = seen[angle]
        now = time.time()
        if size > last_size:                      # growing normally
            seen[angle] = (size, now)
            return
        stalled = alive and (now - last_grow) > self.wd_stall
        if not (stalled or not alive) or run["restart"] >= self.wd_max_restarts:
            return
        n = run["restart"] + 1
        logger.warning("watchdog: camera %s %s — restarting to segment r%d",
                       angle, "died" if not alive else "stalled", n)
        if alive:   # SIGINT -> gst `-e` EOS -> mp4mux finalizes the pre-drop file
            self._docker("kill", "--signal=INT", run["name"])
            for _ in range(8):
                if run["name"] not in self._running():
                    break
                time.sleep(1)
        self._docker("kill", run["name"])          # force-remove if it lingered (--rm cleans)
        new_path = self._seg_path(run, n)
        new_name = f"{self._container_name(run['label'], angle)}_r{n}"
        cp = subprocess.run(self._docker_run_cmd(new_name, run["cam"], new_path),
                            capture_output=True, text=True, stdin=subprocess.DEVNULL)
        if cp.returncode != 0:
            logger.error("watchdog relaunch failed for %s: %s", angle, cp.stderr.strip()[-200:])
            return
        run["restart"] = n
        run["name"] = new_name
        run["path"] = new_path
        run["segments"].append(new_path)
        run.setdefault("seg_starts", []).append(time.time())
        seen[angle] = (-1, time.time())

    def _stop_watchdog(self, label: str) -> None:
        with self._wd_lock:
            sess = self._sessions.get(label)
        if not sess:
            return
        sess["stop_evt"].set()
        t = sess.get("thread")
        if t and t.is_alive():
            t.join(timeout=self.wd_poll + 3)

    def _seg_ok(self, path: str) -> bool:
        """A segment is usable only if ffprobe can read a positive duration — a
        crashed (SIGKILL'd) container leaves an unfinalized mp4 with no moov, which
        must be DROPPED, not fed to concat (it would break the whole concat)."""
        if not (os.path.isfile(path) and os.path.getsize(path) > 0):
            return False
        r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "csv=p=0", path], capture_output=True, text=True,
                           stdin=subprocess.DEVNULL)
        try:
            return float(r.stdout.strip()) > 0
        except (ValueError, TypeError):
            return False

    def _seg_duration(self, path: str) -> Optional[float]:
        cp = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                             "-of", "csv=p=0", path], capture_output=True, text=True,
                            stdin=subprocess.DEVNULL)
        try:
            return float(cp.stdout.strip())
        except (ValueError, TypeError):
            return None

    def _stall_gaps(self, run: Dict, segs: List[str],
                    durs: List[Optional[float]]) -> List[float]:
        """Wall-clock seconds LOST before each segment (index-aligned with `segs`;
        entry 0 is always 0.0).

        A watchdog restart takes real time — the stall is detected after
        REC_FFMPEG_STALL_SEC of frozen timeline, then ffmpeg is finalized,
        respawned and has to re-preroll RTSP. That is ~20s of the game during
        which this camera recorded nothing. Butting the segments together
        DELETES that time from the angle's timeline, which is why one angle
        ends up ~20s shorter than the other four and slides progressively out
        of sync with them (annotator reports, 2026-08-17 and 2026-08-19).

        Preferred source is the `seg_starts` stamped at restart; a segment with
        no stamp (e.g. runs recovered from the pid journal after a service
        restart) falls back to mtime - duration, which is the moment ffmpeg
        began writing it."""
        starts_by_path = dict(zip(run.get("segments", []), run.get("seg_starts", [])))

        def started_at(path: str, dur: Optional[float]) -> Optional[float]:
            t = starts_by_path.get(path)
            if t is not None:      # not `if t:` — a 0.0 stamp is a real timestamp
                return float(t)
            try:                       # fallback: finalize time minus what it holds
                return os.path.getmtime(path) - (dur or 0.0)
            except OSError:
                return None

        gaps = [0.0]
        for i in range(1, len(segs)):
            prev_start = started_at(segs[i - 1], durs[i - 1])
            this_start = started_at(segs[i], durs[i])
            prev_dur = durs[i - 1]
            if prev_start is None or this_start is None or prev_dur is None:
                gaps.append(0.0)
                continue
            gap = this_start - (prev_start + prev_dur)
            # Clamp: sub-frame jitter is not a gap, and a clock glitch must never
            # be able to inject an absurd hole into a master.
            gaps.append(gap if MIN_STALL_GAP_SEC <= gap <= MAX_STALL_GAP_SEC else 0.0)
        return gaps

    def _concat_segments(self, label: str) -> Dict[str, Dict]:
        """After stop: for any camera the watchdog restarted, concat its READABLE
        segments [master + _wd/.rN] back into the single master {label}_{angle}.mp4
        so ingest sees one file per camera (an unfinalized crashed segment is
        dropped, the recovery kept).

        The stall window between segments is PRESERVED, not deleted: each file's
        `duration` directive is widened by its successor's gap, so the concat
        demuxer starts that successor at its true offset. `-c copy` still holds —
        the master keeps a real hole exactly where the camera was down, the same
        way it already keeps holes for frames lost to RTSP packet loss, and every
        angle stays on one shared timeline.

        Returns {angle: {segments, restarts, inserted_s, gaps}} for the caller to
        surface on the session/ingestion record."""
        with self._wd_lock:
            sess = self._sessions.get(label)
        if not sess:
            return {}
        report: Dict[str, Dict] = {}
        for run in sess["runs"]:
            if len(run["segments"]) <= 1:
                continue                       # never restarted — master is fine as-is
            segs = [p for p in run["segments"] if self._seg_ok(p)]
            master = os.path.join(run["session_dir"], f"{label}_{run['angle']}.mp4")
            if not segs:
                logger.error("watchdog: no readable segments for %s — footage lost", run["angle"])
                continue
            durs = [self._seg_duration(p) for p in segs]
            gaps = self._stall_gaps(run, segs, durs)
            inserted = round(sum(gaps), 3)
            tmp, lst = master + ".concat.mp4", master + ".concat.txt"
            try:
                with open(lst, "w") as f:
                    for i, p in enumerate(segs):
                        f.write(f"file '{p}'\n")
                        # `duration` sets where the NEXT file starts, so widening it
                        # by that file's gap re-opens the stall window. Only useful
                        # when a successor exists and we know this file's length.
                        nxt = gaps[i + 1] if i + 1 < len(gaps) else 0.0
                        if nxt > 0.0 and durs[i]:
                            f.write(f"duration {durs[i] + nxt:.6f}\n")
                cp = subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y",
                                     "-f", "concat", "-safe", "0", "-i", lst, "-c", "copy", tmp],
                                    capture_output=True, text=True, stdin=subprocess.DEVNULL)
                if cp.returncode == 0 and os.path.isfile(tmp) and os.path.getsize(tmp) > 0:
                    for p in segs:
                        if p != master:
                            try:
                                os.unlink(p)
                            except OSError:
                                pass
                    os.replace(tmp, master)
                    report[run["angle"]] = {
                        "segments": len(segs), "restarts": run.get("restart", 0),
                        "inserted_s": inserted,
                        "gaps": [round(g, 3) for g in gaps[1:]]}
                    logger.info("watchdog: concatenated %d segments for %s "
                                "(re-inserted %.1fs of stall gap: %s)",
                                len(segs), run["angle"], inserted,
                                [round(g, 1) for g in gaps[1:]])
                else:
                    logger.error("watchdog concat failed for %s: %s", run["angle"],
                                 cp.stderr.strip()[-200:])
            except Exception as e:  # noqa: BLE001
                logger.error("watchdog concat error %s: %s", run["angle"], e)
            finally:
                try:
                    os.unlink(lst)
                except OSError:
                    pass
        return report

    def _cleanup_session(self, label: str) -> None:
        with self._wd_lock:
            self._sessions.pop(label, None)
        try:
            os.rmdir(os.path.join(self.cfg.output_dir, label, "_wd"))  # empty after concat
        except OSError:
            pass
        if self.engine == "ffmpeg":
            try:
                os.unlink(self._ff_journal_path(label))
            except OSError:
                pass

    def stop(self, label: str, outputs: Optional[List[Dict]] = None,
             finalize_timeout: int = 30) -> Dict[str, object]:
        self._stop_watchdog(label)  # stop self-healing before teardown (no restarts mid-stop)
        if self.engine == "ffmpeg":
            self._ff_stop(label, finalize_timeout)
        else:
            for name in self._session_containers(label):
                self._docker("kill", "--signal=INT", name)  # clean per-camera EOS
            deadline = time.monotonic() + finalize_timeout
            while time.monotonic() < deadline:
                if not self._session_containers(label):
                    break
                time.sleep(1)
            else:
                for name in self._session_containers(label):
                    self._docker("kill", name)  # force
        # merge any watchdog restart segments -> one master, stall gaps preserved
        concat = self._concat_segments(label)
        # probe only what was actually recorded: caller-supplied outputs, else
        # scan the session dir (also catches a camera that dropped mid-recording).
        if outputs is None:
            outputs = self._scan_outputs(label)
        results = [{**o, **_probe(o["path"]), **({"stall": concat[o["angle"]]}
                                                 if o["angle"] in concat else {})}
                   for o in outputs]
        skew = _check_angle_skew(results)
        audio = self._stop_audio(os.path.join(self.cfg.output_dir, label))
        self._cleanup_session(label)
        return {"label": label, "stopped_at": _utcnow(), "files": results,
                "audio": audio, "angle_skew_s": skew}

    def _scan_outputs(self, label: str) -> List[Dict]:
        """Discover recorded files in the session dir as [{angle, id, path}]."""
        session_dir = os.path.join(self.cfg.output_dir, label)
        found: List[Dict] = []
        if os.path.isdir(session_dir):
            for fn in sorted(os.listdir(session_dir)):
                if fn.endswith(".mp4") and fn.startswith(label + "_"):
                    angle = fn[len(label) + 1:-4]
                    cam = next((c for c in self.cfg.cameras if c.angle == angle), None)
                    found.append({"angle": angle, "id": cam.id if cam else "",
                                  "path": os.path.join(session_dir, fn)})
        return found

    # ---- host-side audio capture (court audio for cross-correlation sync) ----
    def _start_audio(self, label: str, camera_ids: Optional[List[str]],
                     session_dir: str) -> List[Dict]:
        """Best-effort: one host ffmpeg per camera capturing audio-only, running
        alongside the per-camera video containers. PIDs are journaled to
        <session>/audio_capture.json so stop() can finalize them even across a
        service restart. Never raises — audio must not break the video path."""
        if not RECORD_AUDIO:
            return []
        cams = self.cfg.cameras if not camera_ids else \
            [c for cid in camera_ids for c in [self.cfg.camera_by_id(cid)] if c]
        procs: List[Dict] = []
        for cam in cams:
            out = os.path.join(session_dir, f"{label}_{cam.angle}.m4a")
            try:
                p = subprocess.Popen(_audio_cmd(cam, self.cfg, out),
                                     stdin=subprocess.DEVNULL,
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
                procs.append({"angle": cam.angle, "pid": p.pid, "path": out})
            except OSError as e:  # noqa: BLE001
                logger.warning("audio capture failed to start (%s): %s", cam.angle, e)
        try:
            with open(os.path.join(session_dir, AUDIO_JOURNAL), "w") as f:
                json.dump(procs, f)
        except OSError:
            pass
        return procs

    def _stop_audio(self, session_dir: str) -> List[Dict]:
        """SIGINT each audio ffmpeg (clean finalize), reap, and return the files
        that actually captured data. Best-effort; never raises."""
        try:
            with open(os.path.join(session_dir, AUDIO_JOURNAL)) as f:
                procs = json.load(f)
        except (OSError, ValueError):
            return []
        for pr in procs:
            try:
                os.kill(pr["pid"], signal.SIGINT)
            except OSError:
                pass
        deadline = time.time() + 15
        while time.time() < deadline and any(_pid_alive(pr["pid"]) for pr in procs):
            time.sleep(0.5)
        for pr in procs:
            if _pid_alive(pr["pid"]):
                try:
                    os.kill(pr["pid"], signal.SIGKILL)
                except OSError:
                    pass
        return [{"angle": pr["angle"], "path": pr["path"]} for pr in procs
                if os.path.isfile(pr["path"]) and os.path.getsize(pr["path"]) > 1024]


def _check_angle_skew(results: List[Dict]) -> Optional[float]:
    """Alarm when the tracking angles disagree on how long the game was.

    Every tracking camera films the same wall-clock window, so their durations
    must match within a second or two. When one comes out materially shorter it
    lost real time mid-game, and the annotation editor CANNOT rescue that: it
    only carries a constant per-angle offset, while lost time is a step that
    misaligns everything after it. Loud on purpose — this exact failure sat
    undetected in two shipped games until the annotators found it by hand.

    Returns the spread in seconds (None when there is nothing to compare)."""
    durs = {r["angle"]: r["duration"] for r in results
            if r.get("ok") and r.get("duration") and r.get("angle") in TRACKING_ANGLES}
    if len(durs) < 2:
        return None
    lo, hi = min(durs.values()), max(durs.values())
    skew = round(hi - lo, 3)
    pretty = ", ".join(f"{a}={d:.1f}s" for a, d in sorted(durs.items()))
    if skew > ANGLE_SKEW_ALARM_SEC:
        short = min(durs, key=durs.get)
        logger.error("ANGLE SKEW %.1fs — %s is short (%s). These angles will NOT stay "
                     "in sync; a constant per-angle offset cannot fix lost time. "
                     "Check the watchdog restarts for this session.",
                     skew, short, pretty)
    else:
        logger.info("angle durations agree within %.2fs (%s)", skew, pretty)
    return skew


def _probe(path: str) -> Dict[str, object]:
    """Finalized-file check + metadata. Returns actual fps (avg_frame_rate — the
    real delivered rate, which for the frame-dropping FLIR capture differs from
    the nominal rate) and resolution, so the fps-parametric shot-detection
    pipeline gets the true capture rate rather than a hard-coded assumption."""
    if not os.path.isfile(path):
        return {"ok": False, "reason": "missing", "duration": None, "size": 0,
                "fps": None, "width": None, "height": None}
    size = os.path.getsize(path)
    cp = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=avg_frame_rate,width,height:format=duration",
         "-of", "json", path],
        capture_output=True, text=True, stdin=subprocess.DEVNULL)
    dur = fps = width = height = None
    try:
        info = json.loads(cp.stdout or "{}")
        dur = float(info.get("format", {}).get("duration"))
        st = (info.get("streams") or [{}])[0]
        width, height = st.get("width"), st.get("height")
        rate = st.get("avg_frame_rate") or ""
        if "/" in rate:
            num, den = rate.split("/")
            fps = round(float(num) / float(den), 3) if float(den) else None
    except (ValueError, TypeError, KeyError):
        pass
    return {"ok": dur is not None and size > 0, "duration": dur, "size": size,
            "fps": fps, "width": width, "height": height}


def _utcnow() -> str:
    # ISO8601 UTC 'Z'; avoids importing tz each call site
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="AGX recording controller")
    ap.add_argument("action", choices=["dry-run", "start", "stop", "status"])
    ap.add_argument("--label", required=True)
    ap.add_argument("--cameras", help="comma-separated camera ids (default: all)")
    ap.add_argument("--config", default=CONFIG_PATH)
    args = ap.parse_args()

    cfg = load_config(args.config)
    ctl = RecordingController(cfg)
    cam_ids = args.cameras.split(",") if args.cameras else None

    if args.action == "dry-run":
        p = ctl.plan(args.label, cam_ids)
        print("session_dir:", p["session_dir"])
        for r in p["runs"]:
            print(f"\n{r['angle']} ({r['id']}) -> {r['path']}")
            cmd = r.get("cmd") or _single_cam_ffmpeg(r["cam"], cfg, r["path"], ctl.ff_stimeout_us)
            print("  ", " ".join(shlex.quote(x) for x in cmd))
    elif args.action == "start":
        print(json.dumps(ctl.start(args.label, cam_ids), indent=2))
    elif args.action == "stop":
        print(json.dumps(ctl.stop(args.label), indent=2))
    elif args.action == "status":
        print(json.dumps({"label": args.label, "recording": ctl.is_recording(args.label)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

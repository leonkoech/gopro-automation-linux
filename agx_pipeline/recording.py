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
import subprocess
import sys
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


def _to_container_path(host_path: str, cfg: Config) -> str:
    """Map a host path under app_mount to the container's /app/data mount."""
    rel = os.path.relpath(host_path, cfg.app_mount)
    return os.path.join("/app/data", rel)


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
        return bool(self._session_containers(label))

    def _run_cmd(self, cam: Camera, session_dir: str, label: str) -> Dict[str, object]:
        """One camera's {angle, id, name, path, cmd} (cmd = full docker run)."""
        host_path = os.path.join(session_dir, f"{label}_{cam.angle}.mp4")
        container_path = _to_container_path(host_path, self.cfg)
        name = self._container_name(label, cam.angle)
        cmd = list(self.cfg.docker_cmd) + [
            "run", "-d", "--name", name, "--rm",
            "--privileged", "--runtime", "nvidia", "--net=host",
            "-v", f"{self.cfg.app_mount}:/app/data", "--workdir", "/app/data",
            self.cfg.docker_image,
        ] + _single_cam_gst(cam, self.cfg, container_path)
        return {"angle": cam.angle, "id": cam.id, "name": name, "path": host_path, "cmd": cmd}

    def plan(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        cams = self.cfg.cameras if not camera_ids else \
            [c for cid in camera_ids for c in [self.cfg.camera_by_id(cid)] if c]
        if not cams:
            raise ValueError("no valid cameras selected")
        session_dir = os.path.join(self.cfg.output_dir, label)
        return {"session_dir": session_dir,
                "runs": [self._run_cmd(c, session_dir, label) for c in cams]}

    def _launch(self, run: Dict) -> bool:
        cp = subprocess.run(run["cmd"], capture_output=True, text=True, stdin=subprocess.DEVNULL)
        if cp.returncode != 0:
            logger.error("docker run failed for %s: %s", run["angle"], cp.stderr.strip()[-300:])
            return False
        return True

    def start(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        if self.is_recording(label):
            raise RuntimeError(f"session {label} already recording")
        p = self.plan(label, camera_ids)
        os.makedirs(p["session_dir"], exist_ok=True)  # host side (best-effort)
        live = [r for r in p["runs"] if self._launch(r)]
        if not live:
            raise RuntimeError("no camera containers started")
        self._verify_and_retry(live)
        outputs = [{"angle": r["angle"], "id": r["id"], "path": r["path"]} for r in live]
        logger.info("recording started (per-camera) label=%s angles=%s",
                    label, [r["angle"] for r in live])
        return {"label": label, "container": f"{CONTAINER_PREFIX}{label}",
                "outputs": outputs, "started_at": _utcnow()}

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

    def stop(self, label: str, outputs: Optional[List[Dict]] = None,
             finalize_timeout: int = 30) -> Dict[str, object]:
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
        # probe only what was actually recorded: caller-supplied outputs, else
        # scan the session dir (also catches a camera that dropped mid-recording).
        if outputs is None:
            outputs = self._scan_outputs(label)
        results = [{**o, **_probe(o["path"])} for o in outputs]
        return {"label": label, "stopped_at": _utcnow(), "files": results}

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
            print("  ", " ".join(shlex.quote(x) for x in r["cmd"]))
    elif args.action == "start":
        print(json.dumps(ctl.start(args.label, cam_ids), indent=2))
    elif args.action == "stop":
        print(json.dumps(ctl.stop(args.label), indent=2))
    elif args.action == "status":
        print(json.dumps({"label": args.label, "recording": ctl.is_recording(args.label)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

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
def _rtsp_branch(cam: Camera, cfg: Config, out_path: str, idx: int) -> List[str]:
    """One rtspsrc->depay->parse->mp4mux->filesink branch (matches record_zowie.sh)."""
    url = f"rtsp://{cam.ip}:{cfg.rtsp_port}{cfg.rtsp_path}"
    name = f"r{idx}"
    return [
        "rtspsrc", f"location={url}", "protocols=tcp", f"name={name}",
        f"{name}.", "!", "application/x-rtp,media=video",
        "!", "queue", "max-size-buffers=30",
        "!", "rtph265depay", "!", "h265parse", "!", "mp4mux",
        "!", "filesink", f"location={out_path}",
    ]


def build_gst_args(cameras: List[Camera], cfg: Config, session_dir: str,
                   label: str) -> Dict[str, object]:
    """Return {'gst': [...args...], 'outputs': [{angle, id, path}, ...]}."""
    gst: List[str] = ["gst-launch-1.0", "-e"]
    outputs = []
    for i, cam in enumerate(cameras):
        # container sees session_dir under the app mount
        fname = f"{label}_{cam.angle}.mp4"
        host_path = os.path.join(session_dir, fname)
        container_path = _to_container_path(host_path, cfg)
        gst += _rtsp_branch(cam, cfg, container_path, i)
        outputs.append({"angle": cam.angle, "id": cam.id, "path": host_path})
    return {"gst": gst, "outputs": outputs}


def _to_container_path(host_path: str, cfg: Config) -> str:
    """Map a host path under app_mount to the container's /app/data mount."""
    rel = os.path.relpath(host_path, cfg.app_mount)
    return os.path.join("/app/data", rel)


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class RecordingController:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def _docker(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(list(self.cfg.docker_cmd) + list(args),
                              capture_output=True, text=True, stdin=subprocess.DEVNULL)

    def container_name(self, label: str) -> str:
        return CONTAINER_PREFIX + label

    def is_recording(self, label: str) -> bool:
        cp = self._docker("ps", "--filter", f"name=^{self.container_name(label)}$",
                          "--format", "{{.Names}}")
        return self.container_name(label) in cp.stdout

    def plan(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        cams = self.cfg.cameras if not camera_ids else \
            [c for cid in camera_ids for c in [self.cfg.camera_by_id(cid)] if c]
        if not cams:
            raise ValueError("no valid cameras selected")
        session_dir = os.path.join(self.cfg.output_dir, label)
        built = build_gst_args(cams, self.cfg, session_dir, label)
        docker_run = list(self.cfg.docker_cmd) + [
            "run", "-d", "--name", self.container_name(label), "--rm",
            "--privileged", "--runtime", "nvidia", "--net=host",
            "-v", f"{self.cfg.app_mount}:/app/data", "--workdir", "/app/data",
            self.cfg.docker_image,
        ] + built["gst"]
        return {"session_dir": session_dir, "outputs": built["outputs"],
                "docker_run": docker_run}

    def start(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        if self.is_recording(label):
            raise RuntimeError(f"session {label} already recording")
        p = self.plan(label, camera_ids)
        os.makedirs(p["session_dir"], exist_ok=True)  # host side (best-effort)
        cp = subprocess.run(p["docker_run"], capture_output=True, text=True,
                            stdin=subprocess.DEVNULL)
        if cp.returncode != 0:
            raise RuntimeError(f"docker run failed: {cp.stderr.strip()[-400:]}")
        time.sleep(2)
        if not self.is_recording(label):
            logs = self._docker("logs", self.container_name(label)).stdout[-400:]
            raise RuntimeError(f"container did not stay up. logs: {logs}")
        return {"label": label, "container": self.container_name(label),
                "outputs": p["outputs"], "started_at": _utcnow()}

    def stop(self, label: str, outputs: Optional[List[Dict]] = None,
             finalize_timeout: int = 30) -> Dict[str, object]:
        name = self.container_name(label)
        self._docker("kill", "--signal=INT", name)  # clean EOS
        # wait for container to exit (files finalize during shutdown)
        for _ in range(finalize_timeout):
            if not self.is_recording(label):
                break
            time.sleep(1)
        else:
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
        print("outputs:", json.dumps(p["outputs"], indent=2))
        print("\ndocker command:\n ", " ".join(shlex.quote(x) for x in p["docker_run"]))
    elif args.action == "start":
        print(json.dumps(ctl.start(args.label, cam_ids), indent=2))
    elif args.action == "stop":
        print(json.dumps(ctl.stop(args.label), indent=2))
    elif args.action == "status":
        print(json.dumps({"label": args.label, "recording": ctl.is_recording(args.label)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Z-CAM HTTP API poller — detects recording state transitions.

Runs a daemon thread per camera, polling GET http://{ip}/ctrl/rec
to track when recording starts and stops. When a recording ends
(st changes 1→0), triggers a callback with the latest file list.

Z-CAM API reference:
https://github.com/imaginevision/Z-Camera-Doc/tree/master/E2/protocol
"""

import re
import threading
import time
import logging
import requests
from typing import Callable, Dict, List, Optional

from ..config import ZCamCamera, ZCamConfig

logger = logging.getLogger('zcam.camera_watcher')


class CameraState:
    UNKNOWN = 'unknown'
    IDLE = 'idle'
    RECORDING = 'recording'
    ERROR = 'error'


class ZCamApiPoller:
    """Polls Z-CAM REST API for recording state changes."""

    def __init__(self, config: ZCamConfig,
                 on_recording_ended: Optional[Callable] = None):
        """
        Args:
            config: Z-CAM configuration
            on_recording_ended: callback(camera: ZCamCamera, files: list[dict])
                called when a camera transitions from recording → idle
        """
        self.config = config
        self.on_recording_ended = on_recording_ended
        self._states: Dict[str, str] = {}
        self._stop_event = threading.Event()
        self._threads: Dict[str, threading.Thread] = {}

    def start(self):
        """Start polling all cameras."""
        for cam in self.config.cameras:
            self._states[cam.cam_id] = CameraState.UNKNOWN
            t = threading.Thread(
                target=self._poll_camera,
                args=(cam,),
                daemon=True,
                name=f'zcam-poller-{cam.cam_id}',
            )
            t.start()
            self._threads[cam.cam_id] = t
        logger.info(f"Started polling {len(self.config.cameras)} Z-CAM cameras")

    def stop(self):
        """Stop all polling threads."""
        self._stop_event.set()
        for t in self._threads.values():
            t.join(timeout=5)
        logger.info("All Z-CAM pollers stopped")

    def _poll_camera(self, camera: ZCamCamera):
        """Poll a single camera in a loop."""
        prev_state = CameraState.UNKNOWN

        while not self._stop_event.is_set():
            try:
                current_state = self._get_recording_state(camera)
                self._states[camera.cam_id] = current_state

                # Detect recording → idle transition
                if prev_state == CameraState.RECORDING and current_state == CameraState.IDLE:
                    logger.info(f"Recording ended on {camera.cam_id} ({camera.label})")
                    self._handle_recording_ended(camera)

                prev_state = current_state

            except requests.RequestException as e:
                logger.error(f"Network error polling {camera.cam_id} ({camera.ip}): {e}")
                self._states[camera.cam_id] = CameraState.ERROR
            except Exception as e:
                logger.error(f"Unexpected error polling {camera.cam_id}: {e}")
                self._states[camera.cam_id] = CameraState.ERROR

            self._stop_event.wait(self.config.poll_interval_sec)

    def _get_recording_state(self, camera: ZCamCamera) -> str:
        """GET http://{ip}/ctrl/rec → parse recording state.

        Z-CAM returns: {"code":0,"msg":"","st":0}
        st: 0 = idle, 1 = recording
        """
        resp = requests.get(f'http://{camera.ip}/ctrl/rec', timeout=5)
        resp.raise_for_status()
        data = resp.json()
        st = data.get('st', 0)
        return CameraState.RECORDING if st == 1 else CameraState.IDLE

    def _handle_recording_ended(self, camera: ZCamCamera):
        """Fetch file list and trigger callback."""
        try:
            files = self._list_latest_files(camera)
            if self.on_recording_ended and files:
                # Pass the most recently created file (last in list)
                self.on_recording_ended(camera, files[-1:])
            elif not files:
                logger.warning(f"No files found on {camera.cam_id} after recording ended")
        except Exception as e:
            logger.error(f"Error fetching files from {camera.cam_id}: {e}")

    def _list_latest_files(self, camera: ZCamCamera) -> List[dict]:
        """GET http://{ip}/DCIM/ → list recording files.

        Z-CAM serves an HTML directory listing. We parse it to extract
        file paths for .MOV/.MP4 files.
        """
        resp = requests.get(f'http://{camera.ip}/DCIM/', timeout=10)
        resp.raise_for_status()

        # First, find all folders (e.g., 100ZCAM, 101ZCAM)
        folders = re.findall(r'href="(/DCIM/([^/"]+)/)"', resp.text)
        if not folders:
            # Try direct file listing
            return self._parse_file_listing(resp.text, camera)

        # Get files from the last folder (most recent recordings)
        all_files = []
        for folder_path, folder_name in folders:
            try:
                folder_resp = requests.get(
                    f'http://{camera.ip}{folder_path}', timeout=10
                )
                folder_resp.raise_for_status()
                files = self._parse_file_listing(folder_resp.text, camera, folder_name)
                all_files.extend(files)
            except Exception as e:
                logger.error(f"Error listing folder {folder_name} on {camera.cam_id}: {e}")

        return all_files

    def _parse_file_listing(self, html: str, camera: ZCamCamera,
                            folder: str = "") -> List[dict]:
        """Parse Z-CAM DCIM listing into file info dicts."""
        files = []
        # Match: href="/DCIM/100ZCAM/Z0001.MOV" or relative paths
        for match in re.finditer(r'href="(/DCIM/([^/]+)/([^"]+))"', html):
            full_path, file_folder, filename = match.groups()
            if filename.lower().endswith(('.mov', '.mp4')):
                files.append({
                    'folder': file_folder,
                    'filename': filename,
                    'url': f'http://{camera.ip}{full_path}',
                    'cam_id': camera.cam_id,
                })

        # Also try simpler patterns for flat listings
        if not files and folder:
            for match in re.finditer(r'href="([^"]+\.(mov|mp4))"', html, re.IGNORECASE):
                filename = match.group(1).split('/')[-1]
                files.append({
                    'folder': folder,
                    'filename': filename,
                    'url': f'http://{camera.ip}/DCIM/{folder}/{filename}',
                    'cam_id': camera.cam_id,
                })

        return files

    def get_all_states(self) -> dict:
        """Return current state of all cameras."""
        return dict(self._states)

    def get_camera_state(self, cam_id: str) -> str:
        """Return current state of a single camera."""
        return self._states.get(cam_id, CameraState.UNKNOWN)

"""
Thread-safe MJPEG frame buffer and stream generator.

The GStreamer appsink callback writes JPEG frames into FrameBuffer.
Flask routes read frames out via generate_mjpeg_stream().

Pattern matches LogBuffer in logging_service.py — thread-safe
container with update/read semantics using Lock + Event.
"""

import threading
import time
import logging
from typing import Optional, Generator

logger = logging.getLogger('zcam.flask_viewer')


class FrameBuffer:
    """Thread-safe single-frame buffer for MJPEG streaming.

    One instance per camera. The GStreamer appsink callback calls
    update() with new JPEG bytes; Flask consumers call wait_for_frame()
    to block until a new frame is available.
    """

    def __init__(self, cam_id: str):
        self.cam_id = cam_id
        self._frame: Optional[bytes] = None
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._timestamp: float = 0.0
        self._frame_count: int = 0

    def update(self, jpeg_bytes: bytes):
        """Called by GStreamer appsink callback with a new JPEG frame."""
        with self._lock:
            self._frame = jpeg_bytes
            self._timestamp = time.monotonic()
            self._frame_count += 1
        self._event.set()

    def get_frame(self) -> Optional[bytes]:
        """Get the latest frame (non-blocking)."""
        with self._lock:
            return self._frame

    def wait_for_frame(self, timeout: float = 2.0) -> Optional[bytes]:
        """Wait for a new frame, then return it."""
        self._event.wait(timeout=timeout)
        self._event.clear()
        return self.get_frame()

    @property
    def has_frame(self) -> bool:
        with self._lock:
            return self._frame is not None

    @property
    def age_seconds(self) -> float:
        """How stale the current frame is, in seconds."""
        with self._lock:
            if self._timestamp == 0:
                return float('inf')
            return time.monotonic() - self._timestamp

    @property
    def frame_count(self) -> int:
        with self._lock:
            return self._frame_count

    def get_status(self) -> dict:
        """Return buffer status for API responses."""
        with self._lock:
            return {
                'cam_id': self.cam_id,
                'has_frame': self._frame is not None,
                'frame_count': self._frame_count,
                'age_seconds': round(time.monotonic() - self._timestamp, 2) if self._timestamp else None,
                'frame_size_bytes': len(self._frame) if self._frame else 0,
            }


def generate_mjpeg_stream(frame_buffer: FrameBuffer) -> Generator[bytes, None, None]:
    """Generator yielding multipart MJPEG frames for a Flask Response.

    Usage in a route:
        return Response(
            generate_mjpeg_stream(frame_buffer),
            mimetype='multipart/x-mixed-replace; boundary=frame'
        )

    The <img> tag in the frontend consumes this as a continuous MJPEG stream.
    """
    BOUNDARY = b'--frame\r\n'

    while True:
        frame = frame_buffer.wait_for_frame(timeout=2.0)
        if frame is None:
            # No frame yet — yield empty to keep connection alive
            continue
        yield (
            BOUNDARY
            + b'Content-Type: image/jpeg\r\n'
            + b'Content-Length: ' + str(len(frame)).encode() + b'\r\n'
            + b'\r\n'
            + frame
            + b'\r\n'
        )

"""
Core GStreamer pipeline manager for Z-CAM cameras.

Builds NDI-receive → encode → tee (KVS + appsink) pipelines.
One pipeline per camera, each running in its own thread.

All gi (GStreamer Python) imports are deferred to method bodies so this
module can be imported on machines without GStreamer installed.
"""

import threading
import time
import logging
from typing import Optional, Callable, Dict

from ..config import ZCamCamera, ZCamConfig

logger = logging.getLogger('zcam.gstreamer.pipeline')


class PipelineState:
    STOPPED = 'stopped'
    STARTING = 'starting'
    RUNNING = 'running'
    ERROR = 'error'
    RESTARTING = 'restarting'


class CameraPipeline:
    """Manages a single GStreamer pipeline for one Z-CAM camera."""

    def __init__(self, camera: ZCamCamera, config: ZCamConfig,
                 frame_callback: Optional[Callable] = None):
        self.camera = camera
        self.config = config
        self.frame_callback = frame_callback  # Called with JPEG bytes from appsink
        self.pipeline = None
        self.state = PipelineState.STOPPED
        self._lock = threading.Lock()
        self._restart_count = 0
        self._max_restarts = 5
        self._main_loop = None
        self._loop_thread = None

    def build_pipeline_string(self) -> str:
        """Build GStreamer pipeline string based on platform."""
        from . import nvenc, software_enc

        cam = self.camera
        ndi_src = f'ndisrc ndi-name="{cam.ndi_name}" ! ndisrcdemux name=demux_{cam.cam_id}'

        if self.config.platform == 'jetson':
            encoder = nvenc.get_encoder_string(self.config)
        else:
            encoder = software_enc.get_encoder_string(self.config)

        kvs_sink = (
            f'kvssink stream-name={cam.kvs_stream_name} '
            f'aws-region={self.config.kvs_region}'
        )

        appsink_name = f'flask_sink_{cam.cam_id}'

        # Pipeline: NDI → encode → tee
        #   tee branch 1: → KVS
        #   tee branch 2: → decode → JPEG → appsink (for MJPEG viewer)
        pipeline = (
            f'{ndi_src} '
            f'demux_{cam.cam_id}.video ! queue ! videoconvert ! {encoder} '
            f'! h264parse ! tee name=t_{cam.cam_id} '
            # KVS branch
            f't_{cam.cam_id}. ! queue leaky=downstream ! {kvs_sink} '
            # MJPEG branch: decode H264 back to raw, then JPEG encode for Flask
            f't_{cam.cam_id}. ! queue leaky=downstream ! decodebin ! videoconvert '
            f'! videorate ! video/x-raw,framerate={self.config.mjpeg_max_fps}/1 '
            f'! jpegenc quality={self.config.mjpeg_quality} '
            f'! appsink name={appsink_name} emit-signals=true max-buffers=2 drop=true'
        )
        return pipeline

    def start(self):
        """Start the GStreamer pipeline."""
        import gi
        gi.require_version('Gst', '1.0')
        from gi.repository import Gst, GLib

        with self._lock:
            if self.state == PipelineState.RUNNING:
                return
            self.state = PipelineState.STARTING

        pipeline_str = self.build_pipeline_string()
        logger.info(f"Starting pipeline for {self.camera.cam_id}")
        logger.debug(f"Pipeline string: {pipeline_str}")

        try:
            self.pipeline = Gst.parse_launch(pipeline_str)
        except GLib.Error as e:
            logger.error(f"Failed to parse pipeline for {self.camera.cam_id}: {e}")
            with self._lock:
                self.state = PipelineState.ERROR
            return

        # Connect appsink signal for MJPEG frames
        appsink = self.pipeline.get_by_name(f'flask_sink_{self.camera.cam_id}')
        if appsink and self.frame_callback:
            appsink.connect('new-sample', self._on_new_sample)

        # Watch bus for errors and EOS
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message::error', self._on_error)
        bus.connect('message::eos', self._on_eos)
        bus.connect('message::state-changed', self._on_state_changed)

        # Start GLib main loop if not already running (needed for bus signals)
        if self._main_loop is None:
            self._main_loop = GLib.MainLoop()
            self._loop_thread = threading.Thread(
                target=self._main_loop.run,
                daemon=True,
                name=f'glib-loop-{self.camera.cam_id}'
            )
            self._loop_thread.start()

        self.pipeline.set_state(Gst.State.PLAYING)

        with self._lock:
            self.state = PipelineState.RUNNING
            self._restart_count = 0

        logger.info(f"Pipeline running for {self.camera.cam_id}")

    def stop(self):
        """Stop the GStreamer pipeline."""
        import gi
        gi.require_version('Gst', '1.0')
        from gi.repository import Gst

        with self._lock:
            if self.pipeline:
                self.pipeline.set_state(Gst.State.NULL)
                self.pipeline = None
            if self._main_loop and self._main_loop.is_running():
                self._main_loop.quit()
            self._main_loop = None
            self.state = PipelineState.STOPPED

        logger.info(f"Pipeline stopped for {self.camera.cam_id}")

    def restart(self):
        """Restart pipeline with exponential backoff."""
        with self._lock:
            if self._restart_count >= self._max_restarts:
                self.state = PipelineState.ERROR
                logger.error(f"Max restarts ({self._max_restarts}) reached for {self.camera.cam_id}")
                return
            self._restart_count += 1
            count = self._restart_count
            self.state = PipelineState.RESTARTING

        delay = min(2 ** count, 30)
        logger.warning(
            f"Restarting pipeline for {self.camera.cam_id} "
            f"(attempt {count}/{self._max_restarts}, delay {delay}s)"
        )
        self.stop()
        time.sleep(delay)
        self.start()

    def _on_new_sample(self, appsink):
        """Handle new frame from appsink — extract JPEG and forward."""
        import gi
        gi.require_version('Gst', '1.0')
        from gi.repository import Gst

        sample = appsink.emit('pull-sample')
        if sample:
            buf = sample.get_buffer()
            success, map_info = buf.map(Gst.MapFlags.READ)
            if success:
                jpeg_bytes = bytes(map_info.data)
                buf.unmap(map_info)
                if self.frame_callback:
                    self.frame_callback(jpeg_bytes)
        return Gst.FlowReturn.OK

    def _on_error(self, bus, msg):
        """Handle pipeline error — trigger restart in background."""
        err, debug = msg.parse_error()
        logger.error(f"Pipeline error for {self.camera.cam_id}: {err}")
        logger.debug(f"Debug info: {debug}")
        threading.Thread(target=self.restart, daemon=True).start()

    def _on_eos(self, bus, msg):
        """Handle end of stream — trigger restart."""
        logger.warning(f"EOS received for {self.camera.cam_id}")
        threading.Thread(target=self.restart, daemon=True).start()

    def _on_state_changed(self, bus, msg):
        """Log pipeline state changes."""
        if msg.src == self.pipeline:
            old, new, pending = msg.parse_state_changed()
            logger.debug(
                f"Pipeline {self.camera.cam_id} state: "
                f"{old.value_nick} → {new.value_nick}"
            )

    def get_status(self) -> dict:
        """Return current pipeline status as a dict."""
        return {
            'cam_id': self.camera.cam_id,
            'label': self.camera.label,
            'state': self.state,
            'restart_count': self._restart_count,
        }


class PipelineManager:
    """Manages all camera pipelines."""

    def __init__(self, config: ZCamConfig):
        self.config = config
        self.pipelines: Dict[str, CameraPipeline] = {}
        self._gst_initialized = False

    def initialize(self, frame_buffers: dict):
        """Create pipelines for all cameras.

        Args:
            frame_buffers: dict of cam_id -> FrameBuffer instance
        """
        if not self._gst_initialized:
            import gi
            gi.require_version('Gst', '1.0')
            from gi.repository import Gst
            Gst.init(None)
            self._gst_initialized = True

        for cam in self.config.cameras:
            fb = frame_buffers.get(cam.cam_id)
            callback = fb.update if fb else None
            self.pipelines[cam.cam_id] = CameraPipeline(cam, self.config, callback)

        logger.info(f"Initialized {len(self.pipelines)} camera pipelines")

    def start_all(self):
        """Start all pipelines, each in its own thread."""
        for p in self.pipelines.values():
            threading.Thread(
                target=p.start, daemon=True,
                name=f'pipeline-start-{p.camera.cam_id}'
            ).start()

    def stop_all(self):
        """Stop all pipelines."""
        for p in self.pipelines.values():
            p.stop()

    def start_camera(self, cam_id: str):
        """Start a single camera's pipeline."""
        if cam_id in self.pipelines:
            self.pipelines[cam_id].start()

    def stop_camera(self, cam_id: str):
        """Stop a single camera's pipeline."""
        if cam_id in self.pipelines:
            self.pipelines[cam_id].stop()

    def get_all_status(self) -> list:
        """Return status of all pipelines."""
        return [p.get_status() for p in self.pipelines.values()]

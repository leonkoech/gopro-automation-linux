"""
NVENC hardware encoder configuration for Jetson Nano.

Uses nvv4l2h264enc which is available via JetPack.
Do NOT use x264enc on Jetson — always use hardware encoding.
"""

from ..config import ZCamConfig


def get_encoder_string(config: ZCamConfig) -> str:
    """Return GStreamer encoder elements for Jetson NVENC.

    Pipeline fragment: videoconvert output → h264 encoded output
    Requires: nvvidconv and nvv4l2h264enc (included with JetPack 4.6+)
    Verify with: gst-inspect-1.0 nvv4l2h264enc
    """
    return (
        f'nvvidconv ! nvv4l2h264enc '
        f'bitrate={config.bitrate} '
        f'iframeinterval={config.iframe_interval}'
    )

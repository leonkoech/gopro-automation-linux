"""
x264enc software encoder configuration for Orange Pi 3 LTE.

No NVENC available — uses CPU encoding with ultrafast preset
to minimize CPU load alongside the live pipeline.
"""

from ..config import ZCamConfig


def get_encoder_string(config: ZCamConfig) -> str:
    """Return GStreamer encoder elements for x264enc software encoding.

    Pipeline fragment: videoconvert output → h264 encoded output
    Requires: gstreamer1.0-plugins-ugly (sudo apt install gstreamer1.0-plugins-ugly x264)
    Verify with: gst-inspect-1.0 x264enc
    """
    return (
        f'x264enc tune=zerolatency '
        f'bitrate={config.sw_bitrate} '
        f'speed-preset=ultrafast '
        f'key-int-max={config.iframe_interval}'
    )

"""
Z-CAM Streaming Pipeline Blueprint

Self-contained module for Z-CAM E2-N camera streaming:
- NDI|HX receive → GStreamer encode → AWS KVS + MJPEG viewer
- Z-CAM HTTP API polling for recording state detection
- 4K recording download and S3 upload

Registered as a Flask Blueprint on /api/zcam/*
"""

from flask import Blueprint

zcam_bp = Blueprint('zcam', __name__, url_prefix='/api/zcam')

# Import routes to register them on the blueprint
from . import routes  # noqa: E402, F401

"""Windowed high-fps make/miss shot detection for the scorekeeper-trigger
validation (Phase 1). See docs/SHOT_DETECTION_PHASE1_PLAN.md.

`logic` (pure numpy) is importable with no ML deps; `ShotDetector` lazily
imports torch/ultralytics on construction.
"""
from agx_pipeline.shot_detect import logic  # noqa: F401

__all__ = ["logic"]

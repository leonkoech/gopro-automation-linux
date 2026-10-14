"""agx_pipeline.shot_who_live must import as part of the package.

It imported `shot_typing_live` without the package prefix, so every made shot logged
"who enqueue failed: No module named 'shot_typing_live'" (14 Oct live test, 50+ times) even
though the feature is off (SHOT_LIVE_WHO defaults to false).
"""
import importlib

import pytest


@pytest.mark.unit
def test_shot_who_live_imports_and_is_off_by_default(monkeypatch):
    monkeypatch.delenv("SHOT_LIVE_WHO", raising=False)
    mod = importlib.import_module("agx_pipeline.shot_who_live")
    assert mod.who_enabled() is False

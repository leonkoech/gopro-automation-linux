"""The shot detector uses the TensorRT engine only when it is safe, else the .pt.

An engine runs only on the device + TensorRT version that built it and only for the shapes it was built
for. A wrong one does not always raise: an engine built for 640x640 alone returned garbage for the
detector's 640x480 frames (14 Oct 2026: 39 of 3512 ball frames). So it must load AND answer a
real-shaped batch, and every other case must fall back to the .pt with the reason logged.
"""
import importlib
import sys
import types

import pytest


class _FakeYOLO:
    loaded = []
    fail_on = {}          # path -> exception raised at load ("load") or at predict ("predict")

    def __init__(self, path, task=None):
        if self.fail_on.get(path) == "load":
            raise RuntimeError("engine deserialization failed")
        self.path = path
        _FakeYOLO.loaded.append(path)

    def predict(self, frames, **kw):
        if self.fail_on.get(self.path) == "predict":
            raise RuntimeError("input shape outside the optimization profile")
        return []


@pytest.fixture
def detect(monkeypatch, tmp_path):
    torch = types.SimpleNamespace(
        backends=types.SimpleNamespace(cudnn=types.SimpleNamespace(benchmark=True),
                                       mps=types.SimpleNamespace(is_available=lambda: False)),
        cuda=types.SimpleNamespace(is_available=lambda: True))
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "ultralytics", types.SimpleNamespace(YOLO=_FakeYOLO))
    monkeypatch.setenv("SHOT_DET_IMGSZ", "640")
    monkeypatch.setenv("SHOT_DET_BATCH", "16")
    _FakeYOLO.loaded, _FakeYOLO.fail_on = [], {}
    engine = tmp_path / "ball.engine"
    engine.write_bytes(b"x")
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))
    return mod, str(engine)


@pytest.mark.unit
def test_engine_is_used_when_it_loads_and_answers(detect, monkeypatch):
    mod, engine = detect
    monkeypatch.setenv("SHOT_DET_ENGINE", engine)
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "tensorrt" and det.model._engine.path == engine
    assert "ball.pt" not in _FakeYOLO.loaded


@pytest.mark.unit
def test_no_setting_means_the_pt(detect, monkeypatch):
    mod, _ = detect
    monkeypatch.delenv("SHOT_DET_ENGINE", raising=False)
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "pytorch" and det.model.path == "ball.pt"


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["load", "predict"])
def test_an_engine_that_fails_falls_back_to_the_pt(detect, monkeypatch, caplog, failure):
    mod, engine = detect
    monkeypatch.setenv("SHOT_DET_ENGINE", engine)
    _FakeYOLO.fail_on = {engine: failure}
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "pytorch" and det.model.path == "ball.pt"
    assert "not used" in caplog.text


@pytest.mark.unit
def test_missing_engine_file_falls_back(detect, monkeypatch, caplog):
    mod, _ = detect
    monkeypatch.setenv("SHOT_DET_ENGINE", "/nonexistent/ball.engine")
    assert mod.ShotDetector("ball.pt").backend == "pytorch"
    assert "file not found" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize("env,value", [("SHOT_DET_IMGSZ", "1280"), ("SHOT_DET_BATCH", "32")])
def test_shapes_beyond_the_engine_fall_back(monkeypatch, tmp_path, detect, env, value):
    _, engine = detect
    monkeypatch.setenv(env, value)
    monkeypatch.setenv("SHOT_DET_ENGINE", engine)
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))
    assert mod.ShotDetector("ball.pt").backend == "pytorch"


@pytest.mark.unit
def test_an_engine_failing_mid_game_switches_to_the_pt(detect, monkeypatch, caplog):
    mod, engine = detect
    monkeypatch.setenv("SHOT_DET_ENGINE", engine)
    det = mod.ShotDetector("ball.pt")                 # loads and passes its probe
    assert det.backend == "tensorrt"
    _FakeYOLO.fail_on = {engine: "predict"}           # ...then fails during the game
    assert det.model.predict(["frame"]) == []          # this call is answered by the .pt
    assert det.backend == "pytorch" and "ball.pt" in _FakeYOLO.loaded
    assert "failed mid-run" in caplog.text
    _FakeYOLO.fail_on = {}
    det.model.predict(["frame"])
    assert _FakeYOLO.loaded.count("ball.pt") == 1      # loaded once, kept

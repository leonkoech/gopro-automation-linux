"""The shot detector uses the TensorRT engine only when it is safe, else the .pt.

An engine runs only on the device + TensorRT version that built it and only for the shapes it was built
for. A wrong one does not always raise: an engine built for 640x640 alone found 39 of 3512 ball frames
(14 Oct 2026). So it must find the probe frame's ball where the .pt does; calls it was not built for go
to the .pt; a failure mid-run switches to the .pt; and every call is serialised.
"""
import importlib
import json
import os
import sys
import threading
import time
import types

import pytest

PROBE = json.load(open(os.path.join(os.path.dirname(__file__), "..", "agx_pipeline", "shot_detect",
                                    "probe", "probe_ball.json")))


class _T:
    def __init__(self, v):
        self.v = v

    def item(self):
        return self.v


class _Box:
    def __init__(self, cx, cy):
        self.cls, self.conf = _T(0), _T(0.9)
        self.xyxy = [(cx - 5, cy - 5, cx + 5, cy + 5)]


def _result(ball):
    return types.SimpleNamespace(boxes=[_Box(*ball)] if ball else [])


class _FakeYOLO:
    loaded, calls = [], []
    mode = {}            # path -> "load" | "predict" | "garbage" | "offset" | "slow"
    inside = 0
    overlap = False

    def __init__(self, path, task=None):
        if self.mode.get(path) == "load":
            raise RuntimeError("engine deserialization failed")
        self.path = path
        _FakeYOLO.loaded.append(path)

    def predict(self, frames, imgsz=None, **kw):
        _FakeYOLO.calls.append((self.path, imgsz, len(frames)))
        m = self.mode.get(self.path)
        if m == "predict":
            raise RuntimeError("input shape outside the optimization profile")
        if m == "slow":
            _FakeYOLO.inside += 1
            _FakeYOLO.overlap |= _FakeYOLO.inside > 1
            time.sleep(0.05)
            _FakeYOLO.inside -= 1
        ball = None if m == "garbage" else (
            (PROBE["ball_cx"] + 40, PROBE["ball_cy"]) if m == "offset" else (PROBE["ball_cx"], PROBE["ball_cy"]))
        return [_result(ball) for _ in frames]


@pytest.fixture
def detect(monkeypatch, tmp_path):
    torch = types.SimpleNamespace(
        backends=types.SimpleNamespace(cudnn=types.SimpleNamespace(benchmark=True),
                                       mps=types.SimpleNamespace(is_available=lambda: False)),
        cuda=types.SimpleNamespace(is_available=lambda: True))
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "ultralytics", types.SimpleNamespace(YOLO=_FakeYOLO))
    _FakeYOLO.loaded, _FakeYOLO.calls, _FakeYOLO.mode = [], [], {}
    _FakeYOLO.inside, _FakeYOLO.overlap = 0, False
    engine = tmp_path / "ball.engine"
    engine.write_bytes(b"x")
    monkeypatch.setenv("SHOT_DET_ENGINE", str(engine))
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))

    def probe_here(path):                     # the child process's work, done in-process for fakes
        try:
            return mod._probe_ok(_FakeYOLO(path), "cuda")
        except Exception as e:  # noqa: BLE001
            return "%s: %s" % (type(e).__name__, e)
    monkeypatch.setattr(mod, "_probe_in_subprocess", probe_here)
    return mod, str(engine)


@pytest.mark.unit
def test_engine_is_used_when_it_finds_the_probe_ball(detect):
    mod, engine = detect
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "tensorrt"
    assert "ball.pt" not in _FakeYOLO.loaded
    det.model.predict(["f"] * 16, imgsz=640)
    assert _FakeYOLO.calls[-1] == (engine, 640, 16)


@pytest.mark.unit
def test_no_setting_means_the_pt(detect, monkeypatch):
    mod, _ = detect
    monkeypatch.delenv("SHOT_DET_ENGINE")
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "pytorch" and det.model.path == "ball.pt"


@pytest.mark.unit
@pytest.mark.parametrize("failure,why", [("load", "deserialization"), ("predict", "optimization profile"),
                                         ("garbage", "probe ball not found"), ("offset", "px from the .pt")])
def test_an_engine_that_fails_its_probe_falls_back(detect, caplog, failure, why):
    mod, engine = detect
    _FakeYOLO.mode = {engine: failure}
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "pytorch" and det.model.path == "ball.pt"
    assert "not used" in caplog.text and why in caplog.text


@pytest.mark.unit
def test_missing_engine_file_falls_back(detect, monkeypatch, caplog):
    mod, _ = detect
    monkeypatch.setenv("SHOT_DET_ENGINE", "/nonexistent/ball.engine")
    assert mod.ShotDetector("ball.pt").backend == "pytorch"
    assert "file not found" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize("imgsz,n", [(960, 16), (1280, 4), (640, 17)])
def test_a_call_the_engine_was_not_built_for_goes_to_the_pt_and_keeps_the_engine(detect, imgsz, n):
    mod, engine = detect
    det = mod.ShotDetector("ball.pt")
    det.model.predict(["f"] * n, imgsz=imgsz)
    assert _FakeYOLO.calls[-1] == ("ball.pt", imgsz, n)
    det.model.predict(["f"] * 16, imgsz=640)                 # the live loop's call
    assert _FakeYOLO.calls[-1] == (engine, 640, 16) and det.backend == "tensorrt"


@pytest.mark.unit
def test_an_engine_failing_mid_game_switches_to_the_pt(detect, caplog):
    mod, engine = detect
    det = mod.ShotDetector("ball.pt")
    _FakeYOLO.mode = {engine: "predict"}
    det.model.predict(["f"], imgsz=640)                      # answered by the .pt
    assert _FakeYOLO.calls[-1] == ("ball.pt", 640, 1)
    assert det.backend == "pytorch" and "failed mid-run" in caplog.text
    _FakeYOLO.mode = {}
    det.model.predict(["f"], imgsz=640)
    assert _FakeYOLO.calls[-1][0] == "ball.pt" and _FakeYOLO.loaded.count("ball.pt") == 1


@pytest.mark.unit
def test_concurrent_callers_never_share_the_engine(detect):
    mod, engine = detect
    det = mod.ShotDetector("ball.pt")
    _FakeYOLO.mode = {engine: "slow"}
    ts = [threading.Thread(target=det.model.predict, args=(["f"] * 4,), kwargs={"imgsz": 640}) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not _FakeYOLO.overlap


@pytest.mark.unit
@pytest.mark.parametrize("stdout,rc,expect", [
    ("noise\nPROBE_OK\n", 0, None),
    ("PROBE_FAIL probe ball not found\n", 0, "probe ball not found"),
    ("", -6, "probe process exited -6: CUDA error: an illegal memory access was encountered"),
])
def test_subprocess_probe_verdicts(monkeypatch, stdout, rc, expect):
    import subprocess
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a[0], rc, stdout, "Traceback...\nCUDA error: an illegal memory access was encountered\n"))
    assert mod._probe_in_subprocess("/x.engine") == expect


@pytest.mark.unit
def test_subprocess_probe_timeout(monkeypatch):
    import subprocess
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))

    def hang(*a, **k):
        raise subprocess.TimeoutExpired(a[0], k.get("timeout"))
    monkeypatch.setattr(subprocess, "run", hang)
    assert "timed out" in mod._probe_in_subprocess("/x.engine")

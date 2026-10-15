"""The shot detector uses the TensorRT engine only when it is safe, else the .pt.

An engine runs only on the device + TensorRT version that built it and only for the shapes it was built
for. A wrong one does not always raise: an engine built for 640x640 alone found 39 of 3512 ball frames
(14 Oct 2026), and a broken engine's CUDA fault poisoned the whole process (15 Oct). So: the probe runs
in a child process and must find the fixture's ball and hoop where the .pt does; calls the engine was
not built for go to the .pt; failures mid-run switch to the .pt; each model is serialised on its own.
"""
import importlib
import json
import os
import subprocess
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
    def __init__(self, cls, cx, cy, w, h):
        self.cls, self.conf = _T(cls), _T(0.9)
        self.xyxy = [(cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)]


def _result(mode):
    boxes = []
    if mode != "garbage":
        shift = 40 if mode == "offset" else 0
        boxes.append(_Box(0, PROBE["ball_cx"] + shift, PROBE["ball_cy"], 30, 30))
        if mode != "nohoop":
            boxes.append(_Box(1, PROBE["hoop_cx"], PROBE["hoop_cy"], PROBE["hoop_w"], PROBE["hoop_h"]))
    return types.SimpleNamespace(boxes=boxes)


class _FakeYOLO:
    loaded, calls = [], []
    mode = {}            # path -> "load" | "predict" | "fault" | "garbage" | "offset" | "nohoop" | "slow"
    inside = {}
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
        if m == "fault":
            raise RuntimeError("CUDA error: an illegal memory access was encountered")
        if m == "slow":
            _FakeYOLO.inside[self.path] = _FakeYOLO.inside.get(self.path, 0) + 1
            _FakeYOLO.overlap |= _FakeYOLO.inside[self.path] > 1
            time.sleep(0.05)
            _FakeYOLO.inside[self.path] -= 1
        return [_result(m) for _ in frames]


@pytest.fixture
def detect(monkeypatch, tmp_path):
    torch = types.SimpleNamespace(
        backends=types.SimpleNamespace(cudnn=types.SimpleNamespace(benchmark=True),
                                       mps=types.SimpleNamespace(is_available=lambda: False)),
        cuda=types.SimpleNamespace(is_available=lambda: True))
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "ultralytics", types.SimpleNamespace(YOLO=_FakeYOLO))
    _FakeYOLO.loaded, _FakeYOLO.calls, _FakeYOLO.mode = [], [], {}
    _FakeYOLO.inside, _FakeYOLO.overlap = {}, False
    engine = tmp_path / "ball.engine"
    engine.write_bytes(b"x")
    monkeypatch.setenv("SHOT_DET_ENGINE", str(engine))
    monkeypatch.setenv("SHOT_DET_ENGINE_DISABLED_MARK", str(tmp_path / "engine.DISABLED"))
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))

    def probe_here(path):                     # the child process's work, done in-process for fakes
        try:
            return mod._probe_ok(_FakeYOLO(path), "cuda")
        except Exception as e:  # noqa: BLE001
            return "%s: %s" % (type(e).__name__, e)
    monkeypatch.setattr(mod, "_probe_in_subprocess", probe_here)
    return mod, str(engine), tmp_path / "engine.DISABLED"


def _live_call(det):
    det.model.predict(["f"] * 16, imgsz=640)


@pytest.mark.unit
def test_engine_is_used_when_it_passes_the_probe(detect):
    mod, engine, _ = detect
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "tensorrt"
    _live_call(det)
    assert _FakeYOLO.calls[-1] == (engine, 640, 16)
    probed = [n for p, _, n in _FakeYOLO.calls if p == engine][:3]
    assert probed == list(mod.PROBE_BATCHES)


@pytest.mark.unit
def test_no_setting_means_the_pt(detect, monkeypatch):
    mod, _, _ = detect
    monkeypatch.delenv("SHOT_DET_ENGINE")
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "pytorch" and det.model.path == "ball.pt"


@pytest.mark.unit
@pytest.mark.parametrize("failure,why", [("load", "deserialization"), ("predict", "optimization profile"),
                                         ("garbage", "ball not found"), ("nohoop", "hoop not found"),
                                         ("offset", "px from the .pt")])
def test_an_engine_that_fails_its_probe_falls_back(detect, caplog, failure, why):
    mod, engine, _ = detect
    _FakeYOLO.mode = {engine: failure}
    det = mod.ShotDetector("ball.pt")
    assert det.backend == "pytorch" and det.model.path == "ball.pt"
    assert "not used" in caplog.text and why in caplog.text


@pytest.mark.unit
def test_missing_engine_file_falls_back(detect, monkeypatch, caplog):
    mod, _, _ = detect
    monkeypatch.setenv("SHOT_DET_ENGINE", "/nonexistent/ball.engine")
    assert mod.ShotDetector("ball.pt").backend == "pytorch"
    assert "file not found" in caplog.text


@pytest.mark.unit
def test_disabled_mark_keeps_the_engine_off(detect, caplog):
    mod, _, mark = detect
    mark.write_text("faulted\n")
    assert mod.ShotDetector("ball.pt").backend == "pytorch"
    assert "disabled after a GPU fault" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize("imgsz,n", [(960, 16), (1280, 4), (640, 17)])
def test_a_call_the_engine_was_not_built_for_goes_to_the_pt_and_keeps_the_engine(detect, imgsz, n):
    mod, engine, _ = detect
    det = mod.ShotDetector("ball.pt")
    det.model.predict(["f"] * n, imgsz=imgsz)
    assert _FakeYOLO.calls[-1] == ("ball.pt", imgsz, n)
    _live_call(det)
    assert _FakeYOLO.calls[-1] == (engine, 640, 16) and det.backend == "tensorrt"


@pytest.mark.unit
def test_ordinary_failures_need_three_strikes(detect, caplog):
    mod, engine, mark = detect
    det = mod.ShotDetector("ball.pt")
    _FakeYOLO.mode = {engine: "predict"}
    for i in range(mod.ENGINE_STRIKES):
        _live_call(det)
        assert _FakeYOLO.calls[-1] == ("ball.pt", 640, 16)        # each answered by the .pt
        assert det.backend == ("tensorrt" if i < mod.ENGINE_STRIKES - 1 else "pytorch")
    _FakeYOLO.mode = {}
    _live_call(det)
    assert _FakeYOLO.calls[-1][0] == "ball.pt" and not mark.exists()
    assert _FakeYOLO.loaded.count("ball.pt") == 1                 # loaded once, up front


@pytest.mark.unit
def test_a_gpu_fault_switches_off_at_once_and_marks_it(detect, caplog):
    mod, engine, mark = detect
    det = mod.ShotDetector("ball.pt")
    _FakeYOLO.mode = {engine: "fault"}
    _live_call(det)
    assert _FakeYOLO.calls[-1] == ("ball.pt", 640, 16) and det.backend == "pytorch"
    assert mark.exists() and "faulted the GPU" in caplog.text


@pytest.mark.unit
def test_concurrent_callers_never_share_the_engine(detect):
    mod, engine, _ = detect
    det = mod.ShotDetector("ball.pt")
    _FakeYOLO.mode = {engine: "slow"}
    ts = [threading.Thread(target=_live_call, args=(det,)) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not _FakeYOLO.overlap


@pytest.mark.unit
def test_a_slow_pt_call_does_not_block_the_engine(detect):
    mod, engine, _ = detect
    det = mod.ShotDetector("ball.pt")
    _FakeYOLO.mode = {"ball.pt": "slow"}
    slow = threading.Thread(target=det.model.predict, args=(["f"] * 4,), kwargs={"imgsz": 960})
    slow.start()
    time.sleep(0.01)
    t = time.time()
    _live_call(det)                                     # the live loop on the engine
    assert time.time() - t < 0.04
    slow.join()


@pytest.mark.unit
@pytest.mark.parametrize("stdout,rc,expect", [
    ("noise\nPROBE_OK\n", 0, None),
    ("PROBE_FAIL batch 3: probe hoop not found\n", 0, "batch 3: probe hoop not found"),
    ("", 1, "probe process exited 1: torch.AcceleratorError: CUDA error: an illegal memory access"),
])
def test_subprocess_probe_verdicts(monkeypatch, stdout, rc, expect):
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))

    class FakeProc:
        def __init__(self, cmd, stdout=None, stderr=None, **kw):
            stdout_f, stderr_f = stdout, stderr
            stdout_f.write(FakeProc.out)
            stderr_f.write("Traceback...\ntorch.AcceleratorError: CUDA error: an illegal memory access\n"
                           "Search for `cudaErrorIllegalAddress' for more information.\n")
            self.pid = 1

        def wait(self, timeout=None):
            return rc
    FakeProc.out = stdout
    monkeypatch.setattr(subprocess, "Popen", FakeProc)
    assert mod._probe_in_subprocess("/x.engine") == expect


@pytest.mark.unit
def test_subprocess_probe_timeout_kills_and_never_blocks(monkeypatch):
    mod = importlib.reload(importlib.import_module("agx_pipeline.shot_detect.detect"))
    killed = []

    class Hung:
        def __init__(self, *a, **k):
            self.pid = 4242

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired("probe", timeout)  # even after the kill: never blocks
    monkeypatch.setattr(subprocess, "Popen", Hung)
    monkeypatch.setattr(os, "killpg", lambda pid, sig: killed.append(pid))
    assert "timed out" in mod._probe_in_subprocess("/x.engine")
    assert killed == [4242]

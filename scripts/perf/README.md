# Performance & feasibility scripts

Measurement tools for the two latency goals — **time to clip** (trigger → clip
playable) and **time to annotator-ready** (game stop → openable in the
annotation tool) — plus the feasibility tests behind the continuous-proxy
design and the TensorRT evaluation.

Nothing here writes to production. Everything reads state that already exists
(Firestore docs, `docker events`, the journal) or writes to scratch.

## Running them

Most need the service's environment, not a bare shell — in particular
`LD_LIBRARY_PATH`, without which `import torch` fails on `libcudss.so.0`:

```bash
cd /home/dev/gopro-automation-linux
set -a; . .env.agx; set +a
```

The Firestore readers find `uball-gopro-fleet-firebase-adminsdk.json` in the
repo root, so run them from there or set `FIREBASE_CREDENTIALS_PATH`.

Run on the **AGX** unless noted. Several share the GPU with recording and
transcode — check `/health` and prefer a no-games window.

## What's here

### Measuring the running system

| Script | What it answers |
|---|---|
| `clip_latency_firestore.py` | End-to-end time to clip, retroactively, from the game docs. Needs nothing deployed. **Only sees clips that were produced** — shots the detector never scanned are invisible to it. |
| `ingestion_run_timings.py` | Post-game stage durations and throughput from `ingestion-runs`. Compare **GB/min**, not duration: duration moves with game length and angle count. |
| `ingestion_run_log.py` | The log lines behind a `completed_with_errors` run — which stage, which angle, why. |
| `transcode_container_times.py` | Per-clip Stage-4 wall time from a `docker events` capture. Every `_transcode_hw` call is its own container, so start→die is the whole cost. |
| `clip_stage_timings.py` | Per-stage breakdown from the Grafana annotations. Needs the annotated build deployed. |

Capture for `transcode_container_times.py`:

```bash
nohup docker events --filter type=container --filter event=start \
  --filter event=die --format '{{json .}}' > ~/docker_events_$(date +%Y%m%d).jsonl 2>&1 &
```

Leave it past the night-end ingest so it catches the per-angle transcodes too.

### P3 feasibility — continuous 1080p proxies on all four angles

Staged so each gates the next. Stages 1, 2 and 4 have passed; stage 3 needs a
game night.

| Script | Stage |
|---|---|
| `rtsp_session_limit.sh` | **1** — how many concurrent RTSP sessions a camera serves, and whether a new one disturbs those already connected. |
| `p3_proxy_throughput.sh` | **2** — can the box sustain N continuous 4K→1080p pipelines at realtime. Also **3**, run during a game and judged on capture health rather than frame counts. |
| `p3_proxy_quality.sh` | **4** — is the P3 proxy as good as today's ingest proxy. Isolates the one real difference: `nvvidconv` vs `nvvideoconvert`. |

### Detector throughput and TensorRT

| Script | What it answers |
|---|---|
| `trt_kickstart.py` | Environment check → export → parity → benchmark. `--check` alone takes seconds and finds environment blockers before an hour of export. |
| `backtest_compare.py` | Reads one backtest report, or diffs two. The `.pt` vs `.engine` delta is the adoption decision. |
| `detector_backend_bench.py` | Inference throughput per backend on identical pre-decoded frames. |
| `detector_decode_bench.py` | The decode path (`bgr24` vs `gray`) and the cost curve across `imgsz`. |

## Traps worth knowing

**`SHOT_DET_IMGSZ` has two different defaults** — `detect.py:30` says 1280,
`live.py:200` says 640. The live loop runs 640. A benchmark or an engine built
at the wrong one measures or serves the wrong path.

**A TensorRT engine must be exported `dynamic=True`.** `_infer_batch` ends a
window on a short chunk whenever the frame count is not a multiple of
`DET_BATCH`, and `backtest/scan.py` predicts on single frames. A static engine
fails both. Engines are also specific to the GPU *and* the TensorRT version —
the host has 10.3, the DeepStream container has 10.7, and they are not
interchangeable.

**Decode dominates the detector's frame path.** ffmpeg's H.264 decode is ~2.75
ms/frame and everything downstream of the pipe is hidden behind it, so shrinking
pipe traffic buys almost nothing. Stride is applied *after* decode, so it
reduces inference work only.

**Benchmarks that divide evenly by the batch size can hide a broken build.**
480 and 6000 frames both divide by 16, so a fixed-batch engine passed the
benchmark and then failed on the first single-frame call.

# Live annotation streaming — proof of concept

**Run on the production AGX, 2026-09-24, `recording: false`, no games for two weeks.**
Nothing in `agx_pipeline/` was modified, imported or restarted. The PoC is three
standalone files writing only to their own output root and their own S3 prefix.

**Verdict: the approach works, and it is cheaper on the box than the plan assumed.
Two of the plan's stated mechanisms were wrong and are corrected below.**

---

## 1. What was proven, end to end

Live camera → hardware transcode → HLS → S3 → CloudFront → a browser page, running
continuously with both angles:

| Stage | Evidence |
|---|---|
| Camera | Zowietek FL `10.1.10.142` + FR `10.1.10.92`, both up |
| Source format | `hevc 3840x2160` (ffprobe) |
| Transcode | NVDEC → `nvvidconv` → NVENC, `===== NvVideo: NVENC =====` in the gst log |
| Output | `h264 Constrained Baseline 1280x720`, ~2.6 Mbps measured from segment sizes |
| Packaging | HLS `EVENT` playlist, `EXT-X-PROGRAM-DATE-TIME` on **every** segment (68/68, later 121/121) |
| Upload | 121 segments/angle in S3 under `live-poc/<session>/{FL,FR}/` |
| Delivery | CloudFront `HTTP/2 200`, `content-type: application/vnd.apple.mpegurl` and `video/mp2t` |
| Liveness | playlist grew **117 → 120 segments in 12s** — exactly real time |
| Freshness | newest segment PDT `16:09:19.121`, wall-clock `16:09:23` → **4.1s behind at the CDN** |

Watch it: `https://d22gul8sdref0l.cloudfront.net/live-poc/<session>/index.html`
(the player page is served from the same distribution as the stream, so it is
same-origin with the playlist).

---

## 2. Two corrections to the plan

### 2.1 The cameras are HEVC, so stream-copy is impossible

`§2.2` of the plan proposed adding a second output to the existing highlight
ffmpeg. That still holds structurally, but the plan implied the live rendition
could ride cheaply alongside a `-c copy` segment writer. It cannot be a copy:

```
codec_name=hevc   width=3840   height=2160
```

**Chrome and hls.js do not play HEVC in HLS.** Safari does; Chrome does not, and
the annotators are not all on Safari. So transcoding to H.264 is **forced, not an
optimisation** — which also means the live rendition cannot be free, and the
encoder question below becomes critical path rather than a detail.

(It also explains something about the existing highlight buffer: those `-c copy`
segments are HEVC in MP4. They cut fine, but they were never browser-playable.)

### 2.2 There is no `h264_nvenc` on this box

The plan's command used `-c:v h264_nvenc`. That encoder does not exist here:

```
$ ffmpeg -encoders | grep -i nvenc      # nothing
$ ffmpeg ... -c:v h264_v4l2m2m ...
[h264_v4l2m2m] Could not find a valid device
[h264_v4l2m2m] can't configure encoder
```

Stock ffmpeg 4.4.2 on Jetson cannot reach the hardware encoder at all. It is only
available through the NVIDIA GStreamer elements — which is exactly what ingest's
own `_transcode_hw` already uses.

**So the shape changes: GStreamer does the hardware work and pipes MPEG-TS into
ffmpeg, and ffmpeg does nothing but mux HLS.** ffmpeg stays in the pipeline
because it is what writes `PROGRAM-DATE-TIME`, which the entire rebase rests on.

```
gst-launch-1.0 rtspsrc ! rtph265depay ! h265parse ! nvv4l2decoder
  ! nvvidconv ! video/x-raw(memory:NVMM),width=1280,height=720
  ! nvv4l2h264enc bitrate=2500000 iframeinterval=120 idrinterval=120
  ! h264parse ! mpegtsmux ! fdsink
| ffmpeg -use_wallclock_as_timestamps 1 -f mpegts -i - -c copy
  -f hls -hls_time 4 -hls_playlist_type event
  -hls_flags append_list+program_date_time+independent_segments ...
```

---

## 3. Cost on the box — better than assumed

Measured with **both** angles encoding simultaneously:

| Resource | Measured | Meaning |
|---|---|---|
| **GPU (CUDA)** | `GR3D_FREQ 0%` | **Zero.** NVDEC/NVENC are separate silicon from the SMs the CV jobs use |
| CPU | 14.5% + 14.0% of one core | ~30% of one core out of 12 |
| Bitrate | 1.27–1.32 MB per 4s segment | ~2.6 Mbps/angle, **~5.2 Mbps for both** |
| Disk | 533 GB free | ~4.5 GB/game/angle |
| S3 PUTs | 2 angles × 4s segments | ~$0.04/game |

**`GR3D_FREQ 0%` is the important one.** The plan asserted that NVENC would not
contend with `shot_typing_live` / `shot_who_live` because it is a different
hardware block. That is now measured rather than assumed — the CUDA cores stayed
at 0% for the entire run.

Phase 0's NVENC-vs-CV question is therefore **answered**. Its uplink question is
not: 5.2 Mbps sustained still has to be measured against the venue's real upstream
on a game night, with the highlight uploads also running.

---

## 4. The timestamp anchor — the part that had to be right

This is what the whole rebase depends on, and the first attempt was wrong in
exactly the way the plan warned about.

### Attempt 1 — `-fflags +genpts`: **FAIL**

```
FAIL  PDT tracks wall-clock (stream spans 100.0s, wall-clock 88.2s, drift -11.8s)
```

Sampled three times as it ran: `-10.9s → -11.3s → -11.6s`. Letting ffmpeg
synthesise timestamps from an assumed frame rate put the timeline ~11s away from
reality and slowly worsening.

### Attempt 2 — `-use_wallclock_as_timestamps 1`: **PASS**

Stamping each packet with its arrival time ties media time to real time by
construction:

```
PASS  EXT-X-PLAYLIST-TYPE:EVENT
PASS  every segment has PROGRAM-DATE-TIME
PASS  PDT advances with segment duration (worst deviation 0.001s)
PASS  EXTINF matches decoded duration (worst 0.029s over 12 segments)
PASS  PDT tracks wall-clock (drift -3.0s)
PASS  ongoing rate error +0.002%  (FL)   /   -0.000%  (FR)
INFO  constant pipeline offset: -5.06s   (identical on both cameras)
```

### What the residual actually is

A first pass tried to separate startup burst from ongoing drift by re-anchoring
the check on a later segment. **That measurement was degenerate** — the anchor PDT
cancels algebraically and you get the live-edge lag back, not a rate. Replaced
with a real one: each segment's file mtime is an independent wall-clock witness,
so `lag_k = mtime_k − (PDT_k + EXTINF_k)`, and the *trend* in `lag_k` is the rate
error.

With that, the answer is unambiguous:

- **Ongoing rate error +0.002% → 0 seconds of error across a 2-hour game.** The
  timeline does not drift.
- **A constant −5.06s pipeline offset**, identical to two decimals on two
  independent cameras. That is camera buffer + RTSP + decode + encode + segment
  close — a property of the pipeline, not of a camera.

**Do not hardcode −5.06.** Measure it per session and subtract it at the anchor.
We already have a camera clock offset that is constant within a game but *flips
sign between games*; a baked-in constant doubled the error on the other game.
The verifier prints this number every run precisely so it stays measured.

### Known artifacts

- The first segment is long (7.9s vs 4s) and carries one `EXT-X-DISCONTINUITY` —
  the RTSP connect burst. Harmless for DVR; worth skipping when sampling.
- Steady-state segments are 3.93–4.03s against a 4s target.

---

## 4b. Unplanned contention test — it held

Partway through the run the box picked up an unrelated job from the two-game
autopilot loop: an ffmpeg seeking into `7cef734e_NL.mp4` at **553% CPU**, taking
the load average from 1.5 to **11.6**.

The stream did not care. After **10.6 minutes** of continuous publishing through
that load:

```
PASS  PDT advances with segment duration (worst deviation 0.001s)
PASS  EXTINF matches decoded duration (worst 0.029s)
PASS  PDT tracks wall-clock (stream spans 635.9s, wall-clock 634.2s, drift -1.8s)
PASS  ongoing rate error +0.000%  ->  +0s across a 2h game
INFO  constant pipeline offset: -5.06s   (unchanged)
```

The constant offset stayed at −5.06s to the same two decimals, and the live-edge
lag actually *improved* to 1.8s. This was not a designed experiment, but it is
the contention evidence Phase 0 wanted: the publisher survives a heavily loaded
box because its work is on the NVDEC/NVENC blocks, not the CPU or the SMs.

Own CPU cost during this, for both angles together: 19.9% + 19.5% (gst) +
1.9% + 1.9% (uploaders) ≈ **43% of one core out of twelve**.

Production was unaffected throughout: `agx-ingestion` stayed `active`,
`/health` (port **5000**, not 8080) reported `recording: false, status: ok`.

---

## 5. Latency

| Stage | Measured |
|---|---|
| Origin → newest segment available | ~3.0–3.4s |
| Measured at the CDN | **4.1s** |
| Player prebuffer (3 × 4s segments) | ~12s |
| **Expected glass-to-glass** | **~16s** |

Comfortably inside the ~30s quoted to Tim, and better than the plan's own 18–20s
estimate. `-hls_time 2` remains available if it ever needs to be lower.

---

## 6. What this does and does not settle

**Settled:** hardware transcode path, HLS packaging, PDT soundness (0% drift),
GPU non-contention, CDN delivery, browser-compatible codec, two angles at once,
bitrate and cost.

**Not settled, and not settleable off a game night:**

- Venue uplink under load, with highlight uploads running (Phase 0's real question)
- T4 rebase against known-good plays — needs the annotation tool and a real game
- Whether an annotator can keep pace with live play
- Behaviour when the highlight recorder fails FL→NL mid-game

**Not attempted on purpose:** nothing in `agx_pipeline/` was touched. Wiring this
into `highlight.py` means modifying the ffmpeg command that production highlight
clips depend on, and that belongs in Phase 1 behind a flag, not in a PoC.

---

## 7. Files

| File | Role |
|---|---|
| `live_hls.sh` | The publisher. `PDT_MODE=wallclock\|genpts` keeps the failed variant reproducible |
| `verify_hls.py` | Checks what the rebase depends on — EVENT, PDT presence, PDT step, EXTINF vs decoded duration, wall-clock tracking, rate error via mtime |
| `hls_uploader.py` | Segments to S3 before the playlist, explicit content types, no-cache on the manifest |
| `player.html` | hls.js page served from the same CloudFront distribution, showing each fragment's PDT and how far behind live it is |
| `start_all.sh` | Detached launcher for both angles |

### Re-running it

```bash
scp scripts/live_poc/* dev@100.116.99.109:/home/dev/live_poc_bin/
ssh dev@100.116.99.109 /home/dev/live_poc_bin/start_all.sh
ssh dev@100.116.99.109 python3 /home/dev/live_poc_bin/verify_hls.py /home/dev/live_poc_wc/FL 4
```

The uploader is capped with `timeout 7200` so a forgotten PoC cannot fill the
bucket. To stop everything:

```bash
ssh dev@100.116.99.109 'pkill -f live_hls.sh; pkill -f hls_uploader.py; pkill -f "gst-launch.*rtsp"'
```

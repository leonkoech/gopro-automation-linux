# Recorder fix: ffmpeg RTSP + timeline-aware watchdog (adopt Geoff's proven logic)

**Status:** approved, implementing on branch `fix/recorder-ffmpeg-timeline-watchdog`.
**Priority:** CRITICAL — this bug lost the tracking footage for **two real games** (below).
**Rule:** implement → pass T1–T5 offline → deploy only in a no-game window with rollback ready.
This doc is the single source of truth for the fix; it is written to survive context compaction.

---

## 1. The incident (verified)

Two games' tracking cameras (FL/NL/FR) recorded **giant files with only minutes of valid video**:

| game | date | recording label | valid FL | file size | verdict |
|---|---|---|---|---|---|
| **Hustle (Rec) vs Premier Mtg (Rec)** | 2026-08-10 | `game_20260811_014215` | 1498s (~25min) | 46 GB | FAILED (froze ~25min in) |
| **"team 1 vs team 2"** (unnamed) | 2026-08-11 | `game_20260812_004241` | 272s (~4.5min) | 40 GB | FAILED (froze ~4.5min in) |

Valid same-nights (NOT affected): `game_20260810_234945` (fcbe6552, 3008s), `game_20260811_004039`
(0d96e12a, 3670s), `game_20260811_234447` (3456s). SL/SR (shot cams) recorded fully on the failed
games too — only FL/NL/FR (tracking) was lost.

**Proof it's a frozen timeline, not a big file:** 42 GB ÷ 272 s = **1.2 Gbps** — impossible for one
H.265 camera. The mdat holds ~2.5 h of frames but the mp4 `moov` only spans 272 s: the camera kept
delivering frames with **frozen timestamps (PTS stuck)**, mp4mux crammed them onto a 272 s timeline,
and the file bloated. ffprobe reports only the moov's 272 s.

---

## 2. Root cause (our recorder — `agx_pipeline/recording.py`)

- Per-camera recording is a **docker `gst-launch`** pipeline (one container per camera):
  `gst-launch-1.0 -e rtspsrc location=rtsp://IP:554/main/av protocols=tcp ! queue ! rtph265depay ! h265parse ! mp4mux ! filesink` (see `_single_cam_gst`).
- Watchdog `_watch_one` (class `RecordingController`) detects a stall by **file-size growth only**:
  ```python
  size = os.path.getsize(run["path"])
  if size > last_size:            # "growing normally" -> resets the stall timer
      seen[angle] = (size, now); return
  stalled = alive and (now - last_grow) > self.wd_stall   # 45s
  ```
- **The hole:** a camera that freezes its timeline but **keeps sending bytes** (frozen/duplicate PTS
  frames) grows the file forever → `size > last_size` stays true → never flagged stalled. gst mp4mux
  silently accepts the frozen-PTS frames. Byte-growth ≠ timeline-progress.
- `REC_WATCHDOG=true` was set; the watchdog ran (poll 20s / stall 45s) but was blind to this mode.

---

## 3. Geoff's proven service (what we're adopting)

Running on the AGX as container **`camrec-v2`**, image `bauersan/jetson-ndi-yolo:webapp`,
`uvicorn app.main:app --port 8000`. His code: **`/app/app/main.py`** (926 lines).
Pull it fresh with: `docker exec camrec-v2 cat /app/app/main.py`.

### His RTSP recorder (the robust path — what we copy)
```python
ffmpeg -hide_banner -nostdin -loglevel warning \
  -progress pipe:1 \
  -rtsp_transport udp \
  <-stimeout|-timeout> 5000000        # socket I/O timeout, microseconds (5s). -stimeout on ffmpeg 4.x, -timeout on 5+
  -i rtsp://IP:554/main/av \
  -map 0:v -c copy \
  [ -f segment -segment_time N -reset_timestamps 1 -segment_format mp4 ]   # optional
  OUT
```
- `-c copy` (bitstream copy) **surfaces** stalls instead of absorbing them: frozen/duplicate PTS are
  **non-monotonic DTS** → ffmpeg drops them and its `-progress out_time` stops advancing.
- `-stimeout 5s` → if the socket goes quiet, ffmpeg **exits** → watchdog restarts.
- `-progress pipe:1` → key=value progress on stdout (parsed as the heartbeat).

His NDI path stays gst (`ndisrc ! ndisrcdemux ! h265parse ! mp4mux ! filesink`) with a **file-growth**
heartbeat — i.e. NDI is STILL vulnerable to the same freeze. **We do not use NDI; our cams are RTSP.**

### His watchdog constants
`POLL_S=2, STALL_S=15, STOP_GRACE_S=15, KILL_GRACE_S=5, HEALTHY_S=30` (run older than this resets
backoff), `BACKOFF_BASE_S=5, BACKOFF_MAX_S=60` (5→10→20→40→60 cap), `MIN_FREE_GB=10`.

### His heartbeat / watchdog logic
- `_read_progress(proc)`: reads ffmpeg `-progress` stdout lines. `out_time_us`/`out_time_ms` →
  `self.progress["out_time_s"]`; key `progress` → `self._last_beat = time.monotonic()`.
- `_heartbeat_ok(now)`: `return now - self._last_beat <= STALL_S`.
- `_watch()` loop (every POLL_S): if `proc.poll() is not None` → **died**; elif not `_heartbeat_ok`
  → **stalled**. On died|stalled: `_finalize_proc` (SIGINT → wait STOP_GRACE → terminate → wait
  KILL_GRACE → kill), compute backoff (consec failures unless run > HEALTHY_S), `_spawn_run()` again.
- `stop()`: set stop event, `_finalize_proc`, join watchdog.

### ⚠️ The one place we HARDEN beyond his code
His heartbeat bumps `_last_beat` on **any** `progress` line — that's "ffmpeg is emitting progress,"
not "the clock is advancing." He's protected mainly because ffmpeg *drops* the bad packets /
`-stimeout` fires. To be bulletproof, **our heartbeat must assert `out_time_s` is INCREASING**, not
just that progress is being emitted. Concretely: keep `last_out_time` + `last_advance_t`; a beat
counts only when `out_time_s > last_out_time`; `stalled = now - last_advance_t > STALL_S`.

---

## 4. The fix — what to build in `agx_pipeline/recording.py`

Replace the docker-gst per-camera recorder + byte-growth watchdog with a **host ffmpeg** recorder +
**timeline heartbeat**, behind an env gate, preserving every integration point.

- **Run ffmpeg on the HOST** (subprocess), not docker — our audio recorder already does this
  (`_audio_cmd` uses host `ffmpeg`), so ffmpeg + ffprobe exist on the host. Gives direct SIGINT +
  stdout `-progress` parsing. (No `docker run`, no `--rm`, no root-owned files.)
- **Command:** Geoff's RTSP ffmpeg command above (detect `-stimeout` vs `-timeout` once, like he does).
- **Per-camera supervisor thread:** spawn ffmpeg with `stdout=PIPE` (progress) + `stderr=PIPE` (log
  tail); a reader thread parses `-progress`; the watchdog checks **`out_time_s` advancing** + process
  liveness; on stall/death → SIGINT-finalize the current file + backoff + respawn to the next segment.
- **Segments + concat on restart:** keep our existing model — each restart writes `{label}_{angle}.rN.mp4`;
  `stop()` concats the ffprobe-valid segments (`_seg_ok` / `_concat_segments`) into one master
  `{label}_{angle}.mp4`. (Alternatively use ffmpeg `-f segment`; but reuse our concat to minimize change.)
- **ENV GATE:** `REC_ENGINE` = `gst` (current, default) | `ffmpeg` (new). Ships dormant; flip to
  `ffmpeg` on the box only after T1–T5. Instant rollback = set back to `gst` + restart.
- **New knobs:** `REC_FFMPEG_STIMEOUT_US=5000000`, `REC_WD_STALL_SEC=15`, `REC_WD_POLL_SEC=2`,
  backoff base/max. Keep the old `REC_WATCHDOG_*` for the gst path.

### Integration points that MUST stay identical (so ingest/highlights/SL-SR are untouched)
- `RecordingController.start(label, camera_ids) -> plan` and `stop(label)` signatures + behavior.
- Output master path: `{cfg.output_dir}/{label}/{label}_{angle}.mp4` (host). Segments `.rN` alongside.
- `is_recording(label)`, `_session_containers`-equivalent (for ffmpeg, track PIDs per label instead
  of container names).
- The **timing sidecar** (`{label}_shot_timing.json`, holds per-camera `spawned_at`) — VERIFY who
  writes it (service.py vs recording.py) and keep it correct; shot timestamps depend on it.
- `Config` / `Camera` (angle, id, ip; `rtsp_port=554`, `rtsp_path=/main/av`, `cameras`, `output_dir`,
  `app_mount`). RTSP URL = `rtsp://{ip}:554/main/av`.
- service.py orchestration: it starts FL/NL/FR via `RecordingController`, SL/SR via the shot recorder,
  plus highlight + audio. Only the FL/NL/FR engine changes.

### TODO to verify during implementation (re-read the live files)
- [ ] `recording.py` full: `start()`, `stop()`, `_seg_path`, `_seg_ok`, `_concat_segments`,
      `_cleanup_session`, the sidecar write. (I read the watchdog + cmd builders; re-read the rest.)
- [ ] Where `spawned_at` / `{label}_shot_timing.json` is produced and consumed
      (`agx_pipeline/service.py`, `agx_pipeline/shot_detect/autodetect.py::_spawned_at`).
- [ ] Confirm host ffmpeg version on the AGX (`ffmpeg -version`) → `-stimeout` vs `-timeout`.
- [ ] Confirm RTSP path/port for our cams (`cameras.json` / `Config`) — assume `:554/main/av`.

---

## 5. Test plan (ALL offline / no live games — must pass before deploy)

- **T1 Normal:** record one real camera ~3 min via the new engine → ffprobe duration ≈ wall time,
  file plays, clean `stop()` finalizes; regression-check filename/paths/sidecar unchanged.
- **T2 Frozen-timeline (the actual bug):** stand up a synthetic RTSP source whose **PTS freezes**
  mid-stream (e.g. mediamtx/`rtsp-simple-server` publishing a looped clip, then feed frozen-timestamp
  packets — or a crafted file with a duplicate-PTS tail served over RTSP). Point the new recorder at
  it → assert the **watchdog detects the stall within ~STALL_S and restarts**, and the salvaged
  segment is valid up to the freeze. This is the case the byte-growth watchdog missed — it MUST catch it.
- **T3 Socket stall:** record a real camera, then `iptables`-block its IP (same method used for the
  earlier gst watchdog test) → assert `-stimeout` fires (ffmpeg exits) + restart + valid segments.
- **T4 Process death:** `kill -9` the ffmpeg pid mid-record → assert supervisor respawns + concat OK.
- **T5 Regression:** master output name/path, `.rN` concat, sidecar, and `is_recording()` behave
  exactly as the gst path so ingest + highlight + shot timing are unaffected.

Capture pass/fail evidence (ffprobe durations, log lines) into this doc under a "Test results" section.

---

## 6. Deploy plan (only after T1–T5 pass + user green-light on a no-game window)

1. Confirm no game recording (real check: `pgrep -af ffmpeg`, GPU idle, and it's outside the game
   window — games ~6:30am IST ≈ ~9pm EDT).
2. Back up: `cp agx_pipeline/recording.py recording.py.bak-YYYYMMDD` on the box (and note the git SHA).
3. Deploy the branch to `dev@100.116.99.109:/home/dev/gopro-automation-linux` and set `REC_ENGINE=ffmpeg`
   in `.env.agx`.
4. Restart the service: `sudo systemctl restart agx-ingestion` (recording runs inside it).
5. Live smoke: trigger a ~2-min record on all of FL/NL/FR → ffprobe each master ≈ 2 min, valid.
6. **Rollback (instant):** set `REC_ENGINE=gst` (or restore `.bak`) + restart. Keep both ready.
7. Validate on the next real game; confirm to the client.

---

## 7. Access / operational facts

- **AGX SSH:** `ssh -i id_rsa dev@100.116.99.109` (key = `id_rsa` in gopro repo root). sudo:
  `echo changeme | sudo -S ...`. ControlMaster socket used in-session.
- **Repo on box:** `/home/dev/gopro-automation-linux`. **Service:** `agx-ingestion` (systemd,
  ExecStart `/usr/bin/python3 -m agx_pipeline.service`, EnvironmentFile `.env.agx`).
- **Logs:** `/home/dev/gopro_logs/gopro_automation.log(.1..)`. Recordings:
  `/home/dev/app/recordings/{label}/`.
- **Deploy safety:** never deploy/restart while a game records; user green-lights the window.
  Build on a branch → user reviews → then deploy (do not self-merge/deploy).

## 9. Progress / results

**Implemented** on `fix/recorder-ffmpeg-timeline-watchdog` (commit `d052bc6`): host ffmpeg RTSP
engine + `out_time_s` timeline watchdog + backoff/restart + pid journal, env-gated
`REC_ENGINE` (default `gst`, unchanged). Both engines' commands verified via `dry-run`; gst path
byte-identical to production baseline (`b10f270`). Fixes applied: stderr→DEVNULL (no pipe deadlock),
reader stops updating shared state after a restart (no stale-clock clobber).

**Tests passed:**
- **T2 (core logic)** ✅ committed `tests/test_recorder_ffmpeg_watchdog.py` (`ff09eb1`): frozen clock
  (alive, 20s idle) → restart; healthy/advancing → no restart; below-threshold → no restart; process
  death → restart; at max-restarts → stop. This is the exact decision the byte-growth watchdog got wrong.
- **T1 (real camera)** ✅ ran the new engine standalone on the AGX against FL (10.1.10.142): recorded
  a valid **19.8s** mp4 (fps 29.7, ~38 Mbps, `ok=True`), `stop()` finalized cleanly, output name/path
  identical to gst. Test artifacts cleaned up; service untouched.
  - NOTE for the standalone path: ffmpeg `-progress pipe:1` requires a **long-lived parent** (the
    service). A CLI that exits mid-record would SIGPIPE ffmpeg — in production the service holds it open.

**Remaining before deploy:**
- **T4 (process death → restart → concat)** and **T3 (socket stall via iptables → -stimeout → restart)**
  end-to-end on the AGX — paused (do not run kill/iptables tests on the box without an explicit OK).
- **T2 (integration)** truest repro = a synthetic frozen-PTS RTSP source (mediamtx + crafted stream) so
  ffmpeg stays responsive while `out_time` freezes and the pre-freeze segment is SIGINT-saved.
- **T5 (regression)** confirm sidecar/ingest unaffected (recording.py outputs unchanged; the shot-timing
  `spawned_at` sidecar is written by the shot recorder / service, not recording.py — not touched).

## 8. Client comms (done)
An apology draft (2 games named, timelines, honest root cause, fix + testing commitment) was written
and shortened for the user to send. The Aug-11 game recorded unnamed ("team 1 vs team 2") — real
matchup unknown from our data; the user fills it in.

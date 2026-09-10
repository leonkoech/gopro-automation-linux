# Postgame Photo Booth — Implementation Plan (Backend · AGX)

> **Plan only.** This PR adds this document and changes no code. It is written for the engineer who will build the feature. It covers what to build, where it plugs into the existing code (file:line anchors are against `main` @ `66cfe82`), and what to measure first.

**Status:** proposal / R&D · **Proposed by:** Tim Shields · **Last updated:** 2026-09-11
**Companion frontend PR:** `leonkoech/gopro-automation-wb` — see the PR description for the link. §3–§5 are identical in both documents.

---

## 0. TL;DR

A **touchscreen TV** at the facility runs one URL: `https://camera.uai.tech/photo-booth`. After a game:

1. A player taps the camera button.
2. A countdown runs on the TV.
3. The AGX takes a **burst of 5 photos** from a **new, dedicated Zowietek camera** mounted at the TV. This is a 7th camera, not one of the 6 game/shot cameras.
4. The photos appear on screen and are saved against **that game** (S3 + Firestore).

**Out of scope for now:**
- **Profile upload.** The data model keeps it easy to add later (§12).
- **Printing.** It is a kiosk-side add-on with no backend work (§12).

**Most of this is wiring.** These already exist:
- the Firestore command relay
- the RTSP frame grab behind "Preview all"
- the highlight-clip S3/CloudFront upload with progressive Firestore writes
- the Pi kiosk script (#87)

**Three pieces are genuinely new:**
1. A **timed burst** capture. Preview's one-shot grab can't be timed.
2. A small **session state machine**, so the TV countdown and the shutter agree.
3. A **booth camera that never enters game recording or ingest.**

## 1. The experience

1. The TV idles on an attract screen with a big camera button and "Photos for: CityBoyz vs Sazoneros".
2. Tap → "Get ready — squeeze in!" while the AGX opens the booth camera stream (~1–3 s).
3. A big **3 · 2 · 1** with beeps.
4. Five white flashes ~0.8 s apart, one per photo.
5. "Developing…" → the photos pop in one by one. Target: all 5 visible within ~10 s of the last flash.
6. **Retake** or **Done**. The screen returns to idle after 45 s.

## 2. What already exists — and why "Preview all" can't be reused as-is

"Preview all" proves the AGX can pull a frame from a Zowietek camera over RTSP and hand the browser an S3 image. The booth needs a different shape:

| | "Preview all" today | Photo booth needs |
|---|---|---|
| Trigger | `agx-commands` action `preview`, **run inline in the relay loop** (`agx_pipeline/relay.py:75-77`). It blocks the heartbeat and auto start/stop for ~10–15 s. | ACK immediately and do the work on a daemon thread — what `highlight_clip` already does (`relay.py:78-83`, `service.py:447-502`) |
| Capture | A new `ffmpeg` per camera with `-frames:v 1`, `scale=640:-2` (`preview.py:31-40`). RTSP connect + keyframe wait + decode takes several seconds and isn't predictable. | 5 full-resolution frames at **chosen instants**, aligned with the on-screen countdown |
| Cameras | All 6 game/shot cameras | The one new booth camera |
| Storage | `camera-previews/…`, presigned for 1 h (`preview.py:25-27,72-77`) | Durable, per game |
| Result | Inside the command doc (`result.previews`) | Written progressively to a small session doc the TV listens to |

**Reused as-is:**
- **Relay command queue.** It is the only way the HTTPS frontend reaches the box: no public HTTP, and mixed content is blocked (`relay.py:1-10`).
- **RTSP grab.** `ffmpeg -rtsp_transport tcp -i rtsp://<ip>:554/main/av` (`preview.py:33-35`).
- **Highlight storage pattern** (`highlight.py`):
  - S3 client singleton (`:71-86`)
  - CloudFront-or-presigned URL (`:60-67`, `:431-439`)
  - dot-path progress writes (`_mark`, `:293-301`)
  - `nice/ionice` subprocess wrapper (`_run`, `:339-347`)
- **Kiosk.** `scripts/pi-kiosk-setup.sh` + README from #87: Chromium kiosk on one URL, autostart, no screen blanking.

## 3. Architecture   *(identical in both PRs)*

```
TOUCH TV (Chromium kiosk)                     FIRESTORE                          AGX  agx_pipeline/service.py
camera.uai.tech/photo-booth

[tap] ──► create photo-booth-sessions/{sid}   status=requested
      ──► add agx-commands {action:"photo_burst", session_id:sid} ──► relay poll (<=3 s)
                                                                        _do_photo_burst(cmd)
                                                                        ├─ ACK {queued:true}  (never blocks the relay)
                                                                        └─ daemon thread:
"Get ready…"          ◄── status=arming ─────────────────────────────── 1. start ffmpeg on the booth RTSP stream
3·2·1 to shoot_at     ◄── status=countdown, shoot_at ────────────────── 2. first frame arrived -> shoot_at = now + 3 s
flash at shoot_at+k·Δ ◄── status=capturing ──────────────────────────── 3. keep frames through the burst window
"Developing…"         ◄── status=processing ─────────────────────────── 4. pick the frame nearest each shot instant
photo k pops in       ◄── photos.{k} = {url, s3_key, …} ─────────────── 5. crop + filter -> JPEG -> S3 -> URL
gallery · retake      ◄── status=ready ──────────────────────────────── 6. clean up temp files
```

**The AGX owns `shoot_at`.** The TV counts down to it using the Firestore-synced clock (`serverNow()` in `src/lib/timeSync.ts`).

The TV can't simply start the countdown on tap. Relay pickup takes up to 3 s, and RTSP connect plus the first keyframe takes another 1–3 s. A TV-side countdown would "fire the shutter" before the camera is ready.

## 4. Decisions (recommended defaults)   *(identical in both PRs)*

| # | Decision | Recommendation | Why |
|---|---|---|---|
| D1 | Camera | **New dedicated Zowietek unit** at the TV (centred above it, eye level), static IP, on the same LAN as the AGX | Decided: the photos come from a new camera, not one of the 6. Game cameras stay untouched. |
| D2 | Booth stream settings | **1080p30 H.264** to start (set in the camera's web UI) | A 3:2 crop is 1620×1080, about 270 dpi on a 4×6 print. It is cheap to decode on the AGX and is what a Phase-2 WebRTC viewfinder needs. Move to 4K only if group shots look soft. |
| D3 | Trigger | New relay action **`photo_burst`**: ACK fast, work on a daemon thread | Same pattern as `highlight_clip`. `preview` runs inline and blocks the relay for 10–15 s. |
| D4 | Where state lives | New small collection **`photo-booth-sessions/{sessionId}`** with a `firebase_game_id` field | The game doc is already hundreds of KB and the scoreboard polls it, so the kiosk should listen to one tiny doc instead. `where firebase_game_id ==` needs no composite index. |
| D5 | Game association | **The TV decides** and passes `firebase_game_id`: most recent `completed` game that ended ≤45 min ago (same window as /recap), else the `active` game, else `null`. The player can switch among the last 3 games. | Game docs have no court/device field, and there is one court today. The command doc already has a `firebase_game_id` field. |
| D6 | Storage | `s3://uball-videos-production/photo-booth/{YYYY-MM-DD}/{gameId or no-game}/{sessionId}/{k}.jpg` plus `{k}_raw.jpg`. Use a CloudFront URL if the distribution serves the prefix, otherwise presigned. **Always store `s3_key`.** | Mirrors highlight clips. Keeping the key lets us re-sign or move URLs later (profiles, privacy). |
| D7 | Vintage filter | Bake it in server-side with built-in ffmpeg filters, and keep the raw crop too | No new Python deps and no CSS filters on the page. What's saved is what's shown, and what gets printed later. |
| D8 | Live viewfinder | **Not in the MVP.** Use a framing guide, a floor marker and instant review with Retake. Spike it in Phase 2. | It's the only hard problem here; don't block the MVP on it. |
| D9 | Feature flag | `PHOTO_BOOTH_ENABLED=false` by default on the AGX (`.env.agx`) | Deploy switched off, then enable between games. |

## 5. Shared contract   *(identical in both PRs)*

### 5.1 `photo-booth-sessions/{sessionId}`

```ts
interface PhotoBoothSession {
  jetson_id: string;                  // "agx-1"
  firebase_game_id: string | null;    // game the photos belong to; null = no recent game
  game_label: string | null;          // "CityBoyz vs Sazoneros" (denormalised for display)
  shots: number;                      // default 5; the AGX clamps to 1..10
  interval_ms: number;                // default 800
  filter: 'none' | 'vintage';         // default 'vintage'
  status: 'requested' | 'arming' | 'countdown' | 'capturing'
        | 'processing' | 'ready' | 'error' | 'cancelled';
  shoot_at: string | null;            // ISO UTC, written by the AGX at 'countdown';
                                      //   shot k fires at shoot_at + k * interval_ms
  photos?: Record<string, {           // keys "0".."n-1", each written as it's ready (dot-path)
    url: string;                      //   displayed variant (filtered)
    raw_url: string;                  //   unfiltered crop
    s3_key: string;
    raw_s3_key: string;
    width: number;
    height: number;
    taken_at: string;                 //   ISO UTC of the frame actually used
  }>;
  error: string | null;               // 'disabled' | 'busy' | 'camera_unreachable' | 'timeout' | free text
  created_at: string;                 // ISO UTC
  updated_at: string;
  created_by: string | null;          // kiosk account email
}
```

**Who writes what.** There is one writer per phase, so no transactions are needed across devices.
- **TV:** creates the doc as `requested`. It may set `cancelled` (✕ button or client timeout), only while the session is still `requested`, `arming` or `countdown`.
- **AGX:** writes everything else.
  - It only picks up sessions in `requested`, so a duplicate command does nothing.
  - It re-reads `status` right before capturing and aborts on `cancelled`.

### 5.2 Command (existing `agx-commands` queue, new action)

```ts
// written by the TV
{ jetson_id: "agx-1", action: "photo_burst", firebase_game_id: "<id>" | null,
  session_id: "<sid>", status: "pending", created_at: "<iso>" }

// written by the relay — an ACK only; results land on the session doc
result: { success: true, queued: true, session_id: "<sid>" }
```

### 5.3 State machine and timing budget (targets; measure them in Phase 0)

| Transition | Driven by | Target | On timeout |
|---|---|---|---|
| `requested` → `arming` | AGX relay poll | ≤ 3 s | TV, after 10 s: set `cancelled` and show "Camera isn't responding" |
| `arming` → `countdown` | AGX, first frame decoded | ≤ 3 s | AGX, after 10 s: `error: camera_unreachable` |
| `countdown` → `capturing` | Reaching `shoot_at` (countdown start + 3 s) | 3 s | — |
| `capturing` → `processing` | AGX | (shots − 1) × interval ≈ 3.2 s | — |
| `processing` → `ready` | AGX; photos appear progressively | ≤ 8 s | TV, after 45 s total: show what arrived |

## 6. Backend implementation guide (file by file)

### 6.1 `agx_pipeline/cameras.json` — a new list, not a 7th game camera

```json
"booth_cameras": [
  { "id": "<zowietek serial>", "ip": "10.1.10.<static>", "angle": "PB" }
]
```

Pin a **static IP**. The NR replacement is on DHCP, and the file's `_note` already warns that it drops out silently when the lease changes.

### 6.2 `agx_pipeline/recording.py` — parse booth cameras separately

- Add `booth_cameras: List[Camera]` to `Config` (`recording.py:82`) and parse it in `load_config()` (`:116`).
- Validate it against a new `BOOTH_ANGLES = ("PB",)`.
- **Do not add `PB` to `VALID_ANGLES`** (`:48`). `load_config` rejects unknown angles (`:122`). Everything that iterates `cfg.cameras` must keep ignoring the booth camera: recording start, audio capture, camrec, ingest, down/up alert emails. A separate list is the isolation guarantee.

### 6.3 New `agx_pipeline/photo_booth.py` (~250–350 lines)

**Config (env vars, default in brackets):**
- `PHOTO_BOOTH_ENABLED` (false)
- `PHOTO_BOOTH_S3_PREFIX` (`photo-booth`)
- `PHOTO_BOOTH_SHOTS` (5)
- `PHOTO_BOOTH_INTERVAL_MS` (800)
- `PHOTO_BOOTH_COUNTDOWN_SEC` (3)
- `PHOTO_BOOTH_WARMUP_TIMEOUT_SEC` (10)
- `PHOTO_BOOTH_LATENCY_MS` (calibrated in Phase 0)
- `PHOTO_BOOTH_CROP` (`3:2`)
- Reuse `UPLOAD_BUCKET`, `UPLOAD_REGION` and `HIGHLIGHT_CDN_DOMAIN`.

**Contents:**
- **`run_session(fb, cfg, session_id)`** is the daemon-thread body. It follows §5.3 and writes through a `_mark(session_id, patch)` helper modelled on `highlight._mark`.
- **Capture** works as described in §7.
- **Pure helpers, unit-tested:** `select_frames(frames, shoot_at, interval_ms, n, latency_ms)`, `s3_key(date, game_id, session_id, k, raw)`, `clamp_shots(n)`.
- **S3:** reuse `highlight._s3_client()`, or lift it into a small shared module so booth and highlights share one warm client.
- **One session at a time:** a module-level `threading.Lock` taken with `acquire(blocking=False)`. If it's busy, write `status: error, error: busy`.
- **Cleanup:**
  - Use a `tempfile.TemporaryDirectory()` **outside** `recordings/{label}/`, because ingest deletes that whole tree.
  - Always kill ffmpeg in `finally`.

### 6.4 `agx_pipeline/service.py`

- **Handler.** Add `_do_photo_burst(cmd)` next to `_do_highlight` (`service.py:447`):
  - validate the flag, a configured booth camera and `session_id`;
  - start `threading.Thread(target=run_session, …, daemon=True, name=f"photobooth-{sid[:8]}")`;
  - return `{"success": True, "queued": True, "session_id": sid}, 202`.
- **Route.** Add `POST /api/photo-booth/burst`, mirroring `/api/highlight` (`:505`). It lets you test from the LAN with `curl`, without the frontend.
- **Relay wiring.** Pass `on_photo_burst=_do_photo_burst` into `Relay(...)` (`:726`).
- **Device status.** In `_device_state()` (`:191`), publish the booth camera with `"role": "photo_booth"` so the dashboard can show it.
  - **Leave it out of `ALERTER.check(cams)` (`:199`)** for the MVP, otherwise every unplug emails UAI.
  - ⚠️ **Ship the frontend role change first.** Today the dashboard lists any camera that isn't `shot_detection` as a game angle (`gopro-automation-wb src/app/page.tsx:116-117`).

### 6.5 `agx_pipeline/relay.py`

- Add `on_photo_burst` to `Relay.__init__` (`relay.py:31-45`).
- Add an `elif action == "photo_burst":` branch next to `highlight_clip` (`:78-83`), with the same "must ACK fast, never block this loop" comment.

### 6.6 `agx_pipeline/preview.py` (optional, useful)

Include booth cameras in `capture_all` with role `photo_booth`. Then "Preview all" doubles as the tool for aiming the new camera when it is mounted.

## 7. Capture design (Phase 1)

**Recommended:** run one ffmpeg per session that writes frames to a temp dir, then pick frames by timestamp afterwards.
- It uses the same RTSP invocation "Preview all" already proves.
- It needs no new Python dependencies (no OpenCV/Pillow in the service).
- Once latency is calibrated, it is accurate enough for posed photos.

1. **Arming.** Start ffmpeg through the same `nice/ionice` wrapper as `highlight._run`:
   ```
   ffmpeg -nostdin -rtsp_transport tcp -i rtsp://<booth_ip>:554/main/av \
          -vf fps=6 -q:v 2 -f image2 <tmp>/f_%05d.jpg
   ```
   Wait for the first file, which means the stream is live and past its first keyframe. If none appears within `WARMUP_TIMEOUT`, set `error: camera_unreachable`.
2. **Countdown.** Set `shoot_at = now + COUNTDOWN_SEC` and write `status: countdown, shoot_at`.
3. **Capturing.** At `shoot_at`, write `capturing`. Stop ffmpeg at `shoot_at + (shots − 1) × interval + LATENCY + 0.5 s`.
4. **Select.** For each `t_k = shoot_at + k × interval`, choose the frame whose `mtime − LATENCY_MS` is nearest `t_k`. This is a pure, unit-tested function. At 6 fps the selection error is ≤ ~85 ms.
5. **Process.** For each selected frame, run one nice'd ffmpeg for the raw version and one for the filtered version:
   ```
   raw:      -vf "crop=ih*3/2:ih" -q:v 2
   vintage:  -vf "crop=ih*3/2:ih,curves=preset=vintage,vignette=PI/5,noise=alls=8:allf=t" -q:v 2
   ```
   `curves`, `vignette` and `noise` are built-in libavfilter filters; confirm with `ffmpeg -filters` on the AGX.

   Upload each photo and write `photos.{k}` **immediately**, so the TV reveals the photos one by one. Then set `ready`.

**Latency calibration (Phase 0, ~10 minutes):**
1. On a phone, open `camera.uai.tech/photo-booth?debug=1`. It shows a large millisecond clock driven by `serverNow()`.
2. Hold the phone in front of the booth camera and run a burst.
3. The difference between the clock visible in each photo and that shot's `t_k` is the end-to-end capture latency. Set `PHOTO_BOOTH_LATENCY_MS` to it.

**If CPU load during a live recording is a problem** (measure with `tegrastats`), either:
- lower the booth stream settings (D2), or
- replace step 1 with a GStreamer hardware-decode pipeline (`rtspsrc ! rtph264depay ! h264parse ! nvv4l2decoder ! …`). The recorder already uses `rtspsrc`.

## 8. Safety — the booth must never hurt a game

The next game often starts 10–15 minutes after the last one ends, so the booth will regularly run while a game is recording.

- **Off the relay loop.** Booth work runs on its own daemon thread and under `nice/ionice`, like highlight cuts.
- **No load on game cameras.** The booth has its own camera, and its list is invisible to recording and ingest (§6.2).
- **Temp files and ffmpeg.** Temp files live outside `recordings/`. ffmpeg is hard-killed after `warmup + countdown + burst + 5 s`.
- **Deploy switched off.** Ship with `PHOTO_BOOTH_ENABLED=false` and follow the usual AGX rules:
  - never deploy or restart while `/health` reports `recording: true`;
  - never restart while an ingest is waiting, because a service restart drops queued pipelines.
- **Don't `rsync --delete` onto the box.** It holds box-local tools that aren't in git. Copy only the changed files.

## 9. Testing plan

**Unit tests (pytest, new `tests/test_photo_booth.py`):**
- `select_frames`: nearest-frame choice, gaps from dropped frames, latency offset, fewer frames than shots.
- `s3_key`: game vs no-game, raw vs filtered.
- `run_session` with a MagicMock Firestore and monkeypatched ffmpeg/upload. Use the pattern in `tests/test_agx_ingest_transcode_toggle.py:34-80`. Check:
  - write order `arming → countdown → capturing → processing → photos.0..4 → ready`;
  - `cancelled` aborts before capture;
  - a held lock gives `busy`;
  - no first frame gives `camera_unreachable`;
  - the flag off gives `disabled`.
- Config: the booth camera never appears in `cfg.cameras`, and `PB` is not in `VALID_ANGLES`.
- Relay: `photo_burst` dispatches to the handler and marks the command `done` with `queued: true`.

**On the AGX, with no game running:**
1. Run `curl -X POST http://<agx>:5000/api/photo-booth/burst -H 'Content-Type: application/json' -d '{"session_id":"test1"}'`. Expect 5 photos in S3 and the session doc at `ready`.
2. Measure time to first frame, end-to-end latency (debug clock) and `tegrastats` during a burst.
3. Repeat during a 5-minute **test recording**. Expect no recorder stalls or watchdog restarts, and no change in live CV latency.
4. `curl -I https://<cdn>/photo-booth/<test>.jpg` confirms whether CloudFront serves the prefix. If it doesn't, use presigned URLs.

## 10. Phases

| Phase | Scope | Backend work | Rough size |
|---|---|---|---|
| **0 — Spike** | Mount and aim the new camera, set a static IP and stream settings; measure latency and CPU; check CloudFront and the ffmpeg filters | Scratch script only | 1–2 days |
| **1 — MVP** | Tap → countdown → 5 photos → saved per game → shown on TV | §6 + §7, flag off by default | ~3 days |
| **2 — Live viewfinder** | Players see themselves while posing | §11 | Spike first |
| **3 — Print** | 4×6 print from the TV | None (kiosk-side) | — |
| **4 — Profiles** | Photo → player profile | Core publish (§12) | Later |

## 11. Phase 2 — live viewfinder options

The browser can't reach the AGX directly; that is why the relay exists. A live picture therefore needs a transport of its own:

| Option | How | For | Against |
|---|---|---|---|
| **A. WebRTC on the LAN (recommended spike)** | `go2rtc` (single static ARM64 binary) on the AGX turns the booth RTSP stream into WebRTC. The service passes the SDP offer/answer through a Firestore doc; video flows TV ↔ AGX over the facility LAN. | <0.5 s latency, no public port, no internet bandwidth, no mixed-content problem | A new binary on the box. Browser H.265-over-WebRTC is unreliable, so the booth stream must be H.264 (D2). ICE must connect TV↔AGX on the LAN. |
| **B. LAN MJPEG** | Port `zcam/flask_viewer/stream.py` (`FrameBuffer`, `generate_mjpeg_stream`) to a `:5000` route, and launch the kiosk Chromium with `--unsafely-treat-insecure-origin-as-secure=http://<agx-ip>:5000` | Cheapest to build | Only works on the kiosk. Depends on Chromium's mixed-content and local-network-access rules, which keep changing; verify on the exact Chromium version. |
| **C. 1 fps framing preview via Firestore** | During `arming`/`countdown` the box writes a ~15 KB JPEG (base64) to one doc | No new infrastructure | ~1 fps, because Firestore sustains about 1 write/s per doc. Good for checking framing, not a mirror. |
| ✗ MJPEG through a public tunnel | — | — | Uses the facility upstream that game-footage uploads need, and puts a live feed of minors on the internet |

## 12. Later (out of scope now)

- **Print (from Tim's proposal).**
  - Attach a 4×6 dye-sub printer to the kiosk computer.
  - The page gets a print-layout view (`@page { size: 4in 6in; margin: 0 }`).
  - Launch Chromium with `--kiosk-printing` so it prints without a dialog.
  - No backend work.
- **Profiles (deferred).**
  - The results screen lists the game roster (`rosterTeam1/2` on the game doc); players tap their name to tag photos (`photos.{k}.player_ids`).
  - Tagged photos are published to Core the same way `core_highlight.py` publishes reels (`CORE_API_BASE_URL` + `X-AGX-Token`, `core_highlight.py:144-146`).
  - This needs a consent policy — these are youth players.

## 13. Open questions

1. **New camera install:** its static IP and exact mount point (centred above the TV?), and confirmation that it can be set to 1080p30 H.264.
2. **Framing:** team photo (landscape 3:2) or individual photo (portrait 2:3)? This sets the crop and the TV layout.
3. **Burst:** 5 shots, 0.8 s apart? Tim's proposal said 5–10; the current ask is 4–5.
4. **No recent game:** allow it and save under `no-game`, or require a game?
5. **Privacy:**
   - Firestore rules were last seen as open `allow read, write` with an **expiry of 2026-12-13**, and CloudFront URLs don't expire.
   - That's acceptable for an in-facility MVP but must be settled before any profile or sharing feature.
   - When those rules expire, the booth breaks along with the rest of the app. Verify the rules in the Firebase console.
6. **Retention:** keep booth photos indefinitely, or add an S3 lifecycle rule on `photo-booth/`?
7. **Filter look:** render 3 ffmpeg presets on the same frame and let the client pick.

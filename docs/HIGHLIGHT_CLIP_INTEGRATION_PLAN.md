# Highlight-Clip Trigger & TV Playback — Research & Integration Plan

**Status:** verified against both repos on 2026-07-28 (see **§11 Verification findings**) — the frontend half held up; four backend assumptions were wrong and the Stage 1 design is revised in place below. Stage 0 implementation started 2026-07-28.
**Owner:** Rohit. **Author of this doc:** carried over from the shot-detection session so a fresh Claude Code session in this repo can execute it cold.

This document has everything needed to build the feature without re-deriving context: the goal, the shot-detection pipeline it connects to, a precise map of **both** repos, the locked decisions, and a staged plan with concrete file/function anchors.

---

## 0. TL;DR

Add a **highlight-clip trigger** and **TV playback** to the courtside system:

1. An admin marks a shot (or, later, an automatic detector fires) → a timestamped entry lands in the game's history.
2. The backend cuts a ~10-second clip of that moment from a watchable camera angle.
3. Each history row gets a **"Watch"** button; clicking it plays that clip **full-screen on the TV display**, then returns to the normal score.

**Near-term scope (this build):** the **manual** trigger + proving the clip can stream to the TV. Everything runs under admin control; nothing is automated yet.
**Later (Stage 2):** an automatic trigger that runs the **v4.3 shot-detection pipeline** on the high-fps shot cams to auto-populate highlights with make/miss.

**Key finding from the research: this is ~80% wiring, not building.** Both repos already contain every pattern each piece needs (an async command relay, a timestamped history feed, hardware clip-cutting, LAN video serving, admin↔TV Firestore lock-step, and a "doc-field → full-screen overlay → clear" pattern on the TV). We add one command action, two buttons, one game-doc field, and one `<video>` overlay branch.

---

## 1. Repos involved

| Repo | Path | Role |
|---|---|---|
| **Backend (this repo)** | `/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux` | Python Flask daemon on the Jetson AGX: camera capture, clip extraction, S3, Firebase, AWS-Batch CV dispatch. |
| **Frontend** | `/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-wb` | Next.js + TypeScript + Firebase "courtside-admin": scorecard, history, and TV display. |
| **Shot detection (Stage 2 only)** | `/Users/rohitkale/Cellstrat/GitHub_Repositories/uball_shot_detection_dual_fusion_v2` | The v4.3 make/miss pipeline (see §2). Only needed when we wire the automatic trigger. |

Both app repos share the **same Firebase project** (`uball-gopro-fleet-firebase-adminsdk.json` in both) — **Firestore is the connective tissue.**

---

## 2. The shot-detection pipeline (v4.3) — context for Stage 2

Lives in `uball_shot_detection_dual_fusion_v2`. Relevant facts:

- **Runner:** `near_v0/highfps/makemiss_v2.py`. Invoked as:
  `SPLIT_FIX=1 DET_WEIGHT=<weight> python3 makemiss_v2.py <video.mp4> <motion2_{tag}.json> <rims.json> <out_prefix> <minutes>`
- **Weights:** `near_v0/weights/ball_yolo26s_gray_hifps_v3_best.pt` (grayscale shot-cam detector, trained on ~1000 annotated frames). `SPLIT_FIX=1` (rim-event splitting) is the validated default.
- **Inputs:** the shot-cam video (SL or SR, 720×540 grayscale @120 fps), a `motion2_*.json` of candidate motion-trigger windows, and `rims.json` (rim ellipse per camera).
- **Outputs:** `<prefix>_verdicts.json` (per shot: time, MAKE/MISS, cause) **and** `<prefix>_reel.mp4` (an annotated clip). So the pipeline already emits a clip as a byproduct.
- **Make/miss rule:** geometry (aperture ρ ≤ 1.10) — the old color classifier is retired.
- **Measured performance (vs frame-accurate human GT):** **97.6% shot detection, 98.9% make/miss** across 3 reviewed cameras; **95% match / 95% agreement vs the Supabase annotation GT** across all 6 cameras using per-camera sync offsets.
- **Deps:** `torch`, `ultralytics`, `opencv`, `numpy` (GPU). A 10-second window is **seconds** of GPU work.
- **Stage-2 note:** `makemiss_v2.py` currently expects a `motion2_*.json` of trigger windows. To run on a single triggered 10 s window you either (a) synthesize one motion event at T, or (b) add a small "process this [T-5, T+5] window directly" entry path. This is a Stage-2 detail, not needed now.

---

## 3. Backend architecture (gopro-automation-linux) — findings

> The repo has **two generations**. The **AGX pipeline (`agx_pipeline/`) is the live runtime** for the Jetson AGX target. `main.py` is the legacy Flask monolith that still hosts the AWS-Batch CV dispatch endpoint and the video-streaming routes. Anchor on `agx_pipeline/` for new work.

**Runtime / entry points**
- `agx_pipeline/service.py` — Flask daemon, port 5000, `python3 -m agx_pipeline.service`. On start it launches a `Relay` (Firebase heartbeat + command consumer) and a schedule scraper.
- `main.py` — legacy Flask app (systemd `gopro-controller.service`, fronted by Cloudflare Tunnel to `https://<jetson>.uai.tech`); hosts CV dispatch + all `/api/videos/.../stream` serving.

**Cameras & footage** (`agx_pipeline/cameras.json`, `recording.py`, `shot_recording.py`)
- **4 tracking cams** — Zowietek NDI/RTSP, angles **FL/FR/NL/NR**, H.265 4K, recorded one Docker gst-launch per camera (`RecordingController`). These are the **watchable / broadcast-style** angles. Transcoded 4K→1080p on ingest.
- **2 FLIR shot cams** — Blackfly S GigE Mono8 grayscale, angles **SL/SR** (basket L/R), **locked 120 fps**, 3 ms exposure, recorded host-side via `aravissrc … ! nvv4l2h264enc` (**hardware NVENC on the AGX**), `AravisRecorder` in `shot_recording.py`. Each start writes **`{label}_shot_timing.json`** with per-camera wall-clock spawn time — **this is the bridge from wall-clock T → shot-cam frame offset.**
- Raw storage: `/home/dev/app/recordings/{label}/{label}_{angle}.mp4` where `label = game_YYYYMMDD_HHMMSS`. Kept-local shot footage: `recordings/shotdet_local/{game_uuid4}/`.

**Clip extraction by timestamp — already exists**
- `video_processing.py` → `VideoProcessor.extract_game_clip(...)` / `_extract_from_single_file` : `ffmpeg -ss <offset> -i <in> -t <duration>` (arbitrary seek + export N seconds). `extract_4k_stream_copy` does `-c copy`.
- `lambda/video_extractor/handler.py` — cleanest reusable template (`build_single_file_cmd`): presigned-URL read → `ffmpeg -ss … -t …` → S3.
- AGX HW path: `agx_pipeline/ingest._transcode_hw` — GStreamer **NVDEC→NVENC** (`nvv4l2decoder ! nvvideoconvert ! nvv4l2h264enc idrinterval=30`). Both paths force **IDR every 30 frames → clips are seekable.**

**GPU / CV today**
- **No on-box ML** (`requirements.txt` has no torch/ultralytics/opencv). The AGX does heavy **video** GPU work (NVDEC/NVENC) but runs no models.
- Heavy CV is **dispatched to AWS Batch** (g4dn.xlarge GPU spot) via `cv_batch_dispatch.CVBatchDispatcher` (`submit_fusion_job`/`submit_merge_job`/`submit_game`); model lives in a separate repo. Driven by a 5-min cron → `POST /api/cv/dispatch-pending` (`main.py` ~line 2834).
- **Critical gap = our greenfield:** this CV path consumes the **1080p tracking angles**, not the **FLIR SL/SR** high-fps footage. **There is no consumer of shot-cam footage today** — `ingest._ingest_shot` merely uploads SL/SR as-is.

**Events / "a shot happened"**
- The canonical event is a **Firebase `basketball-games/{id}.logs[]` entry.** Built by `cv_merge/firebase_emitter.build_cv_shot_log` and appended via `emit_cv_logs` (ArrayUnion). Shape includes `actionType`, ISO `timestamp`, `team`, `payload{points, source, confidence, ...}`, `period`, `gameTime`.
- `plays_sync.create_plays_from_firebase_logs` reads those logs → creates rows in the **Supabase** "plays" table (the annotation backend, `mhbrsftxvxxtfgbajrlc.supabase.co`) via `uball_client.UballClient.create_play`, **already writing `timestamp_seconds`, `start_timestamp (=T-5)`, `end_timestamp (=T+3)`, `source`, `confidence`.** Gated by `CV_PLAYS_ENABLED` (default false) and `CV_EMIT_TARGET` (prod `logs` vs shadow `cv_logs_staging`).

**Communication layer**
- **Firestore backbone**, backend **polls** (no `on_snapshot`). `agx_pipeline/relay.py` `Relay._loop` polls **`agx-commands`** (where `jetson_id==self & status=="pending"`) every ~3 s, dispatches in `_process_commands` by `action`, writes status to `agx-devices/{jetson_id}` and results back to the command doc. Existing actions: `start | stop | preview`.
- REST Flask routes on both apps; frontend contract in `docs/AGX_INGESTION_PIPELINE_PLAN.md` §5.

**Storage & serving**
- S3 `uball-videos-production`, key `{location}/{date}/{game_uuid4}/{date}_{game_uuid4}_{angle}.mp4` (`game_uuid4` = first 4 dash-segments of the Supabase game UUID). Shot SL/SR land in the same folder.
- **HTTP range/partial-content serving in `main.py`:** `/api/videos/<filename>/stream`, `/api/cloud/videos/stream` — `Response(..., 206, mimetype='video/mp4')` with `Content-Range`/`Accept-Ranges`, backed by local files or S3 presigned URLs. **This is how a clip is served to the TV over the LAN.**
- No RTSP/WebRTC egress and no TV output from the backend (the TV is a browser page in the frontend, see §4).

---

## 4. Frontend architecture (gopro-automation-wb) — findings

> `PROJECT_DOCUMENTATION.md` / `QUICK_SUMMARY.md` are **stale**; trust the source under `src/`.

**Single most important fact:** `src/components/BasketballScoreboard.tsx` (~3,244 lines) renders **both** the TV display (`/tv-display`, `mode="tv"`) **and** the operator scorecard (`/admin-controls`, `mode="admin"`), gated by an `isAdmin` flag. Both subscribe to the same Firestore game doc via `onSnapshot` and stay in lock-step.

**Routes**
- `/tv-display` → `<BasketballScoreboard mode="tv" />` — **the TV surface** (full-screen, sidebar hidden).
- `/admin-controls` → `<BasketballScoreboard mode="admin" />` — scorecard + **Game History panel**.
- `/game-logs` — per-game history archive + video-processing controls.
- `/media-browser`, `/basketball/checkin`, `/`, `/admin`, `/zcam`, etc.

**Marking a score / a shot** (all in `BasketballScoreboard.tsx`, admin path)
- Score buttons → `handleScoreButton` (~1189) → `addPlayerScore` / `addScore`; drag-to-score via `acScore` (~1299).
- Every scoring action calls `logGameAction(...)` → `gameService.ts logAction` (~209) building a `GameLog` (`src/types/game.ts` ~40–56) and appending it to **`basketball-games.logs[]`** (`arrayUnion`, ~226). It **also** patches the live snapshot via `pushLive`→`updateLiveState` (dot-paths, ~369).
- **Captured timestamps:** wall-clock ISO (`new Date().toISOString()`), game-clock (`gameTime`/`gameTimeFormatted`), `shotClock`, `period`. **No video-relative timestamp today** — but the backend derives it from `{label}_shot_timing.json` (or `recording.started_at`), so the frontend only needs to pass the wall-clock T it already has.
- `ActionType` union: `src/types/game.ts` (~3–19). Formatter: `src/lib/gameLogFormat.ts` `describeGameLog` (~34).

**History feed — already exists**
- Admin "Game History" panel: toggle ~line 1858, render ~1877–1946 (reverses `game.logs`, one row per event). Each score row already has **"Assign…"** (reassign scorer, `reassignScoreLog` ~1357) and **"Flag"** (`toggleLogTag` ~1392) actions — this is where a **"Watch"** button goes.
- Archive: `src/app/game-logs/page.tsx`.

**TV display**
- The `{!isAdmin && (…)}` block starts ~line 2019 (score, timer, teams). Updated purely by Firestore `onSnapshot` (`subscribeToGame` ~318, `subscribeToActiveGame` ~343; `applyGameSnapshot` ~367 maps `game.live.*` onto state).
- **Reusable overlay precedent (the whole reason this is easy):** the TV already renders full-screen overlays driven by a doc field and clears them — `tvToast` scorer pop-up (built, rendered ~2256–2268, currently disabled behind `SHOW_TV_SCORER_TOAST` ~432) and the pre-game overlay (`showPreGame` ~228). **A highlight `<video>` slots into exactly this pattern.**
- No `<video>` in the scoreboard yet, but plain HTML5 `<video>` exists in `MediaBrowser.tsx` (~851), `VideoManager.tsx` (~150), `CloudVideos.tsx` (~312) — copy from there. No react-player/HLS/WebRTC; not needed for LAN mp4 playback.

**Backend communication (frontend side)**
- Firestore command queue: `deviceService.ts sendCommand(jetsonId, action, firebaseGameId, {force,label})` (~85) writes an **`agx-commands`** doc; `subscribeToCommand` watches the result. `JETSON_ID = "agx-1"` (BasketballScoreboard ~51).
- Direct REST: `src/lib/jetsonApi.ts` (`/api/gopros/{id}/record/*`, `/api/videos`, `.../stream`, `.../download`).
- Backend-produced clips today surface via `ingestion-runs.shots[angle].s3_key` (`src/lib/ingestionService.ts` ~31–42), rendered as status chips in `IngestionCard.tsx` (no play action).

---

## 5. Decisions (locked with Rohit)

| Decision | Choice |
|---|---|
| Which camera plays as the highlight | A **tracking angle (FL/FR or NL/NR)** — watchable 1080p. (The shot cams SL/SR are for detection, not for the audience.) |
| Where clips are served from | **REVISED 2026-07-28:** upload the ~10 s clip to **S3 and put a presigned URL in Firestore**. The original "reuse `main.py` LAN streaming" is broken three ways: `main.py` never runs on the AGX (§11.1), the frontend has no HTTP address for the AGX at all (every `*.uai.tech` base URL is jetson-1/2), and the TV page is served over HTTPS so a plain-HTTP LAN `<video src>` is blocked as mixed content. Fallback option: add the AGX to the Cloudflare tunnel + a range route in `agx_pipeline/service.py` (`<path:>` converter + realpath containment; template `main.py:3396-3452`). |
| Near-term scope | **Manual trigger + streaming test only.** Admin-controlled, not automated. |
| Stage-2 v4.3 compute location | **On the Jetson AGX** (10 s window = seconds; adds torch/ultralytics to the box). Not now. |
| Manual-mark clip window | Proposed default **`[T-8, T+2]`** (mostly the lead-up, since the admin marks just after the play). *To confirm / make adjustable.* |

---

## 6. End-to-end flow (design)

```
TRIGGER  (near-term) admin taps "Mark Shot" on /admin-controls
         → logGameAction('manual_shot', …) writes a timestamped GameLog to basketball-games.logs[]
         → also writes an agx-commands doc {action:"highlight_clip", firebase_game_id, ts, angle, logId}
              (Stage 2 later: an auto detector emits the same GameLog with source="cv")
   │
   ▼
BACKEND  agx_pipeline/relay.py Relay._loop already polls agx-commands → new `highlight_clip` branch in
         _process_commands PLUS a new `on_highlight` callback plumbed through Relay.__init__ from
         service.py (~:410, same wiring as on_preview — the relay has no CFG/_current access; §11.6).
         Handler ACKs immediately and cuts on a daemon thread (the _start_ingestion pattern,
         service.py:314) — relay command handling is synchronous and a long cut stalls the heartbeat.
         → offset = T − started_at, with widened window for preroll slack (§11.3)
         → cut [T-12, T+3] via ffmpeg -c copy from the LIVE-READABLE source (NOT the in-progress
           master — plain mp4mux has no moov until stop; see §11.2 and Stage 1 step 0)
         → write clip OUTSIDE recordings/{label}/ (ingest rmtree's that whole dir; §11.4)
         → upload to S3, presign
         → write {url, status, angle} to a `highlights.{logId}` map on the game doc
           (NOT into logs[] — frontend rewrites the whole logs array in transactions; §11.5)
           + mirror onto the agx-commands result for debugging
   │
   ▼
FRONTEND History row (beside Assign/Flag) gets a "Watch" button
         → on click, write live.highlight = { clipUrl, logId } via the existing pushLive/updateLiveState helper
   │
   ▼
TV       /tv-display applyGameSnapshot already runs on every snapshot → add a branch:
         when live.highlight is set, render a full-screen <video autoPlay> over the score;
         on onEnded (or the field being nulled) → clear back to the score.
```

**All named pieces already exist** except: one `agx-commands` action, two buttons, one game-doc field, one `<video>` overlay branch.

---

## 7. Staged plan

### Stage 0 — prove the TV can stream a clip *(frontend only; testable on the dev server, no Jetson)*
Retires the one real unknown ("can we stream it or not") before building anything around it.
- `BasketballScoreboard.tsx` (TV render): add a `live.highlight = {clipUrl, logId}` branch → full-screen `<video autoPlay muted playsInline>` over the score; `onEnded`/`onError` → clear by writing `live.highlight: null` (Firestore rejects `undefined`; `deleteField` isn't imported). Copy the **pre-game overlay** (`.sb-pregame-overlay`: `position:fixed; inset:0; z-index:9000`) — **NOT `tvToast`**, which is dead code (`SHOW_TV_SCORER_TOAST=false`) and a bottom-corner toast, not full-screen. `muted` is mandatory: the TV has no user gesture and browsers block un-muted autoplay.
- Type edit: `LiveGameState` (`types/game.ts` ~114–124) gains `highlight?: {clipUrl, logId?, requestedAt?} | null`. The live-*write* path (`updateLiveState`/`pushLive`) is untyped `Record<string, unknown>` — writes need no type changes.
- Temporary admin "Play test clip" control that sets `live.highlight` to any reachable **HTTPS** mp4 URL — via `updateLiveState` directly, since `pushLive` is gated on `isGameActive` and silently no-ops post-game.
- **Test:** `npm run dev` with an active game, open `/tv-display`, click → clip plays full-screen and returns to score. (Note: the TV route sits behind `AuthGuard` — the TV browser must be logged in.)

### Stage 1 — manual trigger (near-term deliverable) *(revised 2026-07-28)*
0. **Pick + build the live-cut source** *(new step — unbudgeted in v1 of this doc, see §11.2)*: the raw masters are unreadable while recording (plain `mp4mux`, moov only at EOS). Either **(a)** switch the recording muxer to fragmented mp4 (`mp4mux fragment-duration=1000` in `recording.py:143` — must validate the whole ingest/annotation chain still reads fMP4), or **(b)** run a small parallel highlight recorder on ONE tracking angle (`splitmuxsink` short segments, ring-buffered; cut = concat + trim), leaving the 4K masters untouched. (b) is safer; (a) is less code.
1. **Frontend "Mark Shot"** button in admin → `logGameAction('manual_shot', …)`; add `'manual_shot'` to `ActionType` (`types/game.ts:3-19`) + a case in `describeGameLog` (`gameLogFormat.ts:34`, unknown actions already degrade gracefully to the raw string). Use a stable log id (`newLogId()`, `gameService.ts:30`).
2. **New `agx-commands` action `highlight_clip`** `{firebase_game_id, ts, angle, logId}`. Frontend: widen the action union in `deviceService.ts` (3 places — `AgxCommand.action` ~:62, `sendCommand` ~:87, plus the `addDoc` payload; `sendCommand` has no extra-field pass-through today). Backend: `_process_commands` branch **plus** a new `on_highlight` callback wired through `Relay.__init__` from `service.py` (~:410, same as `on_preview` — the relay has no `CFG`/`_current` access); handler ACKs and cuts on a **daemon thread** (`_start_ingestion` pattern, `service.py:314`) so the 3 s poll loop / heartbeat never stalls.
3. **Backend cut**: ffmpeg `-c copy` via the `extract_4k_stream_copy` path (takes a full output path; pass `add_buffer=0` — `extract_game_clip`'s default `add_buffer=30` silently yields a 70 s clip, and its compress branch invokes `hevc_nvv4l2dec`, an Orin-Nano-patched-ffmpeg decoder that may not exist on the AGX). Offset from `started_at` with the widened `[T-12, T+3]` window (§11.3). Save **outside** `recordings/{label}/` (§11.4), upload to S3, write the presigned URL to `highlights.{logId}` on the game doc + the command result.
4. **Frontend "Watch"** button per history row (`ac-hist-actions` ~:1902) → reads `highlights.{logId}`, sets `live.highlight` via `updateLiveState` → TV plays (Stage 0 machinery).

### Stage 2 — automatic v4.3 trigger (later, on the AGX)
- Add `torch`/`ultralytics`/`opencv` to the AGX (venv or container).
- On a shot-attempt trigger, cut the **SL/SR** `[T-5, T+5]` window (`{label}_shot_timing.json` for the offset), run `makemiss_v2.py` (SPLIT_FIX=1, v3 weight) on it → get MAKE/MISS + exact time; optionally use its `_reel.mp4`.
- Emit the `basketball-games.logs[]` entry with `source="cv"` + `highlight_s3_key`/URL via `cv_merge/firebase_emitter` → `plays_sync` mints the Supabase play (already carries the window). Mind `CV_PLAYS_ENABLED`/`CV_EMIT_TARGET`.
- **Open sub-question for Stage 2:** the real-time "a shot is being attempted" trigger on FL/NL/FR/NR does **not** exist today (the current fusion detector is post-game AWS Batch). Either build a lightweight real-time attempt detector, or drive Stage 2 from the manual mark as the trigger for the v4.3 confirm/clip.

---

## 8. New surface (complete change list)

| Change | Where | Notes |
|---|---|---|
| `live.highlight` game-doc field | frontend `types/game.ts` (`LiveGameState`) + `BasketballScoreboard.tsx` | `{clipUrl, logId, requestedAt}`; set by admin, cleared with `null` (TV self-clears on `onEnded`). |
| TV `<video>` overlay branch | `BasketballScoreboard.tsx` (~2019 block) | copy the **pre-game overlay** (`.sb-pregame-overlay`), not `tvToast`; `<video autoPlay muted playsInline>`. |
| "Play test clip" button (Stage 0, temporary) | `BasketballScoreboard.tsx` admin | scaffolding to validate streaming; writes via `updateLiveState` (bypasses the `isGameActive` gate on `pushLive`). |
| "Mark Shot" button + `manual_shot` log type | `BasketballScoreboard.tsx`, `types/game.ts`, `gameLogFormat.ts` | appears in history automatically. |
| "Watch" button per history row | `BasketballScoreboard.tsx` (~1902 `ac-hist-actions`) | reads `highlights.{logId}`, writes `live.highlight`. |
| `highlights.{logId}` game-doc map | backend writes (dot-path update), frontend reads | avoids the logs[]-whole-array transaction race (§11.5). |
| `highlight_clip` command action | backend `relay.py` `_process_commands` + `on_highlight` callback in `service.py` | ACK fast, cut on daemon thread, result → game doc + command doc. |
| Clip cutter | backend — ffmpeg `-c copy` (`extract_4k_stream_copy` path) from the Stage-1.0 live-readable source | `[T-12, T+3]`, `add_buffer=0`, written **outside** `recordings/{label}/`, uploaded to S3 → presigned URL. |
| Live-cut source (muxer change or parallel segment recorder) | backend `recording.py` | Stage 1 step 0 — required; in-progress masters are unreadable (§11.2). |

---

## 9. Open questions / notes for the executing session

1. **Testing split:** Stage 0 (frontend) is testable locally on `npm run dev`. Stage 1's clip-cutting runs **on the actual Jetson AGX** — code can be written here, but deploy + the on-court/TV test happen on that hardware (deploy via `scripts/deploy_agx.sh`).
2. **Manual-mark window** default `[T-8, T+2]` — confirm span; consider a small UI control to adjust.
3. **Which tracking angle** is the default highlight (FL?), and should the admin be able to switch angle per clip?
4. **Clip cleanup:** raw masters are deleted after upload (`DELETE_RAW_AFTER_TRANSCODE`). Highlights must be cut **before** cleanup, or from the retained 1080p/`shotdet_local` copies, or from S3. Decide the source-of-truth for the cut.
5. **`live.highlight` lifecycle:** who clears it — the TV on `onEnded`, or the admin explicitly? (Recommend TV self-clears on `onEnded`, admin can also cancel.)
6. Do **not** enable the Stage-2 Supabase play emission (`CV_PLAYS_ENABLED`) until the manual path is proven; it writes to the annotation DB.

---

## 10. Reuse-these-patterns cheatsheet

- Async trigger channel → **`agx-commands` + `Relay._loop`/`_process_commands`** (`agx_pipeline/relay.py`) + an `on_*` callback wired in `service.py` (~:410) + daemon-thread work (`service.py:314`).
- Clip cut → **`video_processing.extract_4k_stream_copy`** (`-c copy`, full output path, `add_buffer=0`). *Not* `_transcode_hw` (no seek — whole-file only) and *not* `extract_game_clip` defaults (30 s buffer, Orin-Nano decoder).
- Clip delivery → **S3 presigned URL written to Firestore** (`main.py` does not run on the AGX — §11.1; template for a future AGX range route: `main.py:3396-3452` with a `<path:>` converter + realpath check).
- Admin↔TV signal → **`updateLiveState`/`pushLive` + `subscribeToGame`** (frontend `gameService.ts`; `pushLive` is `isGameActive`-gated).
- TV overlay → **`showPreGame` / `.sb-pregame-overlay`** precedent (fixed inset:0, z-index 9000) in `BasketballScoreboard.tsx` — `tvToast` is dead code and not full-screen.
- History row → renders all `game.logs[]`; actions live at **`ac-hist-actions` (~1902)**.
- Event → play → **`cv_merge/firebase_emitter.build_cv_shot_log` + `emit_cv_logs` + `plays_sync`** (Stage 2).
- Shot-cam wall-clock → frame offset → **`{label}_shot_timing.json`** (Stage 2; FLIR-only — no tracking-cam equivalent exists).

---

## 11. Verification findings (2026-07-28) — why the design above was revised

Both repos were re-explored on their local checkouts (`main` in both; frontend had an uncommitted `BasketballScoreboard.tsx` on top of `f502d7e`). Frontend anchors all VERIFIED with near-zero line drift. Backend findings that changed the design:

1. **`main.py` does not run on the AGX.** `deploy_agx.sh` installs only `agx-ingestion.service` (`python3 -m agx_pipeline.service`); `main.py` imports `flask_cors` which isn't pip-installed there, and both bind port 5000. `agx_pipeline/service.py` has **no** file/stream route. Additionally the frontend has **no HTTP address for `agx-1` at all** (every `*.uai.tech` base URL is jetson-1/2; the scoreboard talks to the AGX only via Firestore), and the HTTPS TV page would block a plain-HTTP LAN `<video src>` as mixed content. Hence: S3 presigned URL in Firestore.
2. **In-progress recordings are unreadable.** Tracking and shot recorders both end in plain `mp4mux` (`recording.py:143`, `shot_recording.py:111`) — moov is written only at EOS (`docker kill --signal=INT` → EOS on stop). ffmpeg cannot open the file mid-game. Live cutting requires Stage 1 step 0 (fragmented mux or a parallel segment recorder). Also: the raw masters are camera-GOP H.265 passthrough — the "IDR every 30 frames" claim only applies to ingest *outputs* and FLIR NVENC recordings, so `-c copy` cuts snap to an unknown keyframe cadence.
3. **`started_at` is loose.** `_current["started_at"]` (`service.py:235`) is stamped before docker launch + RTSP preroll + up to 3×4 s verify/retries → the true first-frame time is 1–5+ s later; only FLIR cams have a per-camera `spawned_at` sidecar. Mitigate with a widened window (`[T-12, T+3]`) and/or add a tracking-cam timing sidecar mirroring `shot_recording.py:142-148`.
4. **`recordings/{label}/` is rmtree'd** after all angles upload (`ingest.py:325-327`, `DELETE_RAW_AFTER_TRANSCODE=true` in prod). Highlights must be written outside that dir (or exist only in S3).
5. **Don't write clip URLs into `logs[]`.** `updateGameLog` (`gameService.ts:235-270`) rewrites the entire logs array in a transaction (operator Assign/Flag taps) — a backend writer would race it. Use a `highlights.{logId}` dot-path map on the game doc instead.
6. **Relay wiring.** `Relay` holds only `fb`, `jetson_id`, and callbacks (`relay.py:31-43`) — a new action needs an `on_highlight` callback from `service.py`, and command handling is synchronous inside the 3 s poll loop (a long cut stalls heartbeat + auto start/stop), so the handler must ACK and thread the work. Claiming is non-transactional (`relay.py:66`) — fine for one relay instance.
7. **Frontend misc.** `tvToast` is dead (`SHOW_TV_SCORER_TOAST=false` at `:137`) and is a corner toast — the full-screen precedent is `.sb-pregame-overlay` (z-index 9000). `pushLive` no-ops when `!isGameActive` (`:672`). `LiveGameState` needs the `highlight` member for reads; writes are untyped. Clear with `null` (Firestore rejects `undefined`). TV route is behind `AuthGuard` — the TV browser must stay logged in. `sendCommand`'s action union is hardcoded in 3 places with no extra-field pass-through. `firestore.rules` is not in the frontend repo — confirm rules allow the new field/collection writes.

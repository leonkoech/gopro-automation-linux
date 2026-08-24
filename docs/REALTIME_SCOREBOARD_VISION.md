# Real-time SL/SR shot detection → live scoreboard (VISION + resume doc)

**Status:** VISION captured 2026-08-10 — NOT yet planned/implemented. User wants this
saved before compaction, then we plan + implement. Companion:
`SHOT_DETECTION_QA_BATCH_PLAN.md` (Phase 1, live), `SHOT_DETECTION_BACKTEST_PLAN.md`,
memory `shot_detection_trigger.md`.

> **Next session after compaction: READ THIS FILE.** It is the resume point for the
> real-time scoreboard work.

---

## The vision (user's words, 2026-08-10)

> "we need to be in real time in our scoreboard also. When trigger is there we know
> make or miss — we add the event as a team event with that make/miss and add at the
> end as a card in the annotation tool too (that is fine). But when we process it
> real-time in SL and SR, when we have it, we update the scoreboard immediately."

Plus, from the prior turn: make the SL/SR footage **readable while recording** (do it,
test it) so live processing is even possible.

### My interpretation (to confirm)
- **Today (Phase 1, live):** shot QA is **post-game** — the make/miss verdict lands
  hours later, after ingest. Trigger = the scorekeeper's `score_added`.
- **The goal:** move make/miss to **real-time during the game** so the **scoreboard
  updates live** (within seconds of the shot), not post-game.
- Two paths converge on the make/miss + a team event + an annotation card:
  1. **Manual trigger** (scorekeeper marks) → make/miss known → team event + card.
  2. **Real-time SL/SR CV** → make/miss detected live → **scoreboard updates
     immediately.**

---

## Hard prerequisite — Option A: readable-while-recording SL/SR (research done)

Live processing is **impossible today** because SL/SR record to a plain `mp4mux`
file whose moov atom is only written at EOS → the growing file is unreadable
mid-game (this is why Phase-1 QA is post-game).

**Research (2026-08-10):** the FLIR **Blackfly S are GigE Vision machine-vision
cameras** — raw Mono8 frames, **NO built-in RTSP/network stream** (Teledyne/FLIR
confirm). Unlike the Zowietek FL/FR (RTSP/NDI encoders). So streaming must be built
**on the AGX** (`aravissrc → H.264 encode → …`), which we already do for recording.

**Fix (contained gst change in `agx_pipeline/shot_recording.py`, remote, no
hardware):** swap the sink from plain `mp4mux ! filesink` to **`splitmuxsink`
(rolling closed segments)** or **fragmented MP4** → footage readable mid-game. This
is the **proven pattern already used for FL/FR** in `agx_pipeline/highlight.py`
(`seg_<ts>_<ANGLE>.mp4`, `_SEG_RE`). (Option B — a true RTSP live feed via a gst RTSP
server — is optional, only if we want to *watch* SL/SR live; not needed for the AI.)
Effort ≈ half a day + a live test; risk: verify segments decode + overlap slightly so
no shot lands on a segment boundary.

---

## Current state we build on
- **Phase 1 QA (LIVE):** deferred, GPU-gated, multi-game-safe worker validates the
  scorekeeper's scored makes post-game (`qa.py`, ~96%/0-false-makes at imgsz 640,
  ~13s/window). Writes `highlights.{logId}.validation` + `game.shot_qa`.
- **Phase 2 auto-detect (dormant/validating):** `autodetect.py` scans SL/SR end-to-end
  to find every shot with no trigger (~80% detect). Runs in the same deferred worker.
- **Scoreboard/scoring today:** scorekeeper taps in the admin/scorecard UI
  (`gopro-automation-wb`); `score_added` logs on the Firebase `basketball-games` doc
  drive the live scoreboard; `register_plays` seeds annotation cards from those logs
  at ingest. Live scoreboard sync = the `live` snapshot on the game doc
  (Admin Controls ↔ TV Display via onSnapshot).

---

## ⚠️ Hard constraints / realities to design around
1. **Recording is sacred.** The AGX GPU must never be starved by CV while a game
   records (highlights + transcode already use it). Phase-1 QA is deferred *exactly*
   for this. Real-time CV runs **during** the game → direct tension. Trigger-driven
   (one ~6s window per score, bounded) is far safer than continuous scanning.
2. **AGX latency.** The detector is ~13–25 fps; one 6s clip ≈ **~13s @ imgsz 640**.
   Plus segment-close latency (~a few s). So "immediate" realistically means
   **~15–25s after the shot**, not instant. Faster needs a lighter model / different
   HW.
3. **CV gives make/miss, NOT point value.** The camera sees the ball through the rim
   (2 vs 3 vs 4 pts is a *shot-location* question — a different model on FL/FR, or the
   scorekeeper's tap). So fully-automated scoring needs the point value from somewhere.
4. **Multiple concurrent games** (tonight = 3 staggered 10min) — real-time per game.
5. **Single scorekeeper invariant** (see memory `scoreboard-single-scorekeeper`) — the
   admin scoreboard treats local state as authoritative; adding a CV writer must not
   break that (don't create a multi-writer race).

---

## DECISIONS (user, 2026-08-10) — design is now FIXED
1. **Score owner = CV auto-updates the score.** Fully automated. The CV becomes the
   scorer: on a detected make it adds points to the live scoreboard itself. The
   scorekeeper's role shifts to corrections / fouls / timeouts (not tapping every make).
   ⇒ To avoid double-counting, CV auto-scoring and manual make-tapping can't both be
   live for the same game — CV owns makes when enabled (per-game flag).
2. **Trigger = CV watches continuously, but at ≤30fps** (down from 120fps native) so we
   don't burn GPU on frames with no ball. This is our existing auto-detect at `stride≈4`
   (~30fps spot pass) run on a rolling live window instead of post-game.
3. **Latency ≈ 15–25s is acceptable.** No lighter-model / new-HW work needed now.
4. **Point value = assume 2 for every CV make** (for now). A shot-location model for
   2/3/4 is a later phase.

### ⚠️ Honest caveats of this v1 design (surface to user, keep visible)
- **Assume-2 undercounts every 3-pointer / 4-pointer** — the live score is systematically
  low whenever a 3 or 4 is made. Scorekeeper must be able to correct.
- **Auto-detect is ~80% recall with occasional phantoms** (backtest) — the live score
  will *drift* (missed makes + rare phantom points). A human correction path is
  essential; CV-added points should be marked as CV-sourced + correctable.
- **Continuous CV during a game uses the GPU while recording** — must degrade/yield to
  recording + highlight transcode, never the reverse. Gate on a lightweight budget, not
  the full is_gpu_free() block (that would never let live CV run during a game).

---

## Plan skeleton (design fixed; build order)
1. **Segmented SL/SR recording** (`shot_recording.py`: `mp4mux`→`splitmuxsink`) + verify
   segments decode mid-game (overlap so no shot straddles a boundary). *(prerequisite)*
2. **Live rolling reader** — enumerate the most-recent *closed* segment(s) for SL/SR.
3. **Live auto-detect loop** — a per-recording-game daemon that scans new segments at
   `stride≈4` (≤30fps) with `detect_stream`/`scan_shots` (built), yields to recording +
   transcode, dedups shots vs. already-scored ones.
4. **Auto-score write** — each fresh CV make → +2 on the live game doc (marked
   `source: "cv"`, correctable) so Admin Controls + TV Display update via onSnapshot;
   respect the single-writer model (CV is the writer when auto-score is enabled).
5. **Team event + annotation card** — reuse the `register_plays` path so each CV make
   also lands as a card in the annotation tool.
6. **GPU governance** — per-game budget/yield; recording + highlight transcode always win.

### ⛔ Deploy safety (tonight)
This touches the **recording pipeline** (`shot_recording.py`) — a red-line change. Do
NOT deploy/restart it minutes before or during tonight's 3 staggered games. Build + test
on a branch; deploy segmented recording only in a confirmed no-game window. Live
auto-score ships behind a per-game flag, default OFF, enabled after a controlled test.

Files in play: `agx_pipeline/shot_recording.py`, `highlight.py` (segment pattern),
`agx_pipeline/shot_detect/{autodetect,validate,detect}.py`, a new live-loop module,
wb `BasketballScoreboard.tsx` + the live game doc.

Files in play: `agx_pipeline/shot_recording.py`, `highlight.py` (segment pattern),
`agx_pipeline/shot_detect/{validate,detect,node,qa}.py`, wb `BasketballScoreboard.tsx`
+ the live game doc.

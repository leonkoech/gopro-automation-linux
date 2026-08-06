# Shot-Detection Trigger — Phase 1 Plan

**Status:** Draft · **Owner:** Rohit · **Last updated:** 2026-08-07
**Branch:** `feat/shot-detection-trigger` · companion: [`SHOT_DETECTION_TRIGGER_CONTEXT.md`](./SHOT_DETECTION_TRIGGER_CONTEXT.md)

---

## 1. Goal (Phase 1)

For **every scorekeeper score trigger**, independently check the high-fps FLIR
cameras (SL/SR) for a made shot at that moment, and **display the agreement** on
the TV — *"clip ready → play registered → validated ✓/✗ → N/M correct tonight."*

Phase 1 is a **shadow validator**: it measures and shows how often CV agrees with
the operator, but it **does not create or change cards** — the scorekeeper stays
ground truth (existing register-plays path is untouched). Disagreements are logged
for review; they are the labeled data + the operator-error signal that earns the
trust needed for Phase 2 (unmanned auto-trigger). Ties directly to the y2Hwx
operator-error problem.

**In scope:** made-shot validation on score triggers; TV agreement display; per-game
agreement log.
**Out of scope (Phase 2+):** auto-triggering from FL/FR/NL/NR when no scorekeeper;
CV creating cards; misses (scorekeeper only logs makes).

## 2. What is already solved (don't rebuild)

| Piece | State |
|---|---|
| **Make/miss decision** | detector **v3** (`ball_yolo26s_gray_hifps_v3_best.pt`) + **aperture rule** (ρ ≤ 1.10), ~95–100%. Self-contained in `makemiss_v2.py` after `feat/aperture-in-runner`; canonical impl `logic.py::decide()`. |
| **Trigger↔footage clock sync** | `shot_recording.py` writes `{label}_shot_timing.json` per game: per-camera wall-clock `spawned_at` + `fps_lock=120`, `do-timestamp=true`. Frame lookup: `N ≈ (T_trigger − spawned_at − pipeline_latency) × fps`. |
| **The trigger itself** | wb `queueClip` → `highlight_clip` command (`ts`, `logId`, `team`, `period`) → AGX relay `_do_highlight`. Phase 1 rides this. |
| **Camera-side math** | `agx_pipeline/side_attribution.scoring_hoop_side(team, period, startingSideTeam1)` → left/right hoop → SL/SR. |
| **Live SL/SR footage** | `shot_recording.py` records SL/SR continuously to the session dir during the game — readable window-by-window in near-real-time. |
| **TV surface** | Firebase `basketball-games/{id}.highlights.{logId}` → `/tv-display`. Extend with a `validation` sub-field. |

## 3. Architecture (per-trigger, near-real-time on the AGX)

```
scorekeeper scores (team, period, ts, logId)
      │  wb queueClip → highlight_clip command
      ▼
AGX relay _do_highlight (existing: cut the tracking-cam clip for TV)
      │
      ├──────────────► existing highlight clip → S3/CloudFront → TV (unchanged)
      │
      └──► NEW: validate_shot(ts, team, period, logId)          [async, shadow]
             1. side = scoring_hoop_side(team, period, start)  → SL or SR
             2. sidecar lookup: ts → [frame_lo, frame_hi] on that cam
                window = [ts − N, ts + M]   (N≈8s reaction lag, M≈2s)
             3. sequential-decode that frame window from the live SL/SR mp4
             4. detect_made_shot(frames, rim_roi)  = detector v3 + aperture decide()
                → { made, shot_time, rho, mechanism, confidence }
             5. write basketball-games/{id}.highlights.{logId}.validation =
                { cv_made, agrees, rho, mechanism, latency_ms }
             6. update game-level tally  highlights_validation = { correct, total }
      ▼
TV (/tv-display) subscribes:
   clip ready → "PLAY REGISTERED (scorekeeper)" → "CV ✓ / ✗" → "CV agreement N/M"
```

**Why near-real-time (not batch):** the SL/SR files are already on disk mid-game and
the trigger gives the exact moment, so we process only a **~10 s window** per score,
not the whole game. Validation may lag the clip by a few seconds (the flow is
clip-first, verdict-second) — that's fine and expected.

## 4. Components & build order

### P1.0 — Port the windowed detector to the AGX  *(the main new code)*
- New `agx_pipeline/shot_detect/` (port from `uball_shot_detection_dual_fusion_v2/near_v0/highfps/`):
  `detect.py` (detector v3 inference), `logic.py` (`decide()` — aperture, copied verbatim),
  the tracking/crossing helpers (`noah_crossings`, `attempt_signature`, `rho`, rim geometry).
  Public API: **`detect_made_shot(frames_or_path, frame_range, rim_roi, fps=120) → verdict`**.
- Deploy the **v3 weight** to the box (`agx_pipeline/models/ball_yolo26s_gray_hifps_v3_best.pt`,
  or S3-cached). Confirm the Orin has torch+ultralytics (the shot cams already run;
  detector inference is new on the box).
- **Rim ROI per camera per court:** SL/SR rim ellipses are stable per mount
  (`mining/rims.json` in the fusion repo: SL≈(355,252), SR≈(402,274)). Ship as a
  small `rims.json` config; add a one-time calibration note.
- **Acceptance:** offline on the box, run `detect_made_shot` on a known window from a
  past game and reproduce that shot's `makemiss_v43/` verdict.
- **✅ VALIDATED off-Orin (2026-08-07)** on real current-AGX SL footage (game
  `736bd664`, a 45 s window) via `scratchpad/shotcheck/smoke.py` on a system-python
  venv (torch 2.8 / ultralytics 8.4 / MPS): detector v3 loads + runs; **auto-rim
  from the hoop = [354.5, 250.1] ≈ the canonical SL rim [355, 252]** (rig matches,
  and hoop-detection is a viable auto-calibration); ball track builds; `logic.decide`
  produces geometrically sensible verdicts (low ρ→MAKE, high ρ→MISS, aperture fires
  on UNCERTAIN). Wiring de-risked. **Still pending:** a GT-accuracy % against
  `makemiss_v43` (needs the 2026-07-20 source videos, not currently available) and
  the Orin ML runtime for on-box deploy.

### P1.1 — Window extraction from the live recording  *(clock sync)*
- Read `{label}_shot_timing.json`; map `T_trigger → frame N` per §2.
- **Sequential decode only** to the window (OpenCV `CAP_PROP_POS_FRAMES` seeks land
  ~13 frames early on this H.264 — a known trap; decode forward instead).
- **Calibrate `pipeline_latency`** (gst settle before first frame): one-time, compare a
  known event's wall-clock to its frame. Expected small (<0.5 s), constant per rig.
- **Acceptance:** for a past game, the mapped window contains the rim-crossing frame.

### P1.2 — Wire into the trigger (shadow, no TV yet)
- In `service.py::_do_highlight` (or a sibling task), after cutting the clip, spawn a
  best-effort `validate_shot(...)` — must **never** delay or fail the clip.
- Write `highlights.{logId}.validation` + game-level tally to Firebase.
- **Acceptance:** on a test game, verdicts appear in Firebase; clip path unaffected.

### P1.3 — TV display validation UI
- `/tv-display` subscribes to `highlights.{logId}.validation` + the tally.
- Sequence: clip → **"PLAY REGISTERED"** → **"CV ✓/✗"** → **"CV agreement N/M tonight"**.
- Keep it unobtrusive (small corner badge); it's a shadow metric, not a call.

### P1.4 — Shadow rollout + review
- Run on 1–2 live games; collect agreement + every disagreement (with the window clip).
- Review disagreements: operator error vs CV miss vs sync/window issue.
- Deliverable: per-game agreement number + a tuned `N` (reaction window) and
  `pipeline_latency`. Gate to Phase 2 only when agreement is high and stable.

## 5. Data model

```
basketball-games/{id}.highlights.{logId}.validation = {
  cv_made: bool,           // CV verdict for the window
  agrees: bool,            // cv_made == (this was a score) ; score ⇒ expect made
  rho: number,             // aperture value at the crossing
  mechanism: "geometry"|"aperture"|"attempt"|"no_event",
  side: "SL"|"SR",
  latency_ms: number,      // trigger → verdict wall time
  window_s: [n_before, m_after],
}
basketball-games/{id}.highlights_validation = { correct: N, total: M }
```

## 6. Key parameters (tunable, start values)

| Param | Start | Notes |
|---|---|---|
| `N` reaction look-back | 8 s | operator presses after the make; refine to the rim-crossing frame |
| `M` tail | 2 s | small forward margin |
| `UNCERTAIN_RHO` | 1.10 | the validated aperture threshold |
| `pipeline_latency` | calibrate | sidecar `spawned_at` → first frame |
| `fps` | 120 | from `fps_lock`; actual ~119.9 |

## 7. Risks & open decisions

1. **Inference latency on the Orin.** ~10 s window × 120 fps ≈ 1,200 frames through
   detector v3. Measure in P1.0; if slow, process a tight sub-window around the
   estimated rim moment and/or subsample. Verdict is allowed to lag the clip.
2. **Trust the operator's side?** Choosing SL vs SR from the operator team button
   re-inherits the y2Hwx error. Phase-1 default: run the indicated side; **stretch:
   run both SL and SR** so CV also validates *which* hoop (catches wrong-team logs).
3. **Reaction-lag window.** `N` must contain the make; then refine the event time to
   the rim-crossing frame (the docs confirm rim-crossing timing is what fixed accuracy).
   Handle >1 shot in a window (take the crossing nearest the trigger).
4. **"Agreement" ≠ ground truth.** The scorekeeper is the error source; frame it as
   agreement, and treat disagreements as *signal*, not CV failure.
5. **Detector runtime on the box.** Confirm torch/ultralytics + the v3 weight run on
   the Orin GPU without disturbing the live recording pipeline.
6. **Classifier fully retired?** `feat/aperture-in-runner` retires it from the
   *decision*; dropping the model load is a small follow-up before shipping to the box.

## 8. What Phase 2 adds (context, not built here)

- **Auto-trigger** from FL/FR/NL/NR (a *separate* spotter model — the gray-hifps
  weights are blind on the color tracking cams) → then the *same* SL/SR make/miss
  confirm. Cascade.
- **CV creates cards** when there's no scorekeeper; reconciliation with priority
  **scorekeeper > automation** (dedup within ±T s).
- Misses (new data the scorekeeper never logs).

## 9. First concrete step

Build **P1.0** (windowed detector on the AGX) — it's the long pole, it's offline-
verifiable against `makemiss_v43/`, and it unblocks everything after. Everything
else (window extraction, trigger wiring, TV) is plumbing around it.

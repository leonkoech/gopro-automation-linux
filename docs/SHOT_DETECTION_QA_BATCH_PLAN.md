# Shot-Detection QA Batch — post-game scorekeeper validation

**Status:** PLANNED (2026-08-08) · branch `feat/shot-detection-trigger`
**Decision:** batch (not live) for now. QA the scorekeeper's **scored makes** against
SL/SR high-fps CV, **auto-hooked into ingest**. Live TV badge deferred (needs
segmented SL/SR + rolling buffer + queue — see §6).
**Companion:** `SHOT_DETECTION_BACKTEST_PLAN.md` (the offline accuracy harness that
proved 95.9% make/miss, 0 false-makes).

---

## 1. Why batch, why this shape (honest rationale)

- Triggering from FL/FR doesn't rescue live SL/SR: the trigger only says *when*; we
  still can't **read** SL/SR mid-game (`mp4mux` writes the moov atom only at EOS →
  the growing file is unreadable). So live make/miss on SL/SR is blocked until the
  recording is segmented/streamable.
- Batch is **proven** (95.9% / 0 false-makes), **testable**, and touches **nothing**
  in the live recording path → zero risk to recording/highlights.
- Post-game the SL/SR mp4 is finalized (moov written on clean stop) → **readable**.
  The timing sidecar (`{label}_shot_timing.json`) is present pre-cleanup → we get the
  FL/FR↔SL/SR clock sync **for free** (no full-game scan, no δ-calibration).

## 2. Goal (the deliverable)

For each **scored make** the scorekeeper logged, check the high-fps shot cam: did the
CV see a made shot in the window? Write the verdict + flag disagreements
(*scored, but CV says miss/none* = a likely scorekeeper over-count or a detection
miss). Output feeds the dashboard + (later) annotation-card enrichment. It is
**shadow/QA only — never mutates scores, cards, or plays.**

## 3. When it runs (ingest hook)

New **STAGE 4.5** in `ingest.py::run_ingestion`, immediately after `_ingest_shot`
([ingest.py:498]) and **before** the session-dir cleanup ([ingest.py:521]). At that
point, in the session dir `{output_dir}/{label}/`:
- `{label}_SL.mp4` / `{label}_SR.mp4` — finalized (readable), still local.
- `{label}_shot_timing.json` — the timing sidecar (fps_lock + per-cam spawned_at).

Gated + best-effort — must **never** break ingest:
- `SHOT_QA_ENABLED` (default **false**) — ships dormant, enabled after testing.
- Skips silently if: not enabled · no sidecar · runtime (torch) missing · no game log.
- Wrapped in try/except like `_ingest_shot`; a failure logs a warning and returns.

## 4. Pipeline (per game)

```
game logs ──filter score_added──▶ [triggers: {ts, team, period, logId}]
                                        │  (serial, one at a time — no concurrency, no OOM)
                                        ▼
  shot_cam_for(team,period,startingSideTeam1) ─▶ SL|SR
  frame_window(sidecar, cam, ts, n_before=8, m_after=2, latency) ─▶ [frame_lo..frame_hi]
  detect_stream(iter_frames(cam, window)) ─▶ make/miss crossings   (STREAMING — capped mem)
                                        ▼
  verdict {cv_made, agrees, primary, n_make, n_miss} ─▶ highlights.{logId}.validation
                                        ▼
  aggregate ─▶ game.shot_qa {n_scored, n_confirmed, n_disagree, disagreements[]}
```

- **n_before = 8 s** (scorekeeper reaction: the press lands *after* the shot — must
  look back far enough). `target_idx` picks the crossing nearest the trigger, so a
  late press still resolves to the right shot. `m_after = 2 s`.
- **Serial** (a plain loop) — post-game, one window at a time. This is the fix for the
  live node's unbounded-thread + OOM problem; here it simply doesn't apply.
- **Detector loaded once + pre-warmed** (one throwaway inference before the loop) so
  the first real window isn't slow.

## 5. Performance + the speed levers

Per window ≈ decode (11 s) + inference (10 s window ⇒ ~1200 frames). At **imgsz 1280**
inference ≈ 25 s/720f ⇒ ~40 s/window; ~90 makes ⇒ **~55 min/game**. Levers (validate
each against the 96% harness before enabling):
- **imgsz 1280→640**: inference ~4× (→ ~21 s/window, ~30 min/game). *Primary lever.*
- **grayscale decode** (model is gray-trained): ~⅓ the pipe bytes → cuts the 11 s decode.
- **coarse→fine window** (30 fps spot over the 10 s window → 120 fps confirm on ±0.5 s):
  biggest win, more code — v2.
- **Guard: skip/defer if a recording is active** so QA never contends with a live game
  on the GPU (post-game jobs must not bleed into the next game).

`SHOT_DET_IMGSZ` (env) is the imgsz knob; ship v1 at the validated 1280, flip to 640
after a harness check.

## 6. Deferred — live TV badge (documented, not built)

Needs: (a) **segmented/streamable SL/SR recording** (`splitmuxsink` or fMP4 — a
recording-pipeline change, tested off-line first) OR a **rolling in-memory SL/SR
buffer** (like `highlight.py`'s `seg_*` buffer), + (b) a **single-worker queue** in
`node.py` (replace the unbounded fire-and-forget threads + `read_window_fast`), +
(c) a **GPU-contention test** vs live recording/transcode/highlight. Only then flip the
live node on.

## 7. Build order

1. **B1a** — `validate_shot` streaming path (`detect_stream` + `iter_frames`, no
   materialize); `imgsz` knob.
2. **B1b** — `qa.py::run_qa(fb, cfg, game_id, label, session_dir, starting_side)` —
   serial driver + aggregate + Firebase writes; pre-warm; `SHOT_QA_ENABLED` gate.
3. **B1c** — ingest hook (STAGE 4.5).
4. **B2** — test on a game manually (dormant → run → inspect verdicts + summary);
   harness-check imgsz 640; run across 3–5 annotated games.
5. **B3** — deploy the dashboard (fix Firebase write) + enable `SHOT_QA_ENABLED`.

## 8. Progress
- 2026-08-08 — scope fixed (QA scored makes, ingest-hooked); plumbing verified
  (sidecar written, hook point, get_game, starting-side). Building B1.

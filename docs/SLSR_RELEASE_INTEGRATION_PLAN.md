# SL/SR Release-Timestamp Integration — Plan

**Goal:** lift shot-TYPE accuracy past the measured ~46% ceiling (and enable WHO) by anchoring
the FL/FR shooter/feet lookup at the **true release moment supplied by the SL/SR high-fps shot
detector**, instead of reconstructing the release from FL/FR ball tracking (benchmarked: plateaus
32–46%, unstable — FL/FR ball detection through flight is too spotty).

**Architecture (user-confirmed):** SL/SR detect the shot (make/miss + precise time) → sync SL↔FL,
SR↔FR → in FL/FR jump back to the release and run detector+tracking on a SHORT window only →
shooter (possession) + feet (grounded) → type + who. Efficient by design: heavy tracking only on
~2–3s per shot.

## Why this wins (from the 28-shot GT benchmark, game 0d96e12a)

- Current best FL/FR-only config = **46%** (possession + gap-release + grounded feet, deployed in
  `Tracking-Cross_camera_association-SAM3/agx_classify.py`).
- ~5 remaining misses are **wrong-shooter** picks: FL/FR "release" lands on a rebounder because the
  card ts marks the OUTCOME (ball at rim) and FL/FR can't see the flight reliably.
- SL/SR run 120fps with a dedicated ball model and already produce a per-frame **ball track**
  `(idx, x, y, rb, conf)` — the flight is cleanly visible there. The release (or at minimum the
  flight's start) is directly extractable.

## What already exists (inventory — verified 2026-08-12)

| Piece | Where | Status |
|---|---|---|
| SL/SR detector v3 (make/miss, ball track, `shot_frame`) | branch `feat/shot-detection-trigger`: `agx_pipeline/shot_detect/{detect,logic}.py` (`rim_visits`, `noah_crossings`, `attempt_signature`, `decide`) | working (~95–100% make/miss); branch paused, local |
| Trigger→SL/SR frame mapping | `shot_detect/window.py` + `{label}_shot_timing.json` sidecar (`spawned_at` + `fps_lock` per cam, written by `shot_recording.py`) | deployed |
| SL/SR→base-coords ts mapping | CV-card ts (`seg*4 + t_shot`, nominal 119.9fps) + **empirical per-game linear fit** `true = A + B*card_ts` in `Tracking…/agx_enrich.py` (fit was identity for 0d96/fcbe) | working; known drift fix parked (`SHOT_CARD_TS_SCALE`, branch `fix/shot-card-timestamp-drift` `eab801b`, slope ~0.940) |
| FL/FR classify plumbing (possession shooter, grounded feet, painted-arc zones, jersey OCR) | `Tracking…/agx_classify.py` (deployed to AGX `scratch_shot_timing/`) | working, benchmarked |
| **Benchmark inputs incl. SL/SR footage** | AGX `recordings/game_20260811_004039/` = 0d96: `_SL.mp4`, `_SR.mp4` (?), `_shot_timing.json`, FL/FR masters | **present — e2e validation possible now** |
| 28-shot human GT | Supabase `plays` (`source='manual'`), game `0d96e12a` | in hand |

## Design

```
SL/SR ball track (120fps)                        FL/FR (30fps)
  rim visit / attempt_signature  ──┐
  walk BACK along the track        │   t_release_slsr
  to flight start = RELEASE  ──────┤        │  map: sidecar (spawned_at,fps_lock)
                                   │        │  + per-game linear fit A+B (agx_enrich)
  emit t_release + made/verdict ───┘        ▼
                                     release_ts_base (FL master coords)
                                            │
                              agx_classify --release-ts <t>
                              shooter = holder at that frame (possession)
                              feet    = grounded frame near it
                              type    = painted-arc zone;  who = jersey OCR
```

**FOV caveat (fold into W1):** SL/SR are rim-focused; a deep shooter may release OUTSIDE their
FOV. Then the extractable anchor is the **flight's entry into frame** (start of the tracked
attempt trajectory) — still 0.3–0.8s before the rim and far more stable than the outcome ts.
Optionally back-extrapolate the tracked parabola to estimated launch. Either way the FL/FR
anchor question becomes "who held the ball just before the flight" — no FL/FR ball tracking needed.

## Work items

- **W1 — Release extraction (the meat).** In `shot_detect/logic.py`: from the ball track, find the
  attempt's flight start — walk back from the rim-approach/crossing to where rim-ward travel begins
  (monotonic approach + vertical velocity flip). Emit `t_release_s` (sec into the SL/SR segment)
  alongside `made`/`verdict`/`shot_frame`. Unit-test offline on 0d96's SL/SR footage against the 28
  GT shots (expected: release ≈ GT-ts − 1–2s).
- **W2 — Time mapping.** `t_release_s` → base coords via the existing sidecar math + the per-game
  linear fit (reuse `agx_enrich.py`'s A/B). For precision, land the parked drift fix
  (`SHOT_CARD_TS_SCALE`) or fold the slope into the fit. Output: `release_ts_base` per shot.
- **W3 — Classify anchor swap (small).** `agx_classify.py`: accept `--release-ts`; when present,
  skip FL/FR release reconstruction entirely — shooter = possession holder at that frame (± a few
  frames), feet = grounded frame near it. All plumbing already exists.
- **W4 — Benchmark e2e.** Run W1+W2 on 0d96's SL/SR footage for the 28 GT shots → feed
  `--release-ts` → measure type% (and WHO with a roster name→number map) vs the 46% baseline.
  Success gate: recover most of the ~5 wrong-shooter misses (→ ~60%+); learn the honest boundary-
  precision residual.
- **W5 — Production wiring (after W4 passes).** Shot pipeline adds `t_release` to each CV card
  payload (`plays_sync` carries it); the enrich/type step consumes it. Roster map for WHO. Lives
  with the paused `feat/shot-detection-trigger` branch work — resuming that branch is part of W5,
  not needed for W1–W4.

## Sequencing & effort

W1 → W2 → W3 → W4 are all runnable NOW against retained footage (no live game needed, no deploys).
W1 is most of the work; W2/W3 are small; W4 is a benchmark run. W5 is a separate deploy decision.

## Risks

1. **SL/SR FOV misses deep releases** → use flight-entry anchor (design above); still strictly
   better than the outcome ts.
2. **SR footage/segment coverage per shot** — verify each GT shot's window exists in the retained
   segments (the live-segment janitor deletes processed segments on newer games; 0d96 predates it
   for these files — verify first in W1).
3. **Sync precision** — the drift fix is parked; the per-game A/B fit covered it for 0d96
   (identity). W2 must sanity-check residuals (<±0.3s) before W4.
4. **Boundary-precision residual** — SL/SR release won't fix shots genuinely on the line; expect
   the post-W4 ceiling to be set by that (next lever after this).

## Status log

- 2026-08-12: Plan written. Inventory verified (incl. 0d96 SL/SR footage + sidecar on the AGX).
- 2026-08-12 (same session) — **W1–W4 EXECUTED, VALIDATED**:
  - **W1** ran on the AGX (`w1_release.py`, deployed shot_detect + v3 weights, imgsz 1280):
    **27/28 shot events found, rim-cross precise**. KEY FINDING: the FOV caveat is the DOMINANT
    case — SL/SR see only ~0.15s of flight (rim-centric FOV), so a *direct* release timestamp is
    not extractable for perimeter shots. **The usable anchor is `rim_base`** (precise ball-at-rim
    time), already in base coords (ingest packages SL/FL on the same session timeline; durations
    match to 0.6s/3670s → W2 mapping is identity; GT-ts scatter vs rim = annotator lag).
  - **W3 (revised design)**: `agx_classify` takes `SHOT_RIM_TS` (video-local) and caps the
    possession-holder search at `rim − SHOT_RIM_CAP_S` (default **1.1s** = 3/4-PT flight-time
    physics prior; lookback 3.3s). Excludes BOTH post-rim rebound holds AND mid-flight
    ball-over-player false holds. Falls back to gap-release without the anchor.
  - **W4 results (28-shot GT benchmark)**: cap 0.6s → **57%**; cap 1.1s → **61%** (17/28), zero
    regressions. Session arc: ~25% → 36 (prox) → 46 (possession+grounded feet) → **61 (rim
    anchor)**. The SL/SR-sync architecture is empirically validated (+15pts over FL/FR-only).
  - **Remaining error buckets**: (a) ~6 near-hoop picks where FL/FR never detects the ball during
    a deep hold → no valid hold in the window (needs better FL/FR ball detection or a
    flight-path-based shooter pick); (b) ~7 boundary-precision shots (feet/arc within px of the
    line — some genuinely borderline). Next levers, in order: W5 production wiring of `rim_ts`
    onto CV cards; roster name→number map to score WHO; then bucket (a).
- 2026-08-12 (W5 session start): **committed** — gopro `1b89c83` (this doc + `scripts/shot_type/`),
  Tracking branch `feat/slsr-rim-anchor` `ba08ca1` (classifier + calibrator).
  - **WHO scored (first number)** via the Supabase roster name→number map: **5/7 correct when OCR
    reads (71% precision), 25% coverage**; FL produced ZERO reads (investigate crops/angle); the 2
    wrong reads grabbed another player's jersey from the crop stack.
  - **W5b DESIGN FINDING (decisive):** the LIVE shadow's `seg*4+t_shot` is NOT a usable rim anchor —
    checked 0d96's Firebase `shot_live` (197 shots, `wallclock` absent in this deployed version)
    against W1's footage-measured `rim_base`: only **6/27 matched within 6s** (live recall gap) and
    the matched ones scatter **+1..+5.6s, growing over the game** (segment drift). The anchor needs
    ±0.3s. **Production design therefore = INGEST-TIME RIM REFINEMENT**: after ingest packages the
    per-game SL/SR files (same timeline as FL/FR masters), run the windowed detector around each
    card's approximate ts (w1_release.py method — 27/28, ±0.05s) → write precise `rim_ts` into the
    card's `events` payload (schema-safe, no migration) → enrich/classify consumes it as
    `SHOT_RIM_TS`. Bonus: also corrects the card ts scatter annotators currently hand-nudge.
    Cost: ~10 GPU-min/game at ingest. Integration point: `ingest.py` STAGE 4.6 (next to
    `create_plays_from_shot_live`) on `feat/shot-detection-trigger`. NOT yet implemented — next
    session's build; needs deploy green-light when ready.

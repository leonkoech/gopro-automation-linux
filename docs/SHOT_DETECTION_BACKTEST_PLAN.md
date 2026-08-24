# Shot-Detection Backtest — Ground-Truth Validation Before Go-Live

**Status:** ACTIVE (started 2026-08-07) · branch `feat/shot-detection-trigger`
**Purpose:** Prove the SL/SR high-fps shot detector against a *fully human-annotated*
past game **before** we flip `SHOT_VALIDATION_ENABLED=true` on a live game.
**Companion docs:** `SHOT_DETECTION_PHASE1_PLAN.md` (the live trigger→validate node),
`SHOT_DETECTION_TRIGGER_CONTEXT.md` (resume context).

---

## 1. The idea (why this exists)

We have a make/miss detector that scores ~95–100% on small frame-level GT. Before we
run it live, we want a **whole-game, ground-truth-backed** number. We already have past
games where **FL, FR, SL, SR were all recorded** *and* the game was **fully annotated by
humans** in the annotation tool (the `plays` table). Those human annotations are our
**ground truth (GT)**.

So we replay a past game two ways and compare both to GT:

```
                    ┌─────────────────────────────────────────────┐
   Human annotations│  GROUND TRUTH  (plays table, source=manual)  │
   (make/miss+time) └─────────────────────────────────────────────┘
                                   ▲                 ▲
                                   │ compare         │ compare
             ┌─────────────────────┴───┐   ┌─────────┴─────────────────────┐
   SETUP 1   │ MANUAL / TRIGGER         │   │ SETUP 2  AUTOMATED            │
             │ Reconstruct the game     │   │ No triggers. Scan the SL/SR   │
             │ timeline from annotation │   │ high-fps end-to-end: coarse   │
             │ timestamps → each becomes│   │ ~30fps spot pass finds shot   │
             │ a "manual trigger" → run │   │ candidates, then the full     │
             │ the DEPLOYED validate    │   │ 120fps window is decided with │
             │ node on the SL/SR window │   │ the SAME v3 weights.          │
             └──────────────────────────┘   └───────────────────────────────┘
```

Both setups run **SL/SR only** (the FLIR high-fps near-rim cams). **We do NOT use FL/FR
for shot logic and we do NOT train/keep separate FL/FR weights.** FL/FR stay what they
are — the tracking / highlight cameras. This was an explicit decision: one weight (v3),
one detection path, both for the manual-trigger validation and the automated scan.

**Outcome we want:**
1. `Manual/trigger accuracy` — how often the deployed node's verdict matches GT (this is
   exactly what goes live in Phase 1).
2. `Automated accuracy` — how well unmanned SL/SR detection matches GT (this is the
   Phase-2 direction: run without a scorekeeper).
3. A per-hoop / per-shot-kind confusion matrix so we know *where* it fails.

If those numbers are good, we go live with confidence. If not, we see exactly what to fix
before touching a live game. **The live node stays OFF (`SHOT_VALIDATION_ENABLED` unset)
throughout the backtest — this is an offline job, it does not enable anything.**

---

## 2. Selected game (frozen)

| field | value |
|---|---|
| Matchup | **Hustle (Rec) vs Akatsuki** |
| Annotation `game_id` | `fdcd9bd4-3615-4b4a-911f-3a5242c561ac` |
| `firebase_game_id` | `wBvfmEBhl1CQgfsmY4zu` |
| Recorded | 2026-07-28 (annotated 2026-07-29) |
| S3 prefix | `s3://uball-videos-production/court-a/2026-07-28/fdcd9bd4-3615-4b4a-911f/` |
| Footage | `_FL.mp4 _FR.mp4 _SL.mp4 _SR.mp4` (all ~2.89 GB) + `_FL_4K _FR_4K` + `_FL.m4a _FR.m4a` |
| GT shot attempts | **180** (92 make / 88 miss) |
| GT by hoop | ~89 LEFT (→SL) / ~91 RIGHT (→SR) |
| GT span | 28.5 s → 2677 s (~44 min) |
| GT source | `source=manual` (human) |

**Why this game:** most-annotated recent game (216 plays total) with a *complete* SL+SR
recording. Backup if anything is wrong with the footage: `2dca7d76` (Uptown 66ers vs
Uptempo, 204 plays, FL/FR/SL/SR all present, same date/prefix).

### Ground-truth shape (the `plays` table)
Each shot attempt row gives us everything the backtest needs:
- `timestamp_seconds` → **`t_track`**: seconds into the tracking-video clock (FL/FR are
  frame-synced via audio cross-correlation, so this is one shared clock).
- `angle` → **hoop**: `LEFT`/`RIGHT`. **This maps straight to the shot cam: LEFT→SL,
  RIGHT→SR.** No team/period/starting-side inference needed — the annotator already told
  us which basket.
- `classification` → **GT make/miss**: `*_MAKE` vs `*_MISS`. Shot kinds: `FG`, `3PT`,
  `4PT`, `FREE_THROW`. Non-shots (`FOUL`,`STEAL`,`TURNOVER`,`REBOUND`,`TIPOFF`) are
  excluded from the make/miss GT.
- `start_timestamp`/`end_timestamp` → the annotated play window (used for match tolerance).

The GT is frozen to `agx_pipeline/shot_detect/backtest/data/fdcd9bd4_gt.json` so the
backtest is reproducible and the box needs no annotation-DB access.

---

## 3. The one hard problem: clock offset δ (tracking ↔ SL/SR)

Annotation times are on the **FL/FR tracking clock**. The **SL/SR FLIR cams are separate
recordings that start at a different wall-clock moment.** So we need, per shot cam, a
constant offset δ mapping tracking-time → shot-cam-time:

```
   SL_time = t_track + δ_SL
   SR_time = t_track + δ_SR
   SL_frame = round(SL_time × fps)         (fps ≈ 119.9)
```

The live pipeline solves this with the per-cam timing sidecar
(`{label}_shot_timing.json`, `spawned_at`+`fps_lock`). **That sidecar is gone for past
games** (cleaned up after ingest). So the backtest estimates δ from the footage itself:

**δ calibration (per cam):**
1. Run the automated coarse scan over the full SL (resp. SR) video → a list of detected
   rim-crossing times `{c_k}` on the shot-cam clock.
2. Take the GT LEFT-hoop (resp. RIGHT) shot times `{t_track}`.
3. Find δ that best aligns the two sets: grid-search δ ∈ [−60, +60] s at 0.1 s, maximizing
   the count of GT shots whose `t_track + δ` lands within τ (≈1.5 s) of a detected
   crossing. With ~90 events per cam this is robust and self-checking (the peak is sharp).
4. Report δ_SL, δ_SR and the alignment quality (how peaked). If the peak is weak → the
   footage/GT don't line up and we stop and investigate before trusting any accuracy #.

δ is a **measured artifact of the backtest** and also validates the live sidecar approach.

---

## 4. Harness (offline, reuses the deployed detector)

New subpackage `agx_pipeline/shot_detect/backtest/` — imported by **nothing** in the
service (offline only). It reuses the exact deployed modules (`detect.ShotDetector`,
`logic.decide`, `validate.validate_shot`) so we're testing the code that ships.

```
agx_pipeline/shot_detect/backtest/
  __init__.py
  gt.py         # fetch GT from annotation Supabase REST → normalize → cache JSON
  data/
    fdcd9bd4_gt.json      # frozen GT (180 shots)
  scan.py       # setup-2 core: full-video coarse(30fps)→confirm(120fps) shot scanner
  calibrate.py  # δ_SL, δ_SR from scan crossings + GT (grid-search alignment)
  run.py        # orchestrator: pull footage → scan → calibrate → setup1 → setup2 → report
  report.py     # metrics (acc / P / R / confusion) → results JSON + Firebase doc
  README.md     # how to run on the box
```

### Pipeline stages (these are the "nodes" the frontend shows)
| # | Node | What it does | Output |
|---|------|--------------|--------|
| 0 | **GT loaded** | freeze/read 180 human shots | `fdcd9bd4_gt.json` |
| 1 | **Timeline reconstructed** | build Firebase game logs from GT times (manual-trigger timeline, visible in dashboard) | `basketball-games/{backtest_id}` |
| 2 | **Footage staged** | pull SL+SR from S3 to box scratch | 2×2.9 GB |
| 3 | **Auto-scan (setup 2)** | coarse 30fps spot → 120fps confirm over full SL & SR | `{cam}` crossings + verdicts |
| 4 | **δ calibrated** | align scan crossings ↔ GT | `δ_SL`, `δ_SR`, peak quality |
| 5 | **Manual/trigger (setup 1)** | `validate_shot` on the SL/SR window at each GT time (deployed node path) | per-shot cv_made |
| 6 | **Compare vs GT** | 3-way: GT vs Manual vs Automated | accuracy, P/R, confusion |

### Setup 1 — manual / trigger (the deployed path)
For each GT shot: window the correct cam (SL/SR from `hoop`) at `t_track + δ`, run
`validate_shot` (window → `read_window_fast` decode → `ShotDetector.detect` → `logic.decide`
→ any-MAKE-in-window). Record `{gt_made, cv_made, agrees}`. **This is byte-for-byte the
live node path**, so its accuracy IS the Phase-1 shadow accuracy.

### Setup 2 — automated (no triggers, Phase-2 direction)
Scan the whole SL and SR videos with the user's coarse→fine idea:
- **Spot pass** at ~30 fps (every 4th frame) — cheap detector pass to find frame ranges
  where the ball is near the rim (candidate crossings). ~4× cheaper than full 120 fps.
- **Confirm pass**: for each candidate, decode the tight 120 fps window and run
  `logic.decide` → make/miss + precise crossing frame.
Produces a detected shot list `{t_shot(cam clock), made}`. Match to GT via `t_track + δ`
within τ → true-positive / false-positive / false-negative + make/miss accuracy on matched
shots.

### Compute budget (runs on the box GPU, ~25 fps inference)
- Full 120 fps scan of one cam ≈ 317k frames → too slow. The **30 fps spot pass ≈ 79k
  frames/cam → ~53 min/cam**; confirm passes are cheap (only near candidates). Setup 2 ≈
  ~2 h for both cams.
- Setup 1 windowed: 180 shots × ~10 s. If confirm-only (spot the window at 30 fps, decide
  at 120 fps) ≈ tens of minutes.
- **Strategy: coarse-to-fine + a subset smoke first.** Run δ-calibration + setup-1 on a
  ~20-shot spread to prove the harness end-to-end and get δ, *then* launch the full run as
  a background job on the box. Log any subsetting (never silently cap coverage).
- Runs on the **box** (GPU, `device=cuda`), never the Mac (OOM-prone). No live-game window.

---

## 5. Manual-trigger timeline reconstruction (Firebase)

Per the plan, the "manual setup" is also made **visible in the dashboard**: reconstruct a
Firebase game doc `basketball-games/{backtest_id}` whose `logs[]` are `score_added`
entries built from the GT (one per `*_MAKE`, timed at `t_track`, team/hoop from `angle`).
This:
- lets us view the reconstructed timeline in game logs / dashboard exactly like a real game,
- is what "run the pipeline again using those triggers" means for the manual path,
- carries a `backtest: true` flag + a distinct id so it is never confused with a real game
  and is trivially deletable.

We do **not** need to fake the live sidecar to get the accuracy number — setup 1 gets δ
from calibration. The reconstruction is for **visibility + the dashboard nodes**.

---

## 6. Frontend nodes (gopro-automation-wb)

Add a **Shot-Detection Backtest** card to the dashboard, modeled on the existing
`IngestionCard` stepper (Transcode→Upload→Register…). It reads a Firebase doc
`shot-backtests/{game_id}` written by `report.py` and renders:
- **Header:** matchup, game id, GT count (180), date.
- **Stepper nodes** (stages 0–6 above): each node shows state (done/running/failed) +
  a one-line stat (e.g. δ_SL=+12.3 s · peak 0.94; Auto-scan: 174 candidates).
- **Three-way comparison panel:** GT vs Manual vs Automated —
  - make/miss accuracy (Manual, Automated),
  - agreement % (Manual↔GT), precision/recall (Automated↔GT),
  - a small confusion matrix + per-hoop (SL/SR) and per-kind (FG/3PT/4PT/FT) breakdown.
- **Node = where, stat = what** (same philosophy as the ingestion stepper).

Gated: the card only appears for docs under `shot-backtests/` — it is a validation
surface, separate from live game UI, and never shows on a real game.

---

## 7. Go / no-go for live

Flip `SHOT_VALIDATION_ENABLED=true` (and deploy the wb `fix/tv-shot-validation` TV badge)
**only after**:
- δ calibration peak is clean on both cams (footage/GT truly align),
- Manual/trigger accuracy vs GT ≥ target (aim ≥ 90% make/miss agreement),
- Automated precision/recall understood (Phase-2 readiness, not a live blocker),
- confusion matrix has no scary systematic failure (e.g. all FTs wrong).

Until then the live node stays dormant. The backtest is the gate.

---

## 8. Progress log
- **2026-08-07** — B0 done: game `fdcd9bd4` selected; footage verified (FL/FR/SL/SR all
  ~2.9 GB in S3); GT frozen (180 shots, 92/88). Plan written. Harness build started.

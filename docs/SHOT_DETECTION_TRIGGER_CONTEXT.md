# Shot-Detection Trigger — Session Context / Resume Doc

**Paused:** 2026-08-06 (to take in new changes from a Tim meeting; MOM incoming).
**Branch:** `feat/shot-detection-trigger` (cut from `origin/main` `a9fbda0`).
**Why paused:** switching back to `main` for higher-priority changes; resume this
branch afterward using this doc.

---

## 1. What was DONE this session (committed)

**Commit `ec0dd95` on `feat/shot-detection-trigger` — removed the OLD CV integration**
(the AWS-Batch / YOLOv11n post-game shot-detection path), to clear the way for the
new high-fps trigger pipeline. 50 files, −6,865 lines. Removed:
- `cv_merge/`, `cv_batch_dispatch.py`, `cv_metrics.py`, `lambda/cv_dispatch/`
- `main.py` → `/api/cv/dispatch-pending` endpoint + helpers
- `deploy/cv-merge/`, `deploy/batch-job-defs/cv-*.json`, `deploy/cloudwatch/` (CV alarms/dashboard)
- `scripts/cv_infra/` (CV IAM/ops/secrets/replay), `docs/CV_SHOT_DETECTION_V1_PLAN.md`, all CV tests
- `plays_sync.py` → stripped the `source='cv'` plumbing (feature gate, `is_cv`
  branches, CV note/confidence, `shot_missed` + `_MISS_BY_POINTS`)

**KEPT (new pipeline needs them):** `agx_pipeline/shot_recording.py` + the
`role="shot_detection"` handling (FLIR SL/SR recording), and
`agx_pipeline/side_attribution.py` (used by highlight clips — only scrubbed its
stale `cv_merge` comment refs).

**Verified:** all files compile; zero dangling CV refs in the tracked tree;
`register-plays` smoke passed (human logs → firebase-source cards intact).

**⚠️ Untracked leftovers NOT committed (rm was blocked by the permission
classifier):** `tests/cv_batch_dispatch/`, `scripts/cv_infra/enable-nl-nr-on-jetsons.sh`.
Clear them manually when convenient (they were never tracked, so harmless).

**Not pushed** — `ec0dd95` is local only on the feature branch.

---

## 2. The feature (user's vision — confirmed correct)

A single shot-event pipeline with a **swappable front-end trigger** and a shared spine:

```
TRIGGER (+timestamp)                 SHARED SPINE
Phase 1: scorekeeper UI  ─┐
                          ├─▶ window [t−N … t+M] → pull SL/SR high-fps footage
Phase 2: auto (FL/FR/NL/NR)┘         → run high-fps make/miss detection (synced)
                                     → make/miss + which rim + refined shot time
                                     → clip (tracking cam) for TV
                                     → card (annotation); priority scorekeeper > auto
```

- **Phase 1** (scorekeeper present): the button is the trigger + ground-truth
  label; high-fps CV runs on the same moment to **validate**. TV shows
  "clip ready → play registered → validated ✓, N/M correct this game."
- **Phase 2** (no scorekeeper): tracking cams generate the trigger; the *same*
  high-fps detector confirms; CV creates the card.
- Both end in the same annotation-card creation; **priority: scorekeeper, else automation.**

---

## 3. Model reality (verified against artifacts)

The **high-fps FLIR SL/SR** line (NOT the old oblique color fusion):
- Cams: SL/SR, one per rim, **720×540 @ ~171 fps**, grayscale, fisheye, near/above rim.
- **Detector** `ball_yolo26s_gray_hifps_v2_best.pt` (ball+hoop, 2 classes, imgsz 1280): mAP50 **0.924** (hard-frame ball AP50 0.874).
- **Make/miss decision** = detector v2 + **Noah geometric layer** + classifier fusion
  (`makemiss_v2.py`, geometry decides ~79%, classifier arbitrates ~21%):
  - **17/17 = 100%** on frame-accurate 10-min GT (ba4d38fa_SL, n=17, small/in-sample).
  - **95.2%** strict (±3s) vs Supabase annotator GT (83 confident pairs);
    89.1% permissive (stated lower bound; gap = GT timestamp jitter + compound plays).
  - The **70.6% (v0)** number is SUPERSEDED — do not quote it.
- **FRESH_VALIDATION.md 0.951 is the OLD oblique color-fusion model** (far_v16 +
  net-motion) — a different lineage; don't conflate with high-fps.

### Make/miss internals — the "99–100%" logic (verified 2026-08-07)

The make/miss decision = **3 ingredients** (all in `uball_shot_detection_dual_fusion_v2/near_v0/highfps/`):
1. **Detector v3** — `near_v0/weights/ball_yolo26s_gray_hifps_v3_best.pt` (grayscale, ~1000-frame trained). ✅ durable. (Newer than the v2 in older notes.)
2. **v4.3 rim-event splitting** (`SPLIT_FIX`) — baked into `makemiss_v2.py` (line 53, default `"1"`). ✅ durable, default-on.
3. **v4.2 aperture rule** (ρ ≤ 1.10 decides make/miss for UNCERTAIN crossings; color classifier retired) — the clean version lives in **`logic.py::decide()`** (`UNCERTAIN_RULE="aperture"`, `UNCERTAIN_RHO=1.10`, → `GEO_MAKE`/`GEO_MISS`). ⚠️ **partially durable.**

**⚠️ Soft spot to fix before Stage-2 leans on it:** the deployed runner `makemiss_v2.py` still decides UNCERTAIN crossings with the **old color classifier** (`classifier_all17.pt`, its `finalize()` path) — the aperture-rule post-hoc transform that actually hit 100% on Game 2 lived in throwaway shell heredocs and is **gone**. The *rule* survives in `logic.py`; the *results* survive in `data/highfps_near_test/makemiss_v43{,_split}/`. So nothing is lost, but the runner isn't self-contained: run it fresh and you get classifier verdicts, not aperture ones.
- **Fix (small, safe, ~2 lines):** bake the aperture rule into `makemiss_v2.py::finalize()` (use `info["rho"] <= 1.10` for UNCERTAIN instead of the classifier `prob >= 0.5`), OR route the runner through `logic.py::decide()`. Deterministically identical to the validated 100%/95% numbers; **retires the color classifier** (drops `classifier_all17.pt` + `phase2_train` from the runtime — one less thing to carry onto the AGX for Stage 2).
- **For the Phase-1 build:** use **detector v3 + `logic.py::decide()` (aperture)** as the make/miss module — that IS the self-contained 99–100% logic. Do NOT wire in the classifier path.

### Where the model + docs live (repos OUTSIDE gopro-automation-linux)
- Weights + detector training/annotation:
  `~/Cellstrat/GitHub_Repositories/Training_frameworks/Uball HighFPS Shot Detection/`
  (`weights/ball_yolo26s_gray_hifps_v2_best.pt`, `src/train_gray.py`, `README.md`).
- Make/miss pipeline + eval artifacts:
  `~/Cellstrat/GitHub_Repositories/uball_shot_detection_dual_fusion_v2/`
  - `near_v0/highfps/makemiss_v2.py` (the runner), `score_supabase_gt.py`
  - `docs/HIGHFPS_SHOTCAM_HANDOFF.md` (the canonical high-fps status doc)
  - `data/highfps_near_test/makemiss_v2_full/SUMMARY.md` + `SUPABASE_GT_MATCH.md`
  - `data/highfps_near_test/arbitration_v42_final/CALLS.md` (the 17 disagreements)

---

## 4. Key insight for the plan

The high-fps pipeline has two parts of very different strength:
- **Make/miss DECISION** (given the window): **~95–100%** ← strong.
- **Spotter / trigger** (finding shots unaided): over-triggers (105 detected vs
  57 GT — warm-ups, putbacks) ← weaker.

**Phase 1 uses the strong part and skips the weak part** — the scorekeeper's press
IS the spotter (tells us *when*), so we only run the make/miss *decision* on the
window. The spotter is purely a **Phase 2** concern. → Build **Phase 1 first**.

---

## 5. Open design issues / decisions (still to resolve)

1. **Clock sync trigger↔FLIR footage @171fps** (the long pole). Scoreboard press is
   a wall-clock; SL/SR footage needs a frame-accurate anchor. ⚠️ Handoff warns:
   OpenCV seeks (`CAP_PROP_POS_FRAMES`) land ~13 frames early on this 171fps H.264 —
   **sequential decode only**. **NEXT ACTION: read `agx_pipeline/shot_recording.py`**
   to learn how FLIR frames are timestamped today.
2. **Don't pick SL vs SR from the operator's team button** (re-inherits the y2Hwx
   error). Run the triggered side AND cross-check, or run both and let CV say which
   rim → CV validates the side/team.
3. **Reaction-lag window:** operator presses ~2–6s AFTER the make. Look back N sec,
   then **refine the event time to the rim-crossing frame** (the docs confirm
   rim-crossing timing is what lifted their accuracy).
4. **Real-time reproduction on the Orin:** the offline batch took 4h10m for 6
   videos on an M4 — run a **tight window + subsample**, not full footage. Set a
   latency budget (validation appears ≤ X s after the clip; clip-first is fine).
5. **Compound plays:** miss→putback within ~3s currently collapse into one verdict.
   Fix in the new pipeline: **one verdict per rim-crossing**.
6. **Phase-2 spotter is net-new:** gray-hifps weights are blind on tracking cams
   (mAP 0.024). FL/FR/NL/NR trigger detection = separate model/logic (a cascade:
   tracking-cam trigger → high-fps confirm).
7. **Validation = agreement, not ground truth** (scorekeeper is the y2Hwx error
   source). Run Phase 1 as a **shadow metric** first; disagreements are the signal
   (operator error OR CV miss) and become labeled data.
8. **Dedup/priority reconciliation** (transition period): scorekeeper event + CV
   event within ±T s = same play (scorekeeper wins, CV attaches confidence);
   CV-only with no scorekeeper nearby = automation creates the card. Pick T.

---

## 6. Existing hooks to reuse (in gopro-automation-linux)

- **Trigger:** wb `queueClip` → `highlight_clip` command (`ts`, `logId`, `team`,
  `period`) → AGX relay `_do_highlight` → `HighlightBuffer`. Phase-1 trigger = this.
- **Side math:** `agx_pipeline/side_attribution.scoring_hoop_side(team, period, startingSideTeam1)`.
- **FLIR recording:** `agx_pipeline/shot_recording.py` (SL/SR high-fps).
- **TV display:** Firebase `basketball-games/{id}.highlights.{logId}` → `/tv-display`.
- **Card creation:** `plays_sync.create_plays_from_firebase_logs` (now firebase-source only).

---

## 7. Resume checklist

1. `git checkout feat/shot-detection-trigger`.
2. Read `agx_pipeline/shot_recording.py` → answer the clock-sync question (issue #1).
3. Get user's call on shadow-metric-first vs harden-first (largely resolved: model is ~95–100%, so ship Phase-1 shadow metric).
4. Write the Phase-1 plan (scorekeeper trigger → window → high-fps make/miss → shadow agreement on TV).

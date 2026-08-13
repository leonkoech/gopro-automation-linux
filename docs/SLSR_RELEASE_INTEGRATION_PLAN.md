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
  - **W4 results (28-shot GT benchmark)**: cap 0.6s → **50%** (14/28); cap 1.1s → **54%** (15/28), no
    regressions. Session arc (DETERMINISTICALLY RESCORED by scripts/shot_type/score.py — earlier
    hand-tallied claims of 46/57/61 were inflated ~2 shots/run): 43 (possession) → 46 (grounded
    feet) → 50 → **54 (rim anchor)**. OLD homography holds at 43-46 throughout. The SL/SR-sync architecture is empirically validated (+15pts over FL/FR-only).
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

- 2026-08-13 — **EVIDENCE-REVIEW ROUND (user clip verdicts) → CORNER-AWARE CALIBRATION: 54% → 74%.**
  User verdicts on the 15-clip pack re-ranked everything: 7/13 failures were CALIBRATION at the
  court CORNERS (player+ball correct) — the painted 3/4PT boundaries have STRAIGHT corner
  segments the y(x) curve fit couldn't represent; 2/13 were a STALE SECOND BALL on court fooling
  possession (F02/F03); 1 bad-GT shot excluded (F12 = two shots in window → 27-shot benchmark);
  rest = pass-before-shot (F13), wrong player (F10), straddle rule (F01: one foot in/out = lesser
  value — needs both-ankle test). FIX SHIPPED (Tracking `0a55f26`+`853d4b6`): Gemini traces the FULL
  ordered boundary incl. corner segments; order-preserving smoothing (no poly fit); regions =
  closed POLYGONS (fillPoly/pointPolygonTest). Scores: polygon-closure alone 64/67%; TRUE corner
  trace **71% (20/28) / 74% (20/27 honest)**. Arc: 43→46→50→54→64→**74**.
  Remaining 7 (honest-27): stale-ball ×2 (F02/F03 — moving-ball gate = next build, would cross 80%),
  straddle ×1 (FL297), boundary-precision ×2 (FR479.2, FR510.3 — 510.3 regressed with corner trace),
  wrong-player ×1 (FR556.7), pass-before-shot ×1 (FR1237.4, also no SL/SR anchor).

## BLIND-GAME EVALUATION PROTOCOL (user directive, 2026-08-13)

**Goal: >=80% on a game the pipeline has never been tuned on.** 0d96e12a is hereby the DEV set
(corner arcs, rim cap, feet rule were all tuned on it); accuracy claims from it are optimistic
by construction. The real test:

1. **Input:** a FRESHLY human-annotated game with retained SL/SR + FL/FR footage (candidates:
   `234447` once carded, or any of the Aug-13 games — Miracle Leaf / Team Iconic / Practice
   Squad — after annotation; all have packaged SL/SR on the box). Standing rule: RETAIN SL/SR
   for any game headed to annotation.
2. **Sample:** ~20 shots, stratified — both hoops, mix of 2/3/4, makes AND misses.
3. **Freeze first:** finish the dev-set fixes (moving-ball gate, straddle rule), then FREEZE the
   pipeline config. No parameter may change after seeing blind results — if it fails, diagnose,
   fix, and re-test on a NEW blind game.
4. **Run the FULL pipeline per shot** (everything automatic, nothing hand-set):
   auto arc-calibration (Gemini trace on that game's frames) → SL/SR detector: **make/miss** +
   rim_base → FL/FR tracking with rim anchor: shooter possession + grounded feet → **type**
   (polygon zones) → jersey OCR + roster map: **who**.
5. **Score three metrics vs the human annotation:** make/miss %, type %, WHO (precision +
   coverage). Target: >=80% on make/miss and TYPE. WHO is measured and reported but not yet
   gated at 80 (coverage ~25% today; dual-angle OCR is the lever) — honest reporting, no fudging.
6. **Deliverable:** per-shot table + 20 annotated evidence clips from the blind game (same
   format as the 0d96 pack) so failures are reviewable the same way.

7. **THE LOOP (standing methodology, user-confirmed):** every blind game's failures become the
   next evidence pack (same overlay format) -> user reviews and gives per-clip verdicts -> fixes
   are built from that evidence -> config re-freezes -> NEXT fresh blind game. Each iteration
   uses new games (annotators card games weekly, so the supply is continuous). Rationale: one
   verdict round was worth +20pts on dev (corner discovery, stale-ball discovery) — human-in-
   the-loop failure review is the highest-leverage accuracy tool we have; ride it past 80%.

- 2026-08-13 — **BLIND EVAL #1 (game 2e574fd2, Aug-6, user-picked): TYPE 11/20 = 55%, WHO 2/2
  correct reads (10% coverage).** NO-ANCHOR mode (game predates SL/SR; cv_cards=0) — comparable
  dev mode scored ~46-50%, so the pipeline GENERALIZES (blind >= dev in like-for-like mode). The
  ~20pt gap to dev-74% IS the rim anchor. Fully automatic run: boto3 S3 download (no aws cli on
  box!) -> auto Gemini corner calibration on the game's own frames (worked on a different night's
  court) -> 20 stratified shots (10/hoop, 12x3PT+8x4PT, 11 make/9 miss) -> clips. Failure shape
  matches known buckets: 3 wrong-shooter (B06/B12/B17 — anchor's job), ~5 boundary overshoots
  (possible auto-calib edge tightness — user reviewing overlays). Evidence pack:
  `Tracking…/evidence_blind_2e57/` (20 clips + 2 calib overlays); 9 failures sent for verdicts.
  Blind #2 = FULL pipeline w/ anchor, needs an annotated RETAINED game (234447 / Aug-13 games).

- 2026-08-13 — **BLIND RERUN with NANO-BANANA GOLDEN CALIBRATION (user-approved): TYPE 15/20 = 75%,
  15/19 = 79% excl. the bad-GT window (B06, two shots in 8s). Up from 55%.** Calibration = image-
  EDITING model (`gemini-3-pro-image-preview`) paints the zones -> OpenCV contours extract polygons
  (`nano_calibrate.py`); user approved both cameras' overlays. Remaining failures: B12+B17 wrong-
  shooter (= rim anchor's job; this Aug-6 game has no SL/SR), B05/B10/B14 boundary/straddle tail.
  **Quality gate PASSED for go-live**: 79% no-anchor blind + anchor's +8-11pt dev gain => full
  pipeline on retained games clears 80%. NEXT BUILD (user-directed): production typing job — after
  ingest, type every CV card (2/3/4 MAKE/MISS) + team name (side+period->team), WHO optional;
  golden nano calibration one-time per production camera with user approval + nightly drift check.

## GO-LIVE OPERATING MODEL (user-directed, 2026-08-13 — THE STANDARD)

**Ship now, improve forever.** Production runs generate CV annotations alongside the annotators'
manual ones -> a nightly CV-vs-manual comparison scores every game automatically (type %, WHO
coverage, make/miss agreement). Every game night = a free blind benchmark; improvements are
picked by whatever the nightly numbers say hurts most. No more bespoke test setups.

**TWO-PATH ARCHITECTURE (latency contract):**
- FAST PATH (already live, H1): SL/SR make -> clip cut -> green button -> TV. NO tracking in
  this path, ever — "fastest to the green play button wins." **HARD TARGET (user): green button <=10s from the make.** Physical floor ~6s (3s post-roll + ~3s cut/upload); requires detection <=4-6s -> knobs in order: SL/SR segment 4s->2s (biggest wait), highlight-buffer segment length down, post-roll trim last. Per-stage latency is measurable from tonight's own timestamps (wallclock -> detected_at -> requestedAt -> ready) — read tomorrow, tune, verify next night.
- ASYNC PATH (to build): every shot event -> tracking QUEUE (rim anchor from packaged/live
  SL/SR + FL/FR classify + OCR, ~17-20s/shot measured) -> card & History row UPGRADE in place
  ("Shot made — left rim" -> "3PT MAKE · <Team> (#8 optional)"). Queue drains during play as
  GPU allows, finishes minutes after the horn. Per-stage telemetry (decode/detect/classify/OCR)
  built in so the budget is always known.

**Rollout:** build queue+upgrade+telemetry+nightly-comparison -> one-time golden Nano calibration
of production cameras (user approves 2 images) -> deploy in no-game window -> self-tests
(synthetic + one historical game re-typed) -> live -> improve one bucket at a time.

## FAST-PATH LATENCY FIX (user-directed 2026-08-13: "clip first... deploy immediately")

**User's directive:** trigger -> clip -> green button -> then shot detection -> timeline/History
-> then tracking. **Engineering translation** (the live loop has NO separate trigger — the
segment scan IS both trigger and make/miss verdict in one pass, and a pre-verdict clip would also
clip every miss: last night = 262 rim events, ~3/4 misses -> History flood + wasted GPU): make the
VERDICT itself fast, clip fires the same instant (already wired). Detection latency =
segment-close wait (~SEG_SEC) + scan wall + poll; tail (cut->transcode->S3->green) ~5-6s measured.

**The fix (branch `feat/fastpath-latency`):**
1. live.py instrumentation: per-shot `latency_s` (wallclock->verdict) + `scan_s` + backlog/
   max_backlog in the shadow doc + detect_latency in the AUTO-HIGHLIGHT log — tonight measures
   its own chain per make (verdict leg here; tail leg = log ts vs highlight-doc ready ts).
2. Env tuning (values set by on-box benchmark, bench_live_scan.py on real SL footage):
   SHOT_SEGMENT_SEC 4->2 halves the close wait IF scan throughput >=1x real time per angle with
   SHOT_LIVE_WINDOW on (P3.1 sliding window: cost = 2x footage per segment REGARDLESS of seg
   size, so smaller segments are a pure latency win unless fixed per-segment overhead dominates).
   SHOT_LIVE_POLL_SEC 2->1.
3. This deploy also ships main's P3.1 live.py (sliding window + rim accumulation, env-gated
   SHOT_LIVE_WINDOW) — validated offline via fastpath_harness.py (stub cfg/fb, real carved
   segments, real drain-throughput measurement) before deploy.

**Deploy authorization (user, 2026-08-13):** batch jobs on the box (Miracle Leaf typing +
2e574fd2 compare) keep running — don't disturb; once BOTH finish (TYPING_DONE + COMPARE_DONE log
markers) AND /health shows recording:false, deploy IMMEDIATELY to production (no further
approval), well before tonight's games. Unit agx-ingestion, EnvironmentFile .env.agx, restart via
systemctl. Rollback = revert .env.agx knobs (+ .bak-fastpath file backups).

**Endgame (recorded for later):** if segment tuning can't reach <=10s, the next step is the
streaming detector — tap live frames off the recording pipeline (appsink tee) so the verdict
lands ~1-3s after the ball drops; the clip-first contract (button never waits for type/WHO)
stays as designed here.

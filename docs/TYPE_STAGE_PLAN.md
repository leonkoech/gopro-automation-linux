# TYPE Stage Plan — 78% → 90–95% (started 2026-08-18)

Detection is closed (6-game recall 92.1%, zero phantoms). This stage improves
shot-TYPE classification. Client target stated 2026-08-17: 90–95%.

## Baseline & data
- Honest baseline: **78%** on the 144-shot blind game (corner-aware polygon
  calibration; dev-set peak 74% on the 27-clip verdict set).
- Eval data now: **814+ GT shots across 6 games** with classes
  (2PT/3PT/4PT × make/miss + FREE_THROW), growing as annotators finish
  SoA/WoB/CityBoyz. Weekend added ~785 cards on Complete games.

## Workstream 1 — TYPE eval harness (first deliverable)
Extend the gt_eval flow: for every matched GT↔CV pair, run the classify chain
(grounded-feet vs calibrated polygons, SL/SR rim-time anchor `SHOT_RIM_TS`)
on the CV shot and score TYPE agreement against the GT class. Output per-game
confusion matrix (2/3/4/FT) + disagreement clips named by GT-vs-CV class.
- Runs on the box (GPU) — post-game gate only (06:00–20:00 UTC floor,
  30-min recording-false streak; the pattern that ran Akatsuki cleanly).
- Reuse detection matches from existing reports where possible; classify is
  the expensive part (~15-20s/clip → batch overnight).

## Workstream 2 — FREE_THROW class (the architecture gap)
User finding: FTs are real 1-pt events currently mislabeled FG/2PT. V1
heuristic, all signals we already extract:
1. **Position**: shooter's grounded feet inside a calibrated FT-stripe polygon
   (add to the per-camera court calibration next to the arc polygons).
2. **Stationarity**: shooter effectively static ≥1.5s pre-release (tracking
   positions) — separates FTs from live mid-range shots at the same spot.
3. Optional later: key-lineup detection (players stacked on the lane).
Classify precedence: FT check BEFORE arc classification; emits FT_MAKE/FT_MISS
(1 pt). Live scorecard: cv_points values gain 1 as a legal value.

## Workstream 3 — known type wins (after FT lands)
- Moving-ball possession gate (stale-2nd-ball errors; +2 confirmed on dev set).
- Straddle both-ankle rule at polygon boundaries.
- Boundary tail precision on the traced arcs.

## Sequencing
1. Harness + FT polygon calibration code (CPU-side, tonight-safe).
2. Gated overnight run: type eval on the 6 GT games → confusion matrix.
3. FT class in the classify chain → re-run → measure.
4. Iterate W3 items against the matrix until ≥90%.
5. Then WHO-rewind prototype (deadline ~Aug 24–27): shot trigger → rewind
   single angle to readable jersey (rim-time anchor) → track forward → roster
   name→number mapping.

## Ops notes
- Box jobs: source `.env.agx`, script-file launchers (pgrep self-match ×5),
  hard UTC-floor gates (test recordings fool "saw a game" heuristics).
- Never GPU during recording; check `/health`.

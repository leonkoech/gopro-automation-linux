# Live deploy checklist — typing v2 + WHO scan

**Never deploy while `/health` shows `recording:true`.**

## Status as of 2026-09-21

- **TYPE v2 is DEPLOYED and verified.** Merged to main and shipped to the box
  on 2026-09-21; `typing preflight ok` appears at boot. Before this, live
  typing had produced nothing for fifteen days — `agx_classify.py` in
  `TYPING_CWD` was a symlink into `/home/dev/scratch_shot_timing`, deleted by
  the 2026-09-03 disk cleanup, so every spawn exited 2 on python's "can't open
  file". Re-running the exact 12 shots that failed on Sep 18: 0 failures,
  7 typed, 5 honest `UNKNOWN` from STRICT.
- **WHO is still OFF**, and not only because of its gate — see the blocker
  below. Its models ARE on the box, staged and md5-verified against local
  copies at `/home/dev/who_deps_staging/` (jersey weights 139 MB,
  `unified_yolo26s.pt` 20 MB, `uball_cc`). They are deliberately NOT in
  `TYPING_CWD`: dropping them there silently re-enables jersey reads inside
  `agx_classify` and starts populating `cv_points.who` with the old WHO v1.
- **main is merged but NOT pushed** to origin — the push needs approval.

## The WHO blocker: SHOT_WHO_ROSTER is never set

`who_scan_live.py` reads the roster once at process start, and an empty value
disables the per-team filter entirely:

```python
ROSTER = {n.strip() for n in os.getenv("SHOT_WHO_ROSTER", "").split(",") if n.strip()}
...
if nn is not None and (not ROSTER or str(nn) in ROSTER):   # empty = NO filter
```

Nothing in the pipeline sets it — `_classify_env()` does not. That filter is
the largest measured accuracy lever there is (**56% → 90%**), so enabling the
gate today ships the 56% version onto annotator cards. Worse, the variable
holds the *shooting team's* numbers, so it changes per possession: a static
env var is the wrong shape for it. It needs to come from the check-in roster
at enqueue time, per shot side.

`scripts/stage_check.py` reports WHO as BLOCKED for exactly this reason even
when every model is present and the gate is on.

## What ships
- **TYPE**: the 86.9% classify stack (505-shot measured), health-gated solver
  rescue. Unchanged from your review copy.
- **WHO**: `who_scan_live.py` — eval-validated scan (90% correct-when-spoken
  strict / 80% at 46% coverage). Seeds from the typing pass's release feet,
  tracks the shooter ±2.5s in the shot clip, jersey-votes with per-team
  roster, speaks only on a dominant vote. Gated by `SHOT_LIVE_WHO_SCAN`.
  The older NL-window WHO v1 (`shot_who_live.py`) stays available behind its
  own `SHOT_LIVE_WHO` gate as fallback.

## Box graft prerequisites (the cloud PYTHONPATH lesson — verify, don't assume)
The classify working dir (`TYPING_CWD`) must contain / resolve:
1. `who_scan_live.py` + `who_from_nl.py` (copied there by deploy, like
   `agx_classify.py`)
2. `uball_cc` importable — either installed in the box venv or
   `PYTHONPATH=<repo>/src` present in `_classify_env()`
3. Jersey weights: `runs/jersey/{legibility_resnet18.pt,`
   `number_localizer_yolo11n.pt, parseq_jersey.pt}` (145 MB) at the path
   `jersey_stack.py` expects, and torch.hub access for the parseq code
   (first call clones from GitHub — pre-warm it)
4. Unified detector weights `unified_yolo26s.pt` (or set `SHOT_WHO_DET`)

**Readiness check (run this first — it covers every item above and prints the
reason and path for anything blocked):**
```bash
python3 scripts/stage_check.py     # exit 1 if an ENABLED stage cannot run
```

**Boot check (run on the box before enabling, catches every silent-import
failure):**
```bash
cd $TYPING_CWD && python3 - <<'EOF'
from uball_cc.tracking.jersey_stack import JerseyStack
from ultralytics import YOLO
JerseyStack(); YOLO("unified_yolo26s.pt")
print("WHO-scan deps OK")
EOF
```

## Env (.env.agx)
```
SHOT_LIVE_WHO_SCAN=true
SHOT_WHO_ROSTER=<shooting-team numbers, comma-sep — from check-in roster>
SHOT_WHO_MIN=3.0          # strict TV mode (90% when spoken)
SHOT_WHO_RATIO=2.5
SHOT_WHO_WIN=2.5
SHOT_WHO_STEP=0.15
SHOT_WHO_NL_OFF=0         # per-venue NL clock offset if NL is ever wired
```
Coverage mode (more names, 80% when spoken): `SHOT_WHO_MIN=2.0
SHOT_WHO_RATIO=1.8`.

## Ops rule (measured, matters)
No duplicate jersey numbers across the two teams on court — the per-team
roster filter is the single biggest accuracy lever (56% → 90% in eval).

## Rollback
`SHOT_LIVE_WHO_SCAN=false` + service restart — typing continues untouched
(scan failures already never block typing).

## Verify after deploy
1. Boot check above prints OK.
2. First live make: `cv_points.{logId}.who_scan` appears (or honest absence).
3. GPU scan latency unchanged (scan runs post-typing on the cut clip,
   ~10–20s; watch first-night queue depth).

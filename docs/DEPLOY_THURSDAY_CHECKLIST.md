# Thursday live deploy — `feat/shot-typing-v2` checklist

Branch: 3 commits (`fe510c8` typing v2 stack, `e4802f8` live WHO v1,
`2a42a6a` WHO scan). Deploy window: Tuesday (no games). **Never deploy while
`/health` shows `recording:true`.**

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

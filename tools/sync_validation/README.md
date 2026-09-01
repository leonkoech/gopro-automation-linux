# Camera sync validation (OFFLINE ONLY — never runs in the live pipeline)

Answers one question with evidence instead of assumption:

> when the trigger fires on SL, does the ball reach the rim at that same instant on FL?

That is what "the cameras are synced" has to mean, and nothing in the live
pipeline checks it. These tools check it after the fact, from the recordings.

## Why this is not wired into the live path

The tracking-camera detector runs at `imgsz=1280` (the specialist was trained
there; at 640 the ball is 12-20 px and effectively invisible). Measured on a
90 s window: the shot-cam model does ~59 inferences/s, the tracking-cam model
~9/s. Putting a second, heavier model in the live loop would add GPU load
during a game and buy nothing live — the answer is only needed when validating,
and validating can happen any time afterwards from S3.

**Do not import these from `agx_pipeline`.** They exist to audit it.

## The two halves

| script | model | reads | gives |
|---|---|---|---|
| `detect_shotcam.py` | `ball_yolo26s_gray_hifps_v3_best.pt` | SL / SR master | rim crossings + MAKE/MISS |
| `detect_trackingcam.py` | `yolo26s_ball_hoop_specialist.pt` | FL / FR 1080p | ball-at-hoop moments |

Both timestamp with **measured** fps (`nb_frames / duration`), never the 120
lock. The lock is wrong by 118.5-119.9 on the shot cams, which is up to 37 s of
error by the end of a game — the same class of bug this whole exercise was
about, so it must not be reintroduced here.

## Result on 2026-08-24 Uptown 66ers vs The Lost Boys, 90 s window @1460 s

| shot cam | crossings found | matched in FL | matched in FR |
|---|---|---|---|
| SL | 3.50, 42.30, 64.50 | **3 / 3** | 0 / 3 |
| SR | 27.00, 77.50 | 0 / 2 | **2 / 2** |

Offsets: FL−SL `+0.40 / +0.49 / +0.57 s`, FR−SR `−0.28 / −0.99 s`.

Two conclusions, both measured rather than argued:

1. **`SL -> FL` and `SR -> FR` is correct.** The `_HOOP_SIDE` mapping in
   `live.py` is right, and reports of "a shot on SL appearing in the FR clip"
   are not a routing fault.
2. **The cameras are synced to well under a second.** A clip landing minutes
   away from its shot was never a sync problem; it was the timestamp. A clip cut
   at the wrong time shows whatever was happening then, which is usually play at
   the far end — a wrong time *looks* like a wrong side, which is what made this
   hard to read from the clips alone.

## Running it

```bash
# a window is enough to validate; a full game takes ~24 min per shot cam
python tools/sync_validation/detect_shotcam.py \
    --video SL.mp4 --angle SL --out sl.json
python tools/sync_validation/detect_trackingcam.py \
    --video FL.mp4 --angle FL --out fl.json --end 90
```

Then compare the two lists: crossings should appear in the paired tracking
camera and be absent from the other. If a crossing appears in *both*, the
proximity threshold is too loose; if in *neither*, look at the hoop detection
before doubting the sync.

Weights are not in this repo. Shot-cam weights ship with the pipeline under
`agx_pipeline/shot_detect/weights/`; the tracking-cam specialist comes from
`Tracking-Cross_camera_association-SAM3/runs/handoff_weights_yolo26s/`.

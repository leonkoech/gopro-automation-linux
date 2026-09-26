# Possession tracker — who shot it, without jersey numbers

Offline research code, **not wired into the live pipeline yet**. It finds the player who
released each shot by following the ball and the players, and types the shot from that
player's feet. Identity (which jersey) is deliberately out of scope; this only has to pick
the right *person*.

## Result so far (2026-09-25, whole-game run in progress)

TYPE (2PT / 3PT / 4PT / FT) against manual GT, same shots, both 14-Sep games:

| | production today | tracker (median takeoff feet) |
|---|---|---|
| cb9e1294 (68 shots) | 58 (85%) | **65 (96%)** |
| 7cef734e (53 shots) | 39 (74%) | **46 (87%)** |
| both (121) | 97 (80%) | **111 (92%)** |

The median-feet rule was picked on these 121 shots and then **frozen**
(`frozen_dev_set_2026-09-25.json` on the box); the remaining ~150 GT shots are the held-out
test. Shooter choice alone: 22/23 on the human shooter labels (the miss looks like a label
error), 33/33 shots checked by eye across three review batches.

## How it works

`perceive.py` (slow, GPU, once per clip, cached as `.npz`):
- SAM3 video tracking, text prompt "basketball player": id, box, mask per frame (15 fps,
  rim −4 s … +2 s)
- ball + hoop detector (the typing stage's own weights): every candidate, not just the best

`holder.py` (fast, CPU, seconds per clip) — every rule below exists because a real clip broke
without it:
1. **Rim arrivals.** Hoop = median box. An arrival is a ball near the hoop that is *falling*
   and *rim-sized* (pixel size matches a ball at rim depth, from the camera pose). A ball
   held high in front of the hoop overlaps it but is 1.3–1.6× too big.
2. **Ball tracked backwards from each arrival.** Long jumps only onto confident detections
   (≥0.3); faint ones only within 40 px of the prediction (a spectator's lap once stole a free
   throw).
3. **Possession = contact.** The ball touches the player's SAM3 mask. Ball-size depth is only
   a tiebreak between two touching players, and a veto at 450 cm for one — it is ±150 cm noisy
   and a jumping player's feet project far behind him.
4. **Shooter = the player touching the ball at release** if he has ≥2 contact frames in the
   0.6 s window, else the majority (a closely guarded dribbler once lost to his defender).
5. **Every arrival is its own shot** (miss + putback = two shooters); bounces with no new touch
   merge into the shot before.
6. **Physics gate:** release height → rim in ≤9 m/s average climb (two balls in a stoppage).
7. **Off-court tracks** (feet on court <30% of the clip: spectators, bench, wall posters) never
   hold the ball. Judged per track over the clip, not per frame — a mid-layup player projects
   behind the baseline for a few frames.
8. **Rim time from the data**, not the clip's nominal time (cuts were up to 1.5 s off).

`eval_full.py`: pairs each CV clip with a GT card (same rule as `confirm_test/loop_score.py`),
picks the attempt nearest the card in time, and types it three ways — production, production's
feet on the per-game fitted arcs, the tracker's feet (maxy / **med** / early) on the same arcs.
The last two differ only in *which player's feet*, so that gap is the tracker.

`score_labels.py`: strict check against the 23 human shooter labels (their feet are
release-pose feet; matched ±0.3 s around the detected release).

## Running it (AGX, only when not recording)

```
box/sam3env.sh     CUDA + SAM3 python path
box/start_full.sh  builds the GT-paired-first queue for both games, starts ONE worker + guard
box/worker.sh      perceive every clip in the queue; waits while /health says recording
box/gameguard.sh   kills perception the moment a game starts
box/score_now.sh   holder on every cached clip + eval for both games
```

Lessons: two workers in parallel are slower than one (GPU saturated). Never `pkill -f` a
pattern that appears in your own ssh command line — put the logic in a script file.

## Getting it into the regular pipeline

The blocker is speed, not accuracy: ~4.3 min per shot on the Orin (SAM3 on 91 frames) =
~12 h per game.

**Measured 2026-09-26 on 147 GT shots** (production 117/147; tracker rule frozen):

| perception | tracker right |
|---|---|
| SAM3 masks, 15 fps (current) | **134 (91%)** |
| boxes instead of masks | 126 (86%) |
| 7.5 fps | 126 (86%) |
| boxes and 7.5 fps | 119 (81%) |

Both the masks and the frame rate carry the gain, so "plain detector boxes" alone is not
enough. Options, cheapest first: a YOLO *segmentation* model + ByteTrack (masks at detector
speed, TensorRT-able — test next), SAM3 via TensorRT on the Orin, or SAM3 on a cloud GPU
(estimated ~45-80 s/shot on an A10G, ~2-4 h/game on one GPU, parallel across GPUs).
Held-out check: 23/26 tracker vs 20/26 production on GT shots the rule was never tuned on.

In order:

1. **How much survives cheaper perception** (no GPU needed — simulated on the SAM3 caches):
   `CONTACT_MODE=box` (boxes instead of masks — what YOLO + a tracker gives) and `FPS_DIV=2`
   (7.5 fps). If boxes hold the gain, perception becomes the existing player detector +
   ByteTrack: seconds per shot.
2. **Per-game calibration fit at ingest.** The fitted arcs alone are worth +5 shots on 7cef.
3. **Shadow mode:** new attribution behind an env flag in the typing stage
   (`SHOT_ATTRIB=track`), writing its answer next to production's for a few game nights.
4. **Switch over** only after shadow nights agree with GT, with the usual deploy rules (never
   during a game, surgical deploy with backups).
5. Later: carry the tracked person into WHO (jersey reading) — identity rides on the track.

Known gaps: the takeoff-feet rule is least sure on shots standing on the 3/4 line; one game-2
spectator sits within 1.5 m of the baseline and survives the off-court filter (the ball rule
covers that clip).

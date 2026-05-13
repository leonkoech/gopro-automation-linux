# CV Shot Detection — Production Integration Plan

**Status**: Proposed
**Owner**: Rohit Kale
**Last updated**: 2026-04-15

This document covers the plan to take the `Uball_dual_angle_fusion` shot-detection
script from a research-grade CLI to a production service that auto-creates shot
cards from game video, ending the need for manual in-game log entry.

---

## 1. Context

Today, cards in the annotation tool are created from **manually-entered game
logs** that operators type during or right after the game. We want to automate
that with computer vision: given the per-game 1080p videos produced by the
existing AWS Batch transcode pipeline, emit shot events (made / missed) that the
existing card-creation ingestion (`dcd12c4 feat: auto-create annotation cards from Firebase logs in ingestion pipeline`) picks up and turns into cards.

The CV logic already exists in
`Uball_dual_angle_fusion/dual_angle_fusion.py` — YOLOv11n detection per angle
plus V3 feature-weighted fusion. In evaluation it hits **95% matched-shot
accuracy / 85% GT coverage** on test games. V1 is about productionizing that,
not re-inventing it.

---

## 2. V1 Scope (and what is explicitly deferred)

**V1 delivers, per game:**


| In V1                                                                             | Out of V1 (V2 / V3)                     |
| --------------------------------------------------------------------------------- | --------------------------------------- |
| Made / missed classification per shot                                             | Player identification                   |
| Per-shot video timestamp (seconds from game start)                                | Jersey-number OCR                       |
| Team attribution (full-court via hoop side + halftime flip)                       | 2pt / 3pt / 4pt distance classification |
| Auto-emission of Firebase game-log entries (existing card pipeline picks them up) | Free-throw detection                    |
| Idempotent re-runs (skip if CV already produced plays for a game)                 | Court-line calibration / homography     |
| Observability: CloudWatch metrics, failure alarms                                 | Realtime / in-game detection            |


"Team attribution via position" works because this is **full-court** play:
each team attacks one hoop per half, swaps at halftime. V1 records the starting
side in the check-in flow; CV detects which hoop the ball went into; combining
the two gives the shooting team. Player attribution stays null on the card
until V3.

---

## 3. Architecture

```
   ┌───────────────────────────────────────────────────┐
   │     Jetson-1 (FR + NL)        Jetson-2 (FL + NR)  │
   └──────────────┬─────────────────┬──────────────────┘
                  │ raw chapters    │
                  ▼                 ▼
           ┌─────────────────────────────┐
           │   S3 raw-chapters/          │
           └──────────────┬──────────────┘
                          │
                          ▼
           ┌─────────────────────────────┐
           │ Existing AWS Batch          │
           │ (ffmpeg-nvenc transcode     │
           │  → 1080p per game per angle)│
           └──────────────┬──────────────┘
                          │ writes court-{loc}/{date}/{game_uuid}/*.mp4
                          │   (4 files: FL, FR, NL, NR)
                          ▼
           ┌─────────────────────────────────────────┐
           │ Existing cron (5 min)                   │
           │ -> POST /api/cv/dispatch-pending        │
           └──────────────┬──────────────────────────┘
                          │
                          ▼
           ┌─────────────────────────────────────────┐
           │ Flask (main.py) — cv_dispatch handler   │
           │  - scans games with 4 angles in S3      │
           │  - dedupes via UBall list_plays         │
           │    (skip if source='cv' rows exist)     │
           │  - submits 2 fusion Batch jobs          │
           │  - submits 1 merge job depending on     │
           │    both fusion jobs                     │
           └──────────────┬──────────────────────────┘
                          │
               ┌──────────┴──────────┐
               ▼                     ▼
         ┌────────────┐       ┌────────────┐
         │ Batch GPU  │       │ Batch GPU  │
         │ Side 1     │       │ Side 2     │
         │ FR + NR    │       │ FL + NL    │
         │ (fusion)   │       │ (fusion)   │
         └─────┬──────┘       └─────┬──────┘
               │ detection_results.json per side
               └──────────┬──────────┘
                          ▼
           ┌─────────────────────────────┐
           │ cv-merge step               │
           │  - merges Side1 + Side2     │
           │  - applies team attribution │
           │    (hoop side + half flip)  │
           │  - writes Firebase logs     │
           └──────────────┬──────────────┘
                          │
                          ▼
           ┌─────────────────────────────┐
           │ Existing ingestion (dcd12c4)│
           │ → Supabase `plays` rows     │
           │ → Annotation tool cards     │
           └─────────────────────────────┘
```

### Key architecture decisions


| Decision           | Choice                                              | Rationale                                                                                                |
| ------------------ | --------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Compute platform   | **AWS Batch (GPU, g4dn.xlarge)**                    | Already using Batch + this instance type for transcode. Fits within the current g-spot vCPU quota (no quota ticket needed). ~$0.12/job. |
| Trigger            | **Existing 5-min cron → `POST /api/cv/dispatch-pending` (Flask on Jetson)** | Mirrors the existing `register_completed_batch_jobs` pattern; reuses boto3 + AWS creds already loaded in `main.py`. No Lambda, no DynamoDB. |
| Latency target     | **Best-effort (no hard SLA)**                       | Cost-optimal; revisit SageMaker when realtime is needed                                                  |
| Camera pairing     | **Side 1 = FR+NR; Side 2 = FL+NL**                  | Matches physical court layout; each pair watches one hoop                                                |
| Team attribution   | **Hoop-side + halftime flip**                       | Works for full-court; simpler than HSV color-match                                                       |
| Player attribution | **Null in V1**                                      | Jersey OCR + player tracking is V3                                                                       |
| Idempotency        | **Skip if CV plays already exist for game**         | Confirmed preference                                                                                     |
| Output path        | **Firebase game logs (same shape as manual entry)** | Piggyback on existing ingestion; no annotation-tool change                                               |


---

## 4. Phases & timeline

Estimates below are engineering-days (1 day = one focused 6-8h working day).
Several phases can overlap when run by the same engineer — e.g., containerize
while the IAM/Batch environment provisions, write the team-attribution logic
while E2E test games queue. The critical-path total is therefore shorter
than the simple sum.

### Phase 0 — Infrastructure provisioning  —  **0.5 day**

- **Prerequisite**: `ANGLES_TO_PROCESS=FL,FR,NL,NR` on both Jetsons (done 2026-04-15 — NL+NR 1080p will now flow)
- AWS Batch GPU compute environment (g4dn.xlarge spot, min 0 / max 4 vCPU, reuses existing compute env pattern)
- AWS Batch CPU compute environment for the merge job (m5.large spot, min 0 / max 2 vCPU)
- New Batch job queues: `cv-shot-detection-queue` (GPU) and `cv-merge-queue` (CPU)
- ECR repos: `uball-cv-fusion` and `uball-cv-merge`
- S3 buckets/prefixes:
  - `s3://uball-cv-models/yolov11/v1/` — versioned model weights + MANIFEST.json
  - `s3://uball-cv-results/court-{loc}/{date}/{game_uuid4}/` — per-game detection JSON per side
- IAM:
  - `cv-batch-job-role` — S3 read (raw-chapters + 1080p + models), S3 write (cv-results)
  - Extend existing Jetson IAM user `rohit` with `batch:SubmitJob` on the new queue + `batch:DescribeJobs` for status polling (probably already has it from transcode)
- SNS topic `cv-pipeline-alerts` for failure alerts

### Phase 1 — Containerize the CV script  —  **1 day**

- Dockerfile from `nvidia/cuda:12.x-cudnn-runtime-ubuntu22.04` base
- Pin: `torch==2.x+cu12x`, `ultralytics>=8.0`, `opencv-python`, `numpy`, `scipy`
- Entrypoint: read env vars `NEAR_S3_KEY`, `FAR_S3_KEY`, `GAME_ID`, `SIDE`, write `RESULT_S3_KEY`
- Fetch model weights from S3 on startup (cached in `/tmp` across warm Batch workers)
- Replace `print(...)` with `logging` at INFO/WARN/ERROR; JSON format for CloudWatch parsing
- Wrap near-detection and far-detection subprocess calls with retry (3x, exponential backoff) and stderr capture into exception message
- Decouple Supabase GT validator from the critical path: `--validate-accuracy` is explicit; default skips it
- Push image to ECR: `uball-cv-fusion:v1`

### Phase 2 — Flask dispatch endpoint + Batch job definitions  —  **0.5 day**

**Reuse path**: this mirrors `register_completed_batch_jobs` (`main.py:2614-2693`) exactly — poll, scan, submit, respond.

- Register 2 Batch job definitions (inline JSON in `deploy/batch-job-defs/`):
  - `cv-fusion-job-def.json` — uses `uball-cv-fusion:v1`, 1 GPU, 4 vCPU, 16 GB, timeout 45 min, retry 2
  - `cv-merge-job-def.json` — uses `uball-cv-merge:v1`, 2 vCPU, 4 GB, timeout 10 min, retry 2
- New module `cv_batch_dispatch.py` (sibling to `aws_batch_transcode.py`):
  - `submit_fusion_job(game_uuid, side, near_s3_key, far_s3_key, result_s3_prefix, model_version) -> job_id`
  - `submit_merge_job(game_uuid, side_a_result_key, side_b_result_key, depends_on=[fusion_job_id_a, fusion_job_id_b]) -> job_id`
- New Flask endpoint in `main.py`:
  - `POST /api/cv/dispatch-pending` — no request body
  - Loop: for each Supabase `games` row where all 4 angle `video_metadata.s3_key`s exist AND no `plays.source='cv'` yet AND `firebase_game_id` is set:
    1. Read Firebase game for `startingSideTeam1` + `leftTeamId` / `rightTeamId`
    2. Submit 2 fusion jobs (one per side) + 1 merge job depending on both
    3. Write `cv_dispatched_at` ISO timestamp on the Firebase basketball-games doc so retries skip it
  - Returns `{"dispatched": N, "skipped": M, "waiting_on_angles": K}`
- Cron wire-up: add `/api/cv/dispatch-pending` to the existing 5-min scheduler that already hits `/api/batch/register-completed`
- CloudWatch custom metrics (posted from Flask using boto3 cloudwatch client, namespace `UBall/CV`):
  - `CVDispatchSubmitted` (count)
  - `CVDispatchSkippedAlreadyProcessed` (count)
  - `CVDispatchWaitingForAngles` (count)

### Phase 3 — Merge + team attribution  —  **1 day**

- New Python entrypoint `merge_results.py` (separate Batch container; CPU-only is fine)
- Fetches both sides' `detection_results.json` from S3
- Computes per-shot team: look up Firebase `basketball-games.{game_id}.startingSides` (Team A attacks Side 1 from start, flip at halftime via `basketball-games.{game_id}.halftimeAt` timestamp)
- If a shot is within `temporal_window` of an event in both sides' output, prefer the higher-confidence one (cross-pair dedup)
- Writes merged `{team_id, classification, timestamp_seconds, confidence, source='cv'}` records
- **Frontend addition**: check-in page needs a "Starting side" picker (left / right hoop) per team. Small — ~2h of UI work folded into this phase.

### Phase 4 — Card emission to Firebase logs  —  **0.5 day**

- Merge job's final step: push shot events into `basketball-games/{game_id}/logs[]` matching the shape produced by the existing manual-entry flow (same fields, `source: 'cv'`, `confidence: 0.xx`)
- The existing ingestion (`dcd12c4`) auto-creates `plays` rows in Supabase from these logs — **no annotation-tool-backend change needed for V1**
- Add a `cv_run_id` field on each log for traceability (which Batch job produced which card)

### Phase 5 — E2E validation on past games  —  **1.5 days**

- Pick 3-5 already-completed games with known human-annotated plays
- Run the full cron → Flask dispatch → Batch → merge → Firebase flow end-to-end (in a staging prefix to not pollute production cards)
- Automated comparison script: for each game, compute precision / recall vs human plays on (timestamp ±1s, classification)
- Tune unmatched-shot confidence thresholds (`--prioritize_coverage` vs default)
- Deliverable: per-game accuracy report signed off by client

### Phase 6 — Observability + hardening  —  **0.5 day**

- CloudWatch metrics: `CVJobSuccess`, `CVJobFailure`, `ShotsDetectedPerGame`, `MeanConfidence`, `JobDurationSeconds`
- CloudWatch alarm on `CVJobFailure > 0 in 1h` → SNS → email
- Runbook doc: how to re-run a failed game, how to rollback model weights, how to bypass CV for a game
- Log retention policy (14 days default)

### Phase 7 — Shadow rollout → production cutover  —  **1 day**

- CV writes logs to `basketball-games/{game_id}/cv_logs_staging[]` (NOT the real logs array) for 2-3 live games
- Client compares CV output to their manual entries side-by-side in the annotation tool
- When greenlit, flip the merge-step target from `cv_logs_staging` → `logs`
- Keep the staging path available for a follow-up testing window

### Summary


| Phase                        | Days                     |
| ---------------------------- | ------------------------ |
| 0 — Infra                    | 0.5                      |
| 1 — Containerize             | 1                        |
| 2 — Flask dispatch + job def | 0.5                      |
| 3 — Merge + team attribution | 1                        |
| 4 — Card emission            | 0.5                      |
| 5 — E2E validation           | 1.5                      |
| 6 — Observability            | 0.5                      |
| 7 — Shadow rollout + cutover | 1                        |
| **V1 total (critical path)** | **~6.5 days**            |
| +25% buffer for unknowns     | **~8-9 days end-to-end** |

---

## 5. Dependencies & prerequisites

1. **[COMPLETE]** `ANGLES_TO_PROCESS` env var expanded to all 4 angles on both Jetsons so NL + NR 1080p actually get produced (fixed 2026-04-15).
2. AWS Batch GPU quota. Current account uses g5.xlarge for transcoding — confirm quota supports an additional CV queue.
3. Existing auto-card ingestion (`dcd12c4`) must continue to pick up Firebase logs. V1 relies on this without modifying it.
4. Check-in flow needs one new UI control: "Starting side" per team (added in Phase 3). Needs client sign-off on placement.
5. Model weights currently committed to the repo (~10 MB) — will be moved to S3 with version tagging in Phase 1. Future retrains replace the S3 object under a new version tag.

---

## 6. V2 — Shot-type classification & court mapping

Scope: V2 turns "made or missed" into "made or missed 2PT / 3PT / 4PT / FT",
per team. Built in-house — no dependency on third-party calibration tooling,
since the court has a non-standard 4-pt zone that no off-the-shelf layout
supports.

What V2 adds:

- **Custom court-dimension input** — the court has a non-standard 4-pt line
and may differ from regulation width/length. V2 starts with an operator-entered
court profile (length, width, 3-pt arc radius, 4-pt zone geometry, free-throw
distance) stored once per court in S3 at `uball-cv-models/courts/{court_id}.json`.
- **In-house camera calibration** — leveraging all 4 GoPros connected to the
Jetsons (FL, FR, NL, NR), a one-time-per-court calibration tool computes a
homography matrix from pixel → court coordinates for each camera. Operator
clicks reference points (four corners, free-throw line intersections, 3-pt
arc apex, 4-pt zone boundary) in a short web UI; we solve the homography and
store per-camera per-court in S3 at
`uball-cv-models/homography/{court_id}/{angle}.json`.
- **4-angle cross-validation** — having 4 cameras means any given court point
is visible in at least 2. The calibration tool cross-checks homographies by
projecting reference points across cameras and flags any matrix with
re-projection error above threshold, forcing a recalibration.
- **Court-line detection overlay** — render the 3-pt line, 4-pt zone, paint
key, and free-throw line on top of a QA preview video using the homography,
so calibration quality is visually verifiable in a 30-second clip before
production use.
- **Ball-position → court-coordinate transform** — at shot release, the
active camera pair's homography converts the ball's pixel location to court
feet from the hoop.
- **Shot-type classifier** — rule tree on distance + position:
  - Free-throw line, stationary shooter, clock inactive → `FT`
  - Inside 3-pt arc → `2PT`
  - Outside 3-pt arc but inside 4-pt zone → `3PT`
  - Inside 4-pt zone → `4PT`
- **Free-throw detection** — requires clock-state integration (either OCR of
on-screen clock, which is separate work, or game-state signal from the
scorekeeping UI during check-in). Initial implementation uses shooter
stationarity at the free-throw line + no other players moving as a heuristic,
then upgrades to clock-OCR later.
- **Backward-compatible card schema** — adds `shot_type` field on plays;
existing cards without it default to `null` and render as "shot" (same as V1).

What V2 enables:

- Points-per-possession, eFG%, true-shooting% — the metrics that matter for
coach analytics.
- Zone-based heatmaps (paint, corner 3s, 4-pt zone, etc.) auto-generated per
game or per player.
- Far better retrospective game reports for the client.

---

## 7. V3 — Player-level attribution

Reference pipeline: Roboflow's **"How to Identify Basketball Players"**
([https://blog.roboflow.com/identify-basketball-players/](https://blog.roboflow.com/identify-basketball-players/)). We adopt the same
architecture at a high level, fine-tuned on our footage.

What V3 adds on top of V1+V2:

- **Detection** — swap V1's YOLOv11n hoop/ball detector to (or add
alongside) **RF-DETR-S** trained on the Roboflow basketball dataset's 10
classes: player, player-in-possession, player-jump-shot, player-layup-dunk,
player-shot-block, ball, ball-in-basket, number, referee, rim. Fine-tune
on our own labelled game footage (seed from Roboflow Universe public dataset).
- **Tracking with re-identification** — **SAM2** (Segment Anything Model 2)
for video segmentation, seeded by RF-DETR bounding boxes in the first frame.
SAM2's temporal memory bank handles occlusions — when a player is blocked and
reappears, the stored appearance features re-identify them. This gives us
persistent per-player track IDs throughout a game.
- **Shooter identification** — for each V1 shot event, backtrack the ball
trajectory a few frames before release; whichever tracked player is closest
to the ball is the shooter.
- **Team assignment (unsupervised)** — **SigLIP** embeddings on player crops,
**UMAP** dimensionality reduction, **K-means (k=2)** to cluster players into
two teams by uniform appearance. No manual team-color entry needed. Result
is cross-checked against V1's hoop-side team label — mismatches flagged for
review.
- **Jersey number recognition** — **ResNet-32** classifier (Roboflow demoed
93% on their data; we expect similar on a fine-tuned version). Sample every
5 frames per tracked player; confirm a number only after 3 consecutive
matching predictions, which kills flicker noise. **SmolVLM2** stays as an
OCR fallback for edge cases (86% on Roboflow's data, used when ResNet-32
confidence is low).
- **Roster linkage** — (detected jersey number + team) resolves to
`players.id` via the per-game roster snapshot already attached to each game
(`games.roster_team1` / `games.roster_team2`, populated at check-in).
Populate `plays.player_a_id` and `plays.player_a` on each card.
- **Annotation-tool UX** — cards with `source='cv'` and `confidence<0.7`
get a subtle "review" badge; coach can one-tap override the player.
- **Runtime expectation** — Roboflow reports 1-2 FPS on an NVIDIA T4 (SAM2
is the bottleneck). On g5.xlarge (A10G) we expect 4-6 FPS; a 10-min game
should finish in 15-30 min, consistent with V1's latency budget.

Failure modes to plan for (all called out in Roboflow's write-up):

- SmolVLM2 produces implausible predictions like "011" or "3000" — validate
against each team's roster jersey set, reject anything out of range.
- SAM2 multi-segment mask errors mislabel the ball or crowd as a player —
keep only the largest connected component per track, drop components past
a distance threshold.
- Distant shooters' jersey numbers are often illegible — in that case fall
back to team-only attribution from V1 (card keeps `player_a=null`).

What V3 enables:

- Per-player shot charts, shooting percentages, and per-player heatmaps
(combined with V2 zones), auto-generated per game.
- Fully unattended card creation for the high-confidence majority — coach
touches only the low-confidence tail.
- Data flywheel: coach confirmations feed back as labeled data to improve
ResNet-32 and the RF-DETR fine-tune over time.

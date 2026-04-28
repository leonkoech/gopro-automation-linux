# CV Pipeline Runbook (V1)

**Scope**: operational playbook for the V1 shot-detection pipeline
(dispatch → fusion → merge → Firebase logs → UBall plays).

**Owner**: CV infra
**SLA**: best-effort, ~15–30 min after game end
**Alarms**: `UBall-CV-JobFailure`, `UBall-CV-DispatchUnhandledError`, `UBall-CV-NeedsReviewStreak`
**Dashboard**: [`UBall-CV-Pipeline`](https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=UBall-CV-Pipeline)

---

## 0. Mental model (30 seconds)

```
Jetson  →  S3 raw chapters  →  ffmpeg-nvenc transcode (existing AWS Batch)
   →  4 × 1080p in S3 (FL/FR/NL/NR)
   →  5-min cron  →  POST /api/cv/dispatch-pending  (Flask on Jetson)
   →  2 × cv-fusion GPU jobs  (Side A = FR+NR, Side B = FL+NL)
   →  cv-merge CPU job  (depends on both fusion jobs succeeding)
   →  Firebase basketball-games.logs[]  (actionType: "cv_shot")
   →  plays_sync pushes UBall plays with source="cv"
```

Both stages talk to `UBall/CV` CloudWatch namespace.

---

## 1. How to reprocess a single failed game

Use when: Phase-8 `UBall-CV-JobFailure` fired, or the merge job succeeded but
the resulting cards look wrong.

**Pick the game**: find the Firebase `basketball-games` doc ID, e.g.
`p9UFiqhnImLIscDbQjaS`, and the Supabase `games.id` UUID it maps to.

```bash
# 1. Delete the bad CV plays (only touches source='cv' rows).
curl -s -X POST "https://mhbrsftxvxxtfgbajrlc.supabase.co/rest/v1/rpc/delete_cv_plays_for_game" \
  -H "apikey: $SUPABASE_SERVICE_ROLE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY" \
  -H "Content-Type: application/json" \
  -d '{"p_game_id": "026f49bb-f9ed-4353-b42f-b0e8b587e35e"}'
# (Or via the Supabase SQL editor:
#   DELETE FROM plays WHERE game_id = '026f49bb-...' AND source = 'cv';)

# 2. Clear the CV-dispatch markers on the Firebase game so the cron re-picks it up.
#    (Fastest: open Firebase console → basketball-games/<id> → delete:
#       cv_dispatched_at, cv_emitted_at, cv_fusion_a_job_id, cv_fusion_b_job_id,
#       cv_merge_job_id, cv_emit_target)

# 3. Trigger dispatch for just that one game:
curl -s -X POST "http://localhost:5000/api/cv/dispatch-pending" \
  -H "Content-Type: application/json" \
  -d '{"firebase_game_id":"p9UFiqhnImLIscDbQjaS"}'
# -> {"dispatched_count":1, "dispatched":[{..."fusion_a_job_id":"...", "merge_job_id":"..."}], ...}

# 4. Watch the jobs:
aws batch list-jobs --job-queue cv-shot-detection-queue --job-status RUNNING
aws batch describe-jobs --jobs <fusion_a_job_id> <fusion_b_job_id> <merge_job_id> \
  --query 'jobs[].{name:jobName,status:status,exit:container.exitCode,reason:statusReason}'
```

---

## 2. How to bypass CV for a specific game

Use when: a game is problematic (partial recording, wrong cameras, etc.) and
you want operators to enter logs manually without CV interference.

Set one field on the Firebase game:

```
basketball-games/<id>.disableCv = true
```

Then clear any existing CV state on the same doc:

```
cv_dispatched_at, cv_emitted_at, cv_*_job_id  →  delete
```

The dispatcher currently does **not** honor `disableCv`. Until it does,
simply leaving `cv_dispatched_at` set with a recent timestamp is sufficient
to keep the dispatcher from re-picking the game.
_(Tracked as a V1.1 improvement: teach `/api/cv/dispatch-pending` to skip
`disableCv=true` games.)_

---

## 3. How to roll back a model version

Use when: new weights regress accuracy measurably on a few games.

Weights live at `s3://uball-cv-models/yolov11/<version>/` with a sibling
`MANIFEST.json`. To roll back:

```bash
# Re-point the default manifest to an older version:
aws s3 cp s3://uball-cv-models/yolov11/v0/MANIFEST.json \
          s3://uball-cv-models/yolov11/v1/MANIFEST.json
# OR: override per-job via the cv-fusion job definition's MODEL_VERSION env var.
```

Option 2 (preferred for a surgical rollback — no global default change):

```bash
# Update the registered cv-fusion job def to pin MODEL_VERSION=v0
aws batch register-job-definition --cli-input-json \
  '{"jobDefinitionName":"cv-fusion","type":"container",
    "containerProperties":{...,"environment":[{"name":"MODEL_VERSION","value":"v0"}]}}'
# Every new dispatch after this uses the new revision.
```

Already-dispatched games keep the version they were submitted with —
`session_info.model_version` on the output JSON records exactly what ran.

---

## 4. How to force a re-run of the entire day

Use when: something systemic changed (model, temporal_window, starting-side
convention) and you want to recompute today's games.

```bash
# 1. In Supabase SQL editor:
DELETE FROM plays WHERE source = 'cv'
  AND game_id IN (SELECT id FROM games WHERE date = '2026-04-15');

# 2. In Firebase, for each basketball-games doc with that date's startedAt:
#    clear cv_dispatched_at + cv_emitted_at + cv_*_job_id.

# 3. Let the next 5-min cron pick them up, or invoke:
curl -s -X POST "http://localhost:5000/api/cv/dispatch-pending" \
  -H "Content-Type: application/json" -d '{"limit":20,"lookback_days":2}'
```

---

## 5. Alarm triage

### `UBall-CV-JobFailure`

1. Dashboard → "CV jobs — success vs failure" widget → identify stage (fusion/merge) and rough time.
2. `aws batch list-jobs --job-queue cv-shot-detection-queue --job-status FAILED --filters name=AFTER_CREATED_AT,values=<epoch>`
3. `aws logs get-log-events --log-group-name /aws/batch/cv-fusion --log-stream-name <stream>` — traceback is there.
4. Common causes + fixes:
   - **Model SHA mismatch** → `MANIFEST.json` out of sync with uploaded weights. Recompute SHA and re-upload.
   - **S3 AccessDenied** → `cv-batch-job-role` is missing a bucket permission. Add it.
   - **Firebase creds missing** (merge only) → Secrets Manager entry not wired. Re-check `cv-merge-job-def.json`.
   - **OOM** → bump `memory` in the job def; re-register; retry the game via section 1.

### `UBall-CV-DispatchUnhandledError`

1. `ssh` the Jetson; `journalctl -u gopro-controller -f | grep CVDispatch`.
2. Usual culprits: AWS creds expired on the Jetson, Firebase service account path wrong, boto SSL on Jetson misbehaving.
3. Bounce the service: `sudo systemctl restart gopro-controller`. Alarm should clear on next cron tick.

### `UBall-CV-NeedsReviewStreak`

1. Means operators aren't setting **Attacking hoop at tip-off** at check-in.
2. Ping the ops channel. No code fix needed — training.
3. The affected games are recoverable: manually set `startingSideTeam1` on the
   Firebase basketball-games doc, clear `cv_emitted_at`, delete source='cv' plays,
   re-dispatch (section 1). Team attribution will replay with the correct sides.

---

## 6. Forensics — "a shot I expected isn't in the cards"

1. Find the game's Firebase doc and look at `logs[]`. CV shots have
   `actionType: "cv_shot"`. Their `payload.source = "cv"` and
   `payload.confidence` tells you if it was a low-confidence drop.
2. The underlying per-side detection JSONs live in S3 at
   `s3://uball-cv-results/{location}/{date}/{game_uuid4}/side-A/detection_results.json`
   and `.../side-B/...`.
3. Look at the `shots` array — every shot the fusion script produced is there
   even if the merge step deduped or dropped it.
4. If the shot is present in a per-side JSON but missing from Firebase logs,
   the merge's cross-side dedup (temporal_window, default 1.0s) is likely
   the cause. Inspect merge CloudWatch logs for the shot's timestamp.

---

## 7. Observability smoke tests

```bash
# Confirm metrics are flowing for the last hour:
aws cloudwatch get-metric-statistics \
  --namespace "UBall/CV" --metric-name CVJobSuccess \
  --statistics Sum --period 3600 \
  --start-time $(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time   $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --dimensions Name=Stage,Value=fusion
```

If `Datapoints: []`, check:
- Did any fusion job run in that window? (`aws batch list-jobs`)
- Is the Jetson's IAM user missing `cloudwatch:PutMetricData`? (most common)
- Is `DISABLE_CV_METRICS=true` set accidentally?

---

## 8. Safe-to-run verification commands

These are all read-only and safe to run during business hours.

```bash
# A. What would the dispatcher do right now? (no side effects)
curl -s -X POST "http://localhost:5000/api/cv/dispatch-pending" \
  -H "Content-Type: application/json" -d '{"dry_run":true,"limit":5}' | jq

# B. How many games have CV plays already?
aws cloudwatch get-metric-statistics --namespace "UBall/CV" --metric-name CVMergeShotsTotal \
  --statistics Sum --period 86400 \
  --start-time $(date -u -d '-7 days' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time   $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --dimensions Name=Stage,Value=merge

# C. Tail the most recent fusion run's logs
aws logs tail /aws/batch/cv-fusion --since 1h --follow
```

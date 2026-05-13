# `uball-cv-dispatch` Lambda

Phase 2.2 of the V1 CV pipeline ([UBA-208](https://linear.app/uball/issue/UBA-208)).
EventBridge-triggered Lambda that submits the AWS Batch DAG
(2× `cv-fusion` + 1× `cv-merge` with `dependsOn`) as soon as all four
transcoded 1080p angles for a game land in S3.

## Flow

```
S3 PutObject on uball-videos-production/court-*/<date>/<uuid4>/<*>.mp4
   ↓ (EventBridge "Object Created", suffix-filtered to _FL/_FR/_NL/_NR.mp4)
Lambda  uball-cv-dispatch
   ├─ parse key → (location, date, uuid4, angle)
   ├─ HEAD-check all 4 angles for the game prefix
   ├─ if <4 present → exit cleanly (next sibling event will retry)
   ├─ debounce DEBOUNCE_SECONDS (default 60s)
   ├─ batch:ListJobs — short-circuit if non-terminal cv-fusion-<uuid4>-* exists
   ├─ CVBatchDispatcher.submit_game() → Side A + Side B + merge w/ dependsOn
   └─ emit CVDispatchSubmitted / -WaitingForAngles / -SkippedAlreadyProcessed / -Errors
```

The Lambda is **stateless** and **idempotent**: a second invocation for
the same game prefix is a no-op as long as the prior bundle is still in
flight. The same `CVBatchDispatcher` class powers the Flask
`/api/cv/dispatch-pending` polling path in `main.py` — both routes
converge on the identical submit-and-stamp logic.

## File layout

```
lambda/cv_dispatch/
├── handler.py         # entry point — parses S3 event, calls CVBatchDispatcher
├── template.yaml      # SAM template (4 EventBridge rules, IAM role ref)
├── Makefile           # build-CvDispatchFunction target for `sam build`
├── requirements.txt   # boto3 (pinned floor)
├── deploy.sh          # one-shot deploy: enables EventBridge on the bucket, sam build, sam deploy
└── README.md          # this file
```

`handler.py` imports two shared modules from the repo root:
`cv_batch_dispatch.py` and `cv_metrics.py`. The Makefile copies them
into the SAM build artefact dir, so they live in the Lambda zip without
being permanently duplicated under `lambda/cv_dispatch/`.

## Prerequisites

| Resource | Owned by | Status |
| --- | --- | --- |
| IAM role `uball-cv-dispatch-lambda` | Phase 0 ([UBA-201](https://linear.app/uball/issue/UBA-201)) | ✅ provisioned (lambda.amazonaws.com trust + cv-dispatch-inline policy) |
| AWS Batch queue `cv-shot-detection-queue` | Phase 0 ([UBA-197](https://linear.app/uball/issue/UBA-197)) | ✅ ENABLED + VALID |
| Batch JD `cv-fusion`, `cv-merge` | Phase 2.1 ([UBA-207](https://linear.app/uball/issue/UBA-207), PR #36) | ⏳ registered by `scripts/cv_infra/register-batch-job-defs.sh` |
| EventBridge on `uball-videos-production` | This `deploy.sh` (one-time) | ⏳ first run enables it |
| AWS SAM CLI (`pip install aws-sam-cli`) | Engineer running deploy | local |

## Deploy

```
cd lambda/cv_dispatch
./deploy.sh                 # us-east-1, enables EventBridge if not on
./deploy.sh us-west-2        # different region
./deploy.sh us-east-1 1      # region + skip EventBridge enable
```

The script:

1. Creates the SAM artifact bucket `uball-lambda-deployments` if missing.
2. Enables EventBridge notifications on `uball-videos-production` if not
   already on (one-time per bucket; idempotent).
3. `sam build --template-file template.yaml` — the Makefile target
   bundles `cv_batch_dispatch.py` + `cv_metrics.py` from repo root.
4. `sam deploy` — creates / updates the CloudFormation stack
   `uball-cv-dispatch`.
5. Prints the deployed function ARN + config for verification.

## Local invoke (smoke test)

The handler ships with both event-shape parsers, so you can invoke it
with either an EventBridge event or a legacy S3 Records event.

```
sam local invoke CvDispatchFunction \
  --event tests/fixtures/eventbridge_object_created.json \
  --env-vars '{"CvDispatchFunction": {"DEBOUNCE_SECONDS": "0", "DISABLE_CV_METRICS": "1"}}'
```

For pure unit-test runs without sam local, the suite in
`tests/test_cv_dispatch_lambda.py` covers parsing, the 4-angle wait,
idempotency, and multi-record dedup — all with boto3 mocked.

## ⚠️ Known issue: CloudWatch namespace mismatch

`cv_metrics.py`, `deploy/cloudwatch/alarms.json`, and
`deploy/cloudwatch/dashboard.json` all use namespace **`UBall/CV`**
(forward slash). The IAM policy on `uball-cv-dispatch-lambda`
(provisioned in Phase 0) only permits
`cloudwatch:PutMetricData` when `cloudwatch:namespace` equals
**`UballCV`** (no slash) — verified via
`aws iam get-role-policy --role-name uball-cv-dispatch-lambda`.

**Effect**: Lambda metric emits will be denied. Errors are swallowed by
`cv_metrics.emit()` so the data pipeline keeps working, but dashboards
won't see `Stage=lambda` data points.

**Fix**: update the IAM policy's `StringEquals` constraint from
`UballCV` to `UBall/CV` (canonical, used by 3 other places). Tracked
on [UBA-201](https://linear.app/uball/issue/UBA-201) follow-up — see PR
description.

## Roll back

```
aws cloudformation delete-stack --stack-name uball-cv-dispatch --region us-east-1
```

Deletes the Lambda + EventBridge rules. The bucket's EventBridge
notification configuration is **not** reverted (S3 keeps it on). To
fully revert, also run:

```
aws s3api put-bucket-notification-configuration \
  --bucket uball-videos-production \
  --notification-configuration '{}'
```

## Companion paths

| Path | Trigger | Use case |
| --- | --- | --- |
| `uball-cv-dispatch` Lambda (this) | EventBridge S3 `Object Created` | Auto-dispatch on game end |
| `POST /api/cv/dispatch-pending` (main.py) | Cron / manual curl | Back-fill, replay, ops re-run |

Both go through `CVBatchDispatcher.submit_game()` and emit the same
metric set — so a single dashboard widget covers both paths.

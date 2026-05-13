# `scripts/cv_infra/cloudwatch/`

Phase 6 observability infra for the V1 CV pipeline. Provisions the
CloudWatch resources whose canonical JSON lives in `deploy/cloudwatch/`:

- **`UBall-CV-Pipeline` dashboard** — 6 widgets covering fusion success
  rate, dispatcher health, shot counts, merge confidence, needs-review
  rate, and job duration. Source: `deploy/cloudwatch/dashboard.json`.
- **3 alarms** that action onto the `uball-cv-failures` SNS topic
  ([UBA-200](https://linear.app/uball/issue/UBA-200)):
  - `UBall-CV-JobFailure` — any `CVJobFailure` in a 1-hour window
  - `UBall-CV-DispatchUnhandledError` — Flask dispatcher raises an
    unhandled exception
  - `UBall-CV-NeedsReviewStreak` — merge emits `needs_review=1` ≥ 3×
    in a single day
  Source: `deploy/cloudwatch/alarms.json`.
- **Log retention** — 14 days on the three CV log groups (UBA-226):
  - `/aws/batch/cv-fusion`
  - `/aws/batch/cv-merge`
  - `/aws/lambda/uball-cv-dispatch`

## Apply

```
# Dry-run — print planned actions, no AWS call:
./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --dry-run

# Apply all 3 (dashboard, alarms, retention):
./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh

# Apply just one piece:
./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --dashboard-only
./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --alarms-only
./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --retention-only

# Audit — exit 4 if any resource is missing or has drifted retention:
./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --check
```

The script is **idempotent** — `put-dashboard` / `put-metric-alarm` /
`put-retention-policy` all replace-in-place. Pre-creates the log
groups so the retention setting is in effect from the first
Batch / Lambda run (otherwise AWS would create them on first write
with "Never expire", which is wrong for V1).

## Pre-flight (script checks each before applying)

| Requirement | Owned by | How the script handles |
| --- | --- | --- |
| `aws` CLI in `$PATH` | local | hard-fail with exit 2 |
| `jq` in `$PATH` (used to substitute SNS ARN into alarms JSON) | local | hard-fail with exit 2 |
| `deploy/cloudwatch/{dashboard,alarms}.json` present | PR #32 | hard-fail with exit 2 |
| Caller account == `840102831548` | dev | hard-fail with exit 2 |
| SNS topic `uball-cv-failures` exists | Phase 0 / [UBA-200](https://linear.app/uball/issue/UBA-200) | hard-fail with exit 2 |
| CW namespace IAM constraint = `UBall/CV` on all 3 CV roles | PR #38 (applied) | not blocking — metric emit happens at runtime |

## SNS topic ARN

Defaults to `arn:aws:sns:us-east-1:840102831548:uball-cv-failures` (Phase
0 spec). Override via env var `SNS_TOPIC_ARN` if you stand up a
separate topic for staging / shadow rollout.

## Drift detection in CI

`--check` exits with code 4 when anything's missing or drifted, so this
can be wired into a daily cron alongside `apply-policies.sh --diff`:

```yaml
# .github/workflows/cv-infra-drift.yml (future)
- run: ./scripts/cv_infra/iam-policies/apply-policies.sh --diff
- run: ./scripts/cv_infra/iam-policies/bootstrap-gha-oidc.sh --check
- run: ./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh --check
- run: ./scripts/cv_infra/secrets/bootstrap-uball-backend-secrets.sh --check
- run: ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --check
```

## Roll back

The dashboard + alarms are pure metadata — deleting them is safe at
any time:

```
aws cloudwatch delete-dashboards --dashboard-names UBall-CV-Pipeline --region us-east-1
aws cloudwatch delete-alarms \
  --alarm-names UBall-CV-JobFailure UBall-CV-DispatchUnhandledError UBall-CV-NeedsReviewStreak \
  --region us-east-1
```

Log retention is not destructive — it controls how long *future*
log events live. Existing events are not affected.

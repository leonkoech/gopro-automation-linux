# CV Batch Job Definitions

These JSON files register the two AWS Batch job definitions used by the
V1 shot-detection pipeline.

## `cv-fusion-job-def.json`

GPU job that runs `uball-cv-fusion:v1` against two 1080p angle videos and
emits `detection_results.json` to S3. Submitted once per court-side pair
(so 2 jobs per game).

**Resources**: 1 GPU, 4 vCPU, 16 GB RAM, 45 min timeout, 2 retries.
**Queue**: `cv-shot-detection-queue` (provisioned in Phase 0).

## `cv-merge-job-def.json`

CPU job that runs `uball-cv-merge:v1` after both fusion jobs succeed.
Merges, attributes team, emits Firebase `cv_shot` logs, drives
`plays_sync` to create UBall plays.

**Resources**: 2 vCPU, 4 GB RAM, 10 min timeout, 2 retries.
**Queue**: `cv-merge-queue` (provisioned in Phase 0).
**Dependencies**: `dependsOn` = [fusion-A jobId, fusion-B jobId].

## Placeholders to replace before registration

Both files contain `PLACEHOLDER_*` strings that Phase 0 replaces with the
real values:

- `PLACEHOLDER_ECR_IMAGE_URI` — set per-job after `docker push`
  (e.g. `840102831548.dkr.ecr.us-east-1.amazonaws.com/uball-cv-fusion:v1`).
- `PLACEHOLDER_CV_BATCH_JOB_ROLE_ARN` — the `cv-batch-job-role` IAM role ARN.
- `PLACEHOLDER_CV_BATCH_EXECUTION_ROLE_ARN` — the `cv-batch-execution-role` IAM role ARN.
- `PLACEHOLDER_UBALL_*_SECRET_ARN` (merge only) — Secrets Manager ARNs for
  the UBall backend credentials.

## Register

```bash
# After replacing placeholders (e.g. with envsubst or sed):
aws batch register-job-definition \
  --region us-east-1 \
  --cli-input-json file://cv-fusion-job-def.json

aws batch register-job-definition \
  --region us-east-1 \
  --cli-input-json file://cv-merge-job-def.json

# Confirm:
aws batch describe-job-definitions \
  --region us-east-1 \
  --job-definition-name cv-fusion --status ACTIVE \
  --query 'jobDefinitions[0].{name:jobDefinitionName,revision:revision,image:containerProperties.image}'
```

# scripts/cv_infra/

AWS Batch + IAM + Secrets-Manager wiring for the V1 CV shot-detection
pipeline. Each script is idempotent; re-running is safe.

| Script | Phase | What it does |
| --- | --- | --- |
| `register-batch-job-defs.sh` | 2.1 (UBA-207) | Substitutes `PLACEHOLDER_*` tokens in `deploy/batch-job-defs/cv-{fusion,merge}-job-def.json` and calls `aws batch register-job-definition` for each. |

## `register-batch-job-defs.sh`

### Pre-flight you must do first

| Requirement | Owned by | Status |
| --- | --- | --- |
| IAM role `uball-cv-batch-execution` (fusion job + execution) | Phase 0 ([UBA-201](https://linear.app/uball/issue/UBA-201)) | ✅ provisioned |
| IAM role `uball-cv-merge-execution` (merge job + execution) | Phase 0 ([UBA-201](https://linear.app/uball/issue/UBA-201)) | ✅ provisioned |
| AWS Batch queue `cv-shot-detection-queue` | Phase 0 ([UBA-197](https://linear.app/uball/issue/UBA-197)) | ✅ ENABLED + VALID |
| ECR image `uball-cv-fusion:v1` | Phase 1 ([UBA-204](https://linear.app/uball/issue/UBA-204)) | ✅ pushed 2026-04-23 |
| ECR image `uball-cv-merge:v1` | Phase 1 (this PR's GHA workflow) | ⏳ first push lands when PR #32 merges + GHA runs |
| Secret `UBALL_BACKEND_URL_SECRET_ARN` in Secrets Manager | Phase 4 ([UBA-219](https://linear.app/uball/issue/UBA-219)) | ❌ not yet |
| Secret `UBALL_AUTH_EMAIL_SECRET_ARN` | Phase 4 ([UBA-219](https://linear.app/uball/issue/UBA-219)) | ❌ not yet |
| Secret `UBALL_AUTH_PASSWORD_SECRET_ARN` | Phase 4 ([UBA-219](https://linear.app/uball/issue/UBA-219)) | ❌ not yet |

The script checks each prerequisite before calling AWS Batch and fails with
exit code `2` if anything is missing.

### Running it

```
# Dry-run — print substituted JSON; never calls AWS:
./scripts/cv_infra/register-batch-job-defs.sh --dry-run

# Register only the fusion JD (safe to do now — no secret deps):
./scripts/cv_infra/register-batch-job-defs.sh --fusion-only

# Once Secrets Manager entries exist (Phase 4):
export UBALL_BACKEND_URL_SECRET_ARN=arn:aws:secretsmanager:us-east-1:840102831548:secret:uball/cv/backend-url-AbCdEf
export UBALL_AUTH_EMAIL_SECRET_ARN=arn:aws:secretsmanager:us-east-1:840102831548:secret:uball/cv/auth-email-GhIjKl
export UBALL_AUTH_PASSWORD_SECRET_ARN=arn:aws:secretsmanager:us-east-1:840102831548:secret:uball/cv/auth-password-MnOpQr
./scripts/cv_infra/register-batch-job-defs.sh             # both
```

### Override defaults

All defaults target the `840102831548` production account in `us-east-1`.
Override any of:

- `AWS_REGION`, `ACCOUNT_ID`
- `FUSION_IMAGE_URI`, `MERGE_IMAGE_URI` — point at a SHA tag instead of `:v1`
  to pin to a specific revision while testing
- `FUSION_JOB_ROLE_ARN`, `FUSION_EXECUTION_ROLE_ARN` (currently same role)
- `MERGE_JOB_ROLE_ARN`, `MERGE_EXECUTION_ROLE_ARN` (currently same role)

### What gets registered

| JD | vCPU | RAM | GPU | Timeout | Queue (at submit time) |
| --- | --- | --- | --- | --- | --- |
| `cv-fusion` | 4 | 16 GB | 1× T4 | 45 min | `cv-shot-detection-queue` |
| `cv-merge` | 2 | 4 GB | — | 10 min | `cv-shot-detection-queue` (V1 — runs CPU job on GPU instance; tracked for follow-up) |

Each `register-job-definition` call creates a new immutable Batch revision.
The dispatcher (`cv_batch_dispatch.py`) calls `submit-job` with the
**unversioned** job-def name, so it always picks up the most recent
revision. Roll back by re-running this script with `FUSION_IMAGE_URI`
pointing at the prior SHA tag.

### After running

```
aws batch describe-job-definitions \
  --region us-east-1 \
  --job-definition-name cv-fusion --status ACTIVE \
  --query 'jobDefinitions[0].{name:jobDefinitionName,revision:revision,image:containerProperties.image}'

aws batch describe-job-definitions \
  --region us-east-1 \
  --job-definition-name cv-merge --status ACTIVE \
  --query 'jobDefinitions[0].{name:jobDefinitionName,revision:revision,image:containerProperties.image}'
```

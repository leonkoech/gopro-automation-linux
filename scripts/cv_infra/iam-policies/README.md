# `scripts/cv_infra/iam-policies/`

Source-of-truth for the inline IAM policies attached to the three CV
pipeline IAM roles. Phase 0 ([UBA-201](https://linear.app/uball/issue/UBA-201))
provisioned these roles ad-hoc via the AWS console and CLI; the inline
policies lived only in AWS state with no repo provenance. The
`cloudwatch:namespace == UballCV` bug found during Phase 2.2 work
prompted dumping them, fixing them, and committing them here.

| Role | Inline policy | File | Trusted principal | Used by |
| --- | --- | --- | --- | --- |
| `uball-cv-dispatch-lambda` | `cv-dispatch-inline` | `cv-dispatch-inline.json` | `lambda.amazonaws.com` | `uball-cv-dispatch` Lambda (PR #37) |
| `uball-cv-batch-execution` | `cv-fusion-inline` | `cv-fusion-inline.json` | `ecs-tasks.amazonaws.com` | `cv-fusion` Batch job |
| `uball-cv-merge-execution` | `cv-merge-inline` | `cv-merge-inline.json` | `ecs-tasks.amazonaws.com` | `cv-merge` Batch job |

## What changed in this PR

All 3 policies had the same one-line bug:

```diff
- "cloudwatch:namespace": "UballCV"
+ "cloudwatch:namespace": "UBall/CV"
```

The canonical namespace used by everything else in the V1 stack is
`UBall/CV` (forward slash) — see `cv_metrics.py:30`,
`deploy/cloudwatch/alarms.json`, and `deploy/cloudwatch/dashboard.json`.
The `UballCV` value would have caused `cloudwatch:PutMetricData` calls
to be silently denied (errors are swallowed inside `cv_metrics.emit()`
so the data pipeline keeps working, but dashboards lose data).

The fix was already applied to AWS via `apply-policies.sh` before this
PR was opened — see PR description for `aws iam get-role-policy`
verification output.

## Apply changes

```
# Show diffs between repo files and live AWS:
./scripts/cv_infra/iam-policies/apply-policies.sh --diff

# Apply (idempotent — re-running is safe):
./scripts/cv_infra/iam-policies/apply-policies.sh

# Dry-run (prints what would happen without calling AWS):
./scripts/cv_infra/iam-policies/apply-policies.sh --dry-run
```

The script checks the caller account matches `840102831548` before
applying, so a misconfigured AWS profile can't accidentally write into
the wrong account.

## Updating a policy

1. Edit the JSON file under `scripts/cv_infra/iam-policies/`.
2. Open a PR.
3. After merge, run `./apply-policies.sh` (or have CI do it via the
   `uball-gha-ecr-push` OIDC role + an `iam:PutRolePolicy` permission —
   the role doesn't exist yet, so do it manually for now).
4. Confirm with `--diff` that live AWS matches the repo.

## What's NOT in here

- The role trust policies (`AssumeRolePolicyDocument`). Those are
  one-time and don't change. If you need them: `aws iam get-role --role-name <name>`.
- The Batch compute environment + queue + JD configs. Those live under
  `deploy/batch-job-defs/` and are registered by
  `scripts/cv_infra/register-batch-job-defs.sh` (Phase 2.1 / PR #36).
- AWS managed policies attached to these roles (if any). The CV
  pipeline roles are inline-only at the moment.

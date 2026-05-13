# `scripts/cv_infra/iam-policies/`

Source-of-truth for the inline IAM policies attached to the three CV
pipeline IAM roles. Phase 0 ([UBA-201](https://linear.app/uball/issue/UBA-201))
provisioned these roles ad-hoc via the AWS console and CLI; the inline
policies lived only in AWS state with no repo provenance. The
`cloudwatch:namespace == UballCV` bug found during Phase 2.2 work
prompted dumping them, fixing them, and committing them here.

| Role | Inline policy | Trust file | Permission file | Trusted principal | Used by |
| --- | --- | --- | --- | --- | --- |
| `uball-cv-dispatch-lambda` | `cv-dispatch-inline` | (Phase 0, not in repo) | `cv-dispatch-inline.json` | `lambda.amazonaws.com` | `uball-cv-dispatch` Lambda (PR #37) |
| `uball-cv-batch-execution` | `cv-fusion-inline` | (Phase 0, not in repo) | `cv-fusion-inline.json` | `ecs-tasks.amazonaws.com` | `cv-fusion` Batch job |
| `uball-cv-merge-execution` | `cv-merge-inline` | (Phase 0, not in repo) | `cv-merge-inline.json` | `ecs-tasks.amazonaws.com` | `cv-merge` Batch job |
| `uball-gha-ecr-push` | `gha-ecr-push-inline` | `gha-ecr-push-trust.json` | `gha-ecr-push-inline.json` | GitHub Actions OIDC | `cv-fusion-image.yml` / `cv-merge-image.yml` GHA workflows |

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

## `uball-gha-ecr-push` — first-time setup

This role + its OIDC provider don't exist yet in the account. Run the
bootstrap script (idempotent — safe to re-run):

```
# What it would do, without touching AWS:
./scripts/cv_infra/iam-policies/bootstrap-gha-oidc.sh --dry-run

# Apply: creates OIDC provider (one-time, account-wide) + the role +
# attaches the inline policy.
./scripts/cv_infra/iam-policies/bootstrap-gha-oidc.sh

# Audit live AWS against the repo files; exits non-zero on drift:
./scripts/cv_infra/iam-policies/bootstrap-gha-oidc.sh --check
```

The trust policy restricts assumption to:

- `repo:leonkoech/gopro-automation-linux:ref:refs/heads/main`
- `repo:leonkoech/gopro-automation-linux:environment:production`
- `repo:rohitmk523/Uball_dual_angle_fusion:ref:refs/heads/main`
- `repo:rohitmk523/Uball_dual_angle_fusion:environment:production`

So GitHub Actions runs from feature branches will be **rejected** at
the STS layer. That's intentional — production credentials must only
be reachable from the protected `main` branch. To allow a feature
branch to trial the workflow, add a temporary `repo:<owner>/<repo>:ref:refs/heads/<branch>` line to
`gha-ecr-push-trust.json` and re-apply.

The permission policy grants only what's needed for `docker push` to
the two CV ECR repos — `ecr:GetAuthorizationToken` (account-scoped)
plus the layer-upload + image-put set scoped to
`uball-cv-fusion` and `uball-cv-merge`.

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

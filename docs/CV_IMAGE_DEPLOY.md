# CV image build + ECR push

**Scope**: Phase 1.5 manual-push runbook for the V1 shot-detection containers
(`uball-cv-fusion`, `uball-cv-merge`). Covers the GitHub Actions hook (Option A,
preferred) and the manual fallback (Option B), plus the rollback snippet.

| Image             | Repo (source)            | Dockerfile                             | ECR repo            | Pushed by GHA |
| ----------------- | ------------------------ | -------------------------------------- | ------------------- | ------------- |
| `uball-cv-fusion` | `Uball_dual_angle_fusion`| `deploy/Dockerfile`                    | `uball-cv-fusion`   | `cv-fusion-image.yml`  in that repo |
| `uball-cv-merge`  | `gopro-automation-linux` | `deploy/cv-merge/Dockerfile`           | `uball-cv-merge`    | `.github/workflows/cv-merge-image.yml` |

Both ECR repos live in `840102831548.dkr.ecr.us-east-1.amazonaws.com` and are
configured `IMMUTABLE` with `scanOnPush: true` (provisioned in Phase 0,
[UBA-198](https://linear.app/uball/issue/UBA-198)).

---

## Option A — GitHub Actions (preferred)

Each repo has a workflow that triggers on push to `main` when the relevant
paths change, builds with `docker buildx --platform linux/amd64`, and pushes
`:v1` plus the git short SHA. Auth is via OIDC against the
`uball-gha-ecr-push` role — no static AWS keys are stored in GitHub.

**Trigger paths**

* Fusion: `deploy/**`, `dual_angle_fusion.py`,
  `Uball_near_angle_shot_detection/**`, `Uball_far_angle_shot_detection/**`
* Merge: `deploy/cv-merge/**`, `cv_merge/**`, `cv_metrics.py`,
  `cv_batch_dispatch.py`

**Manual re-run**

```
# From the repo on GitHub: Actions → cv-*-image → Run workflow → main
# Or via gh CLI:
gh workflow run cv-merge-image.yml --ref main
gh workflow run cv-fusion-image.yml --ref main          # in Uball_dual_angle_fusion
```

---

## Option B — Manual push from a dev machine (fallback)

Use this if GHA is offline or you need to ship a hot-fix from a laptop with
Docker + AWS CLI configured. The dev machine must be amd64 *or* arm64 with
Docker Desktop QEMU enabled (cross-build is slow but works).

### 0. ECR login (once per session)

```
ACCT=840102831548
REGION=us-east-1
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ACCT.dkr.ecr.$REGION.amazonaws.com"
```

### 1. Build + push `uball-cv-fusion`

```
cd Uball_dual_angle_fusion
SHA=$(git rev-parse --short HEAD)
docker buildx build \
  --platform linux/amd64 \
  --tag "$ACCT.dkr.ecr.$REGION.amazonaws.com/uball-cv-fusion:v1" \
  --tag "$ACCT.dkr.ecr.$REGION.amazonaws.com/uball-cv-fusion:$SHA" \
  --file deploy/Dockerfile \
  --push \
  .
```

### 2. Build + push `uball-cv-merge`

```
cd gopro-automation-linux
SHA=$(git rev-parse --short HEAD)
docker buildx build \
  --platform linux/amd64 \
  --tag "$ACCT.dkr.ecr.$REGION.amazonaws.com/uball-cv-merge:v1" \
  --tag "$ACCT.dkr.ecr.$REGION.amazonaws.com/uball-cv-merge:$SHA" \
  --file deploy/cv-merge/Dockerfile \
  --push \
  .
```

### 3. Pre-push sanity (matches UBA-204 acceptance)

```
docker run --platform linux/amd64 --rm \
  "$ACCT.dkr.ecr.$REGION.amazonaws.com/uball-cv-fusion:v1" \
  python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

Acceptance: exit 0, prints `2.4.1` (CUDA availability is `False` on a CPU dev
box — that's expected; the GPU check happens on g4dn during the smoke test).

### 4. Verify both images landed

```
aws ecr describe-images --repository-name uball-cv-fusion --image-ids imageTag=v1 --region "$REGION"
aws ecr describe-images --repository-name uball-cv-merge  --image-ids imageTag=v1 --region "$REGION"
```

---

## Rollback (one-liner)

If `:v1` is bad and you need the prior SHA back as the live pointer:

```
PRIOR_SHA=20260423-180756  # whichever git-SHA tag you want to roll forward to
REPO=uball-cv-fusion       # or uball-cv-merge

# 1. Pull the manifest of the prior SHA
MANIFEST=$(aws ecr batch-get-image \
  --repository-name "$REPO" --image-ids imageTag="$PRIOR_SHA" \
  --region us-east-1 --query 'images[0].imageManifest' --output text)

# 2. Re-tag :v1 to point at it (since :v1 is IMMUTABLE you must delete the
#    existing tag first, then re-push the manifest with tag v1).
aws ecr batch-delete-image --repository-name "$REPO" \
  --image-ids imageTag=v1 --region us-east-1
aws ecr put-image --repository-name "$REPO" \
  --image-tag v1 --image-manifest "$MANIFEST" --region us-east-1
```

The Batch job definition (`cv-fusion-job` / `cv-merge-job`) reads `:v1`, so the
rollback is hot — no Batch redeploy needed. Verify a subsequent Batch run
pulls the rolled-back digest from CloudWatch logs (`Pulled image:` line).

---

## Drift checks

* `:v1` should always equal *one* of the last 10 git-SHA tags (lifecycle
  policy keeps last 10). Use `aws ecr describe-images` and compare digests.
* If `imagePushedAt` for `:v1` is older than HEAD of the source repo by
  more than a deploy cycle, run `gh workflow run` to refresh.

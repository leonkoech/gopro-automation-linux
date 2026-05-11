# `scripts/cv_infra/secrets/`

Bootstrap scripts for AWS Secrets Manager entries used by the CV
pipeline. Phase 4 / [UBA-219](https://linear.app/uball/issue/UBA-219).

| Secret name | Used by | Bootstrap | Linear |
| --- | --- | --- | --- |
| `uball/firebase-admin-cv-merge` | `uball-cv-merge` container — Firebase Admin SDK JSON for emitting `cv_shot` logs | `bootstrap-firebase-secret.sh` | [UBA-219](https://linear.app/uball/issue/UBA-219) |

## Why a separate folder under `cv_infra/`

`cv_infra/iam-policies/` already owns the source-of-truth for IAM. Secrets are a different problem class — the **values** never live in the repo, only the *bootstrap* (name, description, IAM resource pattern). Keeping them separate makes it obvious which files contain secret material (none of these) vs which describe the access surface (iam-policies/).

## `uball/firebase-admin-cv-merge`

### What it holds

The full contents of the Firebase Admin SDK service-account JSON for the production GoPro fleet — the same blob that lives at `uball-gopro-fleet-firebase-adminsdk.json` in dev environments, used by:

- `firebase_service.py` → `credentials.Certificate(path)`
- The merge container, via `FIREBASE_CREDENTIALS_PATH` env

### Why we moved it out of the image

Phase 1's first cut baked the JSON into the merge container at build time. This PR ([UBA-219](https://linear.app/uball/issue/UBA-219)) replaces that with a Secrets-Manager fetch at startup:

- No copy of the JSON in the Docker layer
- No copy in the ECR registry
- No copy on every Batch worker host that pulls the image
- Operator can rotate creds in one place; old image revisions automatically pick up the new value on the next Batch run

### How the merge container consumes it

On startup, `deploy/cv-merge/entrypoint.py` calls
`_load_firebase_creds_from_secrets_manager()` which:

1. Reads `FIREBASE_ADMIN_SECRET_ID` env (default: `uball/firebase-admin-cv-merge`).
2. Calls `secretsmanager:GetSecretValue` — uses the IAM role `uball-cv-merge-execution`, which already permits `Resource: arn:aws:secretsmanager:us-east-1:840102831548:secret:uball/firebase-admin-cv-merge-*` (Phase 0 / [UBA-201](https://linear.app/uball/issue/UBA-201)).
3. Writes the JSON to `/tmp/firebase-admin.json`.
4. Sets `os.environ["FIREBASE_CREDENTIALS_PATH"] = "/tmp/firebase-admin.json"`.
5. Validates the JSON is shape-correct (looks like a service account, not the placeholder marker).

If `LOCAL_MODE=true` or `DRY_RUN=true`, the fetch is skipped — the entrypoint falls back to the file path supplied via the existing `FIREBASE_CREDENTIALS_PATH` env so dev runs without AWS creds keep working.

### Bootstrap

```
# Dry-run — print what would happen:
./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh --dry-run

# Apply — creates the secret with a placeholder value if missing,
# no-op if it already exists:
./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh

# Audit — exit code 4 if missing or still placeholder:
./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh --check
```

### Operator: fill the actual value

After bootstrap creates the entry with a placeholder, populate it from the real Firebase JSON. **Don't paste the JSON in chat, screenshots, or terminal screenshares.**

```
aws secretsmanager put-secret-value \
  --secret-id uball/firebase-admin-cv-merge \
  --secret-string file:///path/to/uball-gopro-fleet-firebase-adminsdk.json \
  --region us-east-1
```

Or via the AWS console: Secrets Manager → `uball/firebase-admin-cv-merge` → Retrieve secret value → Edit.

### Rotate

To rotate the Firebase service-account key:

1. Generate a new key in the Firebase console (Project Settings → Service Accounts → Generate new private key).
2. `aws secretsmanager put-secret-value --secret-id uball/firebase-admin-cv-merge --secret-string file://NEW.json`. Secrets Manager keeps the prior version retrievable for 30 days under stage `AWSPREVIOUS` if you need to roll back.
3. (Optional, recommended) revoke the old key in the Firebase console.
4. No image rebuild needed — the next Batch run picks up the new value.

### Forward-looking — backend secrets

The merge job def also expects three UBall-backend secrets:

- `UBALL_BACKEND_URL`
- `UBALL_AUTH_EMAIL`
- `UBALL_AUTH_PASSWORD`

Those are out of scope for this folder right now — they need a separate IAM scope expansion (the current `uball-cv-merge-execution` policy permits only `uball/firebase-admin-cv-merge-*`). If/when those move into Secrets Manager too, add bootstrap scripts here and expand the IAM resource list. Tracked separately from UBA-219.

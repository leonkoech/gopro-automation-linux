# `scripts/cv_infra/secrets/`

Bootstrap scripts for AWS Secrets Manager entries used by the CV
pipeline. Phase 4 / [UBA-219](https://linear.app/uball/issue/UBA-219).

| Secret name | Used by | Bootstrap | Linear |
| --- | --- | --- | --- |
| `uball/firebase-admin-cv-merge` | `uball-cv-merge` container — Firebase Admin SDK JSON for emitting `cv_shot` logs | `bootstrap-firebase-secret.sh` | [UBA-219](https://linear.app/uball/issue/UBA-219) |
| `uball/cv/backend-url` | `uball-cv-merge` — UBall backend base URL | `bootstrap-uball-backend-secrets.sh` | [UBA-201](https://linear.app/uball/issue/UBA-201) follow-up |
| `uball/cv/auth-email` | `uball-cv-merge` — UBall service-account email | `bootstrap-uball-backend-secrets.sh` | [UBA-201](https://linear.app/uball/issue/UBA-201) follow-up |
| `uball/cv/auth-password` | `uball-cv-merge` — UBall service-account password | `bootstrap-uball-backend-secrets.sh` | [UBA-201](https://linear.app/uball/issue/UBA-201) follow-up |

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

## `uball/cv/{backend-url,auth-email,auth-password}` — UBall backend creds

### What they hold

Credentials for the merge container's `UballClient` to authenticate against the UBall backend when calling `plays_sync.create_plays_from_firebase_logs` after Firebase log emission. The Batch job def injects each into the container as an env var via the ECS `secrets` block — the container code reads `UBALL_BACKEND_URL` / `UBALL_AUTH_EMAIL` / `UBALL_AUTH_PASSWORD` from the environment with no SDK fetch needed.

### Why three separate secrets instead of one JSON

- ECS's `secrets` block injects one secret per env var. A single JSON-typed secret would work via `valueFrom: <arn>:<json-key>::` syntax, but three flat secrets are easier to rotate independently and read in the AWS console.
- Permission scoping is per-secret prefix anyway (`uball/cv/backend-*`, `uball/cv/auth-*`).

### Bootstrap

```
# Apply — creates all 3 with placeholder values if missing:
./scripts/cv_infra/secrets/bootstrap-uball-backend-secrets.sh

# Print ARNs in env-var form (handy for piping into the register script):
./scripts/cv_infra/secrets/bootstrap-uball-backend-secrets.sh --print-arns

# Audit — exit code 4 if any of the 3 is missing or still placeholder:
./scripts/cv_infra/secrets/bootstrap-uball-backend-secrets.sh --check
```

### Operator: fill the 3 values

```
aws secretsmanager put-secret-value --secret-id uball/cv/backend-url   --secret-string 'https://api.uball.example.com'      --region us-east-1
aws secretsmanager put-secret-value --secret-id uball/cv/auth-email    --secret-string '<service-account-email>'             --region us-east-1
aws secretsmanager put-secret-value --secret-id uball/cv/auth-password --secret-string '<service-account-password>'          --region us-east-1
```

Don't paste through chat / screenshots. After filling, `bootstrap-uball-backend-secrets.sh --check` returns exit 0.

### Wire into the Batch job def

```
eval "$(./scripts/cv_infra/secrets/bootstrap-uball-backend-secrets.sh --print-arns)"
./scripts/cv_infra/register-batch-job-defs.sh --merge-only
```

The registration script substitutes the 3 ARNs into the `PLACEHOLDER_*_SECRET_ARN` markers in `deploy/batch-job-defs/cv-merge-job-def.json` and registers a new immutable Batch revision.

### IAM

The merge role `uball-cv-merge-execution` (Phase 0 / [UBA-201](https://linear.app/uball/issue/UBA-201)) had its `SecretsAccess` resource list extended to cover these three by this PR. The canonical JSON for the policy lives in [scripts/cv_infra/iam-policies/cv-merge-inline.json](../iam-policies/cv-merge-inline.json) (PR #38) — after that PR + this one both merge, that file will reflect the live AWS state. Audit with `./scripts/cv_infra/iam-policies/apply-policies.sh --diff`.

# scripts/archive/

One-off remediation and investigation scripts that have already been run
against production. Kept in the repo for historical context — these were
authored to recover from specific incidents and **should not be re-run
without first reading the source comments and reconfirming the data they
touch is still present in the expected state**.

Each script documents its intent in its top-level docstring. The dates in
the filenames or docstrings indicate when the incident occurred.

## Inventory

| Script | Incident date | What it did |
| --- | --- | --- |
| `explore_missing_jetson2_sessions.py` | Feb 23, 2026 | Diagnostic: compared jetson-2 vs jetson-1 sessions, walked S3 chapters + Batch job history. |
| `investigate_sync_issue.py` | Mar 25, 2026 | Diagnostic: FL/FR sync drift on the "Akatsuki vs Hustle +30" game. Queries Firebase, S3, Batch. |
| `fix_mar28_game_split.py` | Mar 28–30, 2026 | Repair: split one Firebase game that contained two physical games (Rim Job vs ? + Hialeah vs Locksmith). |
| `fix_game5_firebase.py` | Mar 31, 2026 | Recovery: reset FR (jetson-1) + FL (jetson-2) sessions to `stopped` so the pipeline reprocesses a fixed game. |
| `fix_apr1_missing_game.py` | Apr 1, 2026 | Repair: created the missing "Akatsuki vs Miracle Leaf" Apr-1 game; set FR + FL sessions back to `stopped` for reprocessing. |
| `fix_fl_reprocess.py` | Apr 3, 2026 | Recovery: renamed Black Team → Team Music, cancelled the stale AWS Batch job, ensured FL session was `stopped`. |
| `investigate_duration_diff.py` | Apr 3, 2026 | Diagnostic: investigated why "Team Music vs Ronselli Ballers" had different durations per angle. |
| `investigate_premier_mtg.py` | Apr 3, 2026 | Diagnostic: duration mismatch on "Team Music vs Premier Mtg (C League)". |
| `missing_sessions_report.json` | Feb 23, 2026 | Output of `explore_missing_jetson2_sessions.py`. Reference data for the Feb-23 incident. |

## When to read these

- A similar incident recurs and you want to see the recovery shape that
  worked before.
- A post-mortem author wants a primary source for what was actually run
  during an incident.
- An engineer needs an example of how the various pipeline pieces
  (Firebase / S3 / Batch / Tailscale) are inspected from a script.

## Credentials note

`fix_fl_reprocess.py` and `investigate_premier_mtg.py` originally
constructed their `boto3.client('batch', ...)` with hardcoded
`aws_access_key_id` / `aws_secret_access_key`. **Those literals were
redacted before archival** and replaced with the default credential
chain (`boto3.client('batch', region_name='us-east-1')`), which picks
up creds from the environment, `~/.aws/credentials`, or the instance
profile.

If you re-purpose either script, set `AWS_ACCESS_KEY_ID` +
`AWS_SECRET_ACCESS_KEY` in your environment (or use `aws sso login` /
`aws configure`) before running.

## When **not** to re-run them

- The script targets a specific game/session ID that may no longer exist
  or may now hold valid data — re-running could corrupt the recovered
  state.
- The fix this script applied has since been promoted into the main
  pipeline (e.g. ghost-session filtering landed in
  `pipeline_session_filter.py`).

If a similar incident occurs, **copy the script up into `scripts/`,
update the IDs + dates, and submit a PR**. Don't run an archived script
against production.

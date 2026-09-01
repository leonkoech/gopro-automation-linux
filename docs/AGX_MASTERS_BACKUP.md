# AGX Session-Masters Backup (started 2026-08-19)

Local session masters on the AGX are archived to S3 and then cleared, so disk
pressure can never fail a game night while the raw footage stays recoverable.

## Where

- **Bucket**: `uball-videos-production`
- **Prefix**: `archive/agx-masters/`
  - `sessions/<label>/…` — per-night session dirs from `/home/dev/app/recordings/game_*`
    (raw 4K tracking masters, FLIR shot-cam masters, audio sidecars, timing json)
  - `legacy-recordings/…` — pre-migration captures from `/home/dev/recordings`
- **Storage class**: `STANDARD_IA` (instant retrieval, ~half standard cost)
- **Manifest**: `archive/agx-masters/manifest.jsonl` (one line per file:
  key, size, action, timestamp) — also kept on the box at
  `/home/dev/archive_manifest.jsonl`

## Safety properties of the archiver (`archive_masters.py`, box)

1. A local file is deleted **only after** its S3 object exists with the exact
   same byte size (upload verified per file; failures keep the local copy).
2. Files whose identical-size object already exists are skipped (idempotent —
   safe to re-run any time).
3. The job **pauses while a game is recording** (polls `/health` every 5 min),
   so it can never compete with capture.
4. `highlight_clips/` and all non-target paths are untouched.

## Restore

```bash
aws s3 cp s3://uball-videos-production/archive/agx-masters/sessions/<label>/ \
    /home/dev/app/recordings/<label>/ --recursive
```
(STANDARD_IA restores instantly — no thaw step.)

## Why this exists

2026-08 incidents: an S3 key-collision overwrote a game's uploaded video
(recovered via bucket versioning), and a capture-starved night's masters were
auto-cleaned before a repair needed them. Rule of thumb since: **every master
survives somewhere** — either locally or in this archive — before deletion.

## Status

- 2026-08-19: initial archive of Aug 11–15 session dirs (~211G) + legacy
  recordings (~74G) launched; box log `/home/dev/archive.log`, completion
  marker `ARCHIVE_DONE uploaded=… skipped=… failed=…`. Post-run, the box
  should sit at roughly 15% disk usage with ~5+ nights of headroom.

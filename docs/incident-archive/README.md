# docs/incident-archive/

Primary-source artefacts from production incidents. Kept here so that:

- Post-mortems can link to a stable in-repo path (Git URLs survive when
  a Slack thread or a shared doc rotates).
- An engineer chasing a recurrence can re-read the original inventory /
  investigation without re-running anything.

## Inventory

| File | Date | What it captures |
| --- | --- | --- |
| `VIDEO_INVENTORY_REPORT_2026-03-13.txt` | 2026-03-13 | Snapshot of the upload queue + S3 video presence across the GoPro fleet at the time of the Mar-13 inventory pass. |
| `VIDEO_SYNC_INVESTIGATION_2026-03-23.txt` | 2026-03-23 | Investigation notes on cross-camera sync drift; superseded by the chapter-offset / wall-clock-anchor fixes in PRs #30 + #31. |

If a related fix lands in code, prefer landing a referenced ADR or doc
in `docs/` over leaving the answer only inside these archives.

# Production Pipeline Hardening Plan

**Status**: Proposed
**Owner**: Rohit Kale
**Sibling of**: `CV_SHOT_DETECTION_V1_PLAN.md`
**Last updated**: 2026-04-16

---

## 1. Context

The GoPro → Jetson → S3 → AWS Batch → UBall → annotation-tool pipeline has
been working end-to-end, but the audit surfaced a pattern: **when something
goes wrong, operators don't know**. Today's failure modes come from:

- End-of-game writes silently failing (handlers catch-and-log, nothing in UI)
- No live visibility into recording / upload / transcode / sync state
- No single dashboard showing per-game pipeline health — operator must
click into each game or ask an engineer to inspect the database
- Mid-session camera drops invisible to the UI — the game keeps running as
if 4 angles were recording
- Plays-sync failures buried in backend logs
- Destructive actions (Delete Game, Sync Rosters) with no confirmation
- No self-serve recovery — a stuck game requires a direct database edit

This plan addresses the trust gap: **operators should always know what
succeeded, what didn't, and what they can do about it.**

V1 of the hardening work is scoped to user-facing confidence features that
require no new AWS services and can ship incrementally. Nothing in this plan
depends on the CV pipeline — the two tracks are independent.

---

## 2. Scope

### In scope (V1 hardening)

- **Reliable error surfacing** — every Firebase / Supabase / Batch write
that can fail shows a toast or inline error on failure
- **Pipeline-state dashboard** — one page listing all recent games with
their per-stage state (recording / uploaded / transcoded / synced / plays)
- **Live recording banner** — always-visible strip on the basketball timer
page showing Jetson + camera health
- **End Game undo window** — 30s grace period before the game is marked
completed; first clear click after that window requires a confirm modal
- **Sync-failed state** — when any backend sync fails, stamp the Firebase
game doc so the UI can show a red badge + a Retry button
- **Operator self-serve tools** — retry upload, reprocess videos, manual
sync, reset stuck slot — no more "please edit the DB" requests
- **Confirm dialogs** — Delete Game, Sync Rosters (when roster has local
edits), and Reset Slot all gated by explicit confirms

### Out of scope

- Halftime auto-detection (hard without clock-state integration; defer)
- Typed backend error schema (requires backend contract discussion; defer)
- Sentry / external observability client (cost + privacy review needed)
- Dark-mode / visual redesign (separate UX track)
- Any CV-pipeline integration (see `CV_SHOT_DETECTION_V1_PLAN.md`)

---

## 3. Findings from the audit

Grouped by severity. All anchors are file:line verified against the current
`main` branch.

### A. Silent failures on the basketball timer page

- `src/app/basketball/page.tsx:371-383` — `handleEndGame()` try/catches
`endGame()` but only `console.error`s on failure. Operator sees the game
cleared from UI state; they have **no idea if Firebase actually saved**.
- `src/app/basketball/page.tsx:441-444` — `setHalf()` writes a
`period_changed` log via `logAction` — no feedback if the network dropped.
This is the exact field the CV merge depends on for halftime flip.
- `src/app/basketball/page.tsx:378` — `markCompleted()` caught but no toast.

### B. No live recording / camera health visibility

- Basketball timer page has **no heartbeat poll** against the Jetsons.
- Mid-session camera drops are invisible to the operator — the game page
keeps ticking as if 4 angles are still recording.
- `firebase_service.py` has session status transitions but the frontend
never subscribes to them in real time.

### C. No pipeline-state dashboard

- `src/app/game-logs/page.tsx` tracks a **single** active transcode job; no
list view showing every recent game with its pipeline state.
- No column for "4/4 angles uploaded", "synced to UBall", "plays created".
- Operator has to click into each game to check — slow and error-prone.

### D. Sync failures don't reach the UI

- `/api/games/sync` (`main.py:1761-1997`) returns structured error JSON but
does **not** write a `sync_failed` or `sync_error` field back to the
Firebase `basketball-games/{id}` doc. Next tick of the ingestion sees a
game without `uballGameId` and retries forever with no visible signal.
- Same pattern for plays_sync (`plays_sync.py:130-133`) — failure warning
lives only in server logs.

### E. Destructive actions without confirm


| Action              | Where                  | Confirm?                           |
| ------------------- | ---------------------- | ---------------------------------- |
| End Game            | `page.tsx:801-814`     | ✅ modal (but no undo window)       |
| Delete Game (admin) | game-logs              | ❌ no confirm                       |
| Delete Slot         | `checkin/page.tsx:311` | ⚠️ basic `confirm()` dialog        |
| Sync Rosters        | basketball page        | ❌ no confirm even when edits exist |
| Clean Jetson        | `AdminConsole.tsx:117` | ✅ `window.confirm()`               |


### F. Inconsistent error UX

- Mix of `console.error`, `alert()`, and toasts across the wb app.
- `syncTeamsFromSupabase()` uses `alert()` — jarring, blocks the thread.
- Check-in page now has toasts (Phase 4-5), but the main basketball page
hasn't adopted the same pattern.

### G. No recovery / self-serve

- Game stuck in `checking_in` → no UI button to reset.
- Upload failed → no "retry upload" button.
- Game needs manual sync → no "sync now" button on game-logs.
- Currently every one of these requires direct DB access to fix.

---

## 4. Architecture for the fixes

Three new small pieces enable most of the improvements:

### 4.1 `pipelineStatusService.ts` (new frontend module)

A single source of truth for "what's the state of game X". Reads a
combined view and normalizes into a machine-friendly shape:

```ts
type PipelineStage =
  | 'recording' | 'uploading' | 'transcoding'
  | 'synced'    | 'plays_ready' | 'failed';

interface GamePipelineState {
  firebaseGameId: string;
  uballGameId: string | null;
  status: 'active' | 'completed' | 'failed';
  anglesUploaded: number;            // 0-4
  anglesTranscoded: number;          // 0-4
  playsCount: number | null;
  errors: PipelineError[];           // any stamped *_error fields
  lastEventAt: string;
  stage: PipelineStage;
}
```

Driven by:

- Firebase `basketball-games/{id}` (plus any new `sync_error` / `upload_error` fields)
- A new backend endpoint `GET /api/pipeline/summary?date=YYYY-MM-DD` that
pre-aggregates S3-key counts + UBall plays counts so the frontend makes
one request per dashboard render, not N.

### 4.2 `<Toast>` component + `useToast()` hook (new frontend utility)

Wraps `radix-ui/react-toast` or a tiny in-house implementation. Exports
one hook + a `ToastContainer` in `_app.tsx`. Every existing `alert()` or
silent catch-and-log becomes `toast.error(message)` in one PR.

### 4.3 Jetson heartbeat endpoint

New endpoint `GET /api/system/heartbeat` on the Jetson Flask app returns:

```json
{
  "jetson_id": "jetson-1",
  "cameras": [
    { "angle": "FL", "interface": "enxd43260ddac87", "connected": true,
      "recording_session_id": "ZIZy7y6bJqmBCDiAqWth", "battery_pct": 87 },
    ...
  ],
  "service_uptime_seconds": 34567,
  "disk_free_gb": 123.4,
  "last_upload_at": "2026-04-16T00:30:00Z",
  "time_offset_seconds": 0.02
}
```

Frontend polls both Jetsons' heartbeats every 15s and folds into a single
"court status" badge on the basketball timer page.

---

## 5. Phases

Estimates are engineering-days, same accounting as the CV V1 plan.

### Phase A — Toast + error-handling overhaul  —  **1 day**

**Goal**: every user-triggered backend write surfaces success or failure.

- Add `<ToastProvider>` in `gopro-automation-wb/src/app/layout.tsx`
- New `src/components/Toast.tsx` + `src/hooks/useToast.ts`
- Replace ALL `alert()` + silent `catch { console.error }` in these files:
  - `src/app/basketball/page.tsx` — handleEndGame, handleStartGame,
  handleScoreChange, handleFoulChange, setHalf, handleSyncTeams
  - `src/lib/gameService.ts` — surface `endGame()` errors back to caller
  - `src/lib/checkinService.ts` — `syncRosters` errors up (UI catches today)
- Also surface when the Firebase write succeeds but took >3s so operators
know a slow network happened (soft green toast).

**Exit**: every file under `src/app/basketball/` has no bare `console.error`
in async handlers; grep confirms zero `alert(` calls.

### Phase B — End-Game undo window  —  **0.5 day**

**Goal**: an accidental End Game click is recoverable for 30s.

- Modify `gameService.endGame()`:
  - Instead of immediately writing `status: 'completed'`, write
  `status: 'ended_pending'` + `pendingCompletionUntil: now + 30s`
  - After 30s, a server-side (cron? or client-side timer) transition
  finalizes to `status: 'completed'`
- On the basketball page, show a yellow "End Game scheduled — undo?" banner
with a 30s countdown. Click Undo → flip status back to `active`, remove
the game_ended log.
- If the client closes before 30s, the next poll from check-in page sees
`ended_pending` and can still offer Undo within the window.

**Exit**: trigger End Game, press Undo within 10s, game returns to active;
score + logs preserved.

### Phase C — Pipeline-state dashboard  —  **1.5 days**

**Goal**: one page. One table. Every game. Every stage.

- New backend endpoint `GET /api/pipeline/summary`:
  - Input: `?days=7` (default 1)
  - Output: list of games with the `GamePipelineState` shape above
  - Implementation: single call to Firebase `list_games` + single call to
  UBall for play counts + per-game S3 HEAD check on the 4 angle keys
  (parallelized via thread pool)
- New frontend page `src/app/games-dashboard/page.tsx`:
  - Sortable table with columns: Date · Game · Status · Angles uploaded
  (with mini 4-dot indicator) · Transcoded · Synced to UBall · Plays (N)
  · Errors (click to expand)
  - Row click → drill into the existing `/basketball/{id}` page
- Per-row action menu: "Retry upload" / "Manual sync" / "Reprocess videos"
(these endpoints exist already but aren't wired into UI)

**Exit**: load the dashboard on a day with 4 completed games; visually
match the truth in Firebase + S3 + UBall without clicking anything.

### Phase D — Live recording banner + Jetson heartbeat  —  **1 day**

**Goal**: the basketball timer page tells you instantly if a camera dropped.

- Add `GET /api/system/heartbeat` on the Jetson (structure above)
- Frontend `HealthBanner` component polls both Jetsons every 15s:
  - Green: both Jetsons up, all expected cameras connected
  - Yellow: one camera missing, one Jetson slow (> 5s response)
  - Red: a Jetson offline OR a camera dropped mid-session
- Show the banner under the basketball timer header at all times during
an active game. Click to expand → per-camera detail (battery, storage,
last chapter timestamp).
- When camera drops mid-session, also write a `camera_dropped` log entry
on the Firebase game so post-game forensics is possible.

**Exit**: unplug one GoPro during a test game → red banner within 15s.

### Phase E — Sync-failed state + retry buttons  —  **0.5 day**

**Goal**: failures in `/api/games/sync` and `plays_sync` are visible + recoverable.

- `main.py:sync_game_to_uball` — on error, write
`basketball-games/{id}.last_sync_error = { at, stage, message }` to
Firebase before returning 500.
- `plays_sync.create_plays_from_firebase_logs` — on any per-play failure,
write `last_plays_sync_error` + count. On success, clear it.
- Frontend: pipeline-state dashboard (Phase C) surfaces these as red
badges. "Retry" button on each game calls the relevant endpoint and
clears the error on success.

**Exit**: intentionally break UBall creds → next sync attempt stamps
Firebase with the error → UI shows red badge → fix creds → click Retry
→ badge clears.

### Phase F — Destructive-action confirms  —  **0.5 day**

**Goal**: nobody deletes anything by accident.

- Reusable `<ConfirmDialog>` component (uses the same `Toast` styling).
- Wire it to:
  - Delete Game (game-logs)
  - Delete Slot (replace the basic `confirm()` in checkin/page.tsx)
  - Sync Rosters (only when the slot has any roster entry with a
  client-generated `player_id` — i.e. a custom-added player that would
  be lost)
  - Reset Slot (new button, Phase G)
- Each confirm modal shows a 1-line impact summary ("This removes 12
plays" / "You have 1 custom-added player; they'll be merged back in").

**Exit**: grep shows zero `window.confirm(` calls in `src/app/` —
everything routes through the reusable component.

### Phase G — Self-serve recovery tools  —  **1 day**

**Goal**: stuck states are fixable from the UI.

- "Reset Slot" button on check-in admin panel for slots in
`status: 'checking_in'` with `age > 2h` → clears roster, sets back to
`teams_assigned`.
- "Retry Upload" button on the pipeline dashboard for games where any
angle is missing from S3 → triggers the existing
`POST /api/sessions/{id}/upload-chapters` for the stuck session.
- "Reprocess Videos" button for a game → triggers
`POST /api/games/process-videos` with the game's ID.
- "Sync to UBall" button for games with no `uballGameId` → triggers
`POST /api/games/sync`.

All buttons immediately show a toast on click ("Queued…" / "Success" /
"Failed: …") and the dashboard row refreshes within 5s.

**Exit**: take any game intentionally stuck in a bad state (no UBall
sync, missing upload, wrong slot status); recover each via the new UI
buttons alone — no DB edit allowed.

### Phase H — Observability & docs  —  **0.5 day**

- Add 2 CloudWatch metrics (reuse `cv_metrics.py` pattern, new
namespace `UBall/Pipeline`):
  - `GamesStuckCount` — games in non-terminal state > 2h
  - `PipelineFailuresCount` — sum of `last_sync_error` / `last_plays_sync_error` flags
- Add alarms (mirrors CV alarms structure)
- Update the runbook (`docs/CV_PIPELINE_RUNBOOK.md`) with a "Production
Hardening" section pointing at each of the new UI tools.

**Exit**: dashboard widget shows a non-zero `GamesStuckCount` when a
stuck game exists; value goes to 0 after the operator clicks Reset Slot.

### Summary


| Phase                     | Topic                             | Days                     |
| ------------------------- | --------------------------------- | ------------------------ |
| A                         | Toast + error-handling overhaul   | 1.0                      |
| B                         | End-Game undo window              | 0.5                      |
| C                         | Pipeline-state dashboard          | 1.5                      |
| D                         | Live recording banner + heartbeat | 1.0                      |
| E                         | Sync-failed state + retry buttons | 0.5                      |
| F                         | Destructive-action confirms       | 0.5                      |
| G                         | Self-serve recovery tools         | 1.0                      |
| H                         | Observability + runbook           | 0.5                      |
| **Total (critical path)** |                                   | **~6.5 days**            |
| +25% buffer for unknowns  |                                   | **~8-9 days end-to-end** |


Phases A + F can ship independently first (low-risk polish).
Phase C is the keystone — D, E, G all plug into its dashboard.

---

## 6. File-by-file inventory (proposed)

### New files


| Path                                                         | Purpose                                     |
| ------------------------------------------------------------ | ------------------------------------------- |
| `gopro-automation-wb/src/components/Toast.tsx`               | App-wide toast container                    |
| `gopro-automation-wb/src/hooks/useToast.ts`                  | Hook surface                                |
| `gopro-automation-wb/src/components/ConfirmDialog.tsx`       | Reusable confirm with impact summary        |
| `gopro-automation-wb/src/components/HealthBanner.tsx`        | Jetson + camera status banner               |
| `gopro-automation-wb/src/lib/pipelineStatusService.ts`       | Frontend normalized pipeline state          |
| `gopro-automation-wb/src/app/games-dashboard/page.tsx`       | Phase-C dashboard                           |
| `gopro-automation-linux/pipeline_summary.py`                 | Backend aggregator for the summary endpoint |
| `gopro-automation-linux/docs/PRODUCTION_PIPELINE_RUNBOOK.md` | Operator-facing runbook (Phase H)           |


### Modified files (grouped)

**Frontend**

- `src/app/layout.tsx` — mount `<ToastProvider>`
- `src/app/basketball/page.tsx` — swap `console.error`/`alert()` for toasts; mount HealthBanner
- `src/app/basketball/checkin/page.tsx` — confirm Sync Rosters if local edits exist
- `src/app/game-logs/page.tsx` — link into new dashboard; add retry buttons
- `src/lib/gameService.ts` — surface errors to caller; add `pendingCompletionUntil` path
- `src/lib/checkinService.ts` — `cancelEnd()` helper for undo window

**Backend (Jetson)**

- `main.py` — 3 new endpoints: `/api/system/heartbeat`, `/api/pipeline/summary`, `/api/pipeline/retry/{game_id}/{stage}`
- `main.py:sync_game_to_uball` — stamp `last_sync_error` on failure, clear on success
- `plays_sync.py` — stamp `last_plays_sync_error` on failure
- `firebase_service.py` — small helper `stamp_game_error(game_id, field, error)`

No schema migrations required — all new state lives on the existing
Firebase `basketball-games` doc.

---

## 7. Verification plan

1. **Unit**: new service/hook modules under `__tests__/` (jest + RTL).
2. **Integration (per phase)**:
  - A: disconnect from network, trigger End Game → red toast with retry
  - B: click End Game, press Undo at 15s → game resumes with state intact
  - C: open dashboard with a known-bad game → bad cells visible in red
  - D: unplug a GoPro during a game → red banner + `camera_dropped` log
  - E: break UBall creds → badge red + Retry button works after fix
  - F: try to Delete Game → modal shows 12 plays impact; Cancel works
  - G: reset a stuck `checking_in` slot → slot status goes back to
    `teams_assigned`, UI refreshes within 5s
3. **End-to-end drill**: take a freshly recorded test game through the
  full pipeline, intentionally introduce one failure at each stage, and
   verify the dashboard + toasts + retry buttons guide recovery without
   any DB edits. Success = "no Claude, no `gcloud firestore`" for a full
   hour.

---

## 8. Open questions

1. **Toast library**: ship with `sonner` (minimal, well-known), or
  hand-roll? Ship cost difference is ~20 min; pick sonner unless you
   prefer zero new deps.
2. **Undo window duration**: 30s default — is that right? Some tournaments
  end back-to-back; too long and the next game can't start.
3. **Heartbeat frequency**: 15s is a good default. On cellular the poll
  cost might be noticeable — consider exponential backoff when stable.
4. **Dashboard ACL**: the pipeline dashboard is admin-only? Any concern
  exposing the "manual sync" button to coaches?
5. **Camera-drop auto-recovery**: if a GoPro drops for <10s, should we
  suppress the red banner? Noise vs miss trade-off.
6. **Priority ordering**: any phase you want moved up? Phase C feels
  highest leverage — everything else hangs off its dashboard.


# Orin-local scoreboard — plan / design for review

**Status:** DRAFT for Rohit's review. Nothing in the write path below is built.
**Companion (built, in review):** `orin_display/` — PR "feat(orin_display): LAN
scoreboard display — read-only SSE relay". That covers the **read** path only.
**Related:** `docs/REALTIME_SCOREBOARD_VISION.md` (CV auto-scoring — different axis).

Everything here is **additive**: new routes, new endpoints, a per-game opt-in
flag. `/admin-controls`, `/tv-display`, `BasketballScoreboard.tsx`, `gameService`,
and the Firestore sync model are **not modified or replaced**.

---

## 1. Problem

The courtside scoreboard lags and glitches during games. It works fine for Rohit
in India. On-site it's an **Amazon Fire TV running Silk** (weak browser) syncing
through **Firestore over congested gym Wi-Fi**. Once, the whole facility's
internet dropped and the board was dead.

Two separable issues:

| | Cause | Fix axis |
|---|---|---|
| **Day-to-day lag** | Fire TV Silk + gym Wi-Fi + cloud round-trip | move the TV's sync onto the LAN (or replace the stick) |
| **Total outage** | entire system is cloud-dependent (page + state + time all from the cloud) | serve + sync locally |

The read-relay PR addresses the first for the **display**. This doc is about
doing the same for the **scoring iPad**, and about surviving an internet outage.

## 2. The hard constraint

The Firebase-hosted pages are **HTTPS**. A browser will not let an HTTPS page
open a `http://` / `ws://` connection to a LAN IP (mixed content). Rohit already
hit this with the highlight clips and routed around it via S3 presigned URLs.

**Consequence:** a LAN-local page cannot be hosted on Firebase. Both the display
and the admin page, to talk to the Orin over the LAN, must be **served by the
Orin itself over plain HTTP**. New routes on a new origin:

```
http://<orin-lan-ip>:8090/            → display   (built: orin_display)
http://<orin-lan-ip>:8090/control     → scorekeeper (this doc)
```

## 3. Target architecture

```
                 ┌──────────────── Orin (agxorin001) ────────────────┐
 iPad  ──HTTP/LAN─┤  orin_display service (:8090)                     │
 /control         │   • holds authoritative live game state (memory)  │
                  │   • POST /control/action  → mutate state          │
 Fire TV ─SSE/LAN─┤   • GET  /events          → push state            │
 /                │   • mirrors state → Firestore live/state (online) ─┼──▶ Firestore ──▶ everything downstream
                  └──────────────────────────────────────────────────┘      (annotation, recap, Core, cloud /tv-display)
```

- The Orin is the **single writer** for an Orin-owned game.
- It mirrors every change to Firestore `basketball-games/{id}/live/state` and the
  parent doc, so all existing consumers keep working unchanged when online.
- Internet down → the iPad and TV keep working on the LAN; the Orin queues the
  Firestore mirror and flushes it on reconnect (its in-memory state is truth).

## 4. Open questions for Rohit (the design decisions)

### 4.1 Writer ownership / the single-scorekeeper invariant
Memory `scoreboard-single-scorekeeper`: the admin scoreboard treats local state
as authoritative. If someone opens the cloud `/admin-controls` **and** the Orin
`/control` for the same game, they're two writers racing.

**Proposal:** a per-game field `liveOwner: "cloud" | "orin"` on the game doc.
- Default `"cloud"` — nothing changes.
- The Orin `/control` page sets it to `"orin"` when a scorekeeper takes the game
  there; the cloud `/admin-controls` goes read-only / shows a banner when it sees
  `liveOwner === "orin"`.
- Only the owner writes `live/state`.

Is that the right mechanism? Does it fit how `pushLive` / the undo stack /
`applyGameSnapshot` assume ownership today?

### 4.2 Firestore mirror — fields & reconciliation
- Which fields does the Orin mirror? (`leftTeam`/`rightTeam` scores+fouls+TOs,
  `period`, `possession`, `clock`, `undoStack`, `playerScores`, `highlight`?)
- On reconnect after an outage: Orin state wins wholesale, or field-merge?
- The parent game doc also carries `status`, `startingSideTeam1`, `half2AtMs`,
  rosters, `logs[]` — `logs[]` is transaction-rewritten by the frontend. Does the
  Orin ever need to append to `logs[]` (for `register_plays` / highlight cuts),
  and if so how does it avoid clobbering a concurrent frontend transaction?

### 4.3 Downstream that reads the game doc
`register_plays`, the AGX recording triggers, `highlight_clip` commands (via
`agx-commands`), recap, Core — all read Firestore. During an internet outage the
Orin has the state but Firestore is stale.
- Recording triggers: the AGX **is** the Orin — could this become a local call
  instead of a Firestore round-trip? Worth it?
- Highlight cuts need the score log's timestamp + team. If scoring happens
  offline, the `highlight_clip` for those makes has to be queued too.

### 4.4 Auth
`/admin-controls` uses Firebase Auth (`AuthContext`). An Orin-served page can't
run Firebase Auth with the internet down. Options: a local PIN, a device token
baked into the Orin, or trust-the-LAN (no auth on `/control`, rely on physical
access). Which is acceptable operationally?

### 4.5 Reuse `BasketballScoreboard` or build lean?
- **Reuse:** representative UI, but needs a transport abstraction so the
  component can target the Orin instead of `gameService`/Firestore. Invasive to a
  file you're actively working on.
- **Lean parallel** (in `orin_display/`, served by the Orin): faster, fully
  decoupled, autodeploys with the backend — but the UI drifts from the real one
  and re-implements score/foul/clock/timeout/possession/undo/player-scores.
- The display half already went lean. Does the scorekeeper half justify the
  reuse cost?

### 4.6 Scope of `/control`
`/admin-controls` also does pre-game check-in, roster editing, schedule/slot
sync, adhoc pregame, auto-end, game creation. How much of that must the Orin
page replicate vs. "cloud handles setup, Orin handles the live game only"?

## 5. Phasing (proposed)

| Phase | What | Risk |
|---|---|---|
| **A — done** | `orin_display/` read-only relay + demo mode | none (read-only, own port) |
| **B** | On-site A/B: Fire TV on `/tv-display` vs `http://orin:8090`. Also the laptop-on-same-Wi-Fi test. Decide: is it the device or the network? | none |
| **C** | If network: `/control` write path behind `liveOwner` flag, default off. Orin mirrors to Firestore. Tested in a confirmed no-game window. | touches the writer model — **needs Rohit** |
| **D** | Offline hardening: mirror queue + reconnect flush; local auth; queued highlight/recording triggers | medium |

If Phase B says "it's the Fire TV," the cheaper answer is a ~$150 mini-PC on
wired ethernet running Chrome on the existing `/tv-display`, and C/D become
optional.

## 6. Deploy safety (from REALTIME_SCOREBOARD_VISION.md)

**Recording is sacred.** The read relay is a separate process on a separate port
and is safe to deploy any time. Anything in Phase C/D that the recording path
depends on (or that restarts `agx-ingestion`) must only land in a confirmed
no-game window, behind a default-off flag, after a controlled test.

## 7. Files in play (when C/D happen)

- `orin_display/` — new endpoints (`/control`, `/control/action`), new page
- `gopro-automation-wb` — only if we reuse `BasketballScoreboard` (transport
  abstraction); otherwise untouched
- game doc — new field `liveOwner`; no schema change to existing fields

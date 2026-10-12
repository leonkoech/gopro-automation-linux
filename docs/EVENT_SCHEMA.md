# Event schema — draft for review

**Status: proposal. Nothing implements this yet.**

One schema for everything the pipeline emits, so that the clip cutter, the
uploader, the registrar and the annotation tool can consume events without
knowing what a rim is.

This is the first step of separating the sport from the pipeline. The step after
it moves rim geometry, make/miss and shot typing behind a module; this document
defines what crosses the boundary between them. It is written against two sports
on purpose — basketball because we run it, volleyball because it breaks
assumptions basketball lets us keep. A schema validated against one sport is
just that sport's record with general-sounding field names. Soccer and
pickleball were looked at too, and §4 records only what they break rather than
designing for sports we are not building.

---

## 1. What we emit today

Three producers, three unrelated shapes, all basketball-specific.

**The live detector** (`shot_detect/live.py`, the `shadow` record):

```json
{"cam": "SL", "side": "left", "seg": 418, "t_shot": 2.417,
 "made": true, "verdict": "MAKE", "rho": 0.71,
 "wallclock": "2026-09-14T19:22:31.880Z", "detected_at": "2026-09-14T19:22:48.114Z",
 "latency_s": 16.2, "scan_s": 3.9}
```

**Typing** (`shot_typing_live.py`, written to `cv_points.{logId}`):

```json
{"zone": "3PT", "points": 3, "who": "23", "angle": "FL",
 "typed_at": "2026-09-14T19:23:19.004Z", "proc_s": 28.0,
 "confidence": 0.9, "zone_source": "strict", "pose_degenerate": false}
```

**The highlight trigger** (`live.py`, `_maybe_highlight`):

```json
{"logId": "cv_1757877751_left", "ts": "2026-09-14T19:22:31.880Z",
 "side": "left", "firebase_game_id": "...", "pre": 5.0, "post": 2.0}
```

### Four problems in those shapes

**1. "Side" means two different things, and we have already been bitten.**
`side_attribution.py` exists precisely because of this: the scoreboard's
`team` field is "left"/"right" meaning *team identity*, while the detector's
`side` means *physical hoop*, and the two diverge after half-time. One word,
two concepts, a module to translate between them. Any schema that keeps a bare
`side` inherits that confusion and exports it to the next sport.

**2. An event is assumed to be an instant.** `t_shot` is a moment. A basketball
shot is a moment, so nothing has pushed back. A volleyball rally is an interval
and the scoring event is its *end* — nothing crossed a plane; a return failed.
Interval support is nearly free now and a migration later.

**3. Observation, interpretation and telemetry are in one object.** `rho` and
`verdict` are evidence and conclusion; `latency_s` and `scan_s` are how the
pipeline performed; `made` is a basketball fact. They have different audiences,
different lifetimes, and only one of the three should cross the sport boundary.

**4. The two confidences are not comparable.** The detector's `rho` is a
geometric distance; typing's `confidence` is 0.9 or 0.4 — two levels, not a
scale, as its own source comments say. Consumers currently cannot compare them
and nothing says so.

---

## 2. The model

Four kinds of message, not one.

| Kind | Who emits | Sport-aware? | Example |
| --- | --- | --- | --- |
| **Observation** | a detector | no | the ball passed through the hoop plane at T, downward, 0.71 off centre |
| **Interpretation** | the sport module | yes | 3 points, credited to the home team |
| **Enrichment** | any later stage | yes | the shooter was number 23 |
| **Command** | the sport module | no | cut a clip from T−5s to T+2s on the left camera |

Everything left of the boundary emits and consumes **observations** and
**commands**. Only the sport module emits **interpretations**, and only it
reads observations as meaning anything.

A command is deliberately not an event. The clip cutter is told to cut; it does
not infer that it should.

### What "sport-agnostic" covers, and what it does not

**Proposed: sport-agnostic is a claim about the software platform only. Camera
topology is expected to change per sport.**

The recording, transcoding, segmenting and clip-cutting code does not need to
know the sport. Where the cameras go, how many there are and what they are
pointed at does — basketball puts high-frame-rate cameras at two fixed rims
because that is where the decisive moment happens, while a sport whose decisive
moment can occur anywhere along a net or a boundary line needs a different rig.

This matters beyond the schema. A new sport is not only a module: it is a site
survey, a camera count and a calibration. Per-site configuration has to express
that, which is why `where.structure` names a structure from the site's
calibration rather than a camera — the same code reads a two-rim court and a
net, and only the configuration differs.

---

## 3. The schema

### Common envelope

Every message carries this.

```json
{
  "id": "ev_c0a8f1e2d4b7",
  "external_ids": {"annotation_tool": "cv_1757877751_left"},
  "schema": "uai.event.v1",
  "game_id": "7cef734e-...",
  "site_id": "riverside",
  "surface_id": "court-a",
  "kind": "observation.plane_cross",
  "t": {"start": "2026-09-14T19:22:31.880Z", "end": "2026-09-14T19:22:31.880Z"},
  "observed_by": ["SL"],
  "produced_by": {"stage": "shot_detect", "version": "v3-trt-1.90",
                  "method": "aperture"},
  "emitted_at": "2026-09-14T19:22:48.114Z",
  "confidence": {"value": 0.92, "scale": "ordinal"},
  "phase": "unknown"
}
```

| Field | Why it is here |
| --- | --- |
| `id` | so enrichments can refer to an event instead of re-describing it. Derived, not random — see §6 |
| `external_ids` | ids other systems already use for this event, during migration. Never the primary key |
| `surface_id` | which playing surface at the site. `where.structure` is meaningless without it the moment a venue has more than one court, and `cameras.json` currently has a single `location: "court-a"` doing both jobs |
| `schema` | the version, present from the first message, not added after the first breaking change |
| `t.start` / `t.end` | equal for an instant, different for an interval. This is the field that makes volleyball expressible |
| `observed_by` | which sensors saw it. A list: two cameras may witness one event |
| `produced_by.version` | which model and which settings. We have already lost time to an eval drifting from production |
| `confidence.value` | gates human review; it is not a probability and must not be read as one. Producer-specific evidence lives in `evidence`, which generic code never reads |
| `confidence.scale` | mandatory, no default: `calibrated` (earned against ground truth), `ordinal` (ranked only), or `none`. Typing's 0.9/0.4 is `ordinal` |
| `phase` | `warmup`, `play`, `break`, `postgame`, or `unknown` when nothing can yet tell. Corrected by `enrichment.phase`, never rewritten in place — see §6 |

`emitted_at` minus `t.end` is the detection latency, so `latency_s` and `scan_s`
stop being event fields. Pipeline telemetry belongs in the coverage record, not
in the event.

### Observation

Sport-neutral geometry and kinematics. The sport module interprets these; the
pipeline never does.

```json
{
  "kind": "observation.plane_cross",
  "where": {"structure": "goal_left", "direction": "downward"},
  "evidence": {"offset": 0.71, "frames": 4, "unit": "normalized"}
}
```

Proposed observation kinds, with the sports that need them:

| Kind | Meaning | Basketball | Volleyball |
| --- | --- | --- | --- |
| `observation.plane_cross` | an object crossed a defined plane | ball through the hoop | ball over the net |
| `observation.region_enter` | an object entered a defined region | ball in the paint | ball lands in/out |
| `observation.contact` | two tracked things touched | — | ball touches floor |
| `observation.possession_change` | the tracked object changed holder | rebound, turnover | — |
| `observation.absence` | an expected event did not occur in a window | shot clock expiry | no successful return |

`observation.absence` is the one basketball would never have taught us. A
volleyball point is awarded because something *failed to happen*, and there is no
plane crossing to hang it on.

`where.structure` names a thing in the site's calibration — `goal_left`,
`net`, `court_boundary` — not a camera and not a team. The structure list is
per-sport configuration; the field itself is not.

### Interpretation

What it means. Only the sport module emits these.

```json
{
  "kind": "interpretation.score",
  "derived_from": ["ev_c0a8f1e2d4b7"],
  "credit": {"team": "team1", "player": null},
  "value": {"points": 3, "class": "3PT"}
}
```

`credit.team` is who benefits. `where.structure` is which physical thing was
involved. They are separate fields because they are separate facts, and
conflating them is exactly the bug `side_attribution.py` was written to undo.

### Enrichment

A later, better answer about an event already emitted. Never a rewrite.

```json
{
  "kind": "enrichment.identity",
  "derived_from": ["ev_c0a8f1e2d4b7"],
  "credit": {"player": "23"},
  "confidence": {"value": 0.9, "scale": "ordinal"},
  "produced_by": {"stage": "typing", "version": "v2-strict", "method": "release_pose"}
}
```

This is what typing and WHO emit today, writing into a Firestore field instead.
Consumers that already acted on the original event apply the correction; they do
not wait for it. That is the existing decoupling, written down.

| Kind | Corrects | Produced by |
| --- | --- | --- |
| `enrichment.identity` | who the player was | typing, WHO |
| `enrichment.value` | what the score was worth | typing |
| `enrichment.phase` | whether the event was in play at all | the post-game pass today, the live detector later |

`enrichment.phase` is how `deadball.py`'s work reaches an event that was emitted
long before anything could tell. It carries the same `derived_from` as any other
enrichment, so an event's phase history is readable rather than overwritten.

---

## 4. The two-sport test

The same play, in both sports, using only the fields above.

**Basketball — a made three.**

1. `observation.plane_cross` — SL, `goal_left`, downward, offset 0.71
2. `interpretation.score` — 3 points, team1, derived from (1)
3. `command.cut_clip` — T−5s to T+2s, left camera
4. `enrichment.identity` — player 23, confidence 0.9 ordinal, derived from (1)

**Volleyball — a point won on a failed return.**

1. `observation.plane_cross` — ball over `net`, toward the home side
2. `observation.contact` — ball touches `floor`, inside `court_home`,
   `t.start == t.end`
3. `observation.absence` — no `observation.contact` with a player in the window
   between (1) and (2)
4. `interpretation.score` — 1 point, team2, derived from (1), (2) and (3),
   `t.start` = the serve, `t.end` = the floor contact
5. `command.cut_clip` — the rally interval, not a fixed window around a moment

Step 4 is the test. Its `t` is an interval spanning the rally, it derives from
three observations rather than one, and no single observation "is" the point. A
schema with a scalar timestamp and one-to-one derivation cannot express it.

Step 5 is the second test: the clip window comes from the event's own interval
rather than from a pre/post constant. Basketball's fixed 5s/2s becomes a special
case of an interval, not the only shape the cutter understands.

### What other sports would break

Two sports were looked at and deliberately not written up as full examples.
Most of what they would exercise, volleyball already does, and four parallel
worked examples is a document people stop reading. Each breaks one thing that
is genuinely new, and those two things are recorded here rather than designed
for — we are not building either sport, and an abstraction built for four
hypothetical ones usually fits none of them.

**Soccer: an event that is retroactively void.** The goal is a plane crossing,
which is nothing new. Offside and VAR are. The event happened, was emitted, and
consumers acted on it — and then a rule evaluated over earlier state says it
does not count. That is not what `enrichment.value` means: changing 3 to 2 says
the value was wrong, whereas a disallowed goal says the event should never have
scored. Whether an event can be *voided*, as distinct from corrected, is left
open.

Basketball already has a weaker form of this, which is why it is not
hypothetical: the scorekeeper is authoritative over CV, and `cv_points`
deliberately never writes to `logs[]`. That precedence between producers exists
in the code today and appears nowhere in this schema.

**Pickleball: several courts running at once.** A pickleball venue runs four to
eight courts simultaneously. Basketball is one court per facility, and the
system assumes it throughout — `cameras.json` carries a single
`location: "court-a"`, and the coverage record assumes one game per box. This
is the one finding acted on here: `surface_id` is in the envelope from the
start, because `where.structure` says nothing useful once a site has more than
one playing surface, and adding an identifier later is a migration.

It is worth being clear that this is a schema fix only. Several simultaneous
courts is a capacity and topology problem well beyond the envelope, and nothing
in this document addresses it.

---

## 5. What this does not decide

- **Transport.** Events ride a local durable queue on the box; which one is a
  separate choice.
- **Storage.** Whether events are the system of record or a projection of it.
- **The migration.** Today's Firestore shapes have live consumers. Nothing here
  says how we get from one to the other, and that sequencing is its own piece of
  work.
- **Per-sport geometry format.** `where.structure` names a structure; what
  defines a structure (the rim ellipse today) stays sport-specific
  configuration.

## 6. Proposals

These came out of reviewing the draft. They are proposals, not agreements —
nothing here has been through the team, and each is written as a position to
argue with rather than a settled point.

**Carry `observation.absence` from the start.** It costs little and there is no
harm in having it before the sport that forces it. Basketball has one use we do
not currently detect — shot-clock expiry.

**Make confidence a scalar and an evidence object, not one or the other.** The
scalar's sanctioned use is "should a person look at this", not "how likely this
is to be correct". `scale` is mandatory and has no default.

The reason for not stopping at a scalar: the two numbers we have today are a
scale of different things. Typing's 0.9 against 0.4 records which code path ran
— whether STRICT committed or fell back — and its own source calls it two
levels, not a scale. The detector's `rho` is a geometric distance and
`clf_prob`, where it exists, is a probability. In one field they are unrelated
facts wearing the same clothes, and something will eventually threshold, sort or
average them: the annotation tool already flags cards against a 0.7 threshold on
typing's number.

So `evidence` stays per-producer and opaque, read by the sport module and by a
person debugging, never by generic code — which is also what keeps `rho` from
leaking out of the sport module. The scalar stays because consumers need
something simple to gate on, and requiring every consumer to understand `rho`
would defeat the boundary.

`calibrated` has to be earned per producer rather than declared. We have the
ground truth to earn it — 180 shots from the TensorRT parity run, the 505-shot
benchmark, 28 annotated games. Until a producer is calibrated against it, that
producer says `ordinal` and consumers know where they stand.

**`phase` is an envelope field from the start, corrected by enrichment and never
rewritten.** Phase detection will run after the game first and live later; the
field has to survive that change without a schema change.

An event carries the best value known when it is emitted, which is `unknown`
while nothing can tell. The post-game pass does not go back and edit those
events — it emits `enrichment.phase` referring to them, the same way typing
already corrects a score it arrived too late to inform. Consumers that have
already acted apply the correction.

Rewriting the field in place was the alternative and it does not work: events
are emitted to consumers that may have acted on them, so a value that silently
changes afterwards is a value nobody can trust at the moment they read it. When
phase detection goes live, the same field simply starts being right at emit time
and fewer enrichments are produced. Nothing else changes.

**Use one id scheme, derived rather than random, with the old id kept as an alias.**
A single site-unique id with no sport concept in it, so `side` leaves the
identifier.

Derived matters more than it looks. `cv_<epoch>_<side>` is computed from the
event, so re-running the same footage produces the same id — which is free
deduplication, and we do reprocess: backfills, re-cuts, and the deferred scan of
segments the live loop never reached. A randomly generated id would produce a
duplicate on the second pass instead of a match. So the new id is a deterministic
function of a natural key — site, producer, event time, structure — not a random
one.

The annotation tool consumes `cv_<epoch>_<side>` today. That is a migration
constraint, not a reason for a second permanent scheme, so the old form lives in
`external_ids` for as long as it is needed and never as the primary key.

## 7. Open questions for review

Section 6 offers a position on each of the questions this draft opened with;
those positions still need agreeing, and that is the review this document is
asking for. Two questions remain genuinely open, both raised by looking at
sports we are not building (§4):

1. **Can an event be voided, as distinct from corrected?** A disallowed goal is
   not a value that was wrong. Basketball's nearest equivalent today is a
   scorekeeper overruling CV.
2. **What is the precedence between producers?** The scorekeeper already
   outranks CV — `cv_points` never writes to `logs[]` — and that ordering lives
   in code rather than in the schema. If two producers disagree about the same
   event, nothing here says who wins.

Neither needs answering before an implementation starts, and both would be
answered badly in the abstract.

What remains beyond them is in §5: transport, storage, the migration from
today's Firestore shapes, and the per-sport geometry format, none of which this
document sets out to settle.

The next thing that would change this document is an attempt to implement it:
moving rim geometry, make/miss and shot typing behind a module, and finding out
which of these fields turn out to be wrong.

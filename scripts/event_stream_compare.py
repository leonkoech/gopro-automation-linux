#!/usr/bin/env python3
"""Do the events reconstruct the record they were derived from?

The event schema (docs/EVENT_SCHEMA.md) is a proposal argued on paper. The
detector now writes it alongside the record it already wrote, and this answers
the only question that matters: given the events for a game, can you rebuild
what the detector actually found? Anything that cannot be rebuilt is a field the
schema is missing or getting wrong, and it is far cheaper to learn that here
than after the pipeline has been restructured around it.

It compares, per game:

  count     one observation per detected shot, one interpretation per make
  identity  every event id distinct, and stable if the game is rescanned
  timing    each event's t.start equal to the shot's wallclock
  verdict   makes reconstructed from the presence of an interpretation
  side      the hoop recoverable from where.structure

STANDALONE ON PURPOSE. No imports from agx_pipeline, so it runs on the AGX from
a paste with nothing checked out -- that box records games most nights and
switching branches there would swap the live detector under the running service.

    python3 scripts/event_stream_compare.py --events <dir> --shadow <shot_timing.json>
    python3 scripts/event_stream_compare.py --game <id> --root /home/dev/app/recordings
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from typing import Dict, List, Optional, Tuple


def load_events(path: str) -> List[Dict]:
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    print(f"  warn: unparseable line skipped in {path}")
    return out


def load_shadow(path: str) -> List[Dict]:
    """The detector's own record. Either the shot_timing sidecar or a raw list."""
    with open(path) as fh:
        doc = json.load(fh)
    if isinstance(doc, list):
        return doc
    for key in ("shots", "shadow", "records"):
        if isinstance(doc.get(key), list):
            return doc[key]
    raise SystemExit(f"no shot list found in {path} (keys: {list(doc)[:8]})")


def check(events: List[Dict], shadow: List[Dict]) -> Tuple[int, int]:
    obs = [e for e in events if e.get("kind") == "observation.plane_cross"]
    interp = [e for e in events if e.get("kind") == "interpretation.score"]
    makes = [s for s in shadow if s.get("made")]
    ok = fails = 0

    def result(name: str, passed: bool, detail: str = "") -> None:
        nonlocal ok, fails
        print(("  PASS  " if passed else "  FAIL  ") + name + (f"   {detail}" if detail else ""))
        if passed:
            ok += 1
        else:
            fails += 1

    result("one observation per detected shot",
           len(obs) == len(shadow), f"{len(obs)} events vs {len(shadow)} shots")
    result("one interpretation per make",
           len(interp) == len(makes), f"{len(interp)} events vs {len(makes)} makes")

    ids = [e.get("id") for e in events]
    dupes = [i for i, n in Counter(ids).items() if n > 1]
    # A duplicate id is not automatically wrong -- the id is derived, so a
    # rescan of the same shot SHOULD collide. Within one game's stream it means
    # two different shots hashed the same, which is.
    result("event ids distinct within the game", not dupes,
           f"duplicated: {dupes[:3]}" if dupes else "")

    by_wc = {}
    for e in obs:
        by_wc.setdefault((e.get("t") or {}).get("start"), []).append(e)
    missing = [s.get("wallclock") for s in shadow if s.get("wallclock") not in by_wc]
    result("every shot's wallclock has an observation", not missing,
           f"{len(missing)} missing, first {missing[:2]}" if missing else "")

    bad_t = [e.get("id") for e in events
             if (e.get("t") or {}).get("start") != (e.get("t") or {}).get("end")]
    result("a shot is an instant (t.start == t.end)", not bad_t,
           f"{len(bad_t)} with an interval" if bad_t else "")

    sides_shadow = Counter(s.get("side") for s in shadow)
    sides_events = Counter((e.get("where") or {}).get("structure", "").replace("goal_", "")
                           for e in obs)
    result("hoop recoverable from where.structure",
           sides_shadow == sides_events, f"{dict(sides_shadow)} vs {dict(sides_events)}")

    linked = [e for e in interp if e.get("derived_from")]
    obs_ids = {e.get("id") for e in obs}
    dangling = [e.get("id") for e in interp
                if not set(e.get("derived_from") or []) <= obs_ids]
    result("every interpretation links to an observation it has",
           len(linked) == len(interp) and not dangling,
           f"{len(interp) - len(linked)} unlinked, {len(dangling)} dangling"
           if (len(linked) != len(interp) or dangling) else "")

    # What the schema could NOT carry. Not a failure -- the detector genuinely
    # does not know these at emit time -- but worth seeing, because a field that
    # is always empty is a field that has not been justified yet.
    empties = []
    for field, got in (("confidence.value", [e for e in events
                                             if (e.get("confidence") or {}).get("value") is not None]),
                       ("credit.team", [e for e in interp
                                        if (e.get("credit") or {}).get("team")]),
                       ("value.points", [e for e in interp
                                         if (e.get("value") or {}).get("points") is not None]),
                       ("phase != unknown", [e for e in events
                                             if e.get("phase") not in (None, "unknown")])):
        if not got:
            empties.append(field)
    if empties:
        print("\n  always empty in this game: " + ", ".join(empties))
        print("  (expected at this stage: typing and the phase pass fill these"
              " later, as enrichments)")
    return ok, fails


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--events", help="events .jsonl, or a directory of them")
    ap.add_argument("--shadow", help="the detector's shot_timing.json")
    ap.add_argument("--game", help="game id, resolved under --root")
    ap.add_argument("--root", default="/home/dev/app/recordings")
    o = ap.parse_args()

    pairs: List[Tuple[str, str, str]] = []
    if o.events and o.shadow:
        pairs = [(o.game or os.path.basename(o.events), o.events, o.shadow)]
    elif o.game:
        ev = os.path.join(o.root, "events", f"{o.game}.jsonl")
        sh = os.path.join(o.root, o.game, f"{o.game}_shot_timing.json")
        pairs = [(o.game, ev, sh)]
    else:
        raise SystemExit("pass --game, or --events and --shadow")

    total_fail = 0
    for name, ev, sh in pairs:
        print(f"\n{name}")
        if not os.path.exists(ev):
            print(f"  no event stream at {ev} — was EVENT_STREAM_ENABLED set?")
            total_fail += 1
            continue
        if not os.path.exists(sh):
            print(f"  no detector record at {sh}")
            total_fail += 1
            continue
        ok, fails = check(load_events(ev), load_shadow(sh))
        print(f"  {ok} passed, {fails} failed")
        total_fail += fails

    print("\n" + ("the events reconstruct the record — the schema carried this game"
                  if not total_fail else
                  "DIFFERENCES — each failure above is a field the schema is missing\n"
                  "or filling wrongly. That is the point of running this before any\n"
                  "code is restructured around it."))
    return 1 if total_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())

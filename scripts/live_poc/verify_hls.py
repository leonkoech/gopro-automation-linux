#!/usr/bin/env python3
"""Verify a live HLS output against the T0 acceptance criteria.

Checks the things the timestamp-rebase design actually depends on, rather than
"it looked like it played":

  1. EVENT playlist, no premature ENDLIST
  2. every segment carries EXT-X-PROGRAM-DATE-TIME
  3. PDT advances monotonically by ~the segment duration (no gaps, no repeats)
  4. declared EXTINF matches the segment's real decoded duration
  5. PDT tracks real wall-clock (this is the anchor; if it drifts, the rebase
     silently puts every live card at the wrong moment in the VOD)
  6. live-edge lag = now - (last PDT + its duration)

Usage:  ./verify_hls.py /home/dev/live_poc/FL [expected_seg_seconds]
Exit 0 if every check passes, 1 otherwise.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

PDT_RE = re.compile(r"^#EXT-X-PROGRAM-DATE-TIME:(.+)$")
INF_RE = re.compile(r"^#EXTINF:([0-9.]+)")


def parse_pdt(raw: str) -> datetime:
    """ffmpeg writes the offset as -0400 (no colon), which datetime.fromisoformat
    rejects before Python 3.11. Normalise it rather than depending on the
    interpreter version on the box."""
    raw = raw.strip().replace("Z", "+00:00")
    if re.search(r"[+-]\d{4}$", raw):
        raw = raw[:-2] + ":" + raw[-2:]
    return datetime.fromisoformat(raw)


def probe_duration(path: str) -> float | None:
    """Real decoded duration of a TS segment — not what the playlist claims."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "format=duration", "-of", "json", path],
            capture_output=True, text=True, timeout=30).stdout
        return float(json.loads(out)["format"]["duration"])
    except Exception:
        return None


def parse(playlist: str):
    """-> (is_event, has_endlist, [(pdt, extinf, filename)])"""
    lines = open(playlist).read().splitlines()
    is_event = any(l.startswith("#EXT-X-PLAYLIST-TYPE:EVENT") for l in lines)
    has_endlist = any(l.startswith("#EXT-X-ENDLIST") for l in lines)
    segs, pdt, inf = [], None, None
    for line in lines:
        if m := PDT_RE.match(line):
            pdt = parse_pdt(m.group(1))
        elif m := INF_RE.match(line):
            inf = float(m.group(1))
        elif line and not line.startswith("#"):
            segs.append((pdt, inf, line))
            pdt = None
    return is_event, has_endlist, segs


def main() -> int:
    d = sys.argv[1] if len(sys.argv) > 1 else "/home/dev/live_poc/FL"
    want_seg = float(sys.argv[2]) if len(sys.argv) > 2 else 4.0
    playlist = os.path.join(d, "live.m3u8")
    if not os.path.exists(playlist):
        print(f"FAIL  no playlist at {playlist}")
        return 1

    is_event, has_endlist, segs = parse(playlist)
    now = datetime.now(timezone.utc)
    fails: list[str] = []

    print(f"playlist : {playlist}")
    print(f"segments : {len(segs)}")

    # 1 -- EVENT playlist (this is what gives DVR back to tip-off)
    print(f"{'PASS' if is_event else 'FAIL'}  EXT-X-PLAYLIST-TYPE:EVENT")
    if not is_event:
        fails.append("not an EVENT playlist - no DVR")

    # 2 -- every segment has a PDT
    missing = [f for p, _, f in segs if p is None]
    print(f"{'PASS' if not missing else 'FAIL'}  every segment has PROGRAM-DATE-TIME"
          + (f" ({len(missing)} missing)" if missing else ""))
    if missing:
        fails.append(f"{len(missing)} segments without PDT - rebase anchor absent")

    timed = [(p, i, f) for p, i, f in segs if p is not None]
    if len(timed) < 2:
        print("FAIL  need >=2 timed segments to check drift")
        return 1

    # 3 -- PDT advances by ~the segment duration
    worst_step = 0.0
    for (p1, i1, _), (p2, _, _) in zip(timed, timed[1:]):
        step = (p2 - p1).total_seconds()
        worst_step = max(worst_step, abs(step - (i1 or want_seg)))
    ok = worst_step < 0.5
    print(f"{'PASS' if ok else 'FAIL'}  PDT advances with segment duration "
          f"(worst deviation {worst_step:.3f}s)")
    if not ok:
        fails.append(f"PDT step deviates by {worst_step:.3f}s - timeline has gaps")

    # 4 -- declared EXTINF vs real decoded duration
    worst_dur, checked = 0.0, 0
    for _, inf, fn in timed[1:-1][:12]:          # skip first/last, they run short
        real = probe_duration(os.path.join(d, fn))
        if real and inf:
            worst_dur = max(worst_dur, abs(real - inf))
            checked += 1
    ok = checked > 0 and worst_dur < 0.5
    print(f"{'PASS' if ok else 'FAIL'}  EXTINF matches decoded duration "
          f"(worst {worst_dur:.3f}s over {checked} segments)")
    if not ok:
        fails.append("playlist durations disagree with the media")

    # 5 -- PDT tracks real wall-clock. THE critical one.
    first_pdt = timed[0][0]
    span_pdt = (timed[-1][0] - first_pdt).total_seconds() + (timed[-1][1] or want_seg)
    age = (now - first_pdt).total_seconds()
    drift = age - span_pdt
    ok = abs(drift) < 5.0
    print(f"{'PASS' if ok else 'FAIL'}  PDT tracks wall-clock "
          f"(stream spans {span_pdt:.1f}s, wall-clock {age:.1f}s, drift {drift:+.1f}s)")
    if not ok:
        fails.append(f"PDT drifts {drift:+.1f}s from wall-clock - rebase would be wrong by this much")

    # 5b -- ONGOING RATE ERROR, measured against segment write times.
    #
    # Re-anchoring the check above on a later segment does NOT work: the anchor
    # PDT cancels algebraically and you get the live-edge lag back, not a rate.
    # A real rate measurement needs an independent wall-clock witness per
    # segment, and the file's mtime is one -- it is when ffmpeg closed it.
    #
    # For each closed segment: lag_k = mtime_k - (PDT_k + EXTINF_k). A constant
    # lag means the timeline is sound and merely offset by pipeline latency. A
    # lag that TRENDS is the dangerous case, because it grows without bound and
    # a 2h game ends far from where it started.
    lags = []
    for pdt, inf, fn in timed[:-1]:                # last one is still being written
        try:
            mt = datetime.fromtimestamp(os.path.getmtime(os.path.join(d, fn)), timezone.utc)
        except OSError:
            continue
        lags.append((mt - pdt).total_seconds() - (inf or want_seg))
    if len(lags) >= 6:
        head = sum(lags[:3]) / 3
        tail = sum(lags[-3:]) / 3
        span = (timed[-2][0] - timed[0][0]).total_seconds() or 1.0
        trend = tail - head
        rate_err = trend / span * 100
        per_2h = trend / span * 7200
        ok = abs(rate_err) < 0.5
        print(f"{'PASS' if ok else 'FAIL'}  ongoing rate error {rate_err:+.3f}% "
              f"(segment lag {head:+.2f}s -> {tail:+.2f}s over {span:.0f}s "
              f"= {per_2h:+.0f}s of error across a 2h game)")
        if not ok:
            fails.append(f"PDT rate error {rate_err:+.3f}% -> {per_2h:+.0f}s over a 2h game")
        print(f"INFO  constant pipeline offset: {head:+.2f}s "
              f"(camera -> encode -> segment close; subtract it at the anchor, do not "
              f"bake it in as a correction)")

    # 6 -- live-edge lag
    lag = (now - (timed[-1][0])).total_seconds() - (timed[-1][1] or want_seg)
    print(f"INFO  live-edge lag at the origin: {lag:.1f}s "
          f"(+ player prebuffer ~{3 * want_seg:.0f}s = ~{lag + 3 * want_seg:.0f}s glass-to-glass)")

    print()
    if fails:
        print("RESULT: FAIL")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("RESULT: PASS - playlist is sound and the PDT anchor is trustworthy")
    return 0


if __name__ == "__main__":
    sys.exit(main())

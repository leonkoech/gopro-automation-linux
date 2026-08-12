"""Deterministic scorer for the 28-shot GT benchmark result files (no more hand tallies).
Reads 'GT <cam> <ts> <gt_type> :: RESULT ... ZONE_OLD=X ... ZONE_NEW=Y' lines.
Usage: python3 score.py file1.txt [file2.txt ...]"""
import re
import sys

for path in sys.argv[1:]:
    rows = []
    for line in open(path):
        m = re.match(r"GT (FL|FR) ([0-9.]+) (\dPT)", line)
        if not m:
            continue
        zo = re.search(r"ZONE_OLD=(\w+)", line)
        zn = re.search(r"ZONE_NEW=(\w+)", line)
        rows.append((m.group(1), float(m.group(2)), m.group(3),
                     zo.group(1) if zo else None, zn.group(1) if zn else None))
    n = len(rows)
    ok_old = sum(1 for r in rows if r[3] == r[2])
    ok_new = sum(1 for r in rows if r[4] == r[2])
    fails = [(r[0], r[1], r[2], r[4]) for r in rows if r[4] != r[2]]
    print(f"{path}: n={n}  OLD {ok_old}/{n} ({100*ok_old/max(1,n):.0f}%)  "
          f"NEW {ok_new}/{n} ({100*ok_new/max(1,n):.0f}%)")
    print("  NEW failures:", " ".join(f"{c}:{t}({g}->{p})" for c, t, g, p in fails))

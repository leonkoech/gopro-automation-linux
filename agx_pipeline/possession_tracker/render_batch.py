"""Render the next batch of whole-game review videos, in game-time order, with a caption per
shot: GT type | production | tracker (median feet). Usage: render_batch.py <game> <batch_no> [size]"""
import json
import os
import subprocess
import sys

game, b = sys.argv[1], int(sys.argv[2])
size = int(sys.argv[3]) if len(sys.argv) > 3 else 10
out, cdir = {"cb9e1294": ("out", "cb9e1294"), "7cef734e": ("out7", "7cef734e_sync")}[game]
ver = os.environ.get("VER", "v7")
ev = json.load(open("%s/eval_%s_%s.json" % (out, game, ver)))
rows = {r["clip"]: r for r in ev["rows"]}
clips = [f[:-4] for f in os.listdir(out + "/cache") if f.endswith(".npz") and not f.startswith("L_")]
clips.sort(key=lambda c: float(c.rsplit("_", 1)[1]))
pick = clips[(b - 1) * size:b * size]
caps = {}
for c in pick:
    r = rows.get(c)
    caps[c] = ("GT %s %s | prod %s | tracker %s" % (r["gt"], r["result"], r["prod"], r.get("track_med"))
               if r else "no GT card paired (dead ball / not annotated)")
dest = "%s/review_%s/batch_%02d" % (out, ver, b)
os.makedirs(dest, exist_ok=True)
json.dump(caps, open(dest + "/captions.json", "w"), indent=1)
env = dict(os.environ, HOLDER_VER="review_%s/batch_%02d" % (ver, b), CAPTIONS=dest + "/captions.json")
subprocess.run(["python3", "holder.py", out, "/home/dev/confirm_test/clips/" + cdir] + pick, env=env,
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
with open(dest + "/INDEX.md", "w") as f:
    f.write("# %s batch %02d (game-time order)\n\n| clip | verdicts |\n|---|---|\n" % (game, b))
    for c in pick:
        f.write("| %s | %s |\n" % (c, caps[c]))
print(dest, len(pick), "clips;", len(clips), "cached so far")

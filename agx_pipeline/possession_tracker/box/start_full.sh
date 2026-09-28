# builds the GT-paired-first queue for both games and starts ONE worker + the game guard
set -a; . /home/dev/gopro-automation-linux/.env.agx; set +a; . /home/dev/sam3env.sh
cd /home/dev/confirm_test
python3 /home/dev/possession/eval_full.py cb9e1294 types_cb9e1294_med_prod.json cb9e1294-e0ca-4f1b-ac2f-7d890a6a36b6 /home/dev/possession/out v5 /home/dev/typing_eval/fit_cb9e1294 > /dev/null 2>&1
python3 /home/dev/possession/eval_full.py 7cef734e types_7cef734e_sync_med_prod.json 7cef734e-07ed-4ad0-95c8-612308b8d7cf /home/dev/possession/out7 v5 /home/dev/typing_eval/fit_7cef734e > /dev/null 2>&1
cd /home/dev/possession
python3 - <<"PY"
import json, os
q = []
for out, game, cdir in (("out", "cb9e1294", "cb9e1294"), ("out7", "7cef734e", "7cef734e_sync")):
    rows = json.load(open("%s/eval_%s_v5.json" % (out, game)))["rows"]
    paired = [r["clip"] for r in rows]
    allc = sorted(f[:-4] for f in os.listdir("/home/dev/confirm_test/clips/" + cdir) if f.endswith(".mp4"))
    fmt = lambda c: "%s /home/dev/confirm_test/clips/%s/%s.mp4" % (out, cdir, c)
    todo_p = [fmt(c) for c in paired if not os.path.exists("%s/cache/%s.npz" % (out, c))]
    todo_r = [fmt(c) for c in allc if c not in paired and not os.path.exists("%s/cache/%s.npz" % (out, c))]
    q.append((todo_p, todo_r))
    print(game, "paired", len(paired), "paired todo", len(todo_p), "rest todo", len(todo_r))
(a1, a2), (b1, b2) = q
inter = [x for pr in zip(a1, b1) for x in pr] + a1[len(b1):] + b1[len(a1):]
open("full_queue.txt", "w").write("\n".join(inter + a2 + b2) + "\n")
print("queue", len(inter + a2 + b2))
PY
setsid nohup bash worker.sh 0 1 > worker0.log 2>&1 < /dev/null &
sleep 2
setsid nohup bash gameguard.sh > gameguard.log 2>&1 < /dev/null &

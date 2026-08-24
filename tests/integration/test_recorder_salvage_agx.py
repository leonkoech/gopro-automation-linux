"""T3/salvage: record a NORMAL source (~12s, timeline advancing, NO false restart),
then drop the feed. The watchdog must finalize the pre-drop segment (clean moov) and
the concatenated master must still be PLAYABLE (~12s) — footage before the failure is
NOT lost. This is the client-facing promise the byte-growth watchdog broke."""
import os, sys, time, json, subprocess, logging

os.environ["REC_ENGINE"] = "ffmpeg"
os.environ["REC_FFMPEG_STALL_SEC"] = "8"
os.environ["REC_FFMPEG_POLL_SEC"] = "2"
os.environ["REC_FFMPEG_STIMEOUT_US"] = "5000000"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
sys.path.insert(0, "/tmp/rectest")
import recording

BASE = "/tmp/rectest"
cfg_d = {"location": "test", "jetson_id": "test", "rtsp_port": 8554, "rtsp_path": "/normal",
         "docker_image": "x", "app_mount": BASE, "output_dir": f"{BASE}/rec",
         "cameras": [{"id": "T", "ip": "127.0.0.1", "angle": "FL"}], "docker_cmd": "docker"}
json.dump(cfg_d, open(f"{BASE}/cfg_salv.json", "w"))
cfg = recording.load_config(f"{BASE}/cfg_salv.json")

subprocess.run("pkill -f mediamtx; pkill -f testsrc", shell=True)
time.sleep(1)
mtx = subprocess.Popen([f"{BASE}/mediamtx"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)
# NORMAL publisher (real advancing timeline, no setpts)
pub = subprocess.Popen(
    ["ffmpeg", "-hide_banner", "-loglevel", "error", "-re", "-f", "lavfi",
     "-i", "testsrc=size=320x240:rate=30:duration=600", "-an",
     "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "30", "-f", "rtsp",
     "rtsp://127.0.0.1:8554/normal"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(4)

ctl = recording.RecordingController(cfg)
ctl.start("salv", ["T"])
run = ctl._sessions["salv"]["runs"][0]
trace = []
# Phase A: 12s of NORMAL recording — out_time must advance, restart must stay 0
for i in range(6):
    time.sleep(2)
    trace.append(("A", i * 2, round(run["out_time"], 2), run["restart"]))
adv = run["out_time"]
false_restart = run["restart"]
# drop the feed
pub.kill()
subprocess.run("pkill -f testsrc", shell=True)
# Phase B: ~18s — watchdog must detect the drop, finalize (save) the 12s segment, restart
for i in range(9):
    time.sleep(2)
    trace.append(("B", i * 2, round(run["out_time"], 2), run["restart"]))
res = ctl.stop("salv")

for p in (mtx,):
    try:
        p.kill()
    except OSError:
        pass
subprocess.run("pkill -f mediamtx; pkill -f testsrc", shell=True)

files = [(f["angle"], f.get("duration"), f.get("ok"), f.get("size")) for f in res["files"]]
adv_ok = adv > 5.0 and false_restart == 0          # healthy source recorded, no false restart
restarted = run["restart"] >= 1                     # drop was caught
master_ok = any(f.get("ok") and (f.get("duration") or 0) > 5.0 for f in res["files"])  # salvaged+playable
with open(f"{BASE}/salvage_verdict.txt", "w") as f:
    f.write("phase t out_time restarts\n")
    for row in trace:
        f.write("  ".join(str(x) for x in row) + "\n")
    f.write(f"\nsegments: {run['segments']}\n")
    f.write(f"files (angle,dur,ok,size): {files}\n")
    f.write(f"VERDICT normal advanced past 5s w/ NO false restart: {adv_ok} (out_time={adv:.1f}, restart={false_restart})\n")
    f.write(f"VERDICT drop was caught (restart>=1): {restarted}\n")
    f.write(f"VERDICT master salvaged + PLAYABLE (>5s, ok): {master_ok}\n")
    f.write(f"RESULT: {'PASS' if (adv_ok and restarted and master_ok) else 'CHECK'}\n")
print("done")

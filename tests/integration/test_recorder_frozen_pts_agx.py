"""T2 integration: real frozen-PTS RTSP source -> the RecordingController's ffmpeg
watchdog must catch the frozen timeline and restart (the byte-growth watchdog would not).
Self-contained: mediamtx server + a setpts=0 publisher (frozen timeline) + the real
controller. Writes a verdict; cleans up. Run detached on the AGX."""
import os, sys, time, json, subprocess, logging

os.environ["REC_ENGINE"] = "ffmpeg"
os.environ["REC_FFMPEG_STALL_SEC"] = "10"     # shorter stall for the test
os.environ["REC_FFMPEG_POLL_SEC"] = "2"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
sys.path.insert(0, "/tmp/rectest")
import recording

BASE = "/tmp/rectest"
cfg_d = {"location": "test", "jetson_id": "test", "rtsp_port": 8554, "rtsp_path": "/frozen",
         "docker_image": "x", "app_mount": BASE, "output_dir": f"{BASE}/rec",
         "cameras": [{"id": "T", "ip": "127.0.0.1", "angle": "FL"}], "docker_cmd": "docker"}
json.dump(cfg_d, open(f"{BASE}/cfg_test.json", "w"))
cfg = recording.load_config(f"{BASE}/cfg_test.json")

subprocess.run("pkill -f mediamtx; pkill -f testsrc", shell=True)
time.sleep(1)
mtx = subprocess.Popen([f"{BASE}/mediamtx"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)
# FROZEN-PTS publisher: -re feeds frames at 30fps (bytes flow) but setpts=0 freezes the timeline
pub = subprocess.Popen(
    ["ffmpeg", "-hide_banner", "-loglevel", "error", "-re", "-f", "lavfi",
     "-i", "testsrc=size=320x240:rate=30:duration=180", "-vf", "setpts=0", "-an",
     "-c:v", "libx264", "-pix_fmt", "yuv420p", "-f", "rtsp", "rtsp://127.0.0.1:8554/frozen"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(4)

ctl = recording.RecordingController(cfg)
ctl.start("frztest", ["T"])
run = ctl._sessions["frztest"]["runs"][0]
trace = []
for i in range(15):                       # ~30s
    time.sleep(2)
    alive = bool(run.get("proc") and run["proc"].poll() is None)
    trace.append((i * 2, round(run["out_time"], 2), run["restart"],
                  round(time.time() - run["last_advance"], 0), alive))
res = ctl.stop("frztest")

for p in (pub, mtx):
    try:
        p.kill()
    except OSError:
        pass
subprocess.run("pkill -f mediamtx; pkill -f testsrc", shell=True)

restarted = run["restart"] >= 1
froze = run["out_time"] < 3.0
files = [(f["angle"], f.get("duration"), f.get("ok"), f.get("size")) for f in res["files"]]
with open(f"{BASE}/frozen_verdict.txt", "w") as f:
    f.write("t out_time restarts idle alive\n")
    for row in trace:
        f.write("  ".join(str(x) for x in row) + "\n")
    f.write(f"\nfiles: {files}\n")
    f.write(f"VERDICT restart>=1 (caught frozen timeline): {restarted}\n")
    f.write(f"VERDICT out_time stayed frozen (<3s): {froze}\n")
    f.write(f"RESULT: {'PASS' if (restarted and froze) else 'CHECK'}\n")
print("done")

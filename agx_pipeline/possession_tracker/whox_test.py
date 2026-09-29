"""End-to-end test of the live extended-timeline WHO (who_extend_live) on 3 annotated shots, using
a fake highlight buffer built from a recorded game in the recorder's own format (4 s H.265
segments named seg_<epoch>_<CAM>.mp4). Picks shots where the clip alone gave no number and the
offline extension found the right one, and checks the live path reproduces that.
Run on the box in /home/dev/possession with the CUDA env, jersey stack and .env.agx loaded."""
import json
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, "/home/dev/possession")
sys.path.insert(0, "/home/dev/gopro-automation-linux")
os.environ["HIGHLIGHT_BUF_DIR"] = "/home/dev/possession/testbuf"

import queue_worker as Q  # noqa: E402
import who_eval as W  # noqa: E402

Q.WHO_EXTEND = True
BASE = 1800000000


def main():
    ext = [json.loads(l) for l in open("/home/dev/possession/who_extend.jsonl")]
    clipj = {(j["game"], j["t"]): j for j in map(json.loads, open("/home/dev/possession/who_reads.jsonl"))}
    picks = [e for e in ext if e.get("extended_s") and W.speak(e["reads"], None) == e["gt"]
             and "reads" in clipj.get((e["game"], e["t"]), {})
             and W.speak(clipj[(e["game"], e["t"])]["reads"], None) is None][:3]
    for e in picks:
        cam = clipj[(e["game"], e["t"])]["cam"]
        side = "left" if cam == "FL" else "right"
        label = "test_" + e["game"][:8]
        src = "/home/dev/validate/%s/%s.mp4" % (e["game"], cam)
        bdir = os.path.join(os.environ["HIGHLIGHT_BUF_DIR"], label, side)
        shutil.rmtree(bdir, ignore_errors=True)
        os.makedirs(bdir)
        t0 = int(e["t"]) - 28
        for k in range(10):
            ts = t0 + 4 * k
            subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-y", "-ss", str(ts), "-i", src, "-t", "4",
                            "-c:v", "libx265", "-preset", "ultrafast", "-an",
                            os.path.join(bdir, "seg_%d_%s.mp4" % (BASE + ts, cam))], check=True)
        clipdir = "/home/dev/possession/testclips/" + label
        os.makedirs(clipdir, exist_ok=True)
        epoch = BASE + e["t"]
        log_id = "cv_%d_%s" % (int(epoch), side)
        clip = os.path.join(clipdir, "%s_%s.mp4" % (log_id, cam))
        shutil.copyfile("/home/dev/validate/%s/clips/%s_t%07.1f.mp4" % (e["game"], cam, e["t"]), clip)
        job = {"game_id": "TEST", "log_id": log_id, "angle": cam, "clip": clip,
               "pre": 5.0 + (epoch - int(epoch))}
        with tempfile.TemporaryDirectory() as work:
            name, staged = Q.stage_clip(job, work)
            S, res, shot, zone, feet = Q.track(staged, cam, name, work, job["pre"], "fast")
            job["clip"] = clip
            num, top = Q.read_who(staged, res, shot, job) if shot else (None, None)
        print("[whox] %s t=%.1f truth #%s | clip-only: none | live extended: #%s (extra reads %s, error %s)" % (
            e["game"][:8], e["t"], e["gt"], num, job.get("who_extended_reads"), job.get("who_extend_error")),
            flush=True)


if __name__ == "__main__":
    main()

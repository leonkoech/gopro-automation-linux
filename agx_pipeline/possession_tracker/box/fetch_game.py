"""Download a game's annotation-tool FL/FR videos (read-only) to /home/dev/validate/<uuid>/.
These are the videos annotators watch, so manual card times are on the same clock."""
import os
import sys
import time

import boto3

bucket = [l.split("=", 1)[1].strip() for l in open("/home/dev/gopro-automation-linux/.env.agx")
          if l.startswith("UPLOAD_BUCKET=")][0]
s3 = boto3.client("s3")
for g in sys.argv[1:]:
    d = "/home/dev/validate/" + g
    os.makedirs(d, exist_ok=True)
    for o in s3.list_objects_v2(Bucket=bucket, Prefix="videos/%s/" % g).get("Contents", []):
        k = o["Key"]
        cam = "FL" if k.endswith("_FL.mp4") else "FR" if k.endswith("_FR.mp4") else None
        if not cam or os.path.exists("%s/%s.mp4" % (d, cam)):
            continue
        t0 = time.time()
        s3.download_file(bucket, k, "%s/%s.part" % (d, cam))
        os.rename("%s/%s.part" % (d, cam), "%s/%s.mp4" % (d, cam))
        print(g[:8], cam, "%.1f GB in %.0fs" % (o["Size"] / 1e9, time.time() - t0), flush=True)
print("FETCH_DONE", flush=True)

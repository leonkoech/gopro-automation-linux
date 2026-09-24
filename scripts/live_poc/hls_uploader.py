#!/usr/bin/env python3
"""Ship a live HLS directory to S3 as it is written — PROOF OF CONCEPT.

Watches the publisher's output dir and uploads each CLOSED segment, then
rewrites the playlist. Order matters: a playlist that references a segment S3
does not have yet makes the player 404 and stall, so segments always go first.

Content types are set explicitly. Browsers and hls.js are tolerant, but
CloudFront caching is not: a .m3u8 served as binary/octet-stream with a long TTL
is the classic "the stream is frozen for everyone but me" bug. The playlist is
uploaded with no-cache; segments are immutable and cached hard.

STANDALONE: no agx_pipeline imports, writes only under its own S3 prefix.

Usage:
  ./hls_uploader.py /home/dev/live_poc_wc/FL live-poc/<session>/FL [--once]
"""
from __future__ import annotations

import os
import sys
import time

import boto3

BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")
REGION = os.getenv("UPLOAD_REGION", "us-east-1")
CDN = os.getenv("HIGHLIGHT_CDN_DOMAIN", "d22gul8sdref0l.cloudfront.net")
POLL = float(os.getenv("UPLOAD_POLL_SEC", "1.0"))


def main() -> int:
    src = sys.argv[1].rstrip("/")
    prefix = sys.argv[2].strip("/")
    once = "--once" in sys.argv

    s3 = boto3.client("s3", region_name=REGION)
    playlist = os.path.join(src, "live.m3u8")
    sent: set[str] = set()
    n_seg = 0

    print(f"[uploader] {src} -> s3://{BUCKET}/{prefix}/")
    print(f"[uploader] playlist will be https://{CDN}/{prefix}/live.m3u8")

    while True:
        try:
            names = sorted(f for f in os.listdir(src) if f.endswith(".ts"))
        except FileNotFoundError:
            time.sleep(POLL)
            continue

        # The newest .ts is normally still being written by ffmpeg, and uploading
        # a partial segment yields a file the player can fetch but not decode.
        # BUT once the publisher has stopped, ffmpeg has written EXT-X-ENDLIST
        # and nothing is open any more -- then the newest file is complete and
        # MUST go up, or the last segment of every game is missing from S3 and
        # the player 404s on the final seconds. (Measured: 29 of 30 segments
        # reached S3 before this was fixed.)
        finished = False
        try:
            with open(playlist) as fh:
                finished = "#EXT-X-ENDLIST" in fh.read()
        except OSError:
            pass

        for fn in (names if finished else names[:-1]):
            if fn in sent:
                continue
            path = os.path.join(src, fn)
            try:
                s3.upload_file(path, BUCKET, f"{prefix}/{fn}",
                               ExtraArgs={"ContentType": "video/mp2t",
                                          "CacheControl": "public, max-age=31536000, immutable"})
            except Exception as e:  # noqa: BLE001
                print(f"[uploader] segment {fn} failed: {e}", flush=True)
                continue
            sent.add(fn)
            n_seg += 1

        # Playlist last, so it never advertises a segment that is not up yet.
        if os.path.exists(playlist):
            try:
                s3.upload_file(playlist, BUCKET, f"{prefix}/live.m3u8",
                               ExtraArgs={"ContentType": "application/vnd.apple.mpegurl",
                                          "CacheControl": "no-cache, max-age=0"})
            except Exception as e:  # noqa: BLE001
                print(f"[uploader] playlist failed: {e}", flush=True)

        if n_seg and n_seg % 5 == 0:
            print(f"[uploader] {n_seg} segments up", flush=True)
        if once:
            print(f"[uploader] one pass done, {n_seg} segments"
                  + (" (stream finished, last segment included)" if finished else ""))
            return 0
        time.sleep(POLL)


if __name__ == "__main__":
    sys.exit(main())

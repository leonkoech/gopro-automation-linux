#!/usr/bin/env python3
"""
Simple script to upload all segment sessions to S3 using curl.
This version uses curl with retry logic which is more reliable
on Jetson/ARM devices that have SSL issues with boto3 and aws cli.

Run with: python upload_curl.py
Optionally pass --delete to delete segments after successful upload.

Prerequisites:
    sudo apt install curl jq
"""

import os
import sys
import subprocess
import hashlib
import hmac
import datetime
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Configuration from environment
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
UPLOAD_BUCKET = os.getenv("UPLOAD_BUCKET", "jetson-videos-uai")
UPLOAD_LOCATION = os.getenv("UPLOAD_LOCATION", "default-location")
UPLOAD_DEVICE_NAME = os.getenv("UPLOAD_DEVICE_NAME", os.uname().nodename)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Segments directory
SEGMENTS_DIR = os.path.expanduser("~/gopro_videos/segments")


def get_sessions():
    """Get all segment sessions with files."""
    segments_path = Path(SEGMENTS_DIR)
    if not segments_path.exists():
        print(f"Segments directory not found: {SEGMENTS_DIR}")
        return []

    sessions = []
    for session_dir in sorted(segments_path.iterdir()):
        if session_dir.is_dir():
            video_files = list(session_dir.glob("*.mp4")) + list(session_dir.glob("*.MP4"))
            if video_files:
                total_size = sum(f.stat().st_size for f in video_files)
                sessions.append({
                    'name': session_dir.name,
                    'path': str(session_dir),
                    'files': video_files,
                    'size_gb': total_size / (1024**3)
                })
    return sessions


def parse_session_date(session_name):
    """Extract date from session name (format: interfaceId_YYYYMMDD_HHMMSS)."""
    parts = session_name.split('_')
    if len(parts) >= 2:
        date_str = parts[-2]  # YYYYMMDD
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return None


def extract_camera_name(session_name):
    """Extract camera identifier from session name."""
    parts = session_name.split('_')
    if parts:
        interface_id = parts[0]
        if len(interface_id) >= 4:
            return f"GoPro-{interface_id[-4:]}"
    return "Unknown"


def upload_with_curl(local_path, s3_key, max_retries=5):
    """
    Upload a file to S3 using curl with retry logic.
    Uses simple PUT request which is more reliable than multipart.
    """
    s3_url = f"https://{UPLOAD_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{quote(s3_key, safe='/')}"

    # Get file info
    file_size = os.path.getsize(local_path)
    content_type = "video/mp4"

    # Current time for signing
    now = datetime.datetime.utcnow()
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')

    # Create canonical request components
    method = "PUT"
    canonical_uri = "/" + quote(s3_key, safe='/')
    canonical_querystring = ""

    # Calculate content hash (for large files, use UNSIGNED-PAYLOAD)
    if file_size > 100 * 1024 * 1024:  # > 100MB
        payload_hash = "UNSIGNED-PAYLOAD"
    else:
        # For smaller files, calculate actual hash
        sha256 = hashlib.sha256()
        with open(local_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        payload_hash = sha256.hexdigest()

    # Headers to sign
    host = f"{UPLOAD_BUCKET}.s3.{AWS_REGION}.amazonaws.com"
    signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
    canonical_headers = f"content-type:{content_type}\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"

    # Create canonical request
    canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

    # Create string to sign
    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{AWS_REGION}/s3/aws4_request"
    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{canonical_request_hash}"

    # Calculate signature
    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    k_date = sign(('AWS4' + AWS_SECRET_KEY).encode('utf-8'), date_stamp)
    k_region = sign(k_date, AWS_REGION)
    k_service = sign(k_region, 's3')
    k_signing = sign(k_service, 'aws4_request')
    signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    # Create authorization header
    authorization = f"{algorithm} Credential={AWS_ACCESS_KEY}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

    # Build curl command with retries
    for attempt in range(max_retries):
        cmd = [
            "curl",
            "-X", "PUT",
            "-T", str(local_path),
            "-H", f"Content-Type: {content_type}",
            "-H", f"x-amz-date: {amz_date}",
            "-H", f"x-amz-content-sha256: {payload_hash}",
            "-H", f"Authorization: {authorization}",
            "--retry", "3",
            "--retry-delay", "5",
            "--retry-max-time", "300",
            "--connect-timeout", "30",
            "--max-time", "0",  # No timeout for upload
            "-s", "-S",  # Silent but show errors
            "-w", "%{http_code}",
            "-o", "/dev/null",
            s3_url
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            http_code = result.stdout.strip()

            if http_code == "200":
                return f"s3://{UPLOAD_BUCKET}/{s3_key}"
            elif http_code in ["500", "503"]:
                # Retry on server errors
                print(f"    Retry {attempt + 1}/{max_retries} (HTTP {http_code})...")
                continue
            else:
                if result.stderr:
                    raise Exception(f"HTTP {http_code}: {result.stderr}")
                else:
                    raise Exception(f"HTTP {http_code}")

        except subprocess.TimeoutExpired:
            print(f"    Retry {attempt + 1}/{max_retries} (timeout)...")
            continue
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"    Retry {attempt + 1}/{max_retries}: {e}")
                continue
            raise

    raise Exception(f"Failed after {max_retries} attempts")


def upload_session(session, delete_after=False):
    """Upload a single session to S3."""
    session_name = session['name']
    session_path = session['path']
    video_files = sorted(session['files'], key=lambda f: f.name)

    upload_date = parse_session_date(session_name)
    if not upload_date:
        print(f"  Could not parse date from session name, skipping")
        return False

    camera_name = extract_camera_name(session_name)

    print(f"  Date: {upload_date}, Camera: {camera_name}")
    print(f"  Files: {len(video_files)}, Size: {session['size_gb']:.2f} GB")

    uploaded = []
    for idx, video_file in enumerate(video_files):
        if len(video_files) > 1:
            file_camera_name = f"{camera_name}_ch{idx+1:02d}"
        else:
            file_camera_name = camera_name

        # Build S3 key
        filename = f"{UPLOAD_DEVICE_NAME} - {file_camera_name}.mp4"
        s3_key = f"{UPLOAD_LOCATION}/{upload_date}/{filename}"

        file_size_mb = video_file.stat().st_size / (1024 * 1024)
        print(f"  Uploading {idx+1}/{len(video_files)}: {video_file.name} ({file_size_mb:.1f} MB)...")

        try:
            s3_uri = upload_with_curl(video_file, s3_key)
            print(f"    -> {s3_uri}")
            uploaded.append(video_file)
        except Exception as e:
            print(f"    ERROR: {e}")

    if delete_after and len(uploaded) == len(video_files):
        import shutil
        print(f"  Deleting session folder...")
        shutil.rmtree(session_path)
        print(f"  Deleted.")

    return len(uploaded) == len(video_files)


def main():
    delete_after = "--delete" in sys.argv

    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("ERROR: AWS credentials not found in .env file")
        print("Required: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

    print("=" * 60)
    print("GoPro Segment Uploader (curl version)")
    print("=" * 60)
    print(f"Bucket: {UPLOAD_BUCKET}")
    print(f"Location: {UPLOAD_LOCATION}")
    print(f"Device: {UPLOAD_DEVICE_NAME}")
    print(f"Delete after upload: {delete_after}")
    print()

    sessions = get_sessions()
    if not sessions:
        print("No sessions with video files found.")
        sys.exit(0)

    print(f"Found {len(sessions)} sessions with video files:")
    total_size = sum(s['size_gb'] for s in sessions)
    for s in sessions:
        print(f"  - {s['name']}: {len(s['files'])} files, {s['size_gb']:.2f} GB")
    print(f"Total: {total_size:.2f} GB")
    print()

    response = input("Proceed with upload? [y/N] ").strip().lower()
    if response != 'y':
        print("Aborted.")
        sys.exit(0)

    print()

    success_count = 0
    for i, session in enumerate(sessions):
        print()
        print(f"[{i+1}/{len(sessions)}] {session['name']}")
        if upload_session(session, delete_after):
            success_count += 1

    print()
    print("=" * 60)
    print(f"Upload complete: {success_count}/{len(sessions)} sessions uploaded")
    print("=" * 60)


if __name__ == "__main__":
    main()

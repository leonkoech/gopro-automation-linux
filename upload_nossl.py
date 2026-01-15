#!/usr/bin/env python3
"""
Simple script to upload all segment sessions to S3.
This version disables SSL verification to work around OpenSSL issues on Jetson.

WARNING: This disables SSL certificate verification. Only use on trusted networks.

Run with: python upload_nossl.py
Optionally pass --delete to delete segments after successful upload.
"""

import os
import sys
import ssl
import urllib3
from pathlib import Path
from dotenv import load_dotenv

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load .env file
load_dotenv()

import boto3
from botocore.config import Config as BotoConfig

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


class ProgressCallback:
    """Callback class to track upload progress."""

    def __init__(self, total_size: int, filename: str):
        self.total_size = total_size
        self.uploaded = 0
        self.last_percentage = 0
        self.filename = filename

    def __call__(self, bytes_amount: int):
        self.uploaded += bytes_amount
        percentage = int((self.uploaded / self.total_size) * 100)

        if percentage >= self.last_percentage + 10:
            self.last_percentage = percentage
            print(f"    {percentage}%...")


def create_s3_client():
    """Create S3 client with SSL verification disabled."""
    boto_config = BotoConfig(
        retries={
            'max_attempts': 10,
            'mode': 'adaptive'
        },
        connect_timeout=60,
        read_timeout=300,
        max_pool_connections=5
    )

    # Create client with SSL verification disabled
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=boto_config,
        verify=False  # Disable SSL verification
    )

    return s3_client


def upload_file(s3_client, local_path, s3_key):
    """Upload a file to S3."""
    file_size = os.path.getsize(local_path)
    filename = os.path.basename(local_path)

    # Use put_object for smaller files (simpler, more reliable)
    if file_size < 100 * 1024 * 1024:  # < 100MB
        with open(local_path, 'rb') as f:
            s3_client.put_object(
                Bucket=UPLOAD_BUCKET,
                Key=s3_key,
                Body=f,
                ContentType='video/mp4'
            )
    else:
        # For larger files, use upload_file with simple config
        from boto3.s3.transfer import TransferConfig
        transfer_config = TransferConfig(
            multipart_threshold=100 * 1024 * 1024,  # 100MB
            max_concurrency=1,
            multipart_chunksize=100 * 1024 * 1024,  # 100MB chunks
            use_threads=False
        )

        callback = ProgressCallback(file_size, filename)
        s3_client.upload_file(
            local_path,
            UPLOAD_BUCKET,
            s3_key,
            Callback=callback,
            ExtraArgs={'ContentType': 'video/mp4'},
            Config=transfer_config
        )

    return f"s3://{UPLOAD_BUCKET}/{s3_key}"


def upload_session(s3_client, session, delete_after=False):
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
            s3_uri = upload_file(s3_client, str(video_file), s3_key)
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
    print("GoPro Segment Uploader (SSL disabled)")
    print("=" * 60)
    print("WARNING: SSL verification is disabled!")
    print()
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
    print("Creating S3 client...")
    s3_client = create_s3_client()

    success_count = 0
    for i, session in enumerate(sessions):
        print()
        print(f"[{i+1}/{len(sessions)}] {session['name']}")
        if upload_session(s3_client, session, delete_after):
            success_count += 1

    print()
    print("=" * 60)
    print(f"Upload complete: {success_count}/{len(sessions)} sessions uploaded")
    print("=" * 60)


if __name__ == "__main__":
    main()

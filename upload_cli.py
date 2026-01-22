#!/usr/bin/env python3
"""
Simple script to upload all segment sessions to S3 using AWS CLI.
This version uses subprocess calls to aws s3 cp which is more reliable
on Jetson/ARM devices that have SSL issues with boto3.

Run with: python upload_cli.py
Optionally pass --delete to delete segments after successful upload.

Prerequisites:
    sudo apt install awscli
    aws configure  (or use environment variables)
"""

import os
import sys
import subprocess
from pathlib import Path
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
        # Use last 4 chars of interface ID as camera name
        if len(interface_id) >= 4:
            return f"GoPro-{interface_id[-4:]}"
    return "Unknown"


def upload_file_with_cli(local_path, s3_key):
    """Upload a file using AWS CLI."""
    s3_uri = f"s3://{UPLOAD_BUCKET}/{s3_key}"

    # Build the aws s3 cp command
    cmd = [
        "aws", "s3", "cp",
        str(local_path),
        s3_uri,
        "--region", AWS_REGION,
        "--content-type", "video/mp4"
    ]

    # Set environment variables for credentials
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY
    env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_KEY

    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            return s3_uri
        else:
            raise Exception(f"aws s3 cp failed: {result.stderr}")

    except FileNotFoundError:
        raise Exception("AWS CLI not found. Install with: sudo apt install awscli")


def upload_session(session, delete_after=False):
    """Upload a single session to S3."""
    session_name = session['name']
    session_path = session['path']
    video_files = sorted(session['files'], key=lambda f: f.name)

    # Parse metadata
    upload_date = parse_session_date(session_name)
    if not upload_date:
        print(f"  Could not parse date from session name, skipping")
        return False

    camera_name = extract_camera_name(session_name)

    print(f"  Date: {upload_date}, Camera: {camera_name}")
    print(f"  Files: {len(video_files)}, Size: {session['size_gb']:.2f} GB")

    uploaded = []
    for idx, video_file in enumerate(video_files):
        # For multiple files, add chapter number
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
            s3_uri = upload_file_with_cli(video_file, s3_key)
            print(f"    -> {s3_uri}")
            uploaded.append(video_file)
        except Exception as e:
            print(f"    ERROR: {e}")

    # Delete if requested and all files uploaded
    if delete_after and len(uploaded) == len(video_files):
        import shutil
        print(f"  Deleting session folder...")
        shutil.rmtree(session_path)
        print(f"  Deleted.")

    return len(uploaded) == len(video_files)


def check_aws_cli():
    """Check if AWS CLI is installed."""
    try:
        result = subprocess.run(
            ["aws", "--version"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def main():
    # Check for --delete flag
    delete_after = "--delete" in sys.argv

    # Check AWS CLI
    if not check_aws_cli():
        print("ERROR: AWS CLI not found.")
        print("Install with: sudo apt install awscli")
        sys.exit(1)

    # Validate credentials
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("ERROR: AWS credentials not found in .env file")
        print("Required: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

    print("=" * 60)
    print("GoPro Segment Uploader (AWS CLI version)")
    print("=" * 60)
    print(f"Bucket: {UPLOAD_BUCKET}")
    print(f"Location: {UPLOAD_LOCATION}")
    print(f"Device: {UPLOAD_DEVICE_NAME}")
    print(f"Delete after upload: {delete_after}")
    print()

    # Get sessions
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

    # Confirm
    response = input("Proceed with upload? [y/N] ").strip().lower()
    if response != 'y':
        print("Aborted.")
        sys.exit(0)

    print()

    # Upload each session
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

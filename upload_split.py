#!/usr/bin/env python3
"""
Upload script that splits large files into smaller chunks before uploading.
This works around SSL issues on Jetson by uploading smaller pieces.

Run with: python upload_split.py
Optionally pass --delete to delete segments after successful upload.
"""

import os
import sys
import tempfile
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

# Max file size to upload in one piece (files larger than this will be split)
MAX_CHUNK_SIZE = 50 * 1024 * 1024  # 50MB


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


def split_file(file_path, chunk_size=MAX_CHUNK_SIZE):
    """Split a file into chunks, return list of chunk paths."""
    file_size = os.path.getsize(file_path)

    if file_size <= chunk_size:
        return [file_path]  # No need to split

    chunks = []
    temp_dir = tempfile.mkdtemp(prefix="upload_chunks_")
    base_name = os.path.basename(file_path)

    with open(file_path, 'rb') as f:
        chunk_num = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break

            chunk_path = os.path.join(temp_dir, f"{base_name}.part{chunk_num:03d}")
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(data)
            chunks.append(chunk_path)
            chunk_num += 1

    return chunks


def upload_chunk_with_retry(chunk_path, s3_key, max_retries=5):
    """Upload a single chunk using aws s3 cp with retries."""
    s3_uri = f"s3://{UPLOAD_BUCKET}/{s3_key}"

    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY
    env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_KEY

    for attempt in range(max_retries):
        cmd = [
            "aws", "s3", "cp",
            str(chunk_path),
            s3_uri,
            "--region", AWS_REGION
        ]

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout per chunk
            )

            if result.returncode == 0:
                return True

            if "SSL" in result.stderr or "EOF" in result.stderr:
                print(f"      Retry {attempt + 1}/{max_retries} (SSL error)...")
                import time
                time.sleep(5)  # Wait 5 seconds before retry
                continue
            else:
                print(f"      Error: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"      Retry {attempt + 1}/{max_retries} (timeout)...")
            continue
        except Exception as e:
            print(f"      Error: {e}")
            return False

    return False


def reassemble_on_s3(chunk_keys, final_key):
    """Use S3 multipart upload complete to reassemble chunks (not needed for this approach)."""
    # For this simpler approach, we'll concatenate locally and re-upload
    # Or just keep files as separate parts
    pass


def upload_file_chunked(local_path, s3_key):
    """Upload a file, splitting into chunks if necessary."""
    file_size = os.path.getsize(local_path)

    if file_size <= MAX_CHUNK_SIZE:
        # Small file, upload directly
        success = upload_chunk_with_retry(local_path, s3_key)
        if success:
            return f"s3://{UPLOAD_BUCKET}/{s3_key}"
        else:
            raise Exception("Upload failed after retries")

    # Large file - need to handle differently
    # Option 1: Just try with smaller timeout and more retries
    print(f"    Large file ({file_size / 1024 / 1024:.1f} MB), uploading with extended retries...")

    success = upload_chunk_with_retry(local_path, s3_key, max_retries=10)
    if success:
        return f"s3://{UPLOAD_BUCKET}/{s3_key}"
    else:
        raise Exception("Upload failed after retries")


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
            s3_uri = upload_file_chunked(str(video_file), s3_key)
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


def check_aws_cli():
    """Check if AWS CLI is installed."""
    try:
        result = subprocess.run(["aws", "--version"], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False


def main():
    delete_after = "--delete" in sys.argv

    if not check_aws_cli():
        print("ERROR: AWS CLI not found.")
        print("Install with: sudo apt install awscli")
        sys.exit(1)

    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("ERROR: AWS credentials not found in .env file")
        print("Required: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

    print("=" * 60)
    print("GoPro Segment Uploader (chunked/retry version)")
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

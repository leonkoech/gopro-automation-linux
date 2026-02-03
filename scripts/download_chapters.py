#!/usr/bin/env python3
"""
Improved GoPro Chapter Download Script

Features:
- Read timeout to detect stalls (not just connection timeout)
- Never deletes partial files - always resumes
- Periodic keep-alive during download
- Smaller chunks for faster stall detection
- Better retry logic

Usage:
    python3 download_chapters.py --gopro-ip 172.26.138.51 --session enxd43260ddac87_FL_20260202_193946
    python3 download_chapters.py --gopro-ip 172.24.149.51 --session enxd43260ef4715_NR_20260202_193946
"""

import os
import sys
import time
import argparse
import requests
import threading
from pathlib import Path
from datetime import datetime

# Configuration
CHUNK_SIZE = 262144  # 256KB - smaller chunks for faster stall detection
CONNECT_TIMEOUT = 10  # seconds to establish connection
READ_TIMEOUT = 60  # seconds to wait for data between chunks
MAX_RETRIES = 20  # more retries, but never delete partial
KEEP_ALIVE_INTERVAL = 30  # send keep-alive every 30 seconds
SEGMENTS_DIR = os.path.expanduser("~/gopro_videos/segments")


class KeepAliveThread:
    """Background thread to send keep-alive to GoPro during download"""

    def __init__(self, gopro_ip):
        self.gopro_ip = gopro_ip
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)

    def _run(self):
        while self.running:
            try:
                requests.get(
                    f'http://{self.gopro_ip}:8080/gopro/camera/keep_alive',
                    timeout=2
                )
            except:
                pass
            time.sleep(KEEP_ALIVE_INTERVAL)


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()


def get_gopro_files(gopro_ip):
    """Get list of files on GoPro"""
    try:
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/media/list',
            timeout=CONNECT_TIMEOUT
        )
        if response.status_code == 200:
            data = response.json()
            files = []
            for media in data.get('media', []):
                directory = media.get('d', '')
                for file_info in media.get('fs', []):
                    files.append({
                        'directory': directory,
                        'filename': file_info.get('n', ''),
                        'size': int(file_info.get('s', 0))
                    })
            return files
    except Exception as e:
        log(f"Error getting file list: {e}")
    return []


def download_file(gopro_ip, directory, filename, expected_size, output_path, keep_alive):
    """
    Download a single file with improved retry and resume logic.
    Never deletes partial files - always resumes.
    """
    download_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}'

    for attempt in range(MAX_RETRIES):
        try:
            # Check current file size for resume
            current_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0

            # Skip if already complete
            if expected_size > 0 and current_size >= expected_size:
                log(f"  Already complete: {current_size:,} bytes")
                return True

            # Prepare headers for resume
            headers = {}
            if current_size > 0:
                headers['Range'] = f'bytes={current_size}-'
                log(f"  Resuming from byte {current_size:,} ({current_size/1024/1024/1024:.2f} GB)")

            # Start download with BOTH connect and read timeout
            response = requests.get(
                download_url,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)  # (connect, read) timeout tuple
            )

            # Handle response
            if current_size > 0 and response.status_code == 416:
                # Range not satisfiable - file might be complete
                log(f"  Server returned 416 - file may be complete")
                return True

            if current_size > 0 and response.status_code != 206:
                log(f"  Server returned {response.status_code} instead of 206, will append anyway")

            # Determine write mode
            if current_size > 0 and response.status_code == 206:
                mode = 'ab'  # Append
            elif current_size > 0:
                mode = 'ab'  # Still try to append
            else:
                mode = 'wb'  # Fresh start

            # Get content length
            content_length = int(response.headers.get('content-length', 0))
            total_expected = current_size + content_length if content_length else expected_size

            # Download with progress
            bytes_written = 0
            last_progress_time = time.time()
            last_progress_bytes = 0

            with open(output_path, mode) as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)

                        # Progress update every 5 seconds
                        now = time.time()
                        if now - last_progress_time >= 5:
                            current_total = current_size + bytes_written
                            speed = (bytes_written - last_progress_bytes) / (now - last_progress_time) / 1024 / 1024
                            percent = (current_total / total_expected * 100) if total_expected else 0
                            log(f"  Progress: {current_total/1024/1024/1024:.2f} GB / {total_expected/1024/1024/1024:.2f} GB ({percent:.1f}%) - {speed:.1f} MB/s")
                            last_progress_time = now
                            last_progress_bytes = bytes_written

            # Verify final size
            final_size = os.path.getsize(output_path)

            if expected_size > 0 and final_size >= expected_size:
                log(f"  Download complete: {final_size:,} bytes")
                return True
            elif expected_size > 0:
                log(f"  Incomplete: {final_size:,} / {expected_size:,} bytes - will retry")
            else:
                log(f"  Downloaded: {final_size:,} bytes (expected size unknown)")
                return True

        except requests.exceptions.Timeout as e:
            log(f"  Timeout on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
        except requests.exceptions.ConnectionError as e:
            log(f"  Connection error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
        except Exception as e:
            log(f"  Error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")

        # Wait before retry (exponential backoff, max 30 seconds)
        wait_time = min(2 ** attempt, 30)
        log(f"  Waiting {wait_time}s before retry...")
        time.sleep(wait_time)

    log(f"  FAILED after {MAX_RETRIES} attempts")
    return False


def main():
    parser = argparse.ArgumentParser(description='Download GoPro chapters with improved reliability')
    parser.add_argument('--gopro-ip', required=True, help='GoPro IP address (e.g., 172.26.138.51)')
    parser.add_argument('--session', required=True, help='Session folder name (e.g., enxd43260ddac87_FL_20260202_193946)')
    parser.add_argument('--chapters', type=str, help='Comma-separated chapter numbers to download (e.g., 1,2,3). Default: all')
    parser.add_argument('--filter', type=str, help='Filter files by pattern (e.g., "0053" to match GX*0053.MP4)')
    args = parser.parse_args()

    gopro_ip = args.gopro_ip
    session_name = args.session
    session_dir = os.path.join(SEGMENTS_DIR, session_name)

    log(f"=" * 60)
    log(f"GoPro Chapter Download Script")
    log(f"=" * 60)
    log(f"GoPro IP: {gopro_ip}")
    log(f"Session: {session_name}")
    log(f"Output: {session_dir}")
    log(f"=" * 60)

    # Create session directory
    os.makedirs(session_dir, exist_ok=True)

    # Get files on GoPro
    log("Fetching file list from GoPro...")
    files = get_gopro_files(gopro_ip)

    if not files:
        log("ERROR: No files found on GoPro or cannot connect")
        sys.exit(1)

    # Filter to MP4 files only
    mp4_files = [f for f in files if f['filename'].upper().endswith('.MP4')]
    log(f"Found {len(mp4_files)} MP4 files on GoPro")

    # Filter by pattern if specified (e.g., "0053" to match GX*0053.MP4)
    if args.filter:
        mp4_files = [f for f in mp4_files if args.filter in f['filename']]
        log(f"Filtered to {len(mp4_files)} files matching pattern '{args.filter}'")

    # Sort by filename to get chapters in order
    mp4_files.sort(key=lambda x: x['filename'])

    # Filter by chapter numbers if specified
    if args.chapters:
        chapter_nums = [int(c.strip()) for c in args.chapters.split(',')]
        selected_files = []
        for i, f in enumerate(mp4_files):
            if (i + 1) in chapter_nums:
                selected_files.append(f)
        mp4_files = selected_files
        log(f"Downloading chapters: {chapter_nums}")

    # Show files to download
    log(f"\nFiles to download:")
    total_size = 0
    for i, f in enumerate(mp4_files):
        size_gb = f['size'] / 1024 / 1024 / 1024
        log(f"  {i+1}. {f['filename']} ({size_gb:.2f} GB)")
        total_size += f['size']
    log(f"\nTotal: {total_size / 1024 / 1024 / 1024:.2f} GB")

    # Start keep-alive thread
    log("\nStarting keep-alive thread...")
    keep_alive = KeepAliveThread(gopro_ip)
    keep_alive.start()

    # Download each file
    successful = 0
    failed = 0

    try:
        for i, file_info in enumerate(mp4_files):
            filename = file_info['filename']
            directory = file_info['directory']
            expected_size = file_info['size']

            # Create output filename with chapter number
            chapter_num = i + 1
            output_filename = f"chapter_{chapter_num:03d}_{filename}"
            output_path = os.path.join(session_dir, output_filename)

            log(f"\n{'=' * 60}")
            log(f"Downloading chapter {chapter_num}/{len(mp4_files)}: {filename}")
            log(f"Expected size: {expected_size / 1024 / 1024 / 1024:.2f} GB")
            log(f"Output: {output_path}")

            if download_file(gopro_ip, directory, filename, expected_size, output_path, keep_alive):
                successful += 1
                log(f"Chapter {chapter_num} - SUCCESS")
            else:
                failed += 1
                log(f"Chapter {chapter_num} - FAILED")

    finally:
        # Stop keep-alive thread
        keep_alive.stop()

    # Summary
    log(f"\n{'=' * 60}")
    log(f"DOWNLOAD COMPLETE")
    log(f"{'=' * 60}")
    log(f"Successful: {successful}/{len(mp4_files)}")
    log(f"Failed: {failed}/{len(mp4_files)}")

    # List downloaded files
    log(f"\nDownloaded files:")
    for f in sorted(os.listdir(session_dir)):
        if f.endswith('.MP4'):
            fpath = os.path.join(session_dir, f)
            size = os.path.getsize(fpath)
            log(f"  {f}: {size / 1024 / 1024 / 1024:.2f} GB")

    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()

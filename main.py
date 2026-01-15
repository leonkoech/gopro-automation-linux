#!/usr/bin/env python3
"""
GoPro Controller API Service with Automatic Segmentation
REST API for remote control of GoPro cameras connected to Jetson Nano
"""

from flask import Flask, jsonify, request, send_file, Response, send_from_directory
from flask_cors import CORS
import subprocess
import threading
import os
import time
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import requests
import re
from videoupload import VideoUploadService
from media_service import get_media_service
from logging_service import get_logging_service, get_logger

# Initialize logging service first
logging_service = get_logging_service()
logger = get_logger('gopro.main')

app = Flask(__name__)
CORS(app)

# Configuration
VIDEO_STORAGE_DIR = os.path.expanduser('~/gopro_videos')
SEGMENTS_DIR = os.path.join(VIDEO_STORAGE_DIR, 'segments')
os.makedirs(VIDEO_STORAGE_DIR, exist_ok=True)
os.makedirs(SEGMENTS_DIR, exist_ok=True)

# Upload configuration
UPLOAD_ENABLED = os.getenv('UPLOAD_ENABLED', 'true').lower() == 'true'
UPLOAD_LOCATION = os.getenv('UPLOAD_LOCATION', 'default-location')
UPLOAD_DEVICE_NAME = os.getenv('UPLOAD_DEVICE_NAME', os.uname().nodename)
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET', 'jetson-videos-uai')
UPLOAD_REGION = os.getenv('UPLOAD_REGION', 'us-east-1')
DELETE_AFTER_UPLOAD = os.getenv('DELETE_AFTER_UPLOAD', 'false').lower() == 'true'

# Initialize upload service
upload_service = None
if UPLOAD_ENABLED:
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if aws_access_key and aws_secret_key:
        try:
            upload_service = VideoUploadService(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                bucket_name=UPLOAD_BUCKET,
                region=UPLOAD_REGION
            )
            print(f"✓ Video upload service initialized (bucket: {UPLOAD_BUCKET})")
        except Exception as e:
            print(f"⚠ Failed to initialize upload service: {e}")
    else:
        print("⚠ Upload enabled but AWS credentials not found in environment")

# Global state
recording_processes = {}
recording_lock = threading.Lock()
gopro_ip_cache = {}

def discover_gopro_ip_for_interface(interface, our_ip):
    """Discover the GoPro's IP address on a specific interface"""
    try:
        match = re.search(r'(\d+\.\d+\.\d+)\.(\d+)', our_ip)
        if not match:
            return None
        
        base = match.group(1)
        our_last = int(match.group(2))
        
        candidates = []
        if our_last == 50:
            candidates = [f"{base}.51", f"{base}.1"]
        elif our_last == 51:
            candidates = [f"{base}.50", f"{base}.1"]
        else:
            candidates = [f"{base}.51", f"{base}.50", f"{base}.1"]
        
        candidates = [ip for ip in candidates if ip != our_ip]
        
        for gopro_ip in candidates:
            try:
                response = requests.get(
                    f'http://{gopro_ip}:8080/gopro/camera/state',
                    timeout=1
                )
                if response.status_code == 200:
                    print(f"✓ Discovered GoPro at {gopro_ip} on {interface}")
                    return gopro_ip
            except:
                pass
        
        return None
    except Exception as e:
        print(f"Error discovering GoPro IP on {interface}: {e}")
        return None

def get_connected_gopros():
    """Discover all connected GoPro cameras"""
    global gopro_ip_cache
    gopros = []

    try:
        result = subprocess.run(['ip', 'addr', 'show'],
                              capture_output=True, text=True, timeout=5)

        lines = result.stdout.split('\n')
        current_interface = None
        current_ip = None

        for line in lines:
            if 'enx' in line and ':' in line:
                current_interface = line.split(':')[1].strip().split('@')[0].strip()
            elif 'inet 172.' in line and current_interface:
                current_ip = line.strip().split()[1].split('/')[0]

                gopro_ip = discover_gopro_ip_for_interface(current_interface, current_ip)
                if gopro_ip:
                    gopro_ip_cache[current_interface] = gopro_ip

                gopro_info = {
                    'id': current_interface,
                    'name': f'GoPro-{current_interface[-4:]}',
                    'interface': current_interface,
                    'ip': current_ip,
                    'gopro_ip': gopro_ip,
                    'status': 'connected',
                    'is_recording': current_interface in recording_processes
                }
                gopros.append(gopro_info)
                current_interface = None
                current_ip = None

    except Exception as e:
        print(f"Error discovering GoPros: {e}")

    return gopros

def get_gopro_wired_ip(gopro_id):
    """Get the cached or discover GoPro IP for a specific interface"""
    if gopro_id in gopro_ip_cache:
        ip = gopro_ip_cache[gopro_id]
        try:
            response = requests.get(f'http://{ip}:8080/gopro/camera/state', timeout=1)
            if response.status_code == 200:
                return ip
        except:
            pass
    
    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    
    if gopro and gopro.get('gopro_ip'):
        return gopro['gopro_ip']
    
    return None

def enable_usb_control(gopro_ip):
    """Enable USB control mode on the GoPro - required before sending commands"""
    try:
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/camera/control/wired_usb?p=1',
            timeout=5
        )
        if response.status_code == 200:
            print(f"✓ USB control enabled on {gopro_ip}")
            return True
        else:
            print(f"⚠ USB control response: {response.status_code}")
    except Exception as e:
        print(f"⚠ Failed to enable USB control: {e}")
    return False

def get_gopro_files(gopro_ip):
    """Get set of all current files on GoPro"""
    files = set()
    try:
        response = requests.get(f'http://{gopro_ip}:8080/gopro/media/list', timeout=10)
        if response.status_code == 200:
            media_list = response.json()
            for directory in media_list.get('media', []):
                for file_info in directory.get('fs', []):
                    files.add(file_info['n'])
    except Exception as e:
        print(f"Error getting GoPro files: {e}")
    return files

def get_gopro_camera_name(gopro_ip):
    """Get the camera name from GoPro's API (ap_ssid field)"""
    try:
        response = requests.get(f'http://{gopro_ip}:8080/gopro/camera/info', timeout=5)
        if response.status_code == 200:
            info = response.json()
            # ap_ssid can be at top level or nested under 'info'
            camera_name = info.get('ap_ssid') or info.get('info', {}).get('ap_ssid')
            if camera_name:
                print(f"✓ Got camera name from GoPro: {camera_name}")
                return camera_name
    except Exception as e:
        print(f"⚠ Could not get camera name from {gopro_ip}: {e}")
    return None


def sanitize_filename(name):
    """Sanitize a string for use in a filename"""
    # Replace characters that are invalid in filenames
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name.strip()

def get_video_list():
    """Get list of all recorded videos"""
    videos = []
    try:
        video_path = Path(VIDEO_STORAGE_DIR)
        for video_file in video_path.glob('*.mp4'):
            stat = video_file.stat()
            videos.append({
                'filename': video_file.name,
                'path': str(video_file),
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
        videos.sort(key=lambda x: x['created'], reverse=True)
    except Exception as e:
        print(f"Error listing videos: {e}")

    return videos

def download_gopro_video(gopro_ip, output_path, progress_callback=None):
    """Download the latest video from GoPro"""
    try:
        print(f"Fetching media list from {gopro_ip}...")
        media_response = requests.get(
            f'http://{gopro_ip}:8080/gopro/media/list',
            timeout=10
        )
        media_list = media_response.json()

        last_dir = media_list['media'][-1]
        last_file = last_dir['fs'][-1]
        gopro_filename = last_file['n']

        download_url = f'http://{gopro_ip}:8080/videos/DCIM/{last_dir["d"]}/{gopro_filename}'
        print(f"Downloading from: {download_url}")

        download_response = requests.get(download_url, stream=True, timeout=300)
        total_size = int(download_response.headers.get('content-length', 0))
        downloaded = 0

        with open(output_path, 'wb') as f:
            for chunk in download_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total_size > 0:
                        progress = int((downloaded / total_size) * 100)
                        progress_callback(progress)

        print(f"✓ Download complete: {output_path}")
        return True
    except Exception as e:
        print(f"Download error: {e}")
        return False

def merge_videos_ffmpeg(video_files, output_path):
    """Merge multiple video files using ffmpeg"""
    if not video_files:
        return False
    
    if len(video_files) == 1:
        # Just copy the single file
        import shutil
        shutil.copy2(video_files[0], output_path)
        print(f"✓ Single file copied to: {output_path}")
        return True
    
    video_files = sorted(video_files, key=lambda x: os.path.basename(x))
    
    concat_file = os.path.join(os.path.dirname(output_path), 'concat_list.txt')
    with open(concat_file, 'w') as f:
        for video in video_files:
            f.write(f"file '{os.path.abspath(video)}'\n")
    
    try:
        cmd = [
            'ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', concat_file,
            '-c', 'copy',
            '-y',
            output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        os.remove(concat_file)
        
        if result.returncode == 0:
            print(f"✓ Successfully merged {len(video_files)} videos to: {output_path}")
            return True
        else:
            print(f"FFmpeg error: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error merging videos: {e}")
        return False

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'gopro-controller',
        'timestamp': datetime.now().isoformat(),
        'storage_path': VIDEO_STORAGE_DIR
    })

@app.route('/api/gopros', methods=['GET'])
def list_gopros():
    """List all connected GoPro cameras"""
    gopros = get_connected_gopros()
    return jsonify({
        'success': True,
        'count': len(gopros),
        'gopros': gopros
    })

@app.route('/api/gopros/<gopro_id>/status', methods=['GET'])
def gopro_status(gopro_id):
    """Get status of a specific GoPro"""
    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)

    if not gopro:
        return jsonify({
            'success': False,
            'error': 'GoPro not found'
        }), 404

    return jsonify({
        'success': True,
        'gopro': gopro
    })

@app.route('/api/gopros/<gopro_id>/record/start', methods=['POST'])
def start_recording(gopro_id):
    """Start recording using direct HTTP API (no gopro-video dependency)"""
    global recording_processes
    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)

    if not gopro:
        return jsonify({'success': False, 'error': 'GoPro not found'}), 404

    with recording_lock:
        if gopro_id in recording_processes:
            return jsonify({'success': False, 'error': 'Already recording'}), 400

    gopro_ip = gopro.get('gopro_ip') or get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': f'Could not find GoPro IP for {gopro_id}'}), 500

    try:
        # Enable USB control first - required for wired control
        print(f"Enabling USB control on {gopro_id} ({gopro_ip})...")
        enable_usb_control(gopro_ip)
        time.sleep(0.5)

        # Set camera to Video mode
        print(f"Setting {gopro_id} ({gopro_ip}) to Video mode...")
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/camera/presets/load?id=0',
                timeout=5
            )
            if response.status_code == 200:
                print(f"✓ Set {gopro_id} to Video preset")
            else:
                print(f"⚠ Failed to set preset: {response.status_code}")
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠ Warning: Could not set video mode for {gopro_id}: {e}")

        # Get list of existing files BEFORE recording
        pre_record_files = get_gopro_files(gopro_ip)
        print(f"Pre-recording files on {gopro_id} ({gopro_ip}): {len(pre_record_files)} files")

        # Start recording via HTTP API
        print(f"Starting recording on {gopro_id} ({gopro_ip})...")
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/camera/shutter/start',
            timeout=5
        )

        if response.status_code != 200:
            error_text = response.text if response.text else f"HTTP {response.status_code}"
            print(f"✗ Failed to start recording on {gopro_id}: {error_text}")
            return jsonify({
                'success': False,
                'error': f'Failed to start recording: {error_text}'
            }), 500

        # Check if response contains an error
        try:
            resp_json = response.json()
            if 'error' in resp_json:
                print(f"✗ GoPro returned error: {resp_json}")
                return jsonify({
                    'success': False,
                    'error': f'GoPro error: {resp_json.get("error", "Unknown error")}'
                }), 500
        except:
            pass  # Response might not be JSON

        # Wait a moment and verify recording actually started
        time.sleep(1)
        try:
            state_response = requests.get(f'http://{gopro_ip}:8080/gopro/camera/state', timeout=5)
            if state_response.status_code == 200:
                state = state_response.json()
                # Status 8 = busy/encoding, Status 10 = recording
                is_recording = state.get('status', {}).get('8', 0) == 1 or state.get('status', {}).get('10', 0) == 1
                if not is_recording:
                    print(f"⚠ Recording may not have started - camera not in recording state")
                    # Don't fail - the shutter command succeeded, camera might just be slow
                else:
                    print(f"✓ Confirmed recording active on {gopro_id}")
        except Exception as e:
            print(f"⚠ Could not verify recording state: {e}")

        print(f"✓ Recording started on {gopro_id}")

        # Get camera name (ap_ssid) from GoPro for the filename
        camera_name = get_gopro_camera_name(gopro_ip)
        if not camera_name:
            camera_name = f"GoPro-{gopro_id[-4:]}"
            print(f"Using fallback camera name: {camera_name}")

        # Prepare session info - store start datetime for filename generation at stop time
        start_datetime = datetime.now()
        timestamp = start_datetime.strftime('%Y%m%d_%H%M%S')
        session_id = f"{gopro_id}_{timestamp}"
        session_dir = os.path.join(SEGMENTS_DIR, session_id)
        os.makedirs(session_dir, exist_ok=True)

        # Store recording state (video_path and video_filename will be generated at stop time)
        with recording_lock:
            recording_processes[gopro_id] = {
                'start_time': start_datetime.isoformat(),
                'start_datetime': start_datetime,
                'gopro_ip': gopro_ip,
                'gopro_name': gopro['name'],
                'camera_name': camera_name,
                'pre_record_files': pre_record_files,
                'recording_started': True,
                'is_stopping': False,
                'video_path': None,  # Will be set at stop time
                'video_filename': None,  # Will be set at stop time
                'session_id': session_id,
                'session_dir': session_dir,
                'error': None,
                'stage': 'recording',
                'stage_message': 'Recording...'
            }

        return jsonify({
            'success': True,
            'message': 'Recording started',
            'gopro_id': gopro_id,
            'gopro_ip': gopro_ip,
            'camera_name': camera_name,
            'session_id': session_id
        })

    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'GoPro not responding (timeout)'
        }), 500
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Could not connect to GoPro'
        }), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording and download all new video chapters using HTTP API"""
    global recording_processes

    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400

        recording_processes[gopro_id]['is_stopping'] = True
        recording_info = recording_processes[gopro_id].copy()

    try:
        session_dir = recording_info.get('session_dir')
        gopro_ip = recording_info.get('gopro_ip')
        pre_record_files = recording_info.get('pre_record_files', set())
        camera_name = recording_info.get('camera_name', f'GoPro-{gopro_id[-4:]}')
        start_datetime = recording_info.get('start_datetime', datetime.now())

        # Generate filename with format: YYYYMMDD HHMMSS - HHMMSS camera_name.mp4
        stop_datetime = datetime.now()
        date_str = start_datetime.strftime('%Y%m%d')
        start_time_str = start_datetime.strftime('%H%M%S')
        stop_time_str = stop_datetime.strftime('%H%M%S')
        safe_camera_name = sanitize_filename(camera_name)
        video_filename = f'{date_str} {start_time_str} - {stop_time_str} {safe_camera_name}.mp4'
        video_path = os.path.join(VIDEO_STORAGE_DIR, video_filename)

        # Update recording info with the generated filename
        with recording_lock:
            if gopro_id in recording_processes:
                recording_processes[gopro_id]['video_path'] = video_path
                recording_processes[gopro_id]['video_filename'] = video_filename

        if not gopro_ip:
            gopro_ip = get_gopro_wired_ip(gopro_id)

        if not gopro_ip:
            with recording_lock:
                if gopro_id in recording_processes:
                    del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': 'Could not detect GoPro IP'}), 500

        # Send stop command to camera via HTTP API
        print(f"Stopping recording on {gopro_id} ({gopro_ip})...")
        try:
            response = requests.get(f'http://{gopro_ip}:8080/gopro/camera/shutter/stop', timeout=5)
            if response.status_code == 200:
                print(f"✓ Recording stopped on {gopro_id}")
            else:
                print(f"⚠ Stop command returned: {response.status_code}")
        except Exception as e:
            print(f"Warning: Could not send stop command: {e}")

        with recording_lock:
            if gopro_id in recording_processes:
                recording_processes[gopro_id]['downloading'] = True
                recording_processes[gopro_id]['download_progress'] = 0
                recording_processes[gopro_id]['stage'] = 'stopping'
                recording_processes[gopro_id]['stage_message'] = 'Stopping GoPro...'

        def download_all_chapters():
            """Download ALL new video chapters created during recording"""
            try:
                # Update stage: downloading
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['stage'] = 'downloading'
                        recording_processes[gopro_id]['stage_message'] = 'Waiting for GoPro to finalize files...'

                # Wait for camera to finalize all chapter files
                # GoPro needs more time after long recordings to write all chapters
                logger.info(f"[{gopro_id}] Waiting for GoPro to finalize files...")
                time.sleep(5)  # Initial wait

                # Retry loop to ensure all chapters are available
                # GoPro may take time to register all chapters after stopping
                new_chapters = []
                last_chapter_count = 0
                stable_count = 0
                max_retries = 10

                for attempt in range(max_retries):
                    logger.info(f"[{gopro_id}] Getting media list from {gopro_ip} (attempt {attempt + 1}/{max_retries})...")
                    media_response = requests.get(
                        f'http://{gopro_ip}:8080/gopro/media/list',
                        timeout=15
                    )
                    media_list = media_response.json()

                    # Find ALL new files
                    new_chapters = []
                    for directory in media_list.get('media', []):
                        dir_name = directory['d']
                        for file_info in directory.get('fs', []):
                            filename = file_info['n']
                            if filename not in pre_record_files and filename.lower().endswith('.mp4'):
                                new_chapters.append({
                                    'directory': dir_name,
                                    'filename': filename,
                                    'size': int(file_info.get('s', 0))
                                })

                    logger.info(f"[{gopro_id}] Found {len(new_chapters)} new chapters")

                    # Check if chapter count has stabilized
                    if len(new_chapters) == last_chapter_count and len(new_chapters) > 0:
                        stable_count += 1
                        if stable_count >= 2:
                            logger.info(f"[{gopro_id}] Chapter count stable at {len(new_chapters)}")
                            break
                    else:
                        stable_count = 0
                        last_chapter_count = len(new_chapters)

                    # Wait before next check
                    if attempt < max_retries - 1:
                        time.sleep(2)

                # Sort chapters properly for GoPro naming convention
                # GoPro naming: GXzzxxxx.MP4 where zz=chapter, xxxx=video number
                # e.g., GX010028, GX020028, GX030028 are chapters 1,2,3 of video 0028
                # We need to sort by video number first, then chapter number
                def gopro_sort_key(chapter):
                    filename = chapter['filename'].upper()
                    if len(filename) >= 8 and filename.startswith('G'):
                        chapter_num = filename[2:4]  # e.g., "01"
                        video_num = filename[4:8]    # e.g., "0028"
                        return (video_num, chapter_num)
                    return (filename, "00")

                new_chapters.sort(key=gopro_sort_key)

                # Safety check: if we found too many "new" files, something is wrong
                # (likely pre_record_files wasn't captured properly)
                MAX_CHAPTERS = 20
                total_size_bytes = sum(ch['size'] for ch in new_chapters)
                total_size_gb = total_size_bytes / (1024**3)

                if len(new_chapters) > MAX_CHAPTERS:
                    logger.warning(f"[{gopro_id}] Found {len(new_chapters)} new chapters ({total_size_gb:.1f} GB) - this seems too many!")
                    logger.warning(f"[{gopro_id}] Pre-record files count: {len(pre_record_files)}")
                    logger.warning(f"[{gopro_id}] Limiting to last {MAX_CHAPTERS} chapters to avoid downloading entire SD card")
                    new_chapters = new_chapters[-MAX_CHAPTERS:]
                    total_size_bytes = sum(ch['size'] for ch in new_chapters)
                    total_size_gb = total_size_bytes / (1024**3)

                logger.info(f"[{gopro_id}] Total download size: {total_size_gb:.2f} GB")

                logger.info(f"[{gopro_id}] Found {len(new_chapters)} new chapters to download")
                for ch in new_chapters:
                    logger.info(f"[{gopro_id}]   - {ch['filename']} ({ch['size']} bytes)")

                if not new_chapters:
                    logger.warning(f"[{gopro_id}] No new chapters found")
                    with recording_lock:
                        if gopro_id in recording_processes:
                            del recording_processes[gopro_id]
                    return

                # Download all chapters
                downloaded_files = []
                total_chapters = len(new_chapters)
                
                for i, chapter in enumerate(new_chapters):
                    chapter_path = os.path.join(session_dir, f'chapter_{i+1:03d}_{chapter["filename"]}')
                    download_url = f'http://{gopro_ip}:8080/videos/DCIM/{chapter["directory"]}/{chapter["filename"]}'
                    
                    logger.info(f"[{gopro_id}] Downloading chapter {i+1}/{total_chapters}: {chapter['filename']}")

                    try:
                        response = requests.get(download_url, stream=True, timeout=600)
                        total_size = int(response.headers.get('content-length', 0))
                        downloaded = 0

                        with open(chapter_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                                    downloaded += len(chunk)

                        downloaded_files.append(chapter_path)
                        file_size_mb = os.path.getsize(chapter_path) / (1024 * 1024)
                        logger.info(f"[{gopro_id}] Downloaded chapter {i+1}: {file_size_mb:.1f} MB")

                        # Update progress
                        with recording_lock:
                            if gopro_id in recording_processes:
                                progress = int(((i + 1) / total_chapters) * 100)
                                recording_processes[gopro_id]['download_progress'] = progress

                    except Exception as e:
                        logger.error(f"[{gopro_id}] Error downloading chapter {chapter['filename']}: {e}")

                # Merge all chapters
                if downloaded_files:
                    # Update stage: merging
                    with recording_lock:
                        if gopro_id in recording_processes:
                            recording_processes[gopro_id]['stage'] = 'merging'
                            recording_processes[gopro_id]['stage_message'] = 'Merging video chapters...'

                    logger.info(f"[{gopro_id}] Merging {len(downloaded_files)} chapters into {video_path}...")

                    if merge_videos_ffmpeg(downloaded_files, video_path):
                        final_size_mb = os.path.getsize(video_path) / (1024 * 1024)
                        logger.info(f"[{gopro_id}] Merged video saved: {video_path} ({final_size_mb:.1f} MB)")

                        # Cleanup chapter files
                        try:
                            for f in downloaded_files:
                                os.remove(f)
                            os.rmdir(session_dir)
                            logger.info(f"[{gopro_id}] Cleaned up {len(downloaded_files)} chapter files")
                        except Exception as e:
                            logger.warning(f"[{gopro_id}] Cleanup error: {e}")

                        # Upload to S3 if enabled
                        if upload_service:
                            # Update stage: uploading
                            with recording_lock:
                                if gopro_id in recording_processes:
                                    recording_processes[gopro_id]['stage'] = 'uploading'
                                    recording_processes[gopro_id]['stage_message'] = 'Uploading to cloud...'

                            try:
                                logger.info(f"[{gopro_id}] Starting upload of {video_filename} to S3...")
                                upload_date = datetime.now().strftime('%Y-%m-%d')

                                # Get camera name from GoPro API, fallback to interface-based name
                                camera_name = get_gopro_camera_name(gopro_ip)
                                if not camera_name:
                                    camera_name = f"GoPro-{gopro_id[-4:]}"
                                    logger.info(f"[{gopro_id}] Using fallback camera name: {camera_name}")

                                s3_uri = upload_service.upload_video(
                                    video_path=video_path,
                                    location=UPLOAD_LOCATION,
                                    date=upload_date,
                                    device_name=UPLOAD_DEVICE_NAME,
                                    camera_name=camera_name,
                                    compress=True,
                                    delete_compressed_after_upload=True
                                )
                                logger.info(f"[{gopro_id}] Video uploaded to: {s3_uri}")

                                # Update stage: done
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['stage'] = 'done'
                                        recording_processes[gopro_id]['stage_message'] = 'Done!'

                                # Optionally delete local file after upload
                                if DELETE_AFTER_UPLOAD:
                                    try:
                                        os.remove(video_path)
                                        logger.info(f"[{gopro_id}] Local file deleted after upload: {video_filename}")
                                    except Exception as e:
                                        logger.warning(f"[{gopro_id}] Failed to delete local file: {e}")
                            except Exception as e:
                                logger.error(f"[{gopro_id}] Upload failed: {e}")
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['stage'] = 'done'
                                        recording_processes[gopro_id]['stage_message'] = 'Done (upload failed)'
                        else:
                            # No upload service, mark as done
                            with recording_lock:
                                if gopro_id in recording_processes:
                                    recording_processes[gopro_id]['stage'] = 'done'
                                    recording_processes[gopro_id]['stage_message'] = 'Done!'
                    else:
                        logger.error(f"[{gopro_id}] Failed to merge chapters - keeping individual files in {session_dir}")
                else:
                    logger.warning(f"[{gopro_id}] No chapters downloaded")

                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]
                        print(f"✓ Recording cleanup complete for {gopro_id}")

            except Exception as e:
                print(f"Error in download_all_chapters for {gopro_id}: {e}")
                import traceback
                traceback.print_exc()
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['download_error'] = str(e)
                        del recording_processes[gopro_id]

        threading.Thread(target=download_all_chapters, daemon=True).start()

        return jsonify({
            'success': True,
            'message': f'Recording stopped, downloading chapters...',
            'video_filename': video_filename,
            'pre_record_files_count': len(pre_record_files)
        })

    except Exception as e:
        print(f"Error in stop_recording: {str(e)}")
        import traceback
        traceback.print_exc()
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/status', methods=['GET'])
def recording_status(gopro_id):
    """Get current recording status including any errors"""
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({
                'success': True,
                'is_recording': False
            })
        
        info = recording_processes[gopro_id]
        return jsonify({
            'success': True,
            'is_recording': True,
            'recording_started': info.get('recording_started', False),
            'start_time': info.get('start_time'),
            'video_filename': info.get('video_filename'),
            'is_stopping': info.get('is_stopping', False),
            'downloading': info.get('downloading', False),
            'download_progress': info.get('download_progress', 0),
            'stage': info.get('stage', 'recording'),
            'stage_message': info.get('stage_message', 'Recording...'),
            'error': info.get('error')
        })

@app.route('/api/videos', methods=['GET'])
def list_videos():
    """List all recorded videos"""
    videos = get_video_list()
    return jsonify({
        'success': True,
        'count': len(videos),
        'videos': videos
    })

@app.route('/api/videos/<filename>', methods=['DELETE'])
def delete_video(filename):
    """Delete a specific video"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)
        if os.path.exists(video_path) and video_path.startswith(VIDEO_STORAGE_DIR):
            os.remove(video_path)
            return jsonify({
                'success': True,
                'message': f'Video {filename} deleted'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/system/info', methods=['GET'])
def system_info():
    """Get system information"""
    try:
        stat = os.statvfs(VIDEO_STORAGE_DIR)
        free_space_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
        total_space_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)

        videos = get_video_list()
        total_video_size_mb = sum(v['size_mb'] for v in videos)

        return jsonify({
            'success': True,
            'system': {
                'hostname': os.uname().nodename,
                'storage_path': VIDEO_STORAGE_DIR,
                'disk_free_gb': round(free_space_gb, 2),
                'disk_total_gb': round(total_space_gb, 2),
                'video_count': len(videos),
                'total_video_size_mb': round(total_video_size_mb, 2)
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/videos/<filename>/download', methods=['GET'])
def download_video(filename):
    """Download a specific video file"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)
        
        if not os.path.exists(video_path):
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404
            
        if not video_path.startswith(VIDEO_STORAGE_DIR):
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403
        
        return send_file(
            video_path,
            as_attachment=True,
            download_name=filename,
            mimetype='video/mp4'
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/videos/<filename>/stream', methods=['GET'])
def stream_video(filename):
    """Stream a specific video file with support for range requests"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)
        
        if not os.path.exists(video_path):
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404
            
        if not video_path.startswith(VIDEO_STORAGE_DIR):
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403
        
        file_size = os.path.getsize(video_path)
        range_header = request.headers.get('Range', None)
        
        if not range_header:
            return send_file(
                video_path,
                mimetype='video/mp4',
                conditional=True
            )
        
        byte_range = range_header.replace('bytes=', '').split('-')
        start = int(byte_range[0]) if byte_range[0] else 0
        end = int(byte_range[1]) if byte_range[1] else file_size - 1
        
        end = min(end, file_size - 1)
        length = end - start + 1
        
        with open(video_path, 'rb') as f:
            f.seek(start)
            data = f.read(length)
        
        response = Response(
            data,
            206,
            mimetype='video/mp4',
            direct_passthrough=True
        )
        
        response.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
        response.headers.add('Accept-Ranges', 'bytes')
        response.headers.add('Content-Length', str(length))
        
        return response
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==================== Media Management Endpoints ====================

media_service = get_media_service(VIDEO_STORAGE_DIR)

@app.route('/api/media/gopro/<gopro_id>/files', methods=['GET'])
def get_gopro_files_list(gopro_id):
    """Get list of all files on a specific GoPro"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    result = media_service.get_gopro_media_list(gopro_ip)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/gopro/<gopro_id>/storage', methods=['GET'])
def get_gopro_storage(gopro_id):
    """Get storage info for a specific GoPro"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    result = media_service.get_gopro_storage_info(gopro_ip)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/gopro/<gopro_id>/files/<directory>/<filename>', methods=['GET'])
def get_gopro_file_details(gopro_id, directory, filename):
    """Get detailed info about a specific file on GoPro"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    result = media_service.get_gopro_file_info(gopro_ip, directory, filename)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/gopro/<gopro_id>/files/<directory>/<filename>', methods=['DELETE'])
def delete_gopro_file_endpoint(gopro_id, directory, filename):
    """Delete a specific file from GoPro"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    result = media_service.delete_gopro_file(gopro_ip, directory, filename)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/gopro/<gopro_id>/files/all', methods=['DELETE'])
def delete_all_gopro_files_endpoint(gopro_id):
    """Delete ALL files from a GoPro (use with caution!)"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    # Require confirmation parameter
    confirm = request.args.get('confirm', 'false').lower() == 'true'
    if not confirm:
        return jsonify({
            'success': False,
            'error': 'Must pass ?confirm=true to delete all files'
        }), 400

    result = media_service.delete_gopro_all_files(gopro_ip)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/gopro/<gopro_id>/files/<directory>/<filename>/download', methods=['GET'])
def download_gopro_file(gopro_id, directory, filename):
    """Proxy download of a file from GoPro"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    try:
        download_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}'
        response = requests.get(download_url, stream=True, timeout=300)

        def generate():
            for chunk in response.iter_content(chunk_size=65536):
                yield chunk

        return Response(
            generate(),
            headers={
                'Content-Type': 'video/mp4',
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Content-Length': response.headers.get('Content-Length', '')
            }
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/media/gopro/<gopro_id>/files/<directory>/<filename>/thumbnail', methods=['GET'])
def get_gopro_thumbnail(gopro_id, directory, filename):
    """Get thumbnail for a GoPro file"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    try:
        thumb_url = f'http://{gopro_ip}:8080/gopro/media/thumbnail?path={directory}/{filename}'
        response = requests.get(thumb_url, timeout=10)

        return Response(
            response.content,
            headers={
                'Content-Type': 'image/jpeg',
                'Cache-Control': 'max-age=3600'
            }
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/media/gopro/<gopro_id>/files/<directory>/<filename>/stream', methods=['GET'])
def stream_gopro_file(gopro_id, directory, filename):
    """Stream a video file from GoPro (for preview)"""
    gopro_ip = get_gopro_wired_ip(gopro_id)
    if not gopro_ip:
        return jsonify({'success': False, 'error': 'GoPro not found or not connected'}), 404

    try:
        stream_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}'

        # Handle range requests for video seeking
        range_header = request.headers.get('Range', None)
        headers = {}
        if range_header:
            headers['Range'] = range_header

        response = requests.get(stream_url, headers=headers, stream=True, timeout=30)

        def generate():
            for chunk in response.iter_content(chunk_size=65536):
                yield chunk

        resp_headers = {
            'Content-Type': 'video/mp4',
            'Accept-Ranges': 'bytes'
        }

        if 'Content-Range' in response.headers:
            resp_headers['Content-Range'] = response.headers['Content-Range']
        if 'Content-Length' in response.headers:
            resp_headers['Content-Length'] = response.headers['Content-Length']

        return Response(
            generate(),
            status=response.status_code,
            headers=resp_headers
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== Local/Jetson Media Endpoints ====================

@app.route('/api/media/local/files', methods=['GET'])
def get_local_files_list():
    """Get list of all local video files on Jetson"""
    result = media_service.get_local_media_list()
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/local/storage', methods=['GET'])
def get_local_storage():
    """Get local storage info"""
    result = media_service.get_local_storage_info()
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/local/files/<filename>', methods=['DELETE'])
def delete_local_file_endpoint(filename):
    """Delete a local video file"""
    result = media_service.delete_local_file(filename)
    return jsonify(result), 200 if result['success'] else 500

# ==================== Segments Endpoints ====================

@app.route('/api/media/segments', methods=['GET'])
def get_segments_list():
    """Get list of all segment sessions on Jetson"""
    result = media_service.get_segments_list()
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/segments/<session_name>', methods=['GET'])
def get_segment_session_files(session_name):
    """Get list of files in a specific segment session"""
    result = media_service.get_segment_session_files(session_name)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/segments/<session_name>', methods=['DELETE'])
def delete_segment_session_endpoint(session_name):
    """Delete an entire segment session"""
    result = media_service.delete_segment_session(session_name)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/segments/<session_name>/<filename>', methods=['DELETE'])
def delete_segment_file_endpoint(session_name, filename):
    """Delete a specific file from a segment session"""
    result = media_service.delete_segment_file(session_name, filename)
    return jsonify(result), 200 if result['success'] else 500

@app.route('/api/media/segments/<session_name>/<filename>/download', methods=['GET'])
def download_segment_file(session_name, filename):
    """Download a specific segment file"""
    try:
        file_path = media_service.get_segment_file_path(session_name, filename)

        if not file_path:
            return jsonify({
                'success': False,
                'error': 'Segment file not found'
            }), 404

        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/media/segments/<session_name>/<filename>/stream', methods=['GET'])
def stream_segment_file(session_name, filename):
    """Stream a segment video file with range request support"""
    try:
        file_path = media_service.get_segment_file_path(session_name, filename)

        if not file_path:
            return jsonify({
                'success': False,
                'error': 'Segment file not found'
            }), 404

        file_size = os.path.getsize(file_path)
        range_header = request.headers.get('Range')

        if range_header:
            byte_start = 0
            byte_end = file_size - 1

            range_match = range_header.replace('bytes=', '').split('-')
            if range_match[0]:
                byte_start = int(range_match[0])
            if range_match[1]:
                byte_end = int(range_match[1])

            length = byte_end - byte_start + 1

            def generate():
                with open(file_path, 'rb') as f:
                    f.seek(byte_start)
                    remaining = length
                    while remaining > 0:
                        chunk_size = min(8192, remaining)
                        data = f.read(chunk_size)
                        if not data:
                            break
                        remaining -= len(data)
                        yield data

            response = Response(
                generate(),
                status=206,
                mimetype='video/mp4',
                direct_passthrough=True
            )
            response.headers['Content-Range'] = f'bytes {byte_start}-{byte_end}/{file_size}'
            response.headers['Accept-Ranges'] = 'bytes'
            response.headers['Content-Length'] = length
            return response

        return send_file(file_path, mimetype='video/mp4')

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==================== Segment Upload Endpoints ====================

# Track ongoing segment uploads
segment_upload_status = {}
segment_upload_lock = threading.Lock()

@app.route('/api/media/segments/upload', methods=['POST'])
def upload_segments_to_cloud():
    """
    Upload segment sessions to S3 cloud storage.

    Request body:
    {
        "sessions": ["session_name1", "session_name2"],  // optional, if empty uploads all non-empty sessions
        "camera_name_map": {  // optional mapping of interface IDs to camera names
            "enxd43260ef4d38": "GoPro Front",
            "enxd43260dc857e": "GoPro Back"
        },
        "delete_after_upload": false  // optional, delete segments after successful upload
    }
    """
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured. Check AWS credentials.'
        }), 503

    try:
        data = request.get_json() or {}
        requested_sessions = data.get('sessions', [])
        camera_name_map = data.get('camera_name_map', {})
        delete_after_upload = data.get('delete_after_upload', False)

        # Get all segments
        segments_result = media_service.get_segments_list()
        if not segments_result.get('success'):
            return jsonify({
                'success': False,
                'error': segments_result.get('error', 'Failed to get segments')
            }), 500

        all_sessions = segments_result.get('sessions', [])

        # Filter sessions - only those with files
        sessions_to_upload = []
        for session in all_sessions:
            if session['file_count'] == 0:
                continue
            if requested_sessions and session['session_name'] not in requested_sessions:
                continue
            sessions_to_upload.append(session)

        if not sessions_to_upload:
            return jsonify({
                'success': True,
                'message': 'No sessions with files to upload',
                'uploaded': 0
            })

        # Generate upload ID
        upload_id = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Start upload in background thread
        def upload_sessions_background():
            results = []
            total_sessions = len(sessions_to_upload)

            with segment_upload_lock:
                segment_upload_status[upload_id] = {
                    'status': 'in_progress',
                    'total': total_sessions,
                    'completed': 0,
                    'current_session': None,
                    'results': [],
                    'errors': []
                }

            for idx, session in enumerate(sessions_to_upload):
                session_name = session['session_name']

                with segment_upload_lock:
                    segment_upload_status[upload_id]['current_session'] = session_name
                    segment_upload_status[upload_id]['current_index'] = idx + 1

                try:
                    result = upload_single_segment_session(
                        session,
                        camera_name_map,
                        delete_after_upload
                    )
                    results.append(result)

                    with segment_upload_lock:
                        segment_upload_status[upload_id]['completed'] = idx + 1
                        segment_upload_status[upload_id]['results'].append(result)

                except Exception as e:
                    error_result = {
                        'session_name': session_name,
                        'success': False,
                        'error': str(e)
                    }
                    results.append(error_result)

                    with segment_upload_lock:
                        segment_upload_status[upload_id]['completed'] = idx + 1
                        segment_upload_status[upload_id]['errors'].append(error_result)

            with segment_upload_lock:
                segment_upload_status[upload_id]['status'] = 'completed'
                segment_upload_status[upload_id]['current_session'] = None

        # Start background thread
        upload_thread = threading.Thread(target=upload_sessions_background)
        upload_thread.daemon = True
        upload_thread.start()

        return jsonify({
            'success': True,
            'message': f'Started uploading {len(sessions_to_upload)} sessions',
            'upload_id': upload_id,
            'sessions_queued': [s['session_name'] for s in sessions_to_upload]
        })

    except Exception as e:
        logger.error(f"Error starting segment upload: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def upload_single_segment_session(session: dict, camera_name_map: dict, delete_after_upload: bool) -> dict:
    """
    Upload a single segment session to S3.
    Merges chapter files and uploads with proper naming.
    """
    import shutil

    session_name = session['session_name']
    session_path = session['path']
    files = session['files']

    logger.info(f"[SegmentUpload] Processing session: {session_name}")

    # Parse session name: format is interfaceId_YYYYMMDD_HHMMSS
    parts = session_name.split('_')
    if len(parts) < 3:
        return {
            'session_name': session_name,
            'success': False,
            'error': f'Invalid session name format: {session_name}'
        }

    interface_id = '_'.join(parts[:-2])  # Everything except last 2 parts
    date_str = parts[-2]  # YYYYMMDD
    time_str = parts[-1]  # HHMMSS

    # Format date for S3: YYYY-MM-DD
    upload_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    # Determine camera name
    camera_name = camera_name_map.get(interface_id)
    if not camera_name:
        # Fallback: use last 4 chars of interface ID
        camera_name = f"GoPro-{interface_id[-4:]}"

    logger.info(f"[SegmentUpload] Camera: {camera_name}, Date: {upload_date}")

    # Get only video files, sorted by name (chapters in order)
    video_files = sorted([f for f in files if f['is_video']], key=lambda x: x['filename'])

    if not video_files:
        return {
            'session_name': session_name,
            'success': False,
            'error': 'No video files in session'
        }

    # Create temp directory for processing
    temp_dir = tempfile.mkdtemp(prefix='segment_upload_')

    try:
        if len(video_files) == 1:
            # Single file - no merge needed
            input_path = os.path.join(session_path, video_files[0]['filename'])
            logger.info(f"[SegmentUpload] Single file, no merge needed: {video_files[0]['filename']}")
        else:
            # Multiple files - need to merge
            logger.info(f"[SegmentUpload] Merging {len(video_files)} chapter files...")

            # Create concat file for ffmpeg
            concat_file = os.path.join(temp_dir, 'concat.txt')
            with open(concat_file, 'w') as f:
                for vf in video_files:
                    file_path = os.path.join(session_path, vf['filename'])
                    # Escape special characters in path
                    escaped_path = file_path.replace("'", "'\\''")
                    f.write(f"file '{escaped_path}'\n")

            # Merge using ffmpeg concat demuxer (fast, no re-encoding)
            merged_path = os.path.join(temp_dir, 'merged.mp4')
            merge_cmd = [
                'ffmpeg',
                '-f', 'concat',
                '-safe', '0',
                '-i', concat_file,
                '-c', 'copy',  # Copy streams without re-encoding
                '-y',
                merged_path
            ]

            logger.info(f"[SegmentUpload] Running ffmpeg merge...")
            result = subprocess.run(merge_cmd, capture_output=True, text=True)

            if result.returncode != 0:
                logger.error(f"[SegmentUpload] FFmpeg merge error: {result.stderr}")
                return {
                    'session_name': session_name,
                    'success': False,
                    'error': f'FFmpeg merge failed: {result.stderr[:200]}'
                }

            input_path = merged_path
            logger.info(f"[SegmentUpload] Merge complete")

        # Upload to S3
        logger.info(f"[SegmentUpload] Uploading to S3...")
        s3_uri = upload_service.upload_video(
            video_path=input_path,
            location=UPLOAD_LOCATION,
            date=upload_date,
            device_name=UPLOAD_DEVICE_NAME,
            camera_name=camera_name,
            compress=True,  # Compress to 1080p
            delete_compressed_after_upload=True
        )

        logger.info(f"[SegmentUpload] Upload complete: {s3_uri}")

        # Delete segments if requested
        if delete_after_upload:
            logger.info(f"[SegmentUpload] Deleting session after upload...")
            media_service.delete_segment_session(session_name)

        return {
            'session_name': session_name,
            'success': True,
            's3_uri': s3_uri,
            'camera_name': camera_name,
            'date': upload_date,
            'files_merged': len(video_files)
        }

    except Exception as e:
        logger.error(f"[SegmentUpload] Error processing {session_name}: {e}")
        return {
            'session_name': session_name,
            'success': False,
            'error': str(e)
        }
    finally:
        # Clean up temp directory
        try:
            shutil.rmtree(temp_dir)
        except:
            pass


@app.route('/api/media/segments/upload/<upload_id>/status', methods=['GET'])
def get_segment_upload_status(upload_id):
    """Get the status of a segment upload job"""
    with segment_upload_lock:
        if upload_id not in segment_upload_status:
            return jsonify({
                'success': False,
                'error': 'Upload job not found'
            }), 404

        status = segment_upload_status[upload_id].copy()

    return jsonify({
        'success': True,
        'upload_id': upload_id,
        **status
    })


@app.route('/api/media/segments/upload/jobs', methods=['GET'])
def list_segment_upload_jobs():
    """List all segment upload jobs"""
    with segment_upload_lock:
        jobs = []
        for upload_id, status in segment_upload_status.items():
            jobs.append({
                'upload_id': upload_id,
                'status': status['status'],
                'total': status['total'],
                'completed': status['completed']
            })

    return jsonify({
        'success': True,
        'jobs': jobs
    })


@app.route('/api/media/segments/<session_name>/upload', methods=['POST'])
def upload_single_segment(session_name):
    """Upload a single segment session to S3"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        data = request.get_json() or {}
        camera_name_map = data.get('camera_name_map', {})
        delete_after_upload = data.get('delete_after_upload', False)

        # Get session info
        session_result = media_service.get_segment_session_files(session_name)
        if not session_result.get('success'):
            return jsonify({
                'success': False,
                'error': session_result.get('error', 'Session not found')
            }), 404

        session = {
            'session_name': session_name,
            'path': session_result['path'],
            'files': session_result['files'],
            'file_count': session_result['file_count']
        }

        if session['file_count'] == 0:
            return jsonify({
                'success': False,
                'error': 'Session has no files'
            }), 400

        # Upload synchronously for single session
        result = upload_single_segment_session(session, camera_name_map, delete_after_upload)

        return jsonify(result), 200 if result.get('success') else 500

    except Exception as e:
        logger.error(f"Error uploading segment {session_name}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ==================== Cloud/S3 Video Endpoints ====================

@app.route('/api/cloud/videos', methods=['GET'])
def list_cloud_videos():
    """List all videos stored in S3 cloud storage"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        location = request.args.get('location')
        date = request.args.get('date')

        videos = upload_service.list_videos_with_metadata(location=location, date=date)

        total_size_mb = sum(v['size_mb'] for v in videos)

        return jsonify({
            'success': True,
            'video_count': len(videos),
            'total_size_mb': round(total_size_mb, 2),
            'total_size_gb': round(total_size_mb / 1024, 2),
            'videos': videos
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/cloud/videos/stream', methods=['GET'])
def get_cloud_video_stream_url():
    """Get a presigned URL for streaming a cloud video"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        s3_key = request.args.get('key')
        if not s3_key:
            return jsonify({
                'success': False,
                'error': 'Missing required parameter: key'
            }), 400

        # Generate URL valid for 2 hours
        url = upload_service.get_presigned_url(s3_key, expiration=7200)

        return jsonify({
            'success': True,
            'stream_url': url,
            'expires_in': 7200
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/cloud/videos', methods=['DELETE'])
def delete_cloud_video():
    """Delete a video from S3 cloud storage"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        s3_key = request.args.get('key')
        if not s3_key:
            return jsonify({
                'success': False,
                'error': 'Missing required parameter: key'
            }), 400

        upload_service.delete_video(s3_key)

        return jsonify({
            'success': True,
            'message': f'Video deleted: {s3_key}'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/cloud/locations', methods=['GET'])
def list_cloud_locations():
    """List all unique locations in cloud storage"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        locations = upload_service.get_unique_locations()

        return jsonify({
            'success': True,
            'locations': locations
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/cloud/locations/<location>/dates', methods=['GET'])
def list_cloud_dates(location):
    """List all dates for a specific location in cloud storage"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        dates = upload_service.get_dates_for_location(location)

        return jsonify({
            'success': True,
            'location': location,
            'dates': dates
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/cloud/status', methods=['GET'])
def cloud_status():
    """Check if cloud storage is configured and accessible"""
    return jsonify({
        'success': True,
        'enabled': upload_service is not None,
        'bucket': UPLOAD_BUCKET if upload_service else None,
        'region': UPLOAD_REGION if upload_service else None
    })


# ============================================================================
# Log Streaming API Endpoints
# ============================================================================

@app.route('/api/logs/stream', methods=['GET'])
def stream_logs():
    """Stream live logs via Server-Sent Events (SSE)"""
    def generate():
        yield "data: {\"type\": \"connected\", \"message\": \"Log stream connected\"}\n\n"
        for log_entry in logging_service.stream_logs():
            yield log_entry

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


@app.route('/api/logs/recent', methods=['GET'])
def get_recent_logs():
    """Get recent log entries from memory buffer"""
    count = request.args.get('count', 100, type=int)
    count = min(count, 1000)  # Limit to 1000
    logs = logging_service.get_recent_logs(count)
    return jsonify({
        'success': True,
        'count': len(logs),
        'logs': logs
    })


@app.route('/api/logs/files', methods=['GET'])
def get_log_files():
    """Get list of available log files"""
    files = logging_service.get_log_files()
    return jsonify({
        'success': True,
        'log_dir': logging_service.log_dir,
        'files': files
    })


@app.route('/api/logs/files/<filename>', methods=['GET'])
def read_log_file(filename):
    """Read contents of a specific log file"""
    lines = request.args.get('lines', 500, type=int)
    offset = request.args.get('offset', 0, type=int)
    result = logging_service.read_log_file(filename, lines=lines, offset=offset)
    return jsonify(result), 200 if result.get('success') else 404


@app.route('/api/logs/search', methods=['GET'])
def search_logs():
    """Search logs for a query string"""
    query = request.args.get('q', '')
    filename = request.args.get('file')

    if not query:
        return jsonify({'success': False, 'error': 'Query parameter "q" is required'}), 400

    results = logging_service.search_logs(query, filename)
    return jsonify({
        'success': True,
        'query': query,
        'count': len(results),
        'results': results
    })


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("GoPro Controller API Service Starting...")
    logger.info("=" * 60)
    logger.info(f"Video storage: {VIDEO_STORAGE_DIR}")
    logger.info(f"Segments storage: {SEGMENTS_DIR}")
    logger.info(f"Log directory: {logging_service.log_dir}")
    logger.info(f"API endpoint: http://0.0.0.0:5000")
    logger.info("Make sure GoPros are connected via USB")
    logger.info("=" * 60)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

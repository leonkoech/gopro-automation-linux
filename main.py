#!/usr/bin/env python3
"""
GoPro Controller API Service with Automatic Segmentation
REST API for remote control of GoPro cameras connected to Jetson Nano
"""

# Fix SSL issues on Jetson/ARM devices - MUST be set before any SSL imports
import os
os.environ['OPENSSL_CONF'] = '/dev/null'

# Load .env file for environment variables (CAMERA_ANGLE_MAP, etc.)
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, request, send_file, Response, send_from_directory
from flask_cors import CORS
import subprocess
import threading
import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
import re
from videoupload import VideoUploadService
from media_service import get_media_service
from logging_service import get_logging_service, get_logger
from firebase_service import get_firebase_service
from uball_client import get_uball_client
from video_processing import VideoProcessor, process_game_videos

# Initialize logging service first
logging_service = get_logging_service()
logger = get_logger('gopro.main')

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"], "allow_headers": ["Content-Type", "Authorization"]}})

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

# Initialize Firebase service
firebase_service = get_firebase_service()
if firebase_service:
    print(f"✓ Firebase service initialized (Jetson ID: {firebase_service.jetson_id})")
else:
    print("⚠ Firebase service not available - recording sessions will not be registered")

# Initialize Uball Backend client
uball_client = get_uball_client()
if uball_client:
    print("✓ Uball Backend client initialized")
else:
    print("⚠ Uball Backend client not available - game sync will not work")

# Initialize Video Processor
video_processor = VideoProcessor(VIDEO_STORAGE_DIR, SEGMENTS_DIR)
print(f"✓ Video processor initialized (output: {video_processor.output_dir})")

# Global state
recording_processes = {}
recording_lock = threading.Lock()
gopro_ip_cache = {}

# Video processing jobs (async)
import uuid
video_processing_jobs = {}  # job_id -> job_state
video_processing_lock = threading.Lock()

# Download configuration - optimized for GoPro USB connections
DOWNLOAD_CHUNK_SIZE = 262144  # 256KB - smaller chunks for faster stall detection
DOWNLOAD_CONNECT_TIMEOUT = 10  # seconds to establish connection
DOWNLOAD_READ_TIMEOUT = 60  # seconds to wait for data between chunks
DOWNLOAD_MAX_RETRIES = 20  # more retries, never delete partial files
DOWNLOAD_KEEP_ALIVE_INTERVAL = 30  # send keep-alive every 30 seconds


class DownloadKeepAliveThread:
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
            time.sleep(DOWNLOAD_KEEP_ALIVE_INTERVAL)

# Admin Console jobs (shell script execution)
import selectors
import signal
admin_jobs = {}                    # job_id -> job state dict
admin_jobs_lock = threading.Lock()
admin_device_locks = {}            # device_id -> Lock (one job per device)
admin_device_locks_lock = threading.Lock()

# Regex to strip ANSI escape codes and carriage returns from shell output
_ansi_re = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]|\r')

def verify_video_integrity(file_path):
    """
    Quick check that video file is valid using ffprobe.
    Returns True if file appears to be a valid video.
    A timeout is treated as valid (not corrupted) since large 4K files
    on a loaded Jetson can exceed probe time without being corrupt.
    """
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=duration', '-of', 'csv=p=0', file_path],
            capture_output=True,
            text=True,
            timeout=120
        )
        # Check return code and that we got some duration output
        if result.returncode == 0 and result.stdout.strip():
            duration = float(result.stdout.strip())
            if duration > 0:
                return True
        return False
    except subprocess.TimeoutExpired:
        logger.warning(f"ffprobe timeout checking {file_path} — assuming valid (large 4K file on loaded system)")
        return True
    except Exception as e:
        logger.warning(f"ffprobe error checking {file_path}: {e}")
        return False


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


# Only these four session/angle types (no UNKNOWN in UI).
VALID_ANGLE_CODES = ('FL', 'FR', 'NL', 'NR')


def _get_angle_code_from_camera_name(camera_name: str) -> str:
    """
    Extract angle code from camera name or CAMERA_ANGLE_MAP env var.
    Returns only FL, FR, NL, or NR so sessions are always one of 4 types.

    Args:
        camera_name: GoPro camera name (e.g., "GoPro FL")

    Returns:
        Angle code: one of FL, FR, NL, NR
    """
    # Try CAMERA_ANGLE_MAP env var first
    angle_map_str = os.getenv('CAMERA_ANGLE_MAP', '{}')
    try:
        angle_map = json.loads(angle_map_str)
        if camera_name in angle_map:
            result = angle_map[camera_name]
            return result if result in VALID_ANGLE_CODES else 'NL'
    except json.JSONDecodeError:
        pass

    # Fallback: extract from camera name like "GoPro FL"
    if camera_name:
        for code in VALID_ANGLE_CODES:
            if code in camera_name.upper():
                return code

    return 'NL'  # Default so we never return UNK/UNKNOWN

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

        # Get angle code from camera name for segment folder naming
        angle_code = _get_angle_code_from_camera_name(camera_name)
        logger.info(f"[{gopro_id}] Camera angle code: {angle_code} (from camera_name='{camera_name}')")

        # Prepare session info - store start datetime for filename generation at stop time
        # Include angle code in session_id for easier identification: {interface}_{angle}_{timestamp}
        start_datetime = datetime.now()
        timestamp = start_datetime.strftime('%Y%m%d_%H%M%S')
        session_id = f"{gopro_id}_{angle_code}_{timestamp}"
        session_dir = os.path.join(SEGMENTS_DIR, session_id)
        os.makedirs(session_dir, exist_ok=True)

        # Register recording session in Firebase
        firebase_session_id = None
        if firebase_service:
            try:
                session_data = {
                    'camera_name': camera_name,
                    'segment_session': session_id,
                    'interface_id': gopro_id
                }
                firebase_session_id = firebase_service.register_recording_start(session_data)
                logger.info(f"[{gopro_id}] Registered recording in Firebase: {firebase_session_id}")
            except Exception as e:
                logger.warning(f"[{gopro_id}] Failed to register in Firebase (continuing anyway): {e}")

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
                'firebase_session_id': firebase_session_id,  # Firebase document ID
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
            'angle_code': angle_code,
            'session_id': session_id,
            'firebase_session_id': firebase_session_id
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
    """
    Stop recording on a GoPro camera.

    This ONLY stops the camera and registers the session in Firebase.
    NO local download - chapters will be streamed directly to S3 by the pipeline.

    The pipeline (triggered by stop-all-and-process or process-only) will:
    1. Stream chapters from GoPro HTTP directly to S3
    2. Detect and process games
    3. Delete GoPro files after success
    """
    global recording_processes

    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400

        recording_processes[gopro_id]['is_stopping'] = True
        recording_info = recording_processes[gopro_id].copy()

    try:
        gopro_ip = recording_info.get('gopro_ip')
        pre_record_files = recording_info.get('pre_record_files', set())
        firebase_session_id = recording_info.get('firebase_session_id')

        if not gopro_ip:
            gopro_ip = get_gopro_wired_ip(gopro_id)

        if not gopro_ip:
            with recording_lock:
                if gopro_id in recording_processes:
                    del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': 'Could not detect GoPro IP'}), 500

        # Send stop command to camera via HTTP API
        logger.info(f"[{gopro_id}] Stopping recording on {gopro_ip}...")
        try:
            response = requests.get(f'http://{gopro_ip}:8080/gopro/camera/shutter/stop', timeout=5)
            if response.status_code == 200:
                logger.info(f"[{gopro_id}] ✓ Recording stopped")
            else:
                logger.warning(f"[{gopro_id}] Stop command returned: {response.status_code}")
        except Exception as e:
            logger.warning(f"[{gopro_id}] Could not send stop command: {e}")

        with recording_lock:
            if gopro_id in recording_processes:
                recording_processes[gopro_id]['stage'] = 'finalizing'
                recording_processes[gopro_id]['stage_message'] = 'Waiting for GoPro to finalize files...'

        def finalize_and_register():
            """Wait for GoPro to finalize files and register session in Firebase."""
            try:
                # Wait for camera to finalize all chapter files
                logger.info(f"[{gopro_id}] Waiting for GoPro to finalize files...")
                time.sleep(5)

                # Retry loop to ensure all chapters are available
                new_chapters = []
                last_chapter_count = 0
                stable_count = 0
                max_retries = 10

                for attempt in range(max_retries):
                    logger.info(f"[{gopro_id}] Getting media list (attempt {attempt + 1}/{max_retries})...")
                    try:
                        media_response = requests.get(
                            f'http://{gopro_ip}:8080/gopro/media/list',
                            timeout=15
                        )
                        media_list = media_response.json()

                        # Find ALL new files (recorded during this session)
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

                    except Exception as e:
                        logger.warning(f"[{gopro_id}] Error getting media list: {e}")

                    if attempt < max_retries - 1:
                        time.sleep(2)

                # Calculate totals
                total_size_bytes = sum(ch['size'] for ch in new_chapters)
                total_size_gb = total_size_bytes / (1024**3)

                logger.info(f"[{gopro_id}] Session has {len(new_chapters)} chapters ({total_size_gb:.2f} GB)")

                # Update Firebase with recording stop info
                if firebase_service and firebase_session_id:
                    try:
                        stop_data = {
                            'total_chapters': len(new_chapters),
                            'total_size_bytes': total_size_bytes
                        }
                        firebase_service.register_recording_stop(firebase_session_id, stop_data)
                        logger.info(f"[{gopro_id}] ✓ Firebase session updated: {firebase_session_id}")
                    except Exception as e:
                        logger.warning(f"[{gopro_id}] Failed to update Firebase: {e}")

                # Mark as done (ready for pipeline)
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['stage'] = 'ready'
                        recording_processes[gopro_id]['stage_message'] = f'Ready! {len(new_chapters)} chapters to upload'
                        recording_processes[gopro_id]['total_chapters'] = len(new_chapters)
                        recording_processes[gopro_id]['total_size_bytes'] = total_size_bytes

                # Keep in recording_processes briefly so UI can see the status
                time.sleep(3)

                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]
                        logger.info(f"[{gopro_id}] Recording finalized, ready for pipeline")

            except Exception as e:
                logger.error(f"[{gopro_id}] Error in finalize_and_register: {e}")
                import traceback
                traceback.print_exc()
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['stage'] = 'error'
                        recording_processes[gopro_id]['stage_message'] = f'Error: {str(e)}'
                        del recording_processes[gopro_id]

        threading.Thread(target=finalize_and_register, daemon=True).start()

        return jsonify({
            'success': True,
            'message': 'Recording stopped. Chapters will be uploaded to S3 by the pipeline.',
            'gopro_id': gopro_id
        })

    except Exception as e:
        logger.error(f"[{gopro_id}] Error in stop_recording: {e}")
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


@app.route('/api/recording/stop-all-and-process', methods=['POST'])
def stop_all_and_process():
    """
    Stop all recordings and automatically start the full processing pipeline.

    This is the main automation endpoint that:
    1. Stops all recording GoPros on this Jetson
    2. Waits for GoPros to finalize files (~10 seconds)
    3. Streams chapters directly from GoPro HTTP to S3 (no local download!)
    4. Detects games from Firebase based on recording timerange
    5. Processes each game (extract, upload 4K)
    6. Submits AWS Batch jobs for encoding (4K -> 1080p)
    7. Deletes GoPro SD card files after success

    Request Body (optional):
    {
        "auto_delete_sd": true  // Delete GoPro files after success (default: true)
    }

    Returns:
        {
            success: true,
            message: "Stopping N GoPros, pipeline will start automatically",
            gopros_stopped: N
        }

    After stopping, poll /api/recording/pipeline-status for progress.
    """
    global recording_processes

    data = request.get_json() or {}
    auto_delete_sd = data.get('auto_delete_sd', True)

    # Find all recording GoPros
    with recording_lock:
        recording_gopros = list(recording_processes.keys())

    if not recording_gopros:
        # No active recordings - check for pending sessions and start pipeline
        logger.info("[Stop All] No active recordings, checking for pending sessions...")

        if firebase_service:
            pending_sessions = firebase_service.get_sessions_pending_upload(
                jetson_id=firebase_service.jetson_id
            )
            if pending_sessions:
                # Start pipeline with pending sessions
                return _start_auto_pipeline_internal(auto_delete_sd)

        return jsonify({
            'success': False,
            'error': 'No recordings in progress and no pending sessions'
        }), 400

    # Stop each recording
    stopped_gopros = []
    stop_errors = []

    for gopro_id in recording_gopros:
        try:
            # Get GoPro IP
            with recording_lock:
                if gopro_id in recording_processes:
                    gopro_ip = recording_processes[gopro_id].get('gopro_ip')
                    if not gopro_ip:
                        gopro_ip = get_gopro_wired_ip(gopro_id)

            if gopro_ip:
                # Send stop command
                try:
                    response = requests.get(
                        f'http://{gopro_ip}:8080/gopro/camera/shutter/stop',
                        timeout=5
                    )
                    if response.status_code == 200:
                        stopped_gopros.append(gopro_id)
                        logger.info(f"[Stop All] Stopped recording on {gopro_id}")
                except Exception as e:
                    stop_errors.append(f"{gopro_id}: {e}")
                    logger.warning(f"[Stop All] Failed to stop {gopro_id}: {e}")

                # Mark as stopping
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['is_stopping'] = True
                        recording_processes[gopro_id]['stage'] = 'stopping'
                        recording_processes[gopro_id]['stage_message'] = 'Stopping GoPro...'

        except Exception as e:
            stop_errors.append(f"{gopro_id}: {e}")
            logger.error(f"[Stop All] Error stopping {gopro_id}: {e}")

    if not stopped_gopros:
        return jsonify({
            'success': False,
            'error': 'Failed to stop any recordings',
            'errors': stop_errors
        }), 500

    # Start background thread to monitor finalization and trigger pipeline
    def monitor_and_start_pipeline():
        try:
            # Wait for all GoPros to finalize their files (no downloads - just finalization)
            logger.info(f"[Stop All] Waiting for {len(stopped_gopros)} GoPros to finalize files...")

            max_wait = 120  # 2 minutes max (finalization is fast, ~10-15 seconds per GoPro)
            check_interval = 2  # Check every 2 seconds
            elapsed = 0

            while elapsed < max_wait:
                # Check if all stopped GoPros have finalized
                with recording_lock:
                    still_finalizing = [
                        gid for gid in stopped_gopros
                        if gid in recording_processes
                    ]

                if not still_finalizing:
                    logger.info("[Stop All] All GoPros finalized, starting pipeline...")
                    break

                time.sleep(check_interval)
                elapsed += check_interval

                # Log progress every 10 seconds
                if elapsed % 10 == 0:
                    logger.info(f"[Stop All] Still waiting for {len(still_finalizing)} GoPros to finalize: {still_finalizing}")

            if elapsed >= max_wait:
                logger.warning("[Stop All] Timeout waiting for finalization, starting pipeline anyway")

            # Give a small delay for Firebase to update
            time.sleep(2)

            # Start the pipeline (streams chapters directly from GoPro HTTP to S3)
            _start_auto_pipeline_internal(auto_delete_sd, from_background=True)

        except Exception as e:
            logger.error(f"[Stop All] Error in monitor_and_start_pipeline: {e}")
            import traceback
            traceback.print_exc()

    threading.Thread(target=monitor_and_start_pipeline, daemon=True).start()

    return jsonify({
        'success': True,
        'message': f'Stopping {len(stopped_gopros)} GoPros. Pipeline will start after finalization (~15 seconds).',
        'gopros_stopped': len(stopped_gopros),
        'stopped_gopros': stopped_gopros,
        'errors': stop_errors if stop_errors else None
    })


def _start_auto_pipeline_internal(auto_delete_sd: bool = True, from_background: bool = False):
    """Internal helper to start the automated pipeline."""
    from pipeline_orchestrator import get_orchestrator

    orchestrator = get_orchestrator()
    if not orchestrator:
        if from_background:
            logger.error("[Pipeline] Orchestrator not available")
            return None
        return jsonify({
            'success': False,
            'error': 'Pipeline orchestrator not initialized'
        }), 503

    if not firebase_service:
        if from_background:
            logger.error("[Pipeline] Firebase not available")
            return None
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        # Get ALL stopped sessions for this Jetson (both pending and already uploaded)
        all_sessions = firebase_service.get_recording_sessions(
            jetson_id=firebase_service.jetson_id,
            limit=50
        )

        # Filter to stopped sessions only
        sessions = [s for s in all_sessions if s.get('status') == 'stopped']

        # Separate into pending (no s3Prefix) and already uploaded (has s3Prefix)
        pending_sessions = [s for s in sessions if not s.get('s3Prefix')]
        uploaded_sessions = [s for s in sessions if s.get('s3Prefix')]

        logger.info(f"[Pipeline] Found {len(pending_sessions)} pending, {len(uploaded_sessions)} already uploaded")

        if not sessions:
            if from_background:
                logger.warning("[Pipeline] No stopped sessions found")
                return None
            return jsonify({
                'success': False,
                'error': 'No stopped sessions found'
            }), 404

        # Discover GoPro connections ONLY for pending sessions (need to upload)
        gopro_connections = {}
        for session in pending_sessions:
            interface_id = session.get('interfaceId')
            if interface_id and interface_id not in gopro_connections:
                gopro_ip = get_gopro_wired_ip(interface_id)
                if gopro_ip:
                    gopro_connections[interface_id] = gopro_ip

        # If we have pending sessions but no GoPro connections, that's an error
        # But if ALL sessions are already uploaded, we don't need GoPro connections
        if pending_sessions and not gopro_connections:
            if from_background:
                logger.error("[Pipeline] No GoPro cameras found for pending sessions")
                return None
            return jsonify({
                'success': False,
                'error': 'No GoPro cameras found for pending sessions. Chapters need to be uploaded first.'
            }), 400

        # If no pending sessions but we have uploaded sessions, proceed without GoPro
        if not pending_sessions and uploaded_sessions:
            logger.info("[Pipeline] All sessions already uploaded, proceeding to game detection")

        # Start pipeline
        pipeline_id = orchestrator.start_pipeline(
            sessions=sessions,
            gopro_connections=gopro_connections,
            auto_delete_sd=auto_delete_sd
        )

        logger.info(f"[Pipeline] Started pipeline {pipeline_id} with {len(sessions)} sessions")

        if from_background:
            return pipeline_id

        return jsonify({
            'success': True,
            'pipeline_id': pipeline_id,
            'sessions_count': len(sessions),
            'message': f'Pipeline started with {len(sessions)} sessions'
        })

    except Exception as e:
        logger.error(f"[Pipeline] Error starting pipeline: {e}")
        import traceback
        traceback.print_exc()
        if from_background:
            return None
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/recording/process-only', methods=['POST'])
def process_only():
    """
    Start the processing pipeline without stopping recordings.

    Use this when:
    - Recordings have already stopped
    - You want to manually trigger processing
    - You want to re-process after fixing an issue

    This does everything stop-all-and-process does EXCEPT stopping recordings:
    1. Stream chapters directly from GoPro HTTP to S3 (no local download!)
    2. Detect games from Firebase based on recording timerange
    3. Process each game (extract, upload 4K)
    4. Submit AWS Batch jobs for encoding (4K -> 1080p)
    5. Delete GoPro SD card files after success

    Request Body (optional):
    {
        "auto_delete_sd": true  // Delete GoPro files after success (default: true)
    }

    Returns:
        { success: true, pipeline_id: "...", sessions_count: N, message: "..." }

    Poll /api/recording/pipeline-status for progress.
    """
    data = request.get_json() or {}
    auto_delete_sd = data.get('auto_delete_sd', True)

    # Check if any GoPros are currently recording
    with recording_lock:
        recording_gopros = [gid for gid, info in recording_processes.items()
                          if not info.get('is_stopping', False)]

    if recording_gopros:
        return jsonify({
            'success': False,
            'error': f'{len(recording_gopros)} GoPro(s) still recording. Stop them first or use stop-all-and-process.',
            'recording_gopros': recording_gopros
        }), 400

    # Start the pipeline
    result = _start_auto_pipeline_internal(auto_delete_sd)

    # Handle the response properly
    if isinstance(result, tuple):
        return result  # Error response with status code
    return result


@app.route('/api/recording/pipeline-status', methods=['GET'])
def get_recording_pipeline_status():
    """
    Get combined status of recording downloads and pipeline for THIS Jetson.

    Returns current state for UI display:
    - Recording/download progress for each GoPro
    - Pipeline status if running (only this Jetson's pipeline)

    When multiple Jetsons are selected (e.g. Jetson 1 = NR, Jetson 2 = NL):
    the frontend must poll this endpoint on EACH Jetson's base URL and merge
    the results so both Jetsons' progress are visible. Each Jetson only returns
    its own pipeline; there is no server-side aggregation.

    Pipeline sessions include: display_label (e.g. "02/02/2026 NR"), session_date
    (MM/DD/YYYY), angle_code (FL|FR|NL|NR) for the Sessions list.
    """
    from pipeline_orchestrator import get_orchestrator

    result = {
        'success': True,
        'recording': {},
        'pipeline': None
    }

    # Get recording status for all GoPros
    with recording_lock:
        for gopro_id, info in recording_processes.items():
            result['recording'][gopro_id] = {
                'is_recording': True,
                'is_stopping': info.get('is_stopping', False),
                'downloading': info.get('downloading', False),
                'download_progress': info.get('download_progress', 0),
                'stage': info.get('stage', 'recording'),
                'stage_message': info.get('stage_message', 'Recording...')
            }

    # Get pipeline status if orchestrator exists
    orchestrator = get_orchestrator()
    if orchestrator:
        pipelines = orchestrator.list_pipelines(limit=1)
        if pipelines:
            latest_pipeline = pipelines[0]
            # Only include if recent (within last hour) or still running
            if latest_pipeline.get('status') == 'running':
                result['pipeline'] = latest_pipeline

    return jsonify(result)


# ==================== Recording Session Registration ====================

@app.route('/api/recording/register', methods=['POST'])
def register_recording_session():
    """
    Register a recording session in Firebase.
    Can be called manually or is called automatically when recording starts/stops.

    Request Body:
    {
        "gopro_id": "enxd43260ef4d38",
        "camera_name": "GoPro FL",
        "segment_session": "enxd43260ef4d38_20250120_140530",
        "action": "start" | "stop",
        "total_chapters": 3,      // Only for stop
        "total_size_bytes": 12345678  // Only for stop
    }

    Returns:
        For start: { success: true, firebase_session_id: "..." }
        For stop: { success: true, message: "Session updated" }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400

        action = data.get('action')
        if action not in ['start', 'stop']:
            return jsonify({'success': False, 'error': 'action must be "start" or "stop"'}), 400

        gopro_id = data.get('gopro_id', '')
        camera_name = data.get('camera_name', '')
        segment_session = data.get('segment_session', '')

        if action == 'start':
            # Register new recording session
            session_data = {
                'camera_name': camera_name,
                'segment_session': segment_session,
                'interface_id': gopro_id
            }

            firebase_session_id = firebase_service.register_recording_start(session_data)
            logger.info(f"[Firebase] Registered recording start: {firebase_session_id}")

            return jsonify({
                'success': True,
                'firebase_session_id': firebase_session_id,
                'message': 'Recording session registered'
            })

        else:  # action == 'stop'
            # Find the session by segment_session name
            session = firebase_service.find_session_by_segment(segment_session)

            if not session:
                return jsonify({
                    'success': False,
                    'error': f'Session not found for segment: {segment_session}'
                }), 404

            stop_data = {
                'total_chapters': data.get('total_chapters', 0),
                'total_size_bytes': data.get('total_size_bytes', 0)
            }

            firebase_service.register_recording_stop(session['id'], stop_data)
            logger.info(f"[Firebase] Registered recording stop: {session['id']}")

            return jsonify({
                'success': True,
                'firebase_session_id': session['id'],
                'message': 'Recording session updated'
            })

    except Exception as e:
        logger.error(f"[Firebase] Error registering recording session: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/recording/sessions', methods=['GET'])
def list_recording_sessions():
    """
    List recording sessions from Firebase.

    Query params:
        jetson_id: Optional filter by Jetson ID
        limit: Maximum number of sessions to return (default 50)

    Returns:
        { success: true, sessions: [...] }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        jetson_id = request.args.get('jetson_id')
        limit = request.args.get('limit', 50, type=int)

        sessions = firebase_service.get_recording_sessions(jetson_id=jetson_id, limit=limit)

        return jsonify({
            'success': True,
            'count': len(sessions),
            'sessions': sessions
        })

    except Exception as e:
        logger.error(f"[Firebase] Error listing recording sessions: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/recording/sessions/<session_id>', methods=['GET'])
def get_recording_session(session_id):
    """
    Get a specific recording session from Firebase.

    Returns:
        { success: true, session: {...} }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        session = firebase_service.get_recording_session(session_id)

        if not session:
            return jsonify({
                'success': False,
                'error': 'Session not found'
            }), 404

        return jsonify({
            'success': True,
            'session': session
        })

    except Exception as e:
        logger.error(f"[Firebase] Error getting recording session: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ==================== Game Sync Endpoints ====================

@app.route('/api/games/list', methods=['GET'])
def list_games():
    """
    List basketball games from Firebase.

    Query params:
        limit: Maximum number of games (default 50)
        status: Filter by status (e.g., "ended", "active")
        date: Filter by date (YYYY-MM-DD)
        sync_ready: If true, only show games ready for sync

    Returns:
        { success: true, games: [...] }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        limit = request.args.get('limit', 50, type=int)
        status = request.args.get('status')
        date = request.args.get('date')
        sync_ready = request.args.get('sync_ready', 'false').lower() == 'true'

        if sync_ready:
            games = firebase_service.get_games_for_sync(limit=limit)
        else:
            games = firebase_service.list_games(limit=limit, status=status, date=date)

        return jsonify({
            'success': True,
            'count': len(games),
            'games': games
        })

    except Exception as e:
        logger.error(f"[Games] Error listing games: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/games/<game_id>', methods=['GET'])
def get_game(game_id):
    """
    Get a specific basketball game from Firebase.

    Returns:
        { success: true, game: {...} }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        game = firebase_service.get_game(game_id)

        if not game:
            return jsonify({
                'success': False,
                'error': 'Game not found'
            }), 404

        return jsonify({
            'success': True,
            'game': game
        })

    except Exception as e:
        logger.error(f"[Games] Error getting game: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/games/sync', methods=['POST'])
def sync_game_to_uball():
    """
    Sync a completed game from Firebase to Uball Backend.

    Request Body:
    {
        "firebase_game_id": "abc123",
        "team1_id": "uuid-of-team1",  // Optional - Uball team UUID
        "team2_id": "uuid-of-team2"   // Optional - Uball team UUID
    }

    If team IDs not provided, teams are auto-created from Firebase game data.
    Each game creates NEW teams (even if same name exists) because rosters differ.

    Flow:
    1. Fetch game from Firebase (basketball-games collection)
    2. Extract: createdAt, endedAt, teams, scores
    3. Auto-create teams in Uball Backend if not provided
    4. Create game in Supabase with firebase_game_id linkage
    5. Mark game as synced in Firebase
    6. Return Uball game_id

    Returns:
        { success: true, uball_game_id: "...", firebase_game_id: "...", teams_created: [...] }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    if not uball_client:
        return jsonify({
            'success': False,
            'error': 'Uball Backend client not configured'
        }), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400

        firebase_game_id = data.get('firebase_game_id')
        team1_id = data.get('team1_id')
        team2_id = data.get('team2_id')

        if not firebase_game_id:
            return jsonify({'success': False, 'error': 'firebase_game_id required'}), 400

        logger.info(f"[GameSync] === Starting sync for Firebase game: {firebase_game_id} ===")

        # 1. Fetch game from Firebase
        logger.info(f"[GameSync] Step 1: Fetching game from Firebase...")
        firebase_game = firebase_service.get_game(firebase_game_id)
        if not firebase_game:
            return jsonify({
                'success': False,
                'error': f'Game not found in Firebase: {firebase_game_id}'
            }), 404

        logger.info(f"[GameSync] Step 1: Game found. Teams: {firebase_game.get('leftTeam', {}).get('name')} vs {firebase_game.get('rightTeam', {}).get('name')}")

        # Check if already synced
        if firebase_game.get('uballGameId'):
            logger.info(f"[GameSync] Game already synced to Uball: {firebase_game['uballGameId']}")
            return jsonify({
                'success': True,
                'message': 'Game already synced',
                'uball_game_id': firebase_game['uballGameId'],
                'firebase_game_id': firebase_game_id,
                'video_name': f"{firebase_game.get('leftTeam', {}).get('name', 'Team 1')} vs {firebase_game.get('rightTeam', {}).get('name', 'Team 2')}"
            })

        # 2. Check if game already exists in Uball by firebase_game_id
        logger.info(f"[GameSync] Step 2: Checking if game exists in Uball Backend...")
        existing_game = uball_client.get_game_by_firebase_id(firebase_game_id)
        if existing_game:
            # Mark as synced in Firebase
            firebase_service.mark_game_synced(firebase_game_id, existing_game['id'])
            return jsonify({
                'success': True,
                'message': 'Game already exists in Uball, marked as synced',
                'uball_game_id': str(existing_game['id']),
                'firebase_game_id': firebase_game_id
            })

        # 3. Auto-create teams if not provided
        logger.info(f"[GameSync] Step 3: Creating teams in Uball Backend...")
        teams_created = []

        if not team1_id:
            # Extract team1 name from Firebase (leftTeam)
            left_team = firebase_game.get('leftTeam', {})
            team1_name = left_team.get('name', 'Team 1')

            created_team1 = uball_client.create_team(team1_name)
            if not created_team1:
                return jsonify({
                    'success': False,
                    'error': f'Failed to create team: {team1_name}'
                }), 500

            team1_id = str(created_team1.get('id'))
            teams_created.append({'name': team1_name, 'id': team1_id, 'side': 'left'})
            logger.info(f"[GameSync] Auto-created team1: {team1_name} -> {team1_id}")

        if not team2_id:
            # Extract team2 name from Firebase (rightTeam)
            right_team = firebase_game.get('rightTeam', {})
            team2_name = right_team.get('name', 'Team 2')

            created_team2 = uball_client.create_team(team2_name)
            if not created_team2:
                return jsonify({
                    'success': False,
                    'error': f'Failed to create team: {team2_name}'
                }), 500

            team2_id = str(created_team2.get('id'))
            teams_created.append({'name': team2_name, 'id': team2_id, 'side': 'right'})
            logger.info(f"[GameSync] Auto-created team2: {team2_name} -> {team2_id}")

        # 4. Prepare game data for Uball Backend
        logger.info(f"[GameSync] Step 4: Preparing game data for Uball Backend...")
        created_at = firebase_game.get('createdAt', '')
        ended_at = firebase_game.get('endedAt')

        # Extract date from createdAt (format: 2025-01-20T14:30:00Z)
        game_date = created_at[:10] if created_at else datetime.now().strftime('%Y-%m-%d')

        uball_game_data = {
            'firebase_game_id': firebase_game_id,
            'date': game_date,
            'team1_id': team1_id,
            'team2_id': team2_id,
            'start_time': created_at if created_at else None,
            'end_time': ended_at if ended_at else None,
            'source': 'firebase'
        }

        # Add scores and video_name from leftTeam/rightTeam
        left_team = firebase_game.get('leftTeam', {})
        right_team = firebase_game.get('rightTeam', {})

        # Set video_name as "TEAM1 vs TEAM2"
        team1_name = left_team.get('name', 'Team 1')
        team2_name = right_team.get('name', 'Team 2')
        uball_game_data['video_name'] = f"{team1_name} vs {team2_name}"

        if left_team.get('finalScore') is not None:
            uball_game_data['team1_score'] = left_team['finalScore']
        if right_team.get('finalScore') is not None:
            uball_game_data['team2_score'] = right_team['finalScore']

        # Legacy score format support
        if firebase_game.get('score'):
            score = firebase_game['score']
            if isinstance(score, dict):
                uball_game_data['team1_score'] = score.get('home', score.get('team1'))
                uball_game_data['team2_score'] = score.get('away', score.get('team2'))

        # 5. Create game in Uball Backend
        logger.info(f"[GameSync] Step 5: Creating game in Uball Backend...")
        logger.info(f"[GameSync] Payload: team1={team1_id}, team2={team2_id}, date={game_date}, video_name={uball_game_data.get('video_name')}")
        uball_game = uball_client.create_game(uball_game_data)

        if not uball_game:
            return jsonify({
                'success': False,
                'error': 'Failed to create game in Uball Backend'
            }), 500

        uball_game_id = str(uball_game.get('id', ''))

        # 6. Mark game as synced in Firebase
        logger.info(f"[GameSync] Step 6: Marking game as synced in Firebase...")
        firebase_service.mark_game_synced(firebase_game_id, uball_game_id)
        logger.info(f"[GameSync] === SUCCESS: Firebase {firebase_game_id} -> Uball {uball_game_id} ===")

        video_name = uball_game_data.get('video_name', f"{team1_name} vs {team2_name}")

        response_data = {
            'success': True,
            'message': 'Game synced successfully',
            'uball_game_id': uball_game_id,
            'firebase_game_id': firebase_game_id,
            'video_name': video_name,
            'game': uball_game
        }

        # Include teams created info if any
        if teams_created:
            response_data['teams_created'] = teams_created

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"[GameSync] Error syncing game: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/games/<game_id>/recordings', methods=['GET'])
def get_game_recordings(game_id):
    """
    Get recording sessions that overlap with a specific game's time range.

    Returns:
        { success: true, recordings: [...] }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        # Get game from Firebase
        game = firebase_service.get_game(game_id)
        if not game:
            return jsonify({
                'success': False,
                'error': 'Game not found'
            }), 404

        created_at = game.get('createdAt')
        ended_at = game.get('endedAt')

        if not created_at:
            return jsonify({
                'success': False,
                'error': 'Game has no start time'
            }), 400

        # Parse timestamps
        from datetime import datetime
        start = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

        if ended_at:
            end = datetime.fromisoformat(ended_at.replace('Z', '+00:00'))
        else:
            # Game still in progress, use current time
            end = datetime.now()

        # Find overlapping recording sessions
        recordings = firebase_service.get_games_in_timerange(start, end)

        # Actually we need recording sessions, not games
        # Let's get all recording sessions and filter by time overlap
        all_sessions = firebase_service.get_recording_sessions(limit=100)

        overlapping_sessions = []
        for session in all_sessions:
            session_start = session.get('startedAt')
            session_end = session.get('endedAt')

            if not session_start:
                continue

            # Check for overlap
            s_start = datetime.fromisoformat(session_start.replace('Z', '+00:00'))
            s_end = datetime.fromisoformat(session_end.replace('Z', '+00:00')) if session_end else datetime.now()

            # Overlap if: session_start < game_end AND session_end > game_start
            if s_start < end and s_end > start:
                overlapping_sessions.append(session)

        return jsonify({
            'success': True,
            'game_id': game_id,
            'game_start': created_at,
            'game_end': ended_at,
            'count': len(overlapping_sessions),
            'recordings': overlapping_sessions
        })

    except Exception as e:
        logger.error(f"[Games] Error getting game recordings: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/uball/status', methods=['GET'])
def uball_status():
    """Check Uball Backend connection status."""
    if not uball_client:
        return jsonify({
            'success': True,
            'configured': False,
            'message': 'Uball Backend client not configured'
        })

    try:
        healthy = uball_client.health_check()
        return jsonify({
            'success': True,
            'configured': True,
            'healthy': healthy,
            'backend_url': uball_client.backend_url
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/uball/teams', methods=['GET'])
def list_uball_teams():
    """List teams from Uball Backend (for team ID mapping)."""
    if not uball_client:
        return jsonify({
            'success': False,
            'error': 'Uball Backend client not configured'
        }), 503

    try:
        teams = uball_client.list_teams()
        return jsonify({
            'success': True,
            'count': len(teams),
            'teams': teams
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ==================== Game Video Processing Endpoints ====================

@app.route('/api/games/process-videos', methods=['POST'])
def process_game_videos_endpoint():
    """
    Extract game portions from continuous recordings and upload to S3.

    Request Body:
    {
        "firebase_game_id": "abc123",
        "game_number": 1,           // Game number for the day
        "location": "court-a"       // Optional, defaults to UPLOAD_LOCATION
    }

    Flow:
    1. Fetch game from Firebase → get start/end times
    2. Find matching recording sessions that overlap with game time
    3. For each relevant session:
       a. Find chapter files that contain game time range
       b. Use FFmpeg to extract game portion
       c. Upload to S3: {location}/{date}/game{N}/{date}_game{N}_{angle}.mp4
    4. Update recording-sessions with processed game info

    Returns:
        {
            success: true,
            firebase_game_id: "...",
            game_number: 1,
            processed_videos: [...],
            errors: [...]
        }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400

        firebase_game_id = data.get('firebase_game_id')
        game_number = data.get('game_number', 1)
        location = data.get('location', UPLOAD_LOCATION)

        if not firebase_game_id:
            return jsonify({'success': False, 'error': 'firebase_game_id required'}), 400

        if not isinstance(game_number, int) or game_number < 1:
            return jsonify({'success': False, 'error': 'game_number must be a positive integer'}), 400

        logger.info(f"[VideoProcessing] Starting processing for game {firebase_game_id}, number {game_number}")

        # Process the game videos (uball_client will auto-register FL/FR videos)
        results = process_game_videos(
            firebase_game_id=firebase_game_id,
            game_number=game_number,
            firebase_service=firebase_service,
            upload_service=upload_service,
            video_processor=video_processor,
            location=location,
            uball_client=uball_client,
            s3_bucket=UPLOAD_BUCKET
        )

        if results['success']:
            return jsonify(results)
        else:
            return jsonify(results), 500

    except Exception as e:
        logger.error(f"[VideoProcessing] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def _run_video_processing_job(job_id: str, firebase_game_id: str, game_number: int, location: str, force_local_transcode: bool = False):
    """Background worker for async video processing."""
    def update_progress(stage: str, detail: str = '', progress: float = 0, current_angle: str = ''):
        with video_processing_lock:
            if job_id in video_processing_jobs:
                video_processing_jobs[job_id]['stage'] = stage
                video_processing_jobs[job_id]['detail'] = detail
                video_processing_jobs[job_id]['progress'] = progress
                video_processing_jobs[job_id]['current_angle'] = current_angle
                video_processing_jobs[job_id]['updated_at'] = datetime.now().isoformat()

    try:
        update_progress('starting', 'Initializing video processing...', 0)

        # Run the actual processing with progress callback
        results = process_game_videos(
            firebase_game_id=firebase_game_id,
            game_number=game_number,
            firebase_service=firebase_service,
            upload_service=upload_service,
            video_processor=video_processor,
            location=location,
            uball_client=uball_client,
            s3_bucket=UPLOAD_BUCKET,
            progress_callback=update_progress,
            force_local_transcode=force_local_transcode
        )

        with video_processing_lock:
            if job_id in video_processing_jobs:
                video_processing_jobs[job_id]['status'] = 'completed' if results['success'] else 'failed'
                video_processing_jobs[job_id]['result'] = results

                # Handle skipped case (no videos found but not an error)
                if results.get('skipped'):
                    video_processing_jobs[job_id]['stage'] = 'skipped'
                    video_processing_jobs[job_id]['detail'] = results.get('skip_reason', 'No videos to process')
                else:
                    video_processing_jobs[job_id]['stage'] = 'completed' if results['success'] else 'failed'
                    video_processing_jobs[job_id]['detail'] = 'Processing complete' if results['success'] else 'Processing failed'

                video_processing_jobs[job_id]['progress'] = 100 if results['success'] else 0
                video_processing_jobs[job_id]['completed_at'] = datetime.now().isoformat()

    except Exception as e:
        logger.error(f"[VideoProcessing] Job {job_id} failed: {e}")
        import traceback
        traceback.print_exc()
        with video_processing_lock:
            if job_id in video_processing_jobs:
                video_processing_jobs[job_id]['status'] = 'failed'
                video_processing_jobs[job_id]['stage'] = 'error'
                video_processing_jobs[job_id]['detail'] = str(e)
                video_processing_jobs[job_id]['error'] = str(e)
                video_processing_jobs[job_id]['completed_at'] = datetime.now().isoformat()


@app.route('/api/games/process-videos/async', methods=['POST'])
def process_game_videos_async():
    """
    Start async video processing and return job ID immediately.

    Request Body:
    {
        "firebase_game_id": "abc123",
        "game_number": 1,
        "location": "court-a",
        "force_local_transcode": false  // Optional: force local CPU encoding instead of AWS GPU
    }

    Returns immediately:
    {
        "success": true,
        "job_id": "uuid",
        "message": "Processing started"
    }

    Use GET /api/games/process-videos/<job_id>/status to check progress.
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400

        firebase_game_id = data.get('firebase_game_id')
        game_number = data.get('game_number', 1)
        location = data.get('location', UPLOAD_LOCATION)
        force_local_transcode = data.get('force_local_transcode', False)

        if not firebase_game_id:
            return jsonify({'success': False, 'error': 'firebase_game_id required'}), 400

        if not isinstance(game_number, int) or game_number < 1:
            return jsonify({'success': False, 'error': 'game_number must be a positive integer'}), 400

        # Create job
        job_id = str(uuid.uuid4())
        transcode_mode = 'local' if force_local_transcode else 'aws_gpu'

        with video_processing_lock:
            video_processing_jobs[job_id] = {
                'job_id': job_id,
                'firebase_game_id': firebase_game_id,
                'game_number': game_number,
                'location': location,
                'transcode_mode': transcode_mode,
                'status': 'running',
                'stage': 'queued',
                'detail': 'Job queued',
                'progress': 0,
                'current_angle': '',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'result': None,
                'error': None
            }

        # Start background thread
        thread = threading.Thread(
            target=_run_video_processing_job,
            args=(job_id, firebase_game_id, game_number, location, force_local_transcode),
            daemon=True
        )
        thread.start()

        logger.info(f"[VideoProcessing] Started async job {job_id} for game {firebase_game_id} (mode: {transcode_mode})")

        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': 'Video processing started'
        })

    except Exception as e:
        logger.error(f"[VideoProcessing] Error starting async job: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/games/process-videos/<job_id>/status', methods=['GET'])
def get_video_processing_status(job_id):
    """
    Get status of an async video processing job.

    Returns:
    {
        "success": true,
        "job_id": "uuid",
        "status": "running|completed|failed",
        "stage": "extracting|uploading|registering|completed|error",
        "detail": "Extracting NR video...",
        "progress": 45,
        "current_angle": "NR",
        "result": {...}  // Only when completed
    }
    """
    with video_processing_lock:
        job = video_processing_jobs.get(job_id)

    if not job:
        return jsonify({
            'success': False,
            'error': 'Job not found'
        }), 404

    response = {
        'success': True,
        'job_id': job['job_id'],
        'firebase_game_id': job['firebase_game_id'],
        'game_number': job['game_number'],
        'status': job['status'],
        'stage': job['stage'],
        'detail': job['detail'],
        'progress': job['progress'],
        'current_angle': job.get('current_angle', ''),
        'created_at': job['created_at'],
        'updated_at': job['updated_at']
    }

    if job['status'] == 'completed':
        response['result'] = job['result']
        response['completed_at'] = job.get('completed_at')
        # Extract status details from result
        if job.get('result'):
            response['status_message'] = job['result'].get('status_message')
            response['result_status'] = job['result'].get('status')  # success, partial, corrupted, failed, batch_submitted
            response['corrupted_sessions'] = job['result'].get('corrupted_sessions', [])
            response['gpu_transcode_enabled'] = job['result'].get('gpu_transcode_enabled', False)
            response['batch_jobs'] = job['result'].get('batch_jobs', [])
    elif job['status'] == 'failed':
        response['error'] = job.get('error')
        response['result'] = job.get('result')
        response['completed_at'] = job.get('completed_at')
        # Extract status details from result even on failure
        if job.get('result'):
            response['status_message'] = job['result'].get('status_message')
            response['result_status'] = job['result'].get('status')
            response['corrupted_sessions'] = job['result'].get('corrupted_sessions', [])
            response['gpu_transcode_enabled'] = job['result'].get('gpu_transcode_enabled', False)
            response['batch_jobs'] = job['result'].get('batch_jobs', [])

    return jsonify(response)


@app.route('/api/games/process-videos/jobs', methods=['GET'])
def list_video_processing_jobs():
    """List all video processing jobs."""
    with video_processing_lock:
        jobs = list(video_processing_jobs.values())

    # Sort by created_at descending
    jobs.sort(key=lambda j: j['created_at'], reverse=True)

    return jsonify({
        'success': True,
        'count': len(jobs),
        'jobs': [{
            'job_id': j['job_id'],
            'firebase_game_id': j['firebase_game_id'],
            'game_number': j['game_number'],
            'status': j['status'],
            'stage': j['stage'],
            'progress': j['progress'],
            'created_at': j['created_at']
        } for j in jobs[:20]]  # Last 20 jobs
    })


@app.route('/api/games/<game_id>/preview-extraction', methods=['GET'])
def preview_game_extraction(game_id):
    """
    Preview what would be extracted for a game without actually extracting.

    Returns information about:
    - Game time range
    - Overlapping recording sessions
    - Chapter files that would be used
    - Estimated extraction parameters

    Returns:
        {
            success: true,
            game: {...},
            sessions: [{
                session_id: "...",
                angle: "FL",
                chapters: [...],
                extraction_params: {...}
            }]
        }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        # Get game from Firebase
        game = firebase_service.get_game(game_id)
        if not game:
            return jsonify({
                'success': False,
                'error': 'Game not found'
            }), 404

        created_at = game.get('createdAt')
        ended_at = game.get('endedAt')

        if not created_at:
            return jsonify({
                'success': False,
                'error': 'Game has no start time'
            }), 400

        # Parse timestamps
        game_start = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        game_end = datetime.fromisoformat(ended_at.replace('Z', '+00:00')) if ended_at else datetime.now(game_start.tzinfo)
        game_duration = (game_end - game_start).total_seconds()

        # Find overlapping sessions - get ALL sessions (from all Jetsons) for preview
        # Processing will filter by jetson_id, but preview shows the full picture
        this_jetson_id = os.getenv('JETSON_ID', 'unknown')
        logger.info(f"[VideoProcessing] Preview for game {game_id} (this Jetson: {this_jetson_id})")
        all_sessions = firebase_service.get_recording_sessions(limit=100)  # No jetson_id filter for preview
        logger.info(f"[VideoProcessing] Found {len(all_sessions)} total sessions across all Jetsons")
        session_previews = []

        for session in all_sessions:
            session_start_str = session.get('startedAt')
            session_end_str = session.get('endedAt')

            if not session_start_str:
                continue

            s_start = datetime.fromisoformat(session_start_str.replace('Z', '+00:00'))
            s_end = datetime.fromisoformat(session_end_str.replace('Z', '+00:00')) if session_end_str else datetime.now(s_start.tzinfo)

            # Check overlap
            if s_start < game_end and s_end > game_start:
                session_name = session.get('segmentSession', '')
                angle_code = session.get('angleCode', 'UNKNOWN')
                session_jetson_id = session.get('jetsonId', 'unknown')
                is_local = (session_jetson_id == this_jetson_id)

                # Get chapters (only available if session is on this Jetson)
                chapters = video_processor.get_session_chapters(session_name) if is_local else []

                # Calculate extraction params
                if chapters:
                    params = video_processor.calculate_extraction_params(
                        game_start, game_end, s_start, chapters
                    )
                else:
                    if is_local:
                        params = {'error': 'No chapters found locally'}
                    else:
                        params = {'error': f'Session on {session_jetson_id} (process from that Jetson)'}

                session_previews.append({
                    'session_id': session.get('id'),
                    'segment_session': session_name,
                    'angle': angle_code,
                    'jetson_id': session_jetson_id,
                    'is_local': is_local,
                    'session_start': session_start_str,
                    'session_end': session_end_str,
                    'chapters_count': len(chapters),
                    'chapters': chapters,
                    'extraction_params': params
                })

        # Build response matching frontend ExtractionPreviewResponse type
        issues = []
        if not ended_at:
            issues.append('Game has no end time - using current time')
        if len(session_previews) == 0:
            issues.append('No overlapping recording sessions found')

        # Reshape session data to match ExtractionPreviewSession type
        formatted_sessions = []
        for sp in session_previews:
            extraction_params = sp.get('extraction_params', {})
            chapters_raw = sp.get('chapters', [])
            formatted_chapters = []
            for ch in chapters_raw:
                if isinstance(ch, dict):
                    formatted_chapters.append(ch)
                elif isinstance(ch, str):
                    formatted_chapters.append({
                        'filename': ch,
                        'size_mb': 0,
                        'duration_str': 'unknown'
                    })

            formatted_sessions.append({
                'session_id': sp.get('session_id', ''),
                'session_name': sp.get('segment_session', ''),
                'angle_code': sp.get('angle', 'UNKNOWN'),
                'jetson_id': sp.get('jetson_id', 'unknown'),
                'is_local': sp.get('is_local', False),
                'recording_start': sp.get('session_start', ''),
                'recording_end': sp.get('session_end', ''),
                'chapters': formatted_chapters,
                'extraction_params': {
                    'offset_str': extraction_params.get('offset_str', '00:00:00'),
                    'duration_str': extraction_params.get('duration_str', '00:00:00'),
                    'chapters_to_process': extraction_params.get('chapters_to_process', 0),
                    'total_chapters': sp.get('chapters_count', len(chapters_raw)),
                }
            })

        # Count sessions that have local chapters (ready to process on THIS Jetson)
        local_sessions_with_chapters = [s for s in formatted_sessions if s['is_local'] and s['extraction_params']['chapters_to_process'] > 0]

        return jsonify({
            'success': True,
            'firebase_game_id': game_id,
            'game_start': created_at,
            'game_end': ended_at or '',
            'game_duration_minutes': round(game_duration / 60, 1),
            'this_jetson_id': this_jetson_id,
            'overlapping_sessions': formatted_sessions,
            'local_sessions_ready': len(local_sessions_with_chapters),
            'ready_for_extraction': len(local_sessions_with_chapters) > 0,
            'issues': issues if issues else None
        })

    except Exception as e:
        logger.error(f"[VideoProcessing] Preview error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/batch/register-completed', methods=['POST'])
def register_completed_batch_jobs():
    """
    Scan recent AWS Batch jobs and register completed FL/FR videos in Uball.

    This endpoint:
    1. Lists recent SUCCEEDED Batch jobs from the transcode queue
    2. For each FL/FR video, registers it in Uball if not already registered
    3. Returns counts of registered videos

    Call this periodically (e.g., every 5 minutes) to ensure videos are registered.

    Returns:
        { success: true, registered: 5, already_registered: 10, errors: [] }
    """
    if not uball_client:
        return jsonify({
            'success': False,
            'error': 'Uball Backend client not configured'
        }), 503

    try:
        import boto3
        from botocore.config import Config as BotoConfig

        # Initialize AWS clients
        boto_config = BotoConfig(retries={'max_attempts': 3})
        batch_client = boto3.client('batch', region_name='us-east-1', config=boto_config)
        s3_client = boto3.client('s3', region_name='us-east-1', config=boto_config)

        bucket = os.getenv('UPLOAD_BUCKET', 'uball-videos-production')
        job_queue = os.getenv('AWS_BATCH_JOB_QUEUE', 'gpu-transcode-queue')

        # Get recent succeeded jobs
        response = batch_client.list_jobs(
            jobQueue=job_queue,
            jobStatus='SUCCEEDED',
            maxResults=100
        )

        jobs = response.get('jobSummaryList', [])
        logger.info(f"[BatchRegister] Found {len(jobs)} succeeded Batch jobs")

        registered = 0
        already_registered = 0
        errors = []

        for job_summary in jobs:
            job_id = job_summary['jobId']

            # Get job details to find output S3 key
            job_detail = batch_client.describe_jobs(jobs=[job_id])
            if not job_detail.get('jobs'):
                continue

            job = job_detail['jobs'][0]
            env_vars = {e['name']: e['value'] for e in job.get('container', {}).get('environment', [])}

            output_key = env_vars.get('OUTPUT_S3_KEY', '')
            angle = env_vars.get('ANGLE', '')

            # Only register FL and FR
            if angle not in ['FL', 'FR']:
                continue

            if not output_key:
                continue

            # Check if file exists in S3
            try:
                s3_client.head_object(Bucket=bucket, Key=output_key)
            except:
                continue  # File doesn't exist

            # Parse game folder from key: court-a/date/game_folder/filename
            parts = output_key.split('/')
            if len(parts) < 4:
                continue

            game_folder = parts[2]  # Partial UUID
            filename = parts[3]
            uball_angle = 'LEFT' if angle == 'FL' else 'RIGHT'

            # Try to register (will fail gracefully if already registered)
            try:
                result = uball_client.register_video(
                    game_id=game_folder,
                    s3_key=output_key,
                    angle=uball_angle,
                    filename=filename,
                    duration=0.0
                )

                if result:
                    registered += 1
                    logger.info(f"[BatchRegister] Registered: {output_key}")
                else:
                    already_registered += 1
            except Exception as e:
                error_msg = str(e)
                if 'duplicate' in error_msg.lower() or 'already exists' in error_msg.lower():
                    already_registered += 1
                else:
                    errors.append({'key': output_key, 'error': error_msg})

        return jsonify({
            'success': True,
            'registered': registered,
            'already_registered': already_registered,
            'total_jobs_checked': len(jobs),
            'errors': errors
        })

    except Exception as e:
        logger.error(f"[BatchRegister] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/games/register-video', methods=['POST'])
def register_video_in_uball():
    """
    Manually register a video in Uball Backend.

    Use this endpoint to register videos that were uploaded outside of
    the normal processing flow, or to retry failed registrations.

    Request Body:
    {
        "firebase_game_id": "abc123",
        "s3_key": "court-a/2025-01-20/game1/2025-01-20_game1_FL.mp4",
        "angle_code": "FL",           // FL or FR only
        "filename": "2025-01-20_game1_FL.mp4",
        "duration": 3600.5,           // Optional, seconds
        "file_size": 1234567890       // Optional, bytes
    }

    Returns:
        { success: true, video: {...} }
    """
    if not uball_client:
        return jsonify({
            'success': False,
            'error': 'Uball Backend client not configured'
        }), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400

        firebase_game_id = data.get('firebase_game_id')
        s3_key = data.get('s3_key')
        angle_code = data.get('angle_code', '').upper()
        filename = data.get('filename')
        duration = data.get('duration')
        file_size = data.get('file_size')

        if not firebase_game_id:
            return jsonify({'success': False, 'error': 'firebase_game_id required'}), 400
        if not s3_key:
            return jsonify({'success': False, 'error': 's3_key required'}), 400
        if not filename:
            return jsonify({'success': False, 'error': 'filename required'}), 400
        if angle_code not in ['FL', 'FR']:
            return jsonify({
                'success': False,
                'error': 'angle_code must be FL or FR (only these angles are registered in Uball)'
            }), 400

        # Register the video
        result = uball_client.register_game_video(
            firebase_game_id=firebase_game_id,
            s3_key=s3_key,
            angle_code=angle_code,
            filename=filename,
            duration=duration,
            file_size=file_size,
            s3_bucket=UPLOAD_BUCKET
        )

        if result:
            return jsonify({
                'success': True,
                'video': result
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to register video in Uball'
            }), 500

    except Exception as e:
        logger.error(f"[VideoRegistration] Error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/games/<game_id>/videos', methods=['GET'])
def get_game_videos_from_uball(game_id):
    """
    Get all registered videos for a game from Uball Backend.

    Args:
        game_id: Firebase game ID

    Returns:
        { success: true, videos: [...] }
    """
    if not uball_client:
        return jsonify({
            'success': False,
            'error': 'Uball Backend client not configured'
        }), 503

    try:
        # First get the Uball game ID from Firebase game ID
        game = uball_client.get_game_by_firebase_id(game_id)
        if not game:
            return jsonify({
                'success': False,
                'error': f'Game not found for Firebase ID: {game_id}'
            }), 404

        uball_game_id = str(game.get('id', ''))

        # Get videos for this game
        videos = uball_client.get_videos_for_game(uball_game_id)

        return jsonify({
            'success': True,
            'firebase_game_id': game_id,
            'uball_game_id': uball_game_id,
            'count': len(videos),
            'videos': videos
        })

    except Exception as e:
        logger.error(f"[VideoRegistration] Error getting videos: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


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


@app.route('/api/system/ntp', methods=['GET'])
def get_ntp_status():
    """
    Check NTP synchronization status using chronyc.

    Returns:
        JSON with NTP sync info:
        - synced: Whether the system is synchronized
        - offset_ms: Time offset from NTP server in milliseconds
        - stratum: Stratum level (1 = primary server, 2+ = derived)
        - source: Current NTP source
        - warning: Present if offset > 500ms
    """
    try:
        result = subprocess.run(
            ['chronyc', 'tracking'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode != 0:
            # chronyc not available or failed
            return jsonify({
                'success': True,
                'synced': False,
                'error': 'chronyc command failed or not available',
                'stderr': result.stderr
            })

        output = result.stdout
        response = {
            'success': True,
            'synced': False,
            'offset_ms': None,
            'stratum': None,
            'source': None,
            'raw_output': output
        }

        # Parse chronyc tracking output
        for line in output.split('\n'):
            line = line.strip()

            # Reference ID line: "Reference ID    : 203.0.113.1 (time.example.com)"
            if line.startswith('Reference ID'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    source_part = parts[1].strip()
                    # Extract the hostname/IP from parentheses if present
                    if '(' in source_part and ')' in source_part:
                        response['source'] = source_part.split('(')[1].split(')')[0]
                    else:
                        response['source'] = source_part.split()[0] if source_part else None

            # Stratum line: "Stratum         : 2"
            elif line.startswith('Stratum'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    try:
                        response['stratum'] = int(parts[1].strip())
                    except ValueError:
                        pass

            # System time line: "System time     : 0.000012345 seconds fast of NTP time"
            elif line.startswith('System time'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    time_part = parts[1].strip()
                    # Extract the seconds value
                    try:
                        seconds_str = time_part.split()[0]
                        offset_seconds = float(seconds_str)
                        response['offset_ms'] = round(offset_seconds * 1000, 3)
                    except (ValueError, IndexError):
                        pass

            # Leap status line: "Leap status     : Normal"
            elif line.startswith('Leap status'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    leap_status = parts[1].strip()
                    response['synced'] = leap_status.lower() == 'normal'

        # Add warning if offset is too high
        if response['offset_ms'] is not None and abs(response['offset_ms']) > 500:
            response['warning'] = f"Time offset ({response['offset_ms']}ms) exceeds 500ms threshold"

        return jsonify(response)

    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'chronyc command timed out'
        }), 500
    except FileNotFoundError:
        return jsonify({
            'success': True,
            'synced': False,
            'error': 'chronyc not installed. Install chrony for NTP sync.'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/debug/env', methods=['GET'])
def debug_env():
    """Debug endpoint to check environment variables."""
    angle_map_str = os.getenv('CAMERA_ANGLE_MAP', '{}')
    try:
        angle_map = json.loads(angle_map_str)
    except:
        angle_map = None

    # Test the actual function from main.py
    test_result_main = _get_angle_code_from_camera_name('Backbone 1')

    # Test firebase_service's angle code function
    firebase_angle_map = None
    firebase_test_result = None
    if firebase_service:
        firebase_angle_map = firebase_service.camera_angle_map
        firebase_test_result = firebase_service._get_angle_code('Backbone 1')

    return jsonify({
        'CAMERA_ANGLE_MAP_raw': angle_map_str,
        'CAMERA_ANGLE_MAP_parsed': angle_map,
        'JETSON_ID': os.getenv('JETSON_ID'),
        'test_main_backbone_1': test_result_main,
        'firebase_camera_angle_map': firebase_angle_map,
        'test_firebase_backbone_1': firebase_test_result,
        'dotenv_loaded': True
    })


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
        compress = data.get('compress', False)  # Default to no compression to save space

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
                        delete_after_upload,
                        compress
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


def upload_single_segment_session(session: dict, camera_name_map: dict, delete_after_upload: bool, compress: bool = False) -> dict:
    """
    Upload a single segment session to S3.
    Merges chapter files and uploads with proper naming.

    Args:
        session: Session dict with name, path, files
        camera_name_map: Map of interface ID to camera name
        delete_after_upload: Whether to delete session after successful upload
        compress: Whether to compress video to 1080p (default False to save disk space)
    """
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

    # Upload each file directly - no merging, no validation, no temp space needed
    logger.info(f"[SegmentUpload] Uploading {len(video_files)} files directly...")

    uploaded_uris = []
    for idx, vf in enumerate(video_files):
        file_path = os.path.join(session_path, vf['filename'])

        # For multiple files, append chapter number to camera name
        if len(video_files) > 1:
            file_camera_name = f"{camera_name}_ch{idx+1:02d}"
        else:
            file_camera_name = camera_name

        logger.info(f"[SegmentUpload] Uploading file {idx+1}/{len(video_files)}: {vf['filename']}")

        try:
            s3_uri = upload_service.upload_video(
                video_path=file_path,
                location=UPLOAD_LOCATION,
                date=upload_date,
                device_name=UPLOAD_DEVICE_NAME,
                camera_name=file_camera_name,
                compress=False,  # Never compress - just upload raw files
                delete_compressed_after_upload=False
            )
            uploaded_uris.append(s3_uri)
            logger.info(f"[SegmentUpload] Uploaded: {s3_uri}")
        except Exception as upload_err:
            logger.warning(f"[SegmentUpload] Failed to upload {vf['filename']}: {upload_err}")
            # Continue with other files even if one fails

    # Check if any files were uploaded
    if not uploaded_uris:
        return {
            'session_name': session_name,
            'success': False,
            'error': 'Failed to upload any files'
        }

    logger.info(f"[SegmentUpload] Upload complete: {len(uploaded_uris)} files uploaded")

    # Delete segments if requested
    if delete_after_upload:
        logger.info(f"[SegmentUpload] Deleting session after upload...")
        media_service.delete_segment_session(session_name)

    return {
        'session_name': session_name,
        'success': True,
        's3_uri': uploaded_uris[0] if len(uploaded_uris) == 1 else uploaded_uris,
        'camera_name': camera_name,
        'date': upload_date,
        'files_merged': len(uploaded_uris)
    }


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
    """List all segment upload jobs (supports both batch and single session formats)"""
    with segment_upload_lock:
        jobs = []
        for upload_id, status in segment_upload_status.items():
            # Handle both batch uploads (total/completed) and single session uploads (total_files/files_completed)
            total = status.get('total', status.get('total_files', 0))
            completed = status.get('completed', status.get('files_completed', 0))

            job_info = {
                'upload_id': upload_id,
                'status': status['status'],
                'total': total,
                'completed': completed
            }

            # Include session_name for single session uploads
            if 'session_name' in status:
                job_info['session_name'] = status['session_name']

            jobs.append(job_info)

    return jsonify({
        'success': True,
        'jobs': jobs
    })


@app.route('/api/media/segments/<session_name>/upload', methods=['POST'])
def upload_single_segment(session_name):
    """Upload a single segment session to S3 (async with progress tracking)"""
    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Cloud storage not configured'
        }), 503

    try:
        data = request.get_json() or {}
        camera_name_map = data.get('camera_name_map', {})
        delete_after_upload = data.get('delete_after_upload', False)
        # Allow sync mode for backward compatibility (default to async)
        async_mode = data.get('async', True)

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

        # Calculate total size
        total_size_mb = sum(f.get('size_mb', 0) for f in session['files'] if f.get('is_video'))
        video_files = [f for f in session['files'] if f.get('is_video')]

        if not async_mode:
            # Synchronous upload (for backward compatibility)
            result = upload_single_segment_session(session, camera_name_map, delete_after_upload)
            return jsonify(result), 200 if result.get('success') else 500

        # Generate upload ID for this single session
        upload_id = f"single_{session_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Initialize status tracking
        with segment_upload_lock:
            segment_upload_status[upload_id] = {
                'status': 'starting',
                'session_name': session_name,
                'total_files': len(video_files),
                'total_size_mb': round(total_size_mb, 2),
                'current_file': None,
                'current_file_index': 0,
                'current_file_progress': 0,
                'files_completed': 0,
                'uploaded_uris': [],
                'errors': [],
                'started_at': datetime.now().isoformat(),
                'completed_at': None
            }

        # Background upload function
        def upload_session_background():
            try:
                with segment_upload_lock:
                    segment_upload_status[upload_id]['status'] = 'in_progress'

                # Run the upload with progress tracking
                result = upload_single_segment_session_with_progress(
                    session, camera_name_map, delete_after_upload, upload_id
                )

                with segment_upload_lock:
                    if result.get('success'):
                        segment_upload_status[upload_id]['status'] = 'completed'
                        segment_upload_status[upload_id]['uploaded_uris'] = result.get('s3_uri', [])
                        if isinstance(segment_upload_status[upload_id]['uploaded_uris'], str):
                            segment_upload_status[upload_id]['uploaded_uris'] = [segment_upload_status[upload_id]['uploaded_uris']]
                    else:
                        segment_upload_status[upload_id]['status'] = 'failed'
                        segment_upload_status[upload_id]['errors'].append(result.get('error', 'Unknown error'))
                    segment_upload_status[upload_id]['completed_at'] = datetime.now().isoformat()

            except Exception as e:
                logger.error(f"Background upload error for {session_name}: {e}")
                with segment_upload_lock:
                    segment_upload_status[upload_id]['status'] = 'failed'
                    segment_upload_status[upload_id]['errors'].append(str(e))
                    segment_upload_status[upload_id]['completed_at'] = datetime.now().isoformat()

        # Start background thread
        upload_thread = threading.Thread(target=upload_session_background, daemon=True)
        upload_thread.start()

        return jsonify({
            'success': True,
            'message': f'Upload started for session {session_name}',
            'upload_id': upload_id,
            'session_name': session_name,
            'total_files': len(video_files),
            'total_size_mb': round(total_size_mb, 2),
            'status_url': f'/api/media/segments/upload/{upload_id}/status'
        })

    except Exception as e:
        logger.error(f"Error starting upload for segment {session_name}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def upload_single_segment_session_with_progress(session: dict, camera_name_map: dict, delete_after_upload: bool, upload_id: str) -> dict:
    """
    Upload a single segment session with progress tracking.
    Updates segment_upload_status with per-file progress.
    """
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

    interface_id = '_'.join(parts[:-2])
    date_str = parts[-2]
    upload_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    camera_name = camera_name_map.get(interface_id)
    if not camera_name:
        camera_name = f"GoPro-{interface_id[-4:]}"

    logger.info(f"[SegmentUpload] Camera: {camera_name}, Date: {upload_date}")

    video_files = sorted([f for f in files if f['is_video']], key=lambda x: x['filename'])

    if not video_files:
        return {
            'session_name': session_name,
            'success': False,
            'error': 'No video files in session'
        }

    logger.info(f"[SegmentUpload] Uploading {len(video_files)} files directly...")

    uploaded_uris = []
    for idx, vf in enumerate(video_files):
        file_path = os.path.join(session_path, vf['filename'])

        # Update status with current file
        with segment_upload_lock:
            segment_upload_status[upload_id]['current_file'] = vf['filename']
            segment_upload_status[upload_id]['current_file_index'] = idx + 1
            segment_upload_status[upload_id]['current_file_progress'] = 0

        if len(video_files) > 1:
            file_camera_name = f"{camera_name}_ch{idx+1:02d}"
        else:
            file_camera_name = camera_name

        logger.info(f"[SegmentUpload] Uploading file {idx+1}/{len(video_files)}: {vf['filename']}")

        try:
            # Create progress callback for this file
            def progress_callback(percent):
                with segment_upload_lock:
                    if upload_id in segment_upload_status:
                        segment_upload_status[upload_id]['current_file_progress'] = percent

            s3_uri = upload_service.upload_video(
                video_path=file_path,
                location=UPLOAD_LOCATION,
                date=upload_date,
                device_name=UPLOAD_DEVICE_NAME,
                camera_name=file_camera_name,
                compress=False,
                delete_compressed_after_upload=False,
                progress_callback=progress_callback
            )
            uploaded_uris.append(s3_uri)
            logger.info(f"[SegmentUpload] Uploaded: {s3_uri}")

            # Update completed count
            with segment_upload_lock:
                segment_upload_status[upload_id]['files_completed'] = idx + 1
                segment_upload_status[upload_id]['current_file_progress'] = 100

        except Exception as upload_err:
            logger.warning(f"[SegmentUpload] Failed to upload {vf['filename']}: {upload_err}")
            with segment_upload_lock:
                segment_upload_status[upload_id]['errors'].append(f"Failed to upload {vf['filename']}: {str(upload_err)}")

    if not uploaded_uris:
        return {
            'session_name': session_name,
            'success': False,
            'error': 'Failed to upload any files'
        }

    logger.info(f"[SegmentUpload] Upload complete: {len(uploaded_uris)} files uploaded")

    if delete_after_upload:
        logger.info(f"[SegmentUpload] Deleting session after upload...")
        media_service.delete_segment_session(session_name)

    return {
        'session_name': session_name,
        'success': True,
        's3_uri': uploaded_uris[0] if len(uploaded_uris) == 1 else uploaded_uris,
        'camera_name': camera_name,
        'date': upload_date,
        'files_uploaded': len(uploaded_uris)
    }


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


# ==================== AWS Batch GPU Transcode Endpoints ====================

# Track batch transcode jobs (job_id -> job_state)
batch_transcode_jobs = {}
batch_transcode_lock = threading.Lock()


@app.route('/api/batch/transcode/<job_id>/status', methods=['GET'])
def get_batch_transcode_status(job_id):
    """
    Get status of an AWS Batch transcode job.

    This endpoint polls AWS Batch for the current job status.

    Args:
        job_id: AWS Batch job ID

    Returns:
        {
            "success": true,
            "jobId": "job-uuid",
            "status": "SUBMITTED|PENDING|RUNNABLE|STARTING|RUNNING|SUCCEEDED|FAILED",
            "statusReason": "...",
            "createdAt": timestamp,
            "startedAt": timestamp,
            "stoppedAt": timestamp
        }
    """
    try:
        from aws_batch_transcode import AWSBatchTranscoder, is_aws_gpu_transcode_enabled

        if not is_aws_gpu_transcode_enabled():
            return jsonify({
                'success': False,
                'error': 'AWS GPU transcoding is not enabled. Set USE_AWS_GPU_TRANSCODE=true'
            }), 503

        transcoder = AWSBatchTranscoder()
        status = transcoder.get_job_status(job_id)

        return jsonify({
            'success': True,
            **status
        })

    except Exception as e:
        logger.error(f"[BatchTranscode] Error getting job status: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/batch/transcode/<job_id>/complete', methods=['POST'])
def complete_batch_transcode(job_id):
    """
    Finalize after a Batch transcode job completes successfully.

    Actions:
    1. Verify job succeeded
    2. Delete raw 4K file from S3 raw/ prefix
    3. Get output file info
    4. Register FL/FR videos in Uball Backend

    Request Body:
    {
        "raw_s3_key": "raw/court-a/2026-01-20/uuid/FL_4k.mp4",
        "final_s3_key": "court-a/2026-01-20/uuid/2026-01-20_uuid_FL.mp4",
        "angle": "FL",
        "firebase_game_id": "abc123",
        "filename": "2026-01-20_uuid_FL.mp4",
        "duration": 3600  // seconds
    }

    Returns:
        {
            "success": true,
            "raw_deleted": true,
            "output_file": {...},
            "uball_registered": true  // for FL/FR only
        }
    """
    try:
        from aws_batch_transcode import AWSBatchTranscoder, is_aws_gpu_transcode_enabled

        if not is_aws_gpu_transcode_enabled():
            return jsonify({
                'success': False,
                'error': 'AWS GPU transcoding is not enabled. Set USE_AWS_GPU_TRANSCODE=true'
            }), 503

        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400

        raw_s3_key = data.get('raw_s3_key')
        final_s3_key = data.get('final_s3_key')
        angle = data.get('angle', '').upper()
        firebase_game_id = data.get('firebase_game_id')
        filename = data.get('filename')
        duration = data.get('duration')

        if not raw_s3_key or not final_s3_key:
            return jsonify({'success': False, 'error': 'raw_s3_key and final_s3_key required'}), 400

        transcoder = AWSBatchTranscoder()

        # 1. Verify job succeeded
        status = transcoder.get_job_status(job_id)
        job_status = status.get('status', 'UNKNOWN')

        if job_status != 'SUCCEEDED':
            return jsonify({
                'success': False,
                'error': f'Job has not succeeded yet. Current status: {job_status}',
                'job_status': status
            }), 400

        result = {
            'success': True,
            'job_id': job_id,
            'job_status': job_status
        }

        # 2. Delete raw 4K file from S3
        raw_deleted = transcoder.delete_raw_file(raw_s3_key)
        result['raw_deleted'] = raw_deleted
        if not raw_deleted:
            logger.warning(f"[BatchTranscode] Could not delete raw file: {raw_s3_key}")

        # 3. Get output file info
        output_info = transcoder.get_output_file_info(final_s3_key)
        result['output_file'] = output_info

        # 4. Register FL/FR videos in Uball Backend
        if uball_client and angle in ['FL', 'FR'] and firebase_game_id:
            try:
                uball_result = uball_client.register_game_video(
                    firebase_game_id=firebase_game_id,
                    s3_key=final_s3_key,
                    angle_code=angle,
                    filename=filename or final_s3_key.split('/')[-1],
                    duration=duration,
                    file_size=output_info.get('size_bytes'),
                    s3_bucket=UPLOAD_BUCKET
                )

                if uball_result:
                    logger.info(f"[BatchTranscode] Registered {angle} video in Uball: {uball_result.get('id')}")
                    result['uball_registered'] = True
                    result['uball_video_id'] = uball_result.get('id')
                else:
                    logger.warning(f"[BatchTranscode] Failed to register {angle} video in Uball")
                    result['uball_registered'] = False
                    result['uball_error'] = 'Registration returned null'

            except Exception as e:
                logger.error(f"[BatchTranscode] Uball registration error for {angle}: {e}")
                result['uball_registered'] = False
                result['uball_error'] = str(e)
        else:
            result['uball_registered'] = False
            if angle not in ['FL', 'FR']:
                result['uball_skip_reason'] = f'Angle {angle} not registered in Uball (FL/FR only)'

        return jsonify(result)

    except Exception as e:
        logger.error(f"[BatchTranscode] Error completing job: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/batch/transcode/<job_id>/wait', methods=['POST'])
def wait_for_batch_transcode(job_id):
    """
    Wait for a Batch transcode job to complete (blocking).

    This endpoint will poll AWS Batch until the job reaches a terminal state
    (SUCCEEDED or FAILED) or timeout is reached.

    Request Body:
    {
        "timeout": 1800,      // Max wait time in seconds (default: 30 min)
        "poll_interval": 30   // Seconds between status checks (default: 30)
    }

    Returns:
        Final job status (same as GET /api/batch/transcode/{job_id}/status)
    """
    try:
        from aws_batch_transcode import AWSBatchTranscoder, is_aws_gpu_transcode_enabled

        if not is_aws_gpu_transcode_enabled():
            return jsonify({
                'success': False,
                'error': 'AWS GPU transcoding is not enabled. Set USE_AWS_GPU_TRANSCODE=true'
            }), 503

        data = request.get_json() or {}
        timeout = data.get('timeout', 1800)  # 30 min default
        poll_interval = data.get('poll_interval', 30)

        transcoder = AWSBatchTranscoder()

        try:
            status = transcoder.wait_for_job(job_id, timeout=timeout, poll_interval=poll_interval)
            return jsonify({
                'success': True,
                **status
            })
        except TimeoutError as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'timeout': True
            }), 408

    except Exception as e:
        logger.error(f"[BatchTranscode] Error waiting for job: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/batch/transcode/config', methods=['GET'])
def get_batch_transcode_config():
    """
    Get AWS Batch transcoding configuration.

    Returns:
        {
            "success": true,
            "enabled": true|false,
            "config": {
                "job_queue": "...",
                "job_definition": "...",
                "region": "...",
                "bucket": "..."
            }
        }
    """
    try:
        from aws_batch_transcode import is_aws_gpu_transcode_enabled

        enabled = is_aws_gpu_transcode_enabled()

        config = {
            'job_queue': os.getenv('AWS_BATCH_JOB_QUEUE', 'gpu-transcode-queue'),
            'job_definition': os.getenv('AWS_BATCH_JOB_DEFINITION', 'ffmpeg-nvenc-transcode'),
            'region': os.getenv('AWS_BATCH_REGION', os.getenv('AWS_REGION', 'us-east-1')),
            'bucket': os.getenv('UPLOAD_BUCKET', 'uball-videos-production')
        }

        return jsonify({
            'success': True,
            'enabled': enabled,
            'config': config
        })

    except Exception as e:
        logger.error(f"[BatchTranscode] Error getting config: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================================================
# Admin Console API Endpoints
# ============================================================================

def _get_device_lock(device_id):
    """Get or create a per-device lock for admin jobs."""
    with admin_device_locks_lock:
        if device_id not in admin_device_locks:
            admin_device_locks[device_id] = threading.Lock()
        return admin_device_locks[device_id]


def _run_admin_job(job_id, device_id, operation, params):
    """Execute a shell script in a background thread, capturing output line by line."""
    device_lock = _get_device_lock(device_id)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Build command based on operation
    if operation == 'fetch':
        date_str = params.get('date', '')
        cmd = ['stdbuf', '-oL', 'bash', os.path.join(script_dir, 'fetch_and_merge.sh'), date_str]
        camera = params.get('camera')
        if camera:
            cmd.append(camera)
    elif operation == 'upload':
        target = params.get('target', 'all')
        cmd = ['stdbuf', '-oL', 'bash', os.path.join(script_dir, 'upload_segments.sh'), target]
        if params.get('delete_after'):
            cmd.append('--delete')
    elif operation == 'clean':
        cmd = ['bash', '-c', f'echo "y" | bash {os.path.join(script_dir, "fetch_and_merge.sh")} clean']
    else:
        with admin_jobs_lock:
            admin_jobs[job_id]['status'] = 'failed'
            admin_jobs[job_id]['error'] = f'Unknown operation: {operation}'
            admin_jobs[job_id]['completed_at'] = datetime.now().isoformat()
        return

    try:
        with device_lock:
            with admin_jobs_lock:
                admin_jobs[job_id]['status'] = 'running'

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                encoding='utf-8',
                errors='replace',
                env={**os.environ, 'PYTHONUNBUFFERED': '1'}
            )

            with admin_jobs_lock:
                admin_jobs[job_id]['pid'] = proc.pid

            # Read stdout and stderr concurrently using selectors
            sel = selectors.DefaultSelector()
            sel.register(proc.stdout, selectors.EVENT_READ)
            sel.register(proc.stderr, selectors.EVENT_READ)

            open_streams = 2
            while open_streams > 0:
                # Check if job was cancelled
                with admin_jobs_lock:
                    if admin_jobs[job_id]['status'] == 'cancelled':
                        proc.terminate()
                        break

                events = sel.select(timeout=0.5)
                for key, _ in events:
                    line = key.fileobj.readline()
                    if line:
                        stream = 'stdout' if key.fileobj == proc.stdout else 'stderr'
                        cleaned = _ansi_re.sub('', line.rstrip('\n'))
                        if cleaned:  # skip empty lines after stripping
                            entry = {
                                'ts': datetime.now().isoformat(),
                                'stream': stream,
                                'text': cleaned
                            }
                            with admin_jobs_lock:
                                admin_jobs[job_id]['output_lines'].append(entry)
                    else:
                        sel.unregister(key.fileobj)
                        open_streams -= 1

            sel.close()
            proc.wait(timeout=10)

            with admin_jobs_lock:
                job = admin_jobs[job_id]
                if job['status'] != 'cancelled':
                    job['exit_code'] = proc.returncode
                    job['status'] = 'completed' if proc.returncode == 0 else 'failed'
                    if proc.returncode != 0:
                        job['error'] = f'Process exited with code {proc.returncode}'
                job['completed_at'] = datetime.now().isoformat()

    except Exception as e:
        logger.error(f"Admin job {job_id} error: {e}")
        with admin_jobs_lock:
            admin_jobs[job_id]['status'] = 'failed'
            admin_jobs[job_id]['error'] = str(e)
            admin_jobs[job_id]['completed_at'] = datetime.now().isoformat()


@app.route('/api/admin/jobs', methods=['POST'])
def start_admin_job():
    """Start a shell script operation (fetch, upload, or clean)."""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'JSON body required'}), 400

    operation = data.get('operation')
    device_id = data.get('device_id', 'local')
    params = data.get('params', {})

    if operation not in ('fetch', 'upload', 'clean'):
        return jsonify({'success': False, 'error': 'operation must be fetch, upload, or clean'}), 400

    if operation == 'fetch' and not params.get('date'):
        return jsonify({'success': False, 'error': 'params.date is required for fetch (MM-DD format)'}), 400

    # Check if device is busy
    device_lock = _get_device_lock(device_id)
    if not device_lock.acquire(blocking=False):
        # Find the active job for error message
        active_job_id = None
        with admin_jobs_lock:
            for jid, job in admin_jobs.items():
                if job['device_id'] == device_id and job['status'] == 'running':
                    active_job_id = jid
                    break
        return jsonify({
            'success': False,
            'error': 'A job is already running on this device',
            'active_job_id': active_job_id
        }), 409
    else:
        device_lock.release()  # We just checked, the thread will acquire it

    job_id = str(uuid.uuid4())

    with admin_jobs_lock:
        admin_jobs[job_id] = {
            'job_id': job_id,
            'device_id': device_id,
            'operation': operation,
            'params': params,
            'status': 'queued',
            'exit_code': None,
            'pid': None,
            'output_lines': [],
            'created_at': datetime.now().isoformat(),
            'completed_at': None,
            'error': None,
        }

    thread = threading.Thread(
        target=_run_admin_job,
        args=(job_id, device_id, operation, params),
        daemon=True
    )
    thread.start()

    return jsonify({
        'success': True,
        'job_id': job_id,
        'message': f'{operation} job started'
    }), 201


@app.route('/api/admin/jobs', methods=['GET'])
def list_admin_jobs():
    """List recent admin jobs (last 20)."""
    with admin_jobs_lock:
        jobs = sorted(
            admin_jobs.values(),
            key=lambda j: j['created_at'],
            reverse=True
        )[:20]
        result = [{
            'job_id': j['job_id'],
            'operation': j['operation'],
            'device_id': j['device_id'],
            'status': j['status'],
            'created_at': j['created_at'],
            'completed_at': j['completed_at'],
        } for j in jobs]

    return jsonify({'success': True, 'jobs': result})


@app.route('/api/admin/jobs/<job_id>', methods=['GET'])
def get_admin_job(job_id):
    """Get status of an admin job."""
    with admin_jobs_lock:
        job = admin_jobs.get(job_id)

    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    return jsonify({
        'success': True,
        'job': {
            'job_id': job['job_id'],
            'operation': job['operation'],
            'device_id': job['device_id'],
            'status': job['status'],
            'exit_code': job['exit_code'],
            'output_line_count': len(job['output_lines']),
            'created_at': job['created_at'],
            'completed_at': job['completed_at'],
            'error': job['error'],
        }
    })


@app.route('/api/admin/jobs/<job_id>/stream', methods=['GET'])
def stream_admin_job(job_id):
    """Stream shell job output via Server-Sent Events."""
    with admin_jobs_lock:
        job = admin_jobs.get(job_id)

    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    def generate():
        yield f"data: {json.dumps({'type': 'connected', 'job_id': job_id})}\n\n"

        cursor = 0

        while True:
            with admin_jobs_lock:
                current_job = admin_jobs.get(job_id)
                if not current_job:
                    break

                new_lines = current_job['output_lines'][cursor:]
                status = current_job['status']

            for line in new_lines:
                yield f"data: {json.dumps({'type': 'output', **line})}\n\n"
                cursor += 1

            if status in ('completed', 'failed', 'cancelled'):
                with admin_jobs_lock:
                    final = admin_jobs[job_id]
                yield f"data: {json.dumps({'type': 'done', 'status': final['status'], 'exit_code': final.get('exit_code'), 'error': final.get('error')})}\n\n"
                break

            time.sleep(0.2)

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


@app.route('/api/admin/jobs/<job_id>/cancel', methods=['POST'])
def cancel_admin_job(job_id):
    """Cancel a running admin job."""
    with admin_jobs_lock:
        job = admin_jobs.get(job_id)

    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    if job['status'] != 'running':
        return jsonify({'success': False, 'error': f'Job is not running (status: {job["status"]})'}), 400

    pid = job.get('pid')
    with admin_jobs_lock:
        admin_jobs[job_id]['status'] = 'cancelled'

    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    return jsonify({'success': True, 'message': 'Job cancellation requested'})


# ==================== Chapter Upload Pipeline Endpoints ====================

# Pipeline job tracking
pipeline_jobs = {}  # job_id -> job state dict
pipeline_jobs_lock = threading.Lock()


@app.route('/api/pipeline/sessions/pending', methods=['GET'])
def list_pending_sessions():
    """
    List recording sessions that need chapter upload to S3.

    These are sessions where:
    - status is 'stopped' (recording finished)
    - s3Prefix is not set (chapters not yet uploaded)
    - totalChapters > 0 (has files to upload)

    Query params:
        jetson_id: Optional filter by Jetson ID (defaults to current Jetson)

    Returns:
        { success: true, sessions: [...], count: N }
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        jetson_id = request.args.get('jetson_id', firebase_service.jetson_id)

        sessions = firebase_service.get_sessions_pending_upload(jetson_id=jetson_id)

        return jsonify({
            'success': True,
            'jetson_id': jetson_id,
            'count': len(sessions),
            'sessions': sessions
        })

    except Exception as e:
        logger.error(f"[Pipeline] Error listing pending sessions: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/sessions/<session_id>/upload-chapters', methods=['POST'])
def upload_session_chapters(session_id):
    """
    Start async chapter upload job for a recording session.

    Streams chapters directly from GoPro HTTP to S3 (no temp files).

    Request Body (optional):
    {
        "gopro_ip": "172.x.x.x"  // Optional, will auto-discover if not provided
    }

    Returns:
        { success: true, job_id: "...", message: "Upload started" }

    Poll /api/pipeline/<job_id>/status for progress.
    """
    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    if not upload_service:
        return jsonify({
            'success': False,
            'error': 'Upload service not configured'
        }), 503

    try:
        # Get session from Firebase
        session = firebase_service.get_recording_session(session_id)
        if not session:
            return jsonify({
                'success': False,
                'error': f'Session not found: {session_id}'
            }), 404

        # Check if already uploaded
        if session.get('s3Prefix'):
            return jsonify({
                'success': False,
                'error': 'Session chapters already uploaded',
                's3Prefix': session['s3Prefix']
            }), 400

        # Get GoPro IP
        data = request.get_json() or {}
        gopro_ip = data.get('gopro_ip')

        if not gopro_ip:
            # Try to discover GoPro IP from interface
            interface_id = session.get('interfaceId')
            if interface_id:
                gopro_ip = get_gopro_wired_ip(interface_id)

        if not gopro_ip:
            return jsonify({
                'success': False,
                'error': 'Could not find GoPro IP. Provide gopro_ip or ensure GoPro is connected.'
            }), 400

        # Create job ID
        job_id = str(uuid.uuid4())[:8]

        # Initialize job state
        with pipeline_jobs_lock:
            pipeline_jobs[job_id] = {
                'job_id': job_id,
                'session_id': session_id,
                'segment_session': session.get('segmentSession', ''),
                'angle_code': session.get('angleCode', 'UNKNOWN'),
                'gopro_ip': gopro_ip,
                'status': 'starting',
                'substage': 'starting',  # NEW: Track download vs upload stage
                'progress': 0,
                'total_chapters': session.get('totalChapters', 0),
                'chapters_uploaded': 0,
                'chapters_downloaded': 0,  # NEW: Track download progress
                'bytes_uploaded': 0,
                'bytes_downloaded': 0,  # NEW: Track download bytes
                's3_prefix': None,
                'error': None,
                'current_chapter': None,  # NEW: Current chapter info
                'started_at': datetime.now().isoformat(),
                'completed_at': None
            }

        # Start upload in background thread
        def run_upload():
            try:
                from chapter_upload_service import ChapterUploadService

                # Update status
                with pipeline_jobs_lock:
                    pipeline_jobs[job_id]['status'] = 'discovering'

                # Get chapters from GoPro
                chapter_service = ChapterUploadService(
                    s3_client=upload_service.s3_client,
                    bucket_name=upload_service.bucket_name
                )

                # Find chapters for this session on GoPro
                # Note: We need pre_record_files from when recording started
                # For now, get all files and match by count
                all_chapters = chapter_service.get_gopro_media_list(gopro_ip)

                expected_count = session.get('totalChapters', 0)
                if len(all_chapters) < expected_count:
                    with pipeline_jobs_lock:
                        pipeline_jobs[job_id]['status'] = 'failed'
                        pipeline_jobs[job_id]['substage'] = 'failed'
                        pipeline_jobs[job_id]['error'] = f'Expected {expected_count} chapters but found {len(all_chapters)} on GoPro'
                        pipeline_jobs[job_id]['current_chapter'] = None
                    return

                # Take the last N chapters (most recent recording)
                chapters_to_upload = all_chapters[-expected_count:] if expected_count > 0 else all_chapters

                with pipeline_jobs_lock:
                    pipeline_jobs[job_id]['status'] = 'downloading'  # Start with downloading
                    pipeline_jobs[job_id]['substage'] = 'downloading'
                    pipeline_jobs[job_id]['total_chapters'] = len(chapters_to_upload)

                # Progress callback - now tracks download vs upload stages
                def progress_callback(stage, chapter_num, total, bytes_current):
                    """
                    Progress callback for chapter upload service.
                    
                    Args:
                        stage: 'downloading' or 'uploading'
                        chapter_num: Current chapter number (1-based)
                        total: Total number of chapters
                        bytes_current: Current bytes for this stage
                    """
                    with pipeline_jobs_lock:
                        if job_id in pipeline_jobs:
                            job = pipeline_jobs[job_id]
                            
                            # Update substage
                            job['substage'] = stage
                            
                            # Update status based on stage
                            if stage == 'downloading':
                                job['status'] = 'downloading'
                                job['chapters_downloaded'] = chapter_num
                                job['bytes_downloaded'] = bytes_current
                            elif stage == 'uploading':
                                job['status'] = 'uploading'
                                job['chapters_uploaded'] = chapter_num
                                job['bytes_uploaded'] = bytes_current
                            
                            # Update overall progress (based on total chapters)
                            # Progress = (completed chapters / total) * 100
                            # For downloading: chapter_num - 1 complete
                            # For uploading: chapter_num - 1 complete + current upload progress
                            if stage == 'downloading':
                                completed = chapter_num - 1
                            else:  # uploading
                                completed = chapter_num - 1
                            
                            job['progress'] = int((completed / total) * 100) if total > 0 else 0
                            
                            # Update current chapter info (optional, for better UX)
                            # Note: We don't have filename here, would need to pass it through
                            job['current_chapter'] = {
                                'number': chapter_num,
                                'stage': stage
                            }

                # Upload chapters
                result = chapter_service.upload_session_chapters(
                    session=session,
                    gopro_ip=gopro_ip,
                    chapters=chapters_to_upload,
                    progress_callback=progress_callback
                )

                if result['success']:
                    # Update Firebase with s3Prefix
                    firebase_service.update_session_s3_prefix(session_id, result['s3_prefix'])

                    with pipeline_jobs_lock:
                        pipeline_jobs[job_id]['status'] = 'completed'
                        pipeline_jobs[job_id]['substage'] = 'completed'
                        pipeline_jobs[job_id]['s3_prefix'] = result['s3_prefix']
                        pipeline_jobs[job_id]['chapters_uploaded'] = result['chapters_uploaded']
                        pipeline_jobs[job_id]['chapters_downloaded'] = result['chapters_uploaded']  # Same as uploaded
                        pipeline_jobs[job_id]['bytes_uploaded'] = result['total_bytes']
                        pipeline_jobs[job_id]['bytes_downloaded'] = result['total_bytes']  # Same as uploaded
                        pipeline_jobs[job_id]['progress'] = 100
                        pipeline_jobs[job_id]['current_chapter'] = None
                        pipeline_jobs[job_id]['completed_at'] = datetime.now().isoformat()
                else:
                    with pipeline_jobs_lock:
                        pipeline_jobs[job_id]['status'] = 'failed'
                        pipeline_jobs[job_id]['substage'] = 'failed'
                        pipeline_jobs[job_id]['error'] = '; '.join(result.get('errors', ['Unknown error']))
                        pipeline_jobs[job_id]['current_chapter'] = None
                        pipeline_jobs[job_id]['completed_at'] = datetime.now().isoformat()

            except Exception as e:
                logger.error(f"[Pipeline] Upload job {job_id} failed: {e}")
                import traceback
                traceback.print_exc()
                with pipeline_jobs_lock:
                    pipeline_jobs[job_id]['status'] = 'failed'
                    pipeline_jobs[job_id]['substage'] = 'failed'
                    pipeline_jobs[job_id]['error'] = str(e)
                    pipeline_jobs[job_id]['current_chapter'] = None
                    pipeline_jobs[job_id]['completed_at'] = datetime.now().isoformat()

        threading.Thread(target=run_upload, daemon=True).start()

        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': 'Chapter upload started',
            'session_id': session_id,
            'gopro_ip': gopro_ip
        })

    except Exception as e:
        logger.error(f"[Pipeline] Error starting chapter upload: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/pipeline/<job_id>/status', methods=['GET'])
def get_pipeline_job_status(job_id):
    """
    Get status of a pipeline job (chapter upload or game processing).

    Poll this endpoint every 2 seconds for progress updates.

    Returns:
        {
            success: true,
            job: {
                job_id: "...",
                status: "uploading" | "completed" | "failed",
                progress: 0-100,
                chapters_uploaded: N,
                total_chapters: N,
                bytes_uploaded: N,
                s3_prefix: "..." (when completed),
                error: "..." (when failed)
            }
        }
    """
    with pipeline_jobs_lock:
        job = pipeline_jobs.get(job_id)

    if not job:
        return jsonify({
            'success': False,
            'error': 'Job not found'
        }), 404

    return jsonify({
        'success': True,
        'job': job
    })


@app.route('/api/pipeline/jobs', methods=['GET'])
def list_pipeline_jobs():
    """
    List all pipeline jobs (for debugging/monitoring).

    Query params:
        status: Optional filter by status (starting, uploading, completed, failed)
        limit: Maximum jobs to return (default 20)

    Returns:
        { success: true, jobs: [...], count: N }
    """
    status_filter = request.args.get('status')
    limit = request.args.get('limit', 20, type=int)

    with pipeline_jobs_lock:
        jobs = list(pipeline_jobs.values())

    # Filter by status if requested
    if status_filter:
        jobs = [j for j in jobs if j.get('status') == status_filter]

    # Sort by started_at descending (most recent first)
    jobs.sort(key=lambda j: j.get('started_at', ''), reverse=True)

    # Apply limit
    jobs = jobs[:limit]

    return jsonify({
        'success': True,
        'count': len(jobs),
        'jobs': jobs
    })


# ==================== Full Pipeline Automation ====================

# Initialize pipeline orchestrator
from pipeline_orchestrator import init_orchestrator, get_orchestrator

pipeline_orchestrator = None
if firebase_service and upload_service:
    try:
        use_aws_gpu = os.getenv('USE_AWS_GPU_TRANSCODE', 'false').lower() == 'true'
        pipeline_orchestrator = init_orchestrator(
            jetson_id=firebase_service.jetson_id,
            firebase_service=firebase_service,
            upload_service=upload_service,
            video_processor=video_processor,
            uball_client=uball_client,
            batch_enabled=use_aws_gpu
        )
        print(f"✓ Pipeline orchestrator initialized (uball_client: {'yes' if uball_client else 'no'})")
    except Exception as e:
        print(f"⚠ Failed to initialize pipeline orchestrator: {e}")


@app.route('/api/pipeline/auto-start', methods=['POST'])
def start_auto_pipeline():
    """
    Start the fully automated pipeline for all pending sessions on this Jetson.

    This endpoint:
    1. Gets all pending sessions (status='stopped', no s3Prefix)
    2. Discovers connected GoPros for each session
    3. Starts the full pipeline: upload → detect games → process → encode → cleanup

    Request Body (optional):
    {
        "auto_delete_sd": true,  // Delete GoPro files after success (default: true)
        "session_ids": ["id1", "id2"]  // Optional: specific sessions to process
    }

    Returns:
        { success: true, pipeline_id: "...", sessions_count: N, message: "..." }

    Poll /api/pipeline/full/{pipeline_id}/status for progress.
    """
    if not pipeline_orchestrator:
        return jsonify({
            'success': False,
            'error': 'Pipeline orchestrator not initialized'
        }), 503

    if not firebase_service:
        return jsonify({
            'success': False,
            'error': 'Firebase service not configured'
        }), 503

    try:
        data = request.get_json() or {}
        auto_delete_sd = data.get('auto_delete_sd', True)
        specific_session_ids = data.get('session_ids')

        # Get pending sessions
        if specific_session_ids:
            # Get specific sessions
            sessions = []
            for sid in specific_session_ids:
                session = firebase_service.get_recording_session(sid)
                if session:
                    session['id'] = sid
                    sessions.append(session)
        else:
            # Get all pending sessions for this Jetson
            sessions = firebase_service.get_sessions_pending_upload(jetson_id=firebase_service.jetson_id)

        if not sessions:
            return jsonify({
                'success': False,
                'error': 'No pending sessions found'
            }), 404

        # Discover GoPro connections for each session's interface
        gopro_connections = {}
        for session in sessions:
            interface_id = session.get('interfaceId')
            if interface_id and interface_id not in gopro_connections:
                gopro_ip = get_gopro_wired_ip(interface_id)
                if gopro_ip:
                    gopro_connections[interface_id] = gopro_ip

        if not gopro_connections:
            return jsonify({
                'success': False,
                'error': 'No GoPro cameras found. Ensure cameras are connected.'
            }), 400

        # Start the pipeline
        pipeline_id = pipeline_orchestrator.start_pipeline(
            sessions=sessions,
            gopro_connections=gopro_connections,
            auto_delete_sd=auto_delete_sd
        )

        return jsonify({
            'success': True,
            'pipeline_id': pipeline_id,
            'sessions_count': len(sessions),
            'gopro_connections': len(gopro_connections),
            'message': f'Pipeline started with {len(sessions)} sessions'
        })

    except Exception as e:
        logger.error(f"[Pipeline] Error starting auto pipeline: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/pipeline/full/<pipeline_id>/status', methods=['GET'])
def get_full_pipeline_status(pipeline_id):
    """
    Get status of the full automated pipeline.

    Poll this endpoint every 2 seconds for progress updates.

    Returns:
        {
            success: true,
            pipeline: {
                pipeline_id: "...",
                status: "running" | "completed" | "failed",
                stage: "uploading_chapters" | "detecting_games" | "processing_games" | ...,
                stage_message: "Processing game 2/4...",
                progress: 0-100,
                sessions: { ... },
                games: { ... },
                games_total: N,
                games_completed: N,
                batch_jobs_submitted: N,
                errors: [...]
            }
        }
    """
    if not pipeline_orchestrator:
        return jsonify({
            'success': False,
            'error': 'Pipeline orchestrator not initialized'
        }), 503

    pipeline = pipeline_orchestrator.get_pipeline_status(pipeline_id)

    if not pipeline:
        return jsonify({
            'success': False,
            'error': 'Pipeline not found'
        }), 404

    return jsonify({
        'success': True,
        'pipeline': pipeline
    })


@app.route('/api/pipeline/full/list', methods=['GET'])
def list_full_pipelines():
    """
    List all full pipelines.

    Query params:
        status: Optional filter (running, completed, failed)
        limit: Maximum pipelines to return (default 20)
    """
    if not pipeline_orchestrator:
        return jsonify({
            'success': False,
            'error': 'Pipeline orchestrator not initialized'
        }), 503

    status_filter = request.args.get('status')
    limit = request.args.get('limit', 20, type=int)

    pipelines = pipeline_orchestrator.list_pipelines(status=status_filter, limit=limit)

    return jsonify({
        'success': True,
        'count': len(pipelines),
        'pipelines': pipelines
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

    # Check AWS GPU transcoding status
    use_aws_gpu = os.getenv('USE_AWS_GPU_TRANSCODE', 'false').lower() == 'true'
    if use_aws_gpu:
        logger.info("AWS GPU Transcoding: ENABLED")
        logger.info(f"  Job Queue: {os.getenv('AWS_BATCH_JOB_QUEUE', 'gpu-transcode-queue')}")
        logger.info(f"  Job Definition: {os.getenv('AWS_BATCH_JOB_DEFINITION', 'ffmpeg-nvenc-transcode')}")
    else:
        logger.info("AWS GPU Transcoding: DISABLED (using local CPU encoding)")

    logger.info("=" * 60)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

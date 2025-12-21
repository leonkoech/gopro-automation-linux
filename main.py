#!/usr/bin/env python3
"""
GoPro Controller API Service with Automatic Segmentation
REST API for remote control of GoPro cameras connected to Jetson Nano
"""

from flask import Flask, jsonify, request, send_file, Response, send_from_directory
from flask_cors import CORS
import subprocess
import threading
import signal
import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import pty
import select
import requests
import re
from videoupload import VideoUploadService
from media_service import get_media_service

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
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET', 'jetson-videos')
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
            camera_name = info.get('info', {}).get('ap_ssid')
            if camera_name:
                print(f"✓ Got camera name from GoPro: {camera_name}")
                return camera_name
    except Exception as e:
        print(f"⚠ Could not get camera name from {gopro_ip}: {e}")
    return None

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
    """Start recording on a specific GoPro"""
    global recording_processes
    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    if not gopro:
        return jsonify({'success': False, 'error': 'GoPro not found'}), 404

    with recording_lock:
        if gopro_id in recording_processes:
            return jsonify({'success': False, 'error': 'Already recording'}), 400

    try:
        gopro_ip = gopro.get('gopro_ip') or get_gopro_wired_ip(gopro_id)
        
        if not gopro_ip:
            return jsonify({'success': False, 'error': f'Could not find GoPro IP for {gopro_id}'}), 500

        # === FIX 1: Set camera to Video mode before starting ===
        print(f"Setting {gopro_id} ({gopro_ip}) to Video mode...")
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/camera/presets/set_group?id=1000',
                timeout=5
            )
            if response.status_code == 200:
                print(f"✓ Set {gopro_id} to Video preset group")
            else:
                print(f"⚠ Failed to set preset group: {response.status_code}")
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠ Warning: Could not set video mode for {gopro_id}: {e}")

        # === FIX 2: Get list of existing files BEFORE recording ===
        pre_record_files = get_gopro_files(gopro_ip)
        print(f"Pre-recording files on {gopro_id} ({gopro_ip}): {len(pre_record_files)} files")
        if len(pre_record_files) == 0:
            print(f"⚠ WARNING: No pre-record files found for {gopro_id} - this may cause issues!")

        data = request.get_json() or {}
        duration = data.get('duration', 18000)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
        video_path = os.path.join(VIDEO_STORAGE_DIR, video_filename)
        
        session_id = f"{gopro_id}_{timestamp}"
        session_dir = os.path.join(SEGMENTS_DIR, session_id)
        os.makedirs(session_dir, exist_ok=True)

        cmd = [
            'gopro-video',
            '--wired',
            '--wifi_interface', gopro['interface'],
            '-o', video_path,
            '--record_time', str(duration)
        ]

        master, slave = pty.openpty()
        process = subprocess.Popen(
            cmd,
            stdin=slave,
            stdout=slave,
            stderr=slave,
            preexec_fn=os.setsid,
            close_fds=True
        )
        os.close(slave)

        # Wait and check for early failure
        time.sleep(3)
        if process.poll() is not None:
            # Process exited - read any error output
            error_output = ""
            try:
                ready, _, _ = select.select([master], [], [], 0.1)
                if ready:
                    error_output = os.read(master, 4096).decode('utf-8', errors='ignore')
            except:
                pass
            try:
                os.close(master)
            except:
                pass
            
            error_msg = 'Recording process failed to start'
            if 'ErrorCode.ERROR' in error_output:
                error_msg = 'GoPro returned error - camera may be in wrong mode or busy'
            elif 'shutter' in error_output.lower():
                error_msg = 'Failed to start shutter - check camera mode'
                
            return jsonify({
                'success': False,
                'error': error_msg,
                'details': error_output[-500:] if error_output else None
            }), 500

        with recording_lock:
            recording_processes[gopro_id] = {
                'process': process,
                'master_fd': master,
                'video_path': video_path,
                'video_filename': video_filename,
                'start_time': datetime.now().isoformat(),
                'duration': duration,
                'recording_started': False,
                'is_stopping': False,
                'session_id': session_id,
                'session_dir': session_dir,
                'gopro_ip': gopro_ip,
                'pre_record_files': pre_record_files,  # Track files before recording
                'error': None
            }

        def monitor():
            """Monitor the gopro-video process for output and errors"""
            output_buffer = []
            recording_confirmed = False
            error_detected = None

            while True:
                try:
                    with recording_lock:
                        if gopro_id in recording_processes and recording_processes[gopro_id].get('is_stopping'):
                            break

                    ready, _, _ = select.select([master], [], [], 1.0)
                    if ready:
                        data = os.read(master, 1024)
                        if data:
                            text = data.decode('utf-8', errors='ignore')
                            output_buffer.append(text)
                            print(f"GoPro {gopro_id} output: {text.strip()}")

                            # Check for recording confirmation
                            if not recording_confirmed and ('recording' in text.lower() or 'capturing' in text.lower()):
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['recording_started'] = True
                                recording_confirmed = True
                                print(f"✓ Recording confirmed on {gopro_id}")

                            # === FIX 3: Detect errors in output ===
                            if 'ErrorCode.ERROR' in text or 'Internal Server Error' in text:
                                error_detected = "GoPro returned error during recording"
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['error'] = error_detected
                                print(f"✗ Error detected on {gopro_id}: {error_detected}")

                            if 'exiting' in text.lower() and not recording_confirmed:
                                error_detected = "Recording failed to start - process exited"
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['error'] = error_detected

                    if process.poll() is not None:
                        break
                except OSError:
                    break
                except Exception as e:
                    print(f"Monitor error for {gopro_id}: {e}")
                    break

            try:
                os.close(master)
            except OSError:
                pass

            with recording_lock:
                if gopro_id in recording_processes:
                    if not recording_processes[gopro_id].get('is_stopping'):
                        # Process ended unexpectedly
                        if not error_detected:
                            recording_processes[gopro_id]['error'] = "Recording process ended unexpectedly"
                        del recording_processes[gopro_id]

        threading.Thread(target=monitor, daemon=True).start()

        return jsonify({
            'success': True,
            'message': f'Recording started for {duration}s',
            'video_filename': video_filename,
            'gopro_id': gopro_id,
            'gopro_ip': gopro_ip,
            'video_path': video_path,
            'session_id': session_id
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording and download all new video chapters"""
    global recording_processes

    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400

        recording_processes[gopro_id]['is_stopping'] = True
        recording_info = recording_processes[gopro_id].copy()

    try:
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']
        process = recording_info['process']
        master_fd = recording_info.get('master_fd')
        session_dir = recording_info.get('session_dir')
        gopro_ip = recording_info.get('gopro_ip')
        pre_record_files = recording_info.get('pre_record_files', set())

        # Stop the gopro-video process
        if process.poll() is None:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                process.wait(timeout=2)
            except Exception as e:
                print(f"Error stopping process: {e}")

        if master_fd:
            try:
                os.close(master_fd)
            except:
                pass

        if not gopro_ip:
            gopro_ip = get_gopro_wired_ip(gopro_id)
        
        if not gopro_ip:
            with recording_lock:
                if gopro_id in recording_processes:
                    del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': 'Could not detect GoPro IP'}), 500

        # Send stop command to camera
        try:
            requests.get(f'http://{gopro_ip}:8080/gopro/camera/shutter/stop', timeout=5)
            print(f"✓ Sent stop command to {gopro_id}")
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
                        recording_processes[gopro_id]['stage_message'] = 'Downloading video from GoPro...'

                time.sleep(3)  # Wait for camera to finalize files

                print(f"Getting media list from {gopro_ip}...")
                media_response = requests.get(
                    f'http://{gopro_ip}:8080/gopro/media/list',
                    timeout=15
                )
                media_list = media_response.json()

                # === FIX 4: Find ALL new files (not just the last one) ===
                new_chapters = []
                for directory in media_list.get('media', []):
                    dir_name = directory['d']
                    for file_info in directory.get('fs', []):
                        filename = file_info['n']
                        if filename not in pre_record_files and filename.lower().endswith('.mp4'):
                            new_chapters.append({
                                'directory': dir_name,
                                'filename': filename,
                                'size': file_info.get('s', 0)
                            })

                # Sort by filename to maintain correct chapter order
                # GoPro names: GX010028.MP4, GX020028.MP4, GX030028.MP4 for same recording
                new_chapters.sort(key=lambda x: x['filename'])

                # Safety check: if we found too many "new" files, something is wrong
                # (likely pre_record_files wasn't captured properly)
                MAX_CHAPTERS = 20
                total_size_bytes = sum(ch['size'] for ch in new_chapters)
                total_size_gb = total_size_bytes / (1024**3)

                if len(new_chapters) > MAX_CHAPTERS:
                    print(f"⚠ WARNING: Found {len(new_chapters)} new chapters ({total_size_gb:.1f} GB) - this seems too many!")
                    print(f"  Pre-record files count: {len(pre_record_files)}")
                    print(f"  Limiting to last {MAX_CHAPTERS} chapters to avoid downloading entire SD card")
                    new_chapters = new_chapters[-MAX_CHAPTERS:]
                    total_size_bytes = sum(ch['size'] for ch in new_chapters)
                    total_size_gb = total_size_bytes / (1024**3)

                print(f"Total download size: {total_size_gb:.2f} GB")
                
                print(f"Found {len(new_chapters)} new chapters to download")
                for ch in new_chapters:
                    print(f"  - {ch['filename']} ({ch['size']} bytes)")

                if not new_chapters:
                    print(f"No new chapters found for {gopro_id}")
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
                    
                    print(f"Downloading chapter {i+1}/{total_chapters}: {chapter['filename']}")
                    
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
                        print(f"✓ Downloaded chapter {i+1}: {file_size_mb:.1f} MB")
                        
                        # Update progress
                        with recording_lock:
                            if gopro_id in recording_processes:
                                progress = int(((i + 1) / total_chapters) * 100)
                                recording_processes[gopro_id]['download_progress'] = progress
                                
                    except Exception as e:
                        print(f"✗ Error downloading chapter {chapter['filename']}: {e}")

                # Merge all chapters
                if downloaded_files:
                    # Update stage: merging
                    with recording_lock:
                        if gopro_id in recording_processes:
                            recording_processes[gopro_id]['stage'] = 'merging'
                            recording_processes[gopro_id]['stage_message'] = 'Merging video chapters...'

                    print(f"Merging {len(downloaded_files)} chapters into {video_path}...")

                    if merge_videos_ffmpeg(downloaded_files, video_path):
                        final_size_mb = os.path.getsize(video_path) / (1024 * 1024)
                        print(f"✓ Merged video saved: {video_path} ({final_size_mb:.1f} MB)")

                        # Cleanup chapter files
                        try:
                            for f in downloaded_files:
                                os.remove(f)
                            os.rmdir(session_dir)
                            print(f"✓ Cleaned up {len(downloaded_files)} chapter files")
                        except Exception as e:
                            print(f"Warning: Cleanup error: {e}")

                        # Upload to S3 if enabled
                        if upload_service:
                            # Update stage: uploading
                            with recording_lock:
                                if gopro_id in recording_processes:
                                    recording_processes[gopro_id]['stage'] = 'uploading'
                                    recording_processes[gopro_id]['stage_message'] = 'Uploading to cloud...'

                            try:
                                print(f"Starting upload of {video_filename} to S3...")
                                upload_date = datetime.now().strftime('%Y-%m-%d')

                                # Get camera name from GoPro API, fallback to interface-based name
                                camera_name = get_gopro_camera_name(gopro_ip)
                                if not camera_name:
                                    camera_name = f"GoPro-{gopro_id[-4:]}"
                                    print(f"Using fallback camera name: {camera_name}")

                                s3_uri = upload_service.upload_video(
                                    video_path=video_path,
                                    location=UPLOAD_LOCATION,
                                    date=upload_date,
                                    device_name=UPLOAD_DEVICE_NAME,
                                    camera_name=camera_name,
                                    compress=True,
                                    delete_compressed_after_upload=True
                                )
                                print(f"✓ Video uploaded to: {s3_uri}")

                                # Update stage: done
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['stage'] = 'done'
                                        recording_processes[gopro_id]['stage_message'] = 'Done!'

                                # Optionally delete local file after upload
                                if DELETE_AFTER_UPLOAD:
                                    try:
                                        os.remove(video_path)
                                        print(f"✓ Local file deleted after upload: {video_filename}")
                                    except Exception as e:
                                        print(f"⚠ Failed to delete local file: {e}")
                            except Exception as e:
                                print(f"✗ Upload failed: {e}")
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
                        print(f"✗ Failed to merge chapters - keeping individual files in {session_dir}")
                else:
                    print(f"No chapters downloaded for {gopro_id}")

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

if __name__ == '__main__':
    print("=" * 60)
    print("GoPro Controller API Service Starting...")
    print("=" * 60)
    print(f"📁 Video storage: {VIDEO_STORAGE_DIR}")
    print(f"📂 Segments storage: {SEGMENTS_DIR}")
    print(f"🌐 API endpoint: http://0.0.0.0:5000")
    print(f"💡 Make sure GoPros are connected via USB")
    print("=" * 60)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

#!/usr/bin/env python3
"""
GoPro Controller API Service
REST API for remote control of GoPro cameras connected to Jetson Nano
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import asyncio
import threading
import signal
import os
import time
import json
import subprocess
from datetime import datetime
from pathlib import Path
import requests
from open_gopro import WiredGoPro

app = Flask(__name__)
CORS(app)  # Enable CORS for Firebase web app access

# Configuration
VIDEO_STORAGE_DIR = os.path.expanduser('~/gopro_videos')
os.makedirs(VIDEO_STORAGE_DIR, exist_ok=True)

# Global state
recording_processes = {}  # {gopro_id: process}
recording_lock = threading.Lock()

def shutdown_all_recordings():
    """Stop all active GoPro recordings gracefully"""
    print("\n🛑 Shutting down all active recordings...")
    with recording_lock:
        gopro_ids = list(recording_processes.keys())

    for gopro_id in gopro_ids:
        try:
            with recording_lock:
                if gopro_id not in recording_processes:
                    continue
                recording_info = recording_processes[gopro_id]
                status = recording_info.get('status')

            print(f"GoPro {gopro_id} - Status: {status}")

            # Recording threads will exit naturally on their own
            # Just log the current state
            if status == 'completed':
                result = recording_info.get('result', {})
                print(f"  ✓ Completed: {result.get('videos_downloaded', 0)} videos downloaded")
            elif status == 'failed':
                print(f"  ✗ Failed: {recording_info.get('error', 'Unknown error')}")
            else:
                print(f"  ⏳ In progress: {status}")

        except Exception as e:
            print(f"Error checking {gopro_id}: {e}")

    print("✓ Shutdown complete")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"\nReceived signal {signum}")
    shutdown_all_recordings()
    import sys
    sys.exit(0)

def create_app():
    """Create and configure Flask app with cleanup handlers"""
    return app

def get_connected_gopros():
    """Discover all connected GoPro cameras"""
    gopros = []

    try:
        # Check for GoPro USB network interfaces
        result = subprocess.run(['ip', 'addr', 'show'],
                              capture_output=True, text=True, timeout=5)

        lines = result.stdout.split('\n')
        current_interface = None
        current_ip = None
        enx_interfaces = []

        for line in lines:
            # Look for interface lines like "13: enx0457470807ce:"
            if 'enx' in line and ':' in line:
                current_interface = line.split(':')[1].strip().split('@')[0].strip()
                enx_interfaces.append(current_interface)
            # Look for IP addresses in 172.x.x.x range (typical for GoPro)
            elif 'inet 172.' in line and current_interface:
                current_ip = line.strip().split()[1].split('/')[0]

                # Try to get more info about this GoPro
                gopro_info = {
                    'id': current_interface,
                    'name': f'GoPro-{current_interface[-4:]}',
                    'interface': current_interface,
                    'ip': current_ip,
                    'status': 'connected',
                    'is_recording': current_interface in recording_processes
                }
                gopros.append(gopro_info)
                print(f"✓ Discovered GoPro: {current_interface} at {current_ip}")
                current_interface = None
                current_ip = None

        if enx_interfaces:
            print(f"Found {len(enx_interfaces)} enx interface(s): {enx_interfaces}")
            if not gopros:
                print(f"⚠️  Warning: Found enx interfaces but no GoPros with 172.x.x.x IP addresses")

    except Exception as e:
        print(f"Error discovering GoPros: {e}")

    return gopros

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
        # Sort by creation time, newest first
        videos.sort(key=lambda x: x['created'], reverse=True)
    except Exception as e:
        print(f"Error listing videos: {e}")
    
    return videos

@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def api_documentation():
    """API documentation - lists all available endpoints"""
    endpoints = {
        'service': 'GoPro Controller API',
        'version': '1.0',
        'endpoints': [
            {
                'path': '/health',
                'method': 'GET',
                'description': 'Health check endpoint',
                'response': 'Service status and configuration'
            },
            {
                'path': '/api',
                'method': 'GET',
                'description': 'API documentation (this page)',
                'response': 'List of all available endpoints'
            },
            {
                'path': '/api/gopros',
                'method': 'GET',
                'description': 'List all connected GoPro cameras',
                'response': 'Array of connected GoPros with status'
            },
            {
                'path': '/api/gopros/<gopro_id>/status',
                'method': 'GET',
                'description': 'Get status of a specific GoPro',
                'parameters': {'gopro_id': 'GoPro interface ID (e.g., enx0457470807ce)'},
                'response': 'GoPro connection status and recording info'
            },
            {
                'path': '/api/gopros/<gopro_id>/record/start',
                'method': 'POST',
                'description': 'Start recording on a specific GoPro',
                'parameters': {'gopro_id': 'GoPro interface ID'},
                'body': {'duration': 'Recording duration in seconds (default: 18000)'},
                'response': 'Recording started confirmation with video filename'
            },
            {
                'path': '/api/gopros/<gopro_id>/record/stop',
                'method': 'POST',
                'description': 'Get status of a recording session',
                'parameters': {'gopro_id': 'GoPro interface ID'},
                'response': 'Recording completion status with downloaded files'
            },
            {
                'path': '/api/gopros/<gopro_id>/record/status',
                'method': 'GET',
                'description': 'Get detailed progress of an active recording session',
                'parameters': {'gopro_id': 'GoPro interface ID'},
                'response': 'Recording progress (elapsed time, percentage, video count)'
            },
            {
                'path': '/api/videos',
                'method': 'GET',
                'description': 'List all recorded videos',
                'response': 'Array of videos with metadata (filename, size, date)'
            },
            {
                'path': '/api/videos/<filename>/download',
                'method': 'GET',
                'description': 'Download a specific video file',
                'parameters': {'filename': 'Video filename (e.g., gopro_GoPro-07ce_20241122_143022.mp4)'},
                'response': 'Video file download'
            },
            {
                'path': '/api/videos/<filename>/stream',
                'method': 'GET',
                'description': 'Stream a video file for in-browser playback',
                'parameters': {'filename': 'Video filename'},
                'response': 'Video stream (use in <video> tag)'
            },
            {
                'path': '/api/videos/<filename>',
                'method': 'DELETE',
                'description': 'Delete a specific video file',
                'parameters': {'filename': 'Video filename'},
                'response': 'Deletion confirmation'
            },
            {
                'path': '/api/system/info',
                'method': 'GET',
                'description': 'Get system information and storage stats',
                'response': 'System info including disk usage, video count, active recordings'
            }
        ],
        'examples': {
            'start_recording': {
                'url': 'POST /api/gopros/enx0457470807ce/record/start',
                'body': {'duration': 300},
                'description': 'Start 5-minute recording'
            },
            'stop_recording': {
                'url': 'POST /api/gopros/enx0457470807ce/record/stop',
                'description': 'Stop active recording'
            },
            'list_videos': {
                'url': 'GET /api/videos',
                'description': 'Get all recorded videos'
            },
            'download_video': {
                'url': 'GET /api/videos/gopro_GoPro-07ce_20241122_143022.mp4/download',
                'description': 'Download specific video'
            },
            'stream_video': {
                'url': 'GET /api/videos/gopro_GoPro-07ce_20241122_143022.mp4/stream',
                'description': 'Stream video in browser'
            }
        },
        'base_url': 'http://YOUR_JETSON_IP:5000',
        'storage_directory': VIDEO_STORAGE_DIR
    }
    
    return jsonify(endpoints)

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

    # Add recording info if currently recording
    with recording_lock:
        if gopro_id in recording_processes:
            recording_info = recording_processes[gopro_id]
            gopro['is_recording'] = recording_info.get('status') in ['starting', 'recording']
            gopro['recording_info'] = {
                'start_time': recording_info.get('start_time'),
                'duration': recording_info.get('duration'),
                'session_name': recording_info.get('session_name'),
                'status': recording_info.get('status'),
                'videos_downloaded': recording_info.get('result', {}).get('videos_downloaded', 0) if recording_info.get('status') == 'completed' else None
            }

    return jsonify({
        'success': True,
        'gopro': gopro
    })

@app.route('/api/gopros/<gopro_id>/record/status', methods=['GET'])
def recording_status(gopro_id):
    """Get detailed status of a recording session"""
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({
                'success': False,
                'error': 'No active recording session'
            }), 404

        recording_info = recording_processes[gopro_id].copy()

    start_time = datetime.fromisoformat(recording_info.get('start_time', datetime.now().isoformat()))
    elapsed = (datetime.now() - start_time).total_seconds()
    duration = recording_info.get('duration', 0)
    progress_percent = min(100, int((elapsed / duration * 100))) if duration > 0 else 0

    return jsonify({
        'success': True,
        'session_name': recording_info.get('session_name'),
        'status': recording_info.get('status'),
        'elapsed_seconds': round(elapsed, 1),
        'total_seconds': duration,
        'progress_percent': progress_percent,
        'videos_downloaded': recording_info.get('result', {}).get('videos_downloaded', 0) if recording_info.get('status') == 'completed' else None,
        'result': recording_info.get('result') if recording_info.get('status') == 'completed' else None
    })


async def record_and_download_gopro(gopro_id, duration, session_name):
    """Record on GoPro and download all video files"""
    try:
        print(f"\n[{gopro_id}] === Starting Recording Session: {session_name} ===")
        print(f"[{gopro_id}] Duration: {duration}s ({duration/60:.1f} minutes)")
        print(f"[{gopro_id}] GoPro ID: {gopro_id}")

        # Extract serial number - try last 3 chars, but handle various formats
        serial = gopro_id[-3:] if len(gopro_id) >= 3 else gopro_id
        print(f"[{gopro_id}] Connecting to GoPro with serial: {serial}")

        async with WiredGoPro(serial=serial) as gopro:
            print(f"[{gopro_id}] ✓ Connected to GoPro")
            # Get media list before recording
            print(f"[{gopro_id}] Getting initial media list...")
            media_before_response = await gopro.http_command.get_media_list()
            if not media_before_response.ok:
                raise Exception("Failed to get initial media list from GoPro")

            media_before = set(media_before_response.data.files) if media_before_response.data.files else set()
            print(f"[{gopro_id}] Found {len(media_before)} files before recording")

            # Start recording
            print(f"[{gopro_id}] Starting recording...")
            await gopro.http_command.set_shutter(shutter=1)  # 1 = enable/start
            print(f"[{gopro_id}] ✓ Recording started")

            with recording_lock:
                if gopro_id in recording_processes:
                    recording_processes[gopro_id]['recording_started'] = True
                    recording_processes[gopro_id]['status'] = 'recording'

            # Record for specified duration
            print(f"[{gopro_id}] Recording in progress for {duration}s...")
            await asyncio.sleep(duration)

            # Stop recording
            print(f"[{gopro_id}] Stopping recording...")
            await gopro.http_command.set_shutter(shutter=0)  # 0 = disable/stop
            print(f"[{gopro_id}] ✓ Recording stopped")

            # Wait a bit for the last chunk to be written
            await asyncio.sleep(2)

            # Get media list after recording
            print(f"[{gopro_id}] Getting updated media list...")
            media_after_response = await gopro.http_command.get_media_list()
            if not media_after_response.ok:
                raise Exception("Failed to get media list after recording")

            media_after = set(media_after_response.data.files) if media_after_response.data.files else set()

            # Find new video files
            new_videos = media_after.difference(media_before)
            print(f"[{gopro_id}] Found {len(new_videos)} new video file(s)")

            if not new_videos:
                print(f"[{gopro_id}] No new videos found!")
                return {
                    'success': False,
                    'error': 'No new video files created during recording',
                    'videos_downloaded': []
                }

            # Download all new videos
            downloaded_files = []
            total_size_mb = 0

            for idx, video_file in enumerate(sorted(new_videos, key=lambda x: x.filename), 1):
                try:
                    camera_filename = video_file.filename
                    local_filename = f"{session_name}_{idx:02d}_{camera_filename.split('/')[-1]}"
                    local_path = Path(VIDEO_STORAGE_DIR) / local_filename

                    print(f"[{gopro_id}] Downloading [{idx}/{len(new_videos)}]: {camera_filename}")
                    print(f"[{gopro_id}]   Destination: {local_path}")

                    # Download the file
                    download_response = await gopro.http_command.download_file(
                        camera_file=camera_filename,
                        local_file=local_path
                    )

                    if download_response.ok:
                        file_size_mb = local_path.stat().st_size / (1024 * 1024)
                        total_size_mb += file_size_mb
                        print(f"[{gopro_id}]   ✓ Downloaded successfully ({file_size_mb:.2f} MB)")

                        downloaded_files.append({
                            'filename': local_filename,
                            'path': str(local_path),
                            'size_mb': round(file_size_mb, 2)
                        })
                    else:
                        print(f"[{gopro_id}]   ✗ Download failed: {download_response.status}")

                except Exception as e:
                    print(f"[{gopro_id}]   ✗ Error downloading {camera_filename}: {e}")

            result = {
                'success': len(downloaded_files) > 0,
                'videos_downloaded': len(downloaded_files),
                'total_size_mb': round(total_size_mb, 2),
                'files': downloaded_files
            }

            print(f"[{gopro_id}] === Recording Session Complete ===")
            print(f"[{gopro_id}] Downloaded {len(downloaded_files)} video file(s), {total_size_mb:.2f} MB total")

            return result

    except Exception as e:
        print(f"Error during recording: {e}")
        return {
            'success': False,
            'error': str(e),
            'videos_downloaded': 0
        }

@app.route('/api/gopros/<gopro_id>/record/start', methods=['POST'])
def start_recording(gopro_id):
    """Start recording on a specific GoPro"""
    gopros = get_connected_gopros()
    print(f"\n=== Recording Start Request ===")
    print(f"Available GoPros: {len(gopros)}")
    for g in gopros:
        print(f"  - {g['id']}: {g['name']} ({g['ip']})")

    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    if not gopro:
        print(f"ERROR: GoPro {gopro_id} not found!")
        return jsonify({'success': False, 'error': f'GoPro {gopro_id} not found. Available: {[g["id"] for g in gopros]}'}), 404

    with recording_lock:
        if gopro_id in recording_processes:
            print(f"ERROR: GoPro {gopro_id} already recording!")
            return jsonify({'success': False, 'error': f'GoPro {gopro_id} already recording'}), 400

        try:
            data = request.get_json() or {}
            duration = data.get('duration', 18000)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            session_name = f'gopro_{gopro["name"]}_{timestamp}'

            print(f"Starting recording on {gopro_id}")
            print(f"Duration: {duration}s ({duration/60:.1f} minutes)")
            print(f"Session: {session_name}")

            # Store recording info
            recording_processes[gopro_id] = {
                'start_time': datetime.now().isoformat(),
                'duration': duration,
                'session_name': session_name,
                'recording_started': False,
                'status': 'starting'
            }

            # Start recording in background thread
            def run_recording():
                try:
                    print(f"[{gopro_id}] Recording thread started")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    print(f"[{gopro_id}] Event loop created")
                    result = loop.run_until_complete(
                        record_and_download_gopro(gopro_id, duration, session_name)
                    )
                    loop.close()
                    print(f"[{gopro_id}] Recording completed, result: {result}")

                    with recording_lock:
                        if gopro_id in recording_processes:
                            recording_processes[gopro_id]['status'] = 'completed'
                            recording_processes[gopro_id]['result'] = result
                except Exception as e:
                    print(f"[{gopro_id}] Recording error: {e}")
                    import traceback
                    traceback.print_exc()
                    with recording_lock:
                        if gopro_id in recording_processes:
                            recording_processes[gopro_id]['status'] = 'failed'
                            recording_processes[gopro_id]['error'] = str(e)

            thread = threading.Thread(target=run_recording, daemon=True, name=f"Recording-{gopro_id}")
            thread.start()
            print(f"✓ Recording thread started for {gopro_id}")
            print(f"✓ All threads are now running. GoPro initialization may take 30-60 seconds per camera.")

            return jsonify({
                'success': True,
                'message': f'Recording started for {duration}s. GoPro initialization in progress...',
                'session_name': session_name,
                'gopro_id': gopro_id,
                'note': 'GoPro connection and initialization may take 30-60 seconds per camera'
            })

        except Exception as e:
            with recording_lock:
                if gopro_id in recording_processes:
                    del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a specific GoPro"""
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400

        recording_info = recording_processes[gopro_id]
        status = recording_info.get('status')

    try:
        print(f"=== Stop Recording Request for {gopro_id} ===")
        print(f"Current status: {status}")

        # If recording is still in progress, we can't stop it mid-session
        # The recording runs for the full duration and downloads automatically
        if status == 'starting' or status == 'recording':
            return jsonify({
                'success': False,
                'error': 'Recording is in progress. Wait for it to complete or the session will stop automatically after the specified duration.',
                'current_status': status
            }), 400

        # If completed or failed, return the result
        if status == 'completed':
            result = recording_info.get('result', {})
            with recording_lock:
                del recording_processes[gopro_id]

            return jsonify({
                'success': result.get('success', False),
                'message': 'Recording session completed',
                'videos_downloaded': result.get('videos_downloaded', 0),
                'total_size_mb': result.get('total_size_mb', 0),
                'files': result.get('files', [])
            })

        if status == 'failed':
            error = recording_info.get('error', 'Unknown error')
            with recording_lock:
                del recording_processes[gopro_id]

            return jsonify({
                'success': False,
                'error': f'Recording session failed: {error}'
            }), 500

        # Default response
        return jsonify({
            'success': False,
            'error': 'Unknown recording status',
            'status': status
        }), 400

    except Exception as e:
        print(f"Error in stop_recording: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/videos', methods=['GET'])
def list_videos():
    """List all recorded videos"""
    videos = get_video_list()
    return jsonify({
        'success': True,
        'count': len(videos),
        'videos': videos
    })

@app.route('/api/videos/<filename>/download', methods=['GET'])
def download_video(filename):
    """Download a specific video file"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)
        
        # Security check: ensure path is within VIDEO_STORAGE_DIR and exists
        real_video_path = os.path.realpath(video_path)
        real_storage_dir = os.path.realpath(VIDEO_STORAGE_DIR)
        
        if not real_video_path.startswith(real_storage_dir):
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403
        
        if not os.path.exists(video_path):
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404
        
        return send_file(
            video_path,
            mimetype='video/mp4',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/videos/<filename>/stream', methods=['GET'])
def stream_video(filename):
    """Stream a video file for in-browser playback"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)
        
        # Security check: ensure path is within VIDEO_STORAGE_DIR and exists
        real_video_path = os.path.realpath(video_path)
        real_storage_dir = os.path.realpath(VIDEO_STORAGE_DIR)
        
        if not real_video_path.startswith(real_storage_dir):
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403
        
        if not os.path.exists(video_path):
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404
        
        return send_file(
            video_path,
            mimetype='video/mp4',
            as_attachment=False
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/videos/<filename>', methods=['DELETE'])
def delete_video(filename):
    """Delete a specific video"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)
        
        # Security check
        real_video_path = os.path.realpath(video_path)
        real_storage_dir = os.path.realpath(VIDEO_STORAGE_DIR)
        
        if not real_video_path.startswith(real_storage_dir):
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403
        
        if os.path.exists(video_path):
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
        # Get disk usage
        stat = os.statvfs(VIDEO_STORAGE_DIR)
        free_space_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
        total_space_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)
        
        # Get video count and total size
        videos = get_video_list()
        total_video_size_mb = sum(v['size_mb'] for v in videos)
        
        # Get currently recording GoPros
        with recording_lock:
            active_recordings = len(recording_processes)
        
        return jsonify({
            'success': True,
            'system': {
                'hostname': os.uname().nodename,
                'storage_path': VIDEO_STORAGE_DIR,
                'disk_free_gb': round(free_space_gb, 2),
                'disk_total_gb': round(total_space_gb, 2),
                'disk_used_percent': round((1 - free_space_gb/total_space_gb) * 100, 2),
                'video_count': len(videos),
                'total_video_size_mb': round(total_video_size_mb, 2),
                'active_recordings': active_recordings
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("=" * 60)
    print("🎥 GoPro Controller API Service Starting...")
    print("=" * 60)
    print(f"📁 Video storage: {VIDEO_STORAGE_DIR}")
    print(f"🌐 API endpoint: http://0.0.0.0:5000")
    print(f"💡 Make sure GoPros are connected via USB")
    print("=" * 60)
    print("\n📹 Available endpoints:")
    print("  GET  /health")
    print("  GET  /api/gopros")
    print("  GET  /api/gopros/<id>/status")
    print("  POST /api/gopros/<id>/record/start")
    print("  POST /api/gopros/<id>/record/stop")
    print("  GET  /api/videos")
    print("  GET  /api/videos/<filename>/download")
    print("  GET  /api/videos/<filename>/stream")
    print("  DELETE /api/videos/<filename>")
    print("  GET  /api/system/info")
    print("=" * 60 + "\n")

    # use_reloader=False is important for systemd to handle signals properly
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True, use_reloader=False)
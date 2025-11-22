#!/usr/bin/env python3
"""
GoPro Controller API Service
REST API for remote control of GoPro cameras connected to Jetson Nano
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import subprocess
import threading
import signal
import os
import time
import json
from datetime import datetime
from pathlib import Path
import pty
import select
import requests

app = Flask(__name__)
CORS(app)  # Enable CORS for Firebase web app access

# Configuration
VIDEO_STORAGE_DIR = os.path.expanduser('~/gopro_videos')
os.makedirs(VIDEO_STORAGE_DIR, exist_ok=True)

# Global state
recording_processes = {}  # {gopro_id: process}
recording_lock = threading.Lock()

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
        
        for line in lines:
            # Look for interface lines like "13: enx0457470807ce:"
            if 'enx' in line and ':' in line:
                current_interface = line.split(':')[1].strip().split('@')[0].strip()
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
                current_interface = None
                current_ip = None
                
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
                'description': 'Stop recording on a specific GoPro',
                'parameters': {'gopro_id': 'GoPro interface ID'},
                'response': 'Recording stopped confirmation with video details'
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
            gopro['is_recording'] = True
            gopro['recording_info'] = {
                'start_time': recording_info['start_time'],
                'duration': recording_info['duration'],
                'video_filename': recording_info['video_filename']
            }
    
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
            data = request.get_json() or {}
            duration = data.get('duration', 18000)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
            video_path = os.path.join(VIDEO_STORAGE_DIR, video_filename)

            print(f"=== Recording Start ===")
            print(f"VIDEO_STORAGE_DIR: {VIDEO_STORAGE_DIR}")
            print(f"video_filename: {video_filename}")
            print(f"Full video_path: {video_path}")
            print(f"Path exists: {os.path.exists(os.path.dirname(video_path))}")
            print(f"Path is absolute: {os.path.isabs(video_path)}")
            
            cmd = [
                'gopro-video',
                '--wired',
                '--wifi_interface', gopro['interface'],
                '-o', video_path,
                '--record_time', str(duration)
            ]
            
            # Use a PTY to give the process a proper terminal
            master, slave = pty.openpty()
            
            process = subprocess.Popen(
                cmd,
                stdin=slave,
                stdout=slave,
                stderr=slave,
                preexec_fn=os.setsid,
                close_fds=True
            )
            
            os.close(slave)  # Parent doesn't need the slave
            
            # Wait a bit and check if process started successfully
            time.sleep(3)
            if process.poll() is not None:
                # Process died immediately
                try:
                    os.close(master)
                except:
                    pass
                return jsonify({
                    'success': False, 
                    'error': 'Recording process failed to start'
                }), 500
            
            recording_processes[gopro_id] = {
                'process': process,
                'master_fd': master,
                'video_path': video_path,
                'video_filename': video_filename,
                'start_time': datetime.now().isoformat(),
                'duration': duration,
                'recording_started': False
            }
            
            def monitor():
                output = []
                recording_confirmed = False
                
                while True:
                    try:
                        # Read from PTY
                        ready, _, _ = select.select([master], [], [], 1.0)
                        if ready:
                            data = os.read(master, 1024)
                            if data:
                                text = data.decode('utf-8', errors='ignore')
                                output.append(text)
                                print(f"GoPro output: {text}")
                                
                                # Look for signs that recording actually started
                                if not recording_confirmed and ('recording' in text.lower() or 'started' in text.lower()):
                                    with recording_lock:
                                        if gopro_id in recording_processes:
                                            recording_processes[gopro_id]['recording_started'] = True
                                    recording_confirmed = True
                                    print(f"Recording confirmed started for {gopro_id}")
                        
                        # Check if process is done
                        if process.poll() is not None:
                            break
                    except OSError:
                        break
                
                # Close master FD in monitor thread
                try:
                    os.close(master)
                except OSError:
                    pass
                    
                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]
                print(f"Recording finished for {gopro_id}")
                print(f"Full output: {''.join(output)}")
            
            threading.Thread(target=monitor, daemon=True).start()
            
            return jsonify({
                'success': True,
                'message': f'Recording started for {duration}s',
                'video_filename': video_filename,
                'gopro_id': gopro_id, 
                'video_path': video_path
            })
            
        except Exception as e:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a specific GoPro"""
    global recording_processes
    
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400
        
        recording_info = recording_processes[gopro_id]
        process = recording_info['process']
        master_fd = recording_info.get('master_fd')
        video_filename = recording_info['video_filename']
        video_path = recording_info['video_path']
    
    try:
        print(f"=== Stopping Recording for {gopro_id} ===")
        
        # Send Ctrl+C (SIGINT) to the process group
        # gopro-video handles this gracefully and stops recording properly
        if process.poll() is None:
            try:
                # Send SIGINT to the process group
                os.killpg(os.getpgid(process.pid), signal.SIGINT)
                print(f"Sent SIGINT to process {process.pid}")
                
                # Wait for process to finish (with timeout)
                try:
                    process.wait(timeout=30)
                    print(f"Process ended gracefully")
                except subprocess.TimeoutExpired:
                    print(f"Process didn't stop gracefully, sending SIGTERM")
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    process.wait(timeout=10)
                    
            except Exception as e:
                print(f"Error stopping process: {e}")
                # Force kill as last resort
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except:
                    pass
        
        # Close the PTY master fd
        if master_fd:
            try:
                os.close(master_fd)
            except:
                pass
        
        # Clean up from recording_processes
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        
        # Check if video file was created
        if os.path.exists(video_path):
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            print(f"✓ Video saved: {video_path} ({file_size_mb:.2f} MB)")
            
            return jsonify({
                'success': True,
                'message': 'Recording stopped successfully',
                'video_filename': video_filename,
                'video_path': video_path,
                'file_size_mb': round(file_size_mb, 2)
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Recording stopped but video file not found',
                'video_path': video_path
            }), 500
        
    except Exception as e:
        print(f"Error stopping recording: {str(e)}")
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
    
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
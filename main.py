#!/usr/bin/env python3
"""
GoPro Controller API Service
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
from datetime import datetime
from pathlib import Path
import pty
import select
import signal
import requests
import threading
import re

app = Flask(__name__)
CORS(app)  # Enable CORS for Firebase web app access

# Configuration
VIDEO_STORAGE_DIR = os.path.expanduser('~/gopro_videos')
os.makedirs(VIDEO_STORAGE_DIR, exist_ok=True)

# Global state
recording_processes = {}  # {gopro_id: process}
recording_lock = threading.Lock()
gopro_ip_cache = {}  # {gopro_id: ip_address}

def discover_gopro_ip_for_interface(interface, our_ip):
    """Discover the GoPro's IP address on a specific interface"""
    try:
        # Parse our IP
        match = re.search(r'(\d+\.\d+\.\d+)\.(\d+)', our_ip)
        if not match:
            return None
        
        base = match.group(1)
        our_last = int(match.group(2))
        
        # Generate candidates
        candidates = []
        if our_last == 50:
            candidates = [f"{base}.51", f"{base}.1"]
        elif our_last == 51:
            candidates = [f"{base}.50", f"{base}.1"]
        else:
            candidates = [f"{base}.51", f"{base}.50", f"{base}.1"]
        
        # Remove our IP
        candidates = [ip for ip in candidates if ip != our_ip]
        
        # Test each
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

                # Discover GoPro IP for this interface
                gopro_ip = discover_gopro_ip_for_interface(current_interface, current_ip)
                if gopro_ip:
                    gopro_ip_cache[current_interface] = gopro_ip

                # Try to get more info about this GoPro
                gopro_info = {
                    'id': current_interface,
                    'name': f'GoPro-{current_interface[-4:]}',
                    'interface': current_interface,
                    'ip': current_ip,
                    'gopro_ip': gopro_ip,  # Add the actual GoPro IP
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
    # Check cache first
    if gopro_id in gopro_ip_cache:
        ip = gopro_ip_cache[gopro_id]
        # Verify it's still valid
        try:
            response = requests.get(f'http://{ip}:8080/gopro/camera/state', timeout=1)
            if response.status_code == 200:
                print(f"Using cached IP {ip} for {gopro_id}")
                return ip
        except:
            print(f"Cached IP {ip} for {gopro_id} is stale")
            pass  # Cache stale, rediscover below
    
    # Rediscover
    print(f"Rediscovering IP for {gopro_id}")
    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    
    if gopro and gopro.get('gopro_ip'):
        print(f"Found IP {gopro['gopro_ip']} for {gopro_id}")
        return gopro['gopro_ip']
    
    print(f"Could not find IP for {gopro_id}")
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
        # Sort by creation time, newest first
        videos.sort(key=lambda x: x['created'], reverse=True)
    except Exception as e:
        print(f"Error listing videos: {e}")

    return videos

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
            data = request.get_json() or {}
            duration = data.get('duration', 18000)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
            video_path = os.path.expanduser(os.path.join(VIDEO_STORAGE_DIR, video_filename))

            print(f"=== Recording Start ===")
            print(f"GoPro ID: {gopro_id}")
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

            # Add to recording_processes BEFORE starting monitor thread
            recording_processes[gopro_id] = {
                'process': process,
                'master_fd': master,
                'video_path': video_path,
                'video_filename': video_filename,
                'start_time': datetime.now().isoformat(),
                'duration': duration,
                'recording_started': False,
                'is_stopping': False  # Track if stop was requested
            }

            def monitor():
                output = []
                recording_confirmed = False

                while True:
                    try:
                        # Check if stop was requested
                        with recording_lock:
                            if gopro_id in recording_processes and recording_processes[gopro_id].get('is_stopping'):
                                print(f"Monitor detected stop request for {gopro_id}, exiting")
                                break

                        # Read from PTY
                        ready, _, _ = select.select([master], [], [], 1.0)
                        if ready:
                            data = os.read(master, 1024)
                            if data:
                                text = data.decode('utf-8', errors='ignore')
                                output.append(text)
                                print(f"GoPro {gopro_id} output: {text}")

                                # Look for signs that recording actually started
                                if not recording_confirmed and ('recording' in text.lower() or 'started' in text.lower()):
                                    with recording_lock:
                                        if gopro_id in recording_processes:
                                            recording_processes[gopro_id]['recording_started'] = True
                                    recording_confirmed = True
                                    print(f"✓ Recording confirmed started for {gopro_id}")

                        # Check if process is done
                        if process.poll() is not None:
                            print(f"Process ended for {gopro_id}, poll={process.poll()}")
                            break
                    except OSError as e:
                        print(f"OSError in monitor for {gopro_id}: {e}")
                        break
                    except Exception as e:
                        print(f"Error in monitor for {gopro_id}: {e}")
                        break

                # Close master FD in monitor thread
                try:
                    os.close(master)
                except OSError:
                    pass

                # Only remove from dict if NOT stopping (stop handler will manage it)
                with recording_lock:
                    if gopro_id in recording_processes:
                        if not recording_processes[gopro_id].get('is_stopping'):
                            print(f"Monitor cleaning up {gopro_id} (natural end)")
                            del recording_processes[gopro_id]
                        else:
                            print(f"Monitor NOT cleaning up {gopro_id} (stop in progress)")
                
                print(f"Recording monitor finished for {gopro_id}")
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

    print(f"\n=== Stop Recording Request ===")
    print(f"GoPro ID: {gopro_id}")
    print(f"Current recording_processes keys: {list(recording_processes.keys())}")

    with recording_lock:
        if gopro_id not in recording_processes:
            print(f"ERROR: {gopro_id} not in recording_processes!")
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400

        # Mark as stopping to prevent monitor from cleaning up
        recording_processes[gopro_id]['is_stopping'] = True
        recording_info = recording_processes[gopro_id].copy()

    try:
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']
        process = recording_info['process']
        master_fd = recording_info.get('master_fd')

        print(f"Process status: {process.poll()}")
        print(f"Video path: {video_path}")

        # FIRST: Kill the gopro-video process immediately
        if process.poll() is None:  # If still running
            try:
                print(f"Sending SIGTERM to process group...")
                # Send SIGTERM to the process group
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                
                # Wait a bit for graceful shutdown
                try:
                    process.wait(timeout=3)
                    print(f"✓ Process terminated gracefully")
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't stop
                    print(f"Process didn't stop, sending SIGKILL...")
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    process.wait(timeout=1)
                    print(f"✓ Process force killed")
                    
            except Exception as e:
                print(f"Error stopping process: {e}")
        else:
            print(f"Process already stopped (poll={process.poll()})")

        # Close the PTY master
        if master_fd:
            try:
                os.close(master_fd)
                print(f"✓ Closed PTY master")
            except Exception as e:
                print(f"Error closing PTY: {e}")

        # Find GoPro IP
        gopro_ip = get_gopro_wired_ip(gopro_id)
        if not gopro_ip:
            print(f"ERROR: Could not detect GoPro IP for {gopro_id}")
            # Clean up and return error
            with recording_lock:
                if gopro_id in recording_processes:
                    del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': 'Could not detect GoPro IP'}), 500

        print(f"Found GoPro IP: {gopro_ip} for {gopro_id}")

        # Send stop command to GoPro
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/camera/shutter/stop',
                timeout=5
            )
            print(f"Stop command response: {response.status_code}")
            if response.status_code != 200:
                print(f"Warning: Stop command returned {response.status_code}")
        except Exception as e:
            print(f"Warning: Could not send stop command to GoPro: {e}")

        # Update status to downloading
        with recording_lock:
            if gopro_id in recording_processes:
                recording_processes[gopro_id]['downloading'] = True
                recording_processes[gopro_id]['download_progress'] = 0
                print(f"✓ Marked as downloading")

        # Start download in background thread
        def download_video():
            try:
                print(f"Waiting 3s for GoPro to finalize...")
                time.sleep(3)

                # Get media list
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

                with open(video_path, 'wb') as f:
                    for chunk in download_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Update progress
                            if total_size > 0:
                                progress = int((downloaded / total_size) * 100)
                                with recording_lock:
                                    if gopro_id in recording_processes:
                                        recording_processes[gopro_id]['download_progress'] = progress
                                if progress % 10 == 0:  # Log every 10%
                                    print(f"Download progress for {gopro_id}: {progress}%")

                print(f"✓ Download complete: {video_path}")
                print(f"File size: {os.path.getsize(video_path)} bytes")

                # Final cleanup
                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]
                        print(f"✓ Cleaned up recording_processes for {gopro_id}")

            except Exception as e:
                print(f"Download error for {gopro_id}: {e}")
                import traceback
                traceback.print_exc()
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['download_error'] = str(e)

        threading.Thread(target=download_video, daemon=True).start()

        return jsonify({
            'success': True,
            'message': 'Recording stopped, downloading video...',
            'video_filename': video_filename,
            'note': 'Check status endpoint for download progress'
        })

    except Exception as e:
        print(f"Error in stop_recording: {str(e)}")
        import traceback
        traceback.print_exc()
        # Make sure to clean up
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
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
        # Get disk usage
        stat = os.statvfs(VIDEO_STORAGE_DIR)
        free_space_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
        total_space_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)

        # Get video count and total size
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
        
        # Security check: ensure the file exists and is within VIDEO_STORAGE_DIR
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
        
        # Send file as attachment (triggers download)
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
        
        # Security check: ensure the file exists and is within VIDEO_STORAGE_DIR
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
        
        # Get file size
        file_size = os.path.getsize(video_path)
        
        # Check if client requested a range
        range_header = request.headers.get('Range', None)
        
        if not range_header:
            # No range requested, send entire file
            return send_file(
                video_path,
                mimetype='video/mp4',
                conditional=True
            )
        
        # Parse range header
        # Format: "bytes=start-end"
        byte_range = range_header.replace('bytes=', '').split('-')
        start = int(byte_range[0]) if byte_range[0] else 0
        end = int(byte_range[1]) if byte_range[1] else file_size - 1
        
        # Ensure end doesn't exceed file size
        end = min(end, file_size - 1)
        length = end - start + 1
        
        # Read the requested chunk
        with open(video_path, 'rb') as f:
            f.seek(start)
            data = f.read(length)
        
        # Create response with partial content
        response = Response(
            data,
            206,  # Partial Content status code
            mimetype='video/mp4',
            direct_passthrough=True
        )
        
        # Set headers for range request
        response.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
        response.headers.add('Accept-Ranges', 'bytes')
        response.headers.add('Content-Length', str(length))
        
        return response
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def monitor_recording(gopro_id):
    """Monitor recording process and clean up when finished"""
    global recording_processes

    if gopro_id in recording_processes:
        process = recording_processes[gopro_id]['process']
        process.wait()

        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]

        print(f"Recording finished for {gopro_id}")

if __name__ == '__main__':
    print("=" * 60)
    print("🎥 GoPro Controller API Service Starting...")
    print("=" * 60)
    print(f"📁 Video storage: {VIDEO_STORAGE_DIR}")
    print(f"🌐 API endpoint: http://0.0.0.0:5000")
    print(f"💡 Make sure GoPros are connected via USB")
    print("=" * 60)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
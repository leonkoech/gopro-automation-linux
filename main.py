#!/usr/bin/env python3
"""
GoPro Controller API Service
REST API for remote control of GoPro cameras connected to Jetson Nano
"""

from flask import Flask, jsonify, request
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
                'recording_started': False  # Track if we've confirmed recording started
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

        recording_info = recording_processes[gopro_id].copy()

    try:
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']

        # Find GoPro IP
        gopro_ip = get_gopro_wired_ip(gopro_id)
        if not gopro_ip:
            return jsonify({'success': False, 'error': 'Could not detect GoPro IP'}), 500

        # Stop recording
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/camera/shutter/stop',
            timeout=5
        )

        if response.status_code != 200:
            return jsonify({'success': False, 'error': f'Stop command failed'}), 500

        # Mark as downloading
        with recording_lock:
            if gopro_id in recording_processes:
                recording_processes[gopro_id]['downloading'] = True
                recording_processes[gopro_id]['download_progress'] = 0

        # Start download in background thread
        def download_video():
            try:
                time.sleep(3)  # Wait for GoPro to finalize

                # Get media list
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

                print(f"✓ Download complete: {video_path}")

                # Clean up
                process = recording_info['process']
                if process.poll() is None:
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                        process.wait(timeout=5)
                    except:
                        pass

                master_fd = recording_info.get('master_fd')
                if master_fd:
                    try:
                        os.close(master_fd)
                    except:
                        pass

                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]

            except Exception as e:
                print(f"Download error: {e}")
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
        print(f"Error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

def get_gopro_wired_ip(gopro_id):
    """Get the actual GoPro IP by checking what gopro-video is using"""
    try:
        # When gopro-video connects, the GoPro creates a network interface
        # We can find the IP by looking at the interface's gateway
        gopros = get_connected_gopros()
        gopro = next((g for g in gopros if g['id'] == gopro_id), None)

        if not gopro:
            return None

        interface = gopro['interface']

        # Use ip route to find the GoPro's IP on this interface
        result = subprocess.run(
            ['ip', 'route', 'show', 'dev', interface],
            capture_output=True,
            text=True,
            timeout=2
        )

        # Parse output to find the network
        # Typical output: "172.20.110.0/24 proto kernel scope link src 172.20.110.50"
        lines = result.stdout.strip().split('\n')
        for line in lines:
            if 'proto kernel' in line or 'scope link' in line:
                # Extract network, GoPro is typically .51
                parts = line.split()
                for part in parts:
                    if '/' in part and '.' in part:  # CIDR notation
                        network = part.split('/')[0]
                        # GoPro is usually .51 in the same subnet
                        base = '.'.join(network.split('.')[:-1])
                        gopro_ip = f"{base}.51"

                        # Verify this IP responds
                        try:
                            test = requests.get(f'http://{gopro_ip}:8080/gopro/camera/state', timeout=1)
                            if test.status_code == 200:
                                print(f"Found GoPro at {gopro_ip}")
                                return gopro_ip
                        except:
                            pass

        # Fallback: try common IPs
        for last_octet in [51, 1]:
            try:
                # Try to get our own IP on this interface
                result = subprocess.run(
                    ['ip', 'addr', 'show', interface],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                # Look for inet address
                import re
                match = re.search(r'inet (\d+\.\d+\.\d+)\.\d+', result.stdout)
                if match:
                    base = match.group(1)
                    gopro_ip = f"{base}.{last_octet}"
                    test = requests.get(f'http://{gopro_ip}:8080/gopro/camera/state', timeout=1)
                    if test.status_code == 200:
                        print(f"Found GoPro at {gopro_ip}")
                        return gopro_ip
            except:
                pass

        return None

    except Exception as e:
        print(f"Error finding GoPro IP: {e}")
        return None


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

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
import logging
from datetime import datetime
from pathlib import Path
import pty
import select
import requests

app = Flask(__name__)
CORS(app)  # Enable CORS for Firebase web app access

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger(__name__)
# Also add Flask's logger
app.logger.setLevel(logging.DEBUG)

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
                logger.debug(f"Found enx interface: {current_interface}")
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
                logger.info(f"Discovered GoPro: {current_interface} at {current_ip}")
                current_interface = None
                current_ip = None

        logger.info(f"Total GoPros discovered: {len(gopros)}")

    except Exception as e:
        logger.exception(f"Error discovering GoPros")

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
    logger.info(f"=== Recording Start Request for {gopro_id} ===")

    gopros = get_connected_gopros()
    logger.info(f"Available GoPros: {[g['id'] for g in gopros]}")

    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    if not gopro:
        logger.error(f"GoPro {gopro_id} not found!")
        return jsonify({'success': False, 'error': 'GoPro not found'}), 404

    with recording_lock:
        if gopro_id in recording_processes:
            logger.warning(f"GoPro {gopro_id} already recording!")
            return jsonify({'success': False, 'error': 'Already recording'}), 400

        try:
            data = request.get_json() or {}
            duration = data.get('duration', 18000)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
            video_path = os.path.expanduser(os.path.join(VIDEO_STORAGE_DIR, video_filename))

            logger.info(f"Recording Start for {gopro_id}")
            logger.info(f"VIDEO_STORAGE_DIR: {VIDEO_STORAGE_DIR}")
            logger.info(f"video_filename: {video_filename}")
            logger.info(f"Full video_path: {video_path}")
            logger.info(f"Duration: {duration}s ({duration/60:.1f} minutes)")

            cmd = [
                'gopro-video',
                '--wired',
                '--wifi_interface', gopro['interface'],
                '-o', video_path,
                '--record_time', str(duration)
            ]
            logger.info(f"Command: {' '.join(cmd)}")

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
            logger.info(f"Process started with PID: {process.pid}")

            # Wait a bit and check if process started successfully
            time.sleep(3)
            if process.poll() is not None:
                # Process died immediately
                logger.error(f"Recording process failed to start for {gopro_id}")
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
                logger.info(f"Monitor thread started for {gopro_id}")
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
                                logger.debug(f"[{gopro_id}] Output: {text.strip()}")

                                # Look for signs that recording actually started
                                if not recording_confirmed and ('recording' in text.lower() or 'started' in text.lower()):
                                    with recording_lock:
                                        if gopro_id in recording_processes:
                                            recording_processes[gopro_id]['recording_started'] = True
                                    recording_confirmed = True
                                    logger.info(f"Recording confirmed started for {gopro_id}")

                        # Check if process is done
                        if process.poll() is not None:
                            logger.info(f"Process for {gopro_id} has exited")
                            break
                    except OSError as e:
                        logger.error(f"OSError in monitor for {gopro_id}: {e}")
                        break

                # Close master FD in monitor thread
                try:
                    os.close(master)
                except OSError:
                    pass

                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]
                logger.info(f"Recording finished for {gopro_id}")
                logger.debug(f"Full output: {''.join(output)}")

            threading.Thread(target=monitor, daemon=True, name=f"Monitor-{gopro_id}").start()

            logger.info(f"Recording started successfully for {gopro_id}")
            return jsonify({
                'success': True,
                'message': f'Recording started for {duration}s',
                'video_filename': video_filename,
                'gopro_id': gopro_id,
                'video_path': video_path
            })

        except Exception as e:
            logger.exception(f"Exception starting recording for {gopro_id}")
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/status', methods=['GET'])
def record_status(gopro_id):
    """Get the status of a recording session"""
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'No active recording'}), 404

        recording_info = recording_processes[gopro_id].copy()

    process = recording_info['process']
    is_running = process.poll() is None

    return jsonify({
        'success': True,
        'gopro_id': gopro_id,
        'is_recording': is_running,
        'filename': recording_info['video_filename'],
        'start_time': recording_info['start_time'],
        'duration': recording_info['duration'],
        'recording_started': recording_info.get('recording_started', False)
    })

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a specific GoPro"""
    global recording_processes

    logger.info(f"=== Stop Recording Request for {gopro_id} ===")

    with recording_lock:
        if gopro_id not in recording_processes:
            logger.warning(f"GoPro {gopro_id} not currently recording")
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400

        recording_info = recording_processes[gopro_id].copy()

    try:
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']
        process = recording_info['process']

        logger.info(f"Stopping recording process for {gopro_id}")
        logger.info(f"Process PID: {process.pid}, Poll status: {process.poll()}")

        # First, terminate the gopro-video process
        if process.poll() is None:
            logger.info(f"Terminating process {process.pid}")
            try:
                # Use terminate() first (works cross-platform)
                process.terminate()
                logger.info(f"Sent SIGTERM to process {process.pid}")
                process.wait(timeout=10)
                logger.info(f"Process terminated successfully")
            except subprocess.TimeoutExpired:
                logger.error(f"Process did not terminate after SIGTERM, killing...")
                try:
                    process.kill()
                    logger.info(f"Sent SIGKILL to process {process.pid}")
                    process.wait(timeout=5)
                    logger.info(f"Process killed successfully")
                except Exception as e2:
                    logger.error(f"Failed to kill process: {e2}")
            except Exception as e:
                logger.error(f"Error terminating process: {e}")
        else:
            logger.info(f"Process already terminated with return code: {process.returncode}")

        # Close master FD
        master_fd = recording_info.get('master_fd')
        if master_fd:
            try:
                os.close(master_fd)
                logger.info(f"Master FD closed")
            except Exception as e:
                logger.debug(f"Error closing master FD: {e}")

        # Remove from recording processes
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]

        logger.info(f"Recording stopped for {gopro_id}")

        return jsonify({
            'success': True,
            'message': 'Recording stopped',
            'video_filename': video_filename,
            'video_path': video_path
        })

    except Exception as e:
        logger.exception(f"Error stopping recording for {gopro_id}")
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

@app.route('/api/videos/<filename>/download', methods=['GET'])
def download_video(filename):
    """Download a specific video file"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)

        # Security check: ensure path is within VIDEO_STORAGE_DIR
        real_video_path = os.path.realpath(video_path)
        real_storage_dir = os.path.realpath(VIDEO_STORAGE_DIR)

        if not real_video_path.startswith(real_storage_dir):
            logger.warning(f"Attempted path traversal: {video_path}")
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403

        if not os.path.exists(video_path):
            logger.warning(f"Video not found: {video_path}")
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404

        logger.info(f"Downloading video: {filename}")
        return send_file(
            video_path,
            mimetype='video/mp4',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        logger.exception(f"Error downloading video {filename}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/videos/<filename>/stream', methods=['GET'])
def stream_video(filename):
    """Stream a video file for in-browser playback"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)

        # Security check: ensure path is within VIDEO_STORAGE_DIR
        real_video_path = os.path.realpath(video_path)
        real_storage_dir = os.path.realpath(VIDEO_STORAGE_DIR)

        if not real_video_path.startswith(real_storage_dir):
            logger.warning(f"Attempted path traversal: {video_path}")
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403

        if not os.path.exists(video_path):
            logger.warning(f"Video not found for streaming: {video_path}")
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404

        logger.info(f"Streaming video: {filename}")
        return send_file(
            video_path,
            mimetype='video/mp4',
            as_attachment=False
        )
    except Exception as e:
        logger.exception(f"Error streaming video {filename}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/videos/<filename>', methods=['DELETE'])
def delete_video(filename):
    """Delete a specific video"""
    try:
        video_path = os.path.join(VIDEO_STORAGE_DIR, filename)

        # Security check: ensure path is within VIDEO_STORAGE_DIR
        real_video_path = os.path.realpath(video_path)
        real_storage_dir = os.path.realpath(VIDEO_STORAGE_DIR)

        if not real_video_path.startswith(real_storage_dir):
            logger.warning(f"Attempted path traversal deletion: {video_path}")
            return jsonify({
                'success': False,
                'error': 'Invalid file path'
            }), 403

        if os.path.exists(video_path):
            os.remove(video_path)
            logger.info(f"Deleted video: {filename}")
            return jsonify({
                'success': True,
                'message': f'Video {filename} deleted'
            })
        else:
            logger.warning(f"Video not found for deletion: {filename}")
            return jsonify({
                'success': False,
                'error': 'Video not found'
            }), 404
    except Exception as e:
        logger.exception(f"Error deleting video {filename}")
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
    logger.info("=" * 60)
    logger.info("🎥 GoPro Controller API Service Starting...")
    logger.info("=" * 60)
    logger.info(f"📁 Video storage: {VIDEO_STORAGE_DIR}")
    logger.info(f"🌐 API endpoint: http://0.0.0.0:5000")
    logger.info(f"💡 Make sure GoPros are connected via USB")
    logger.info("=" * 60)
    logger.info("Starting Flask app...")

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

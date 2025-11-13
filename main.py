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


@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a specific GoPro"""
    global recording_processes
    
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400
        
        recording_info = recording_processes[gopro_id].copy()
    
    try:
        process = recording_info['process']
        master_fd = recording_info.get('master_fd')
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']
        
        print(f"=== Attempting to stop recording for {gopro_id} ===")
        
        # Step 1: Find the GoPro's actual IP
        gopro_ip = get_gopro_wired_ip(gopro_id)
        print(f"GoPro IP detected: {gopro_ip}")
        
        stop_sent = False
        if gopro_ip:
            # Step 2: Send HTTP stop command
            try:
                print(f"Sending stop command to http://{gopro_ip}:8080/gopro/camera/shutter/stop")
                response = requests.get(
                    f'http://{gopro_ip}:8080/gopro/camera/shutter/stop',
                    timeout=5
                )
                print(f"HTTP Response: Status={response.status_code}")
                
                if response.status_code == 200:
                    stop_sent = True
                    print("✓ Stop command sent successfully")
                    print("⏳ Waiting for gopro-video to download the file...")
                    
                    # CRITICAL: Wait for gopro-video to download the file
                    # Monitor file size to know when download is complete
                    download_timeout = 120  # 2 minutes max
                    start_wait = time.time()
                    last_size = 0
                    stable_count = 0
                    
                    while time.time() - start_wait < download_timeout:
                        if os.path.exists(video_path):
                            current_size = os.path.getsize(video_path)
                            print(f"Video file size: {current_size} bytes")
                            
                            if current_size > 0 and current_size == last_size:
                                stable_count += 1
                                # If size hasn't changed for 3 checks (6 seconds), download is done
                                if stable_count >= 3:
                                    print("✓ File size stable, download complete")
                                    break
                            else:
                                stable_count = 0
                            
                            last_size = current_size
                        
                        # Check if process finished on its own
                        if process.poll() is not None:
                            print("✓ gopro-video process finished")
                            break
                        
                        time.sleep(2)
                    
                    # Give it a bit more time just to be safe
                    time.sleep(2)
                    
                else:
                    print(f"✗ Stop command failed with status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                print(f"✗ HTTP request failed: {e}")
        else:
            print("✗ Could not detect GoPro IP")
        
        if not stop_sent:
            print("WARNING: Could not send stop command to GoPro")
            return jsonify({
                'success': False,
                'error': 'Could not send stop command to GoPro',
                'gopro_ip': gopro_ip
            }), 500
        
        # Step 3: Check if download completed
        file_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
        print(f"Final video file size: {file_size} bytes")
        
        # Step 4: Now it's safe to kill the process (if it's still running)
        if process.poll() is None:
            print("Process still running, terminating...")
            try:
                pgid = os.getpgid(process.pid)
                os.killpg(pgid, signal.SIGTERM)
                process.wait(timeout=5)
                print(f"Process exited with code: {process.returncode}")
            except (subprocess.TimeoutExpired, ProcessLookupError):
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        
        # Close PTY
        if master_fd:
            try:
                os.close(master_fd)
            except OSError:
                pass
        
        # Clean up from recording_processes
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        
        video_exists = os.path.exists(video_path)
        
        print(f"=== Stop Recording Results ===")
        print(f"Video exists: {video_exists}")
        print(f"File size: {file_size} bytes")
        print(f"HTTP stop sent: {stop_sent}")
        
        success = stop_sent and file_size > 0
        
        return jsonify({
            'success': success,
            'message': 'Recording stopped and video downloaded' if success else 'Recording stopped but video not downloaded',
            'video_filename': video_filename,
            'video_path': video_path,
            'video_exists': video_exists,
            'file_size': file_size,
            'gopro_ip': gopro_ip,
            'warning': None if success else 'Video file is empty or missing'
        })
        
    except Exception as e:
        print(f"✗ Error stopping recording: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

import requests

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
                for i, part in enumerate(parts):
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


@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a specific GoPro"""
    global recording_processes
    
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({'success': False, 'error': 'Not currently recording'}), 400
        
        recording_info = recording_processes[gopro_id].copy()
    
    try:
        process = recording_info['process']
        master_fd = recording_info.get('master_fd')
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']
        
        print(f"=== Attempting to stop recording for {gopro_id} ===")
        
        # Step 1: Find the GoPro's actual IP
        gopro_ip = get_gopro_wired_ip(gopro_id)
        print(f"GoPro IP detected: {gopro_ip}")
        
        stop_sent = False
        if gopro_ip:
            # Step 2: Send HTTP stop command FIRST (before killing process)
            try:
                print(f"Sending stop command to http://{gopro_ip}:8080/gopro/camera/shutter/stop")
                response = requests.get(
                    f'http://{gopro_ip}:8080/gopro/camera/shutter/stop',
                    timeout=5
                )
                print(f"HTTP Response: Status={response.status_code}, Body={response.text}")
                
                if response.status_code == 200:
                    stop_sent = True
                    print("✓ Stop command sent successfully")
                    # Wait for GoPro to actually stop and save the file
                    time.sleep(3)
                else:
                    print(f"✗ Stop command failed with status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                print(f"✗ HTTP request failed: {e}")
        else:
            print("✗ Could not detect GoPro IP")
        
        if not stop_sent:
            print("WARNING: Could not send stop command to GoPro - it may continue recording!")
        
        # Step 3: Check file size BEFORE killing process
        file_size_before = os.path.getsize(video_path) if os.path.exists(video_path) else 0
        print(f"Video file size before process kill: {file_size_before} bytes")
        
        # Step 4: Now terminate the gopro-video process gracefully
        # Use SIGTERM first (not SIGINT) and wait longer
        try:
            pgid = os.getpgid(process.pid)
            print(f"Sending SIGTERM to process group {pgid}")
            os.killpg(pgid, signal.SIGTERM)
            
            # Wait up to 10 seconds for graceful shutdown
            process.wait(timeout=10)
            print(f"Process exited with code: {process.returncode}")
            
        except subprocess.TimeoutExpired:
            print("Process didn't exit gracefully, sending SIGKILL")
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                process.wait(timeout=3)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                pass
        except ProcessLookupError:
            print("Process already terminated")
        
        # Close PTY
        if master_fd:
            try:
                os.close(master_fd)
            except OSError:
                pass
        
        # Wait for file system to sync
        time.sleep(2)
        
        # Clean up from recording_processes
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        
        # Step 5: Check final video file status
        video_exists = os.path.exists(video_path)
        file_size = os.path.getsize(video_path) if video_exists else 0
        
        print(f"=== Stop Recording Results ===")
        print(f"Video exists: {video_exists}")
        print(f"File size: {file_size} bytes")
        print(f"HTTP stop sent: {stop_sent}")
        print(f"GoPro IP used: {gopro_ip}")
        
        return jsonify({
            'success': stop_sent,  # Only claim success if we actually sent the stop command
            'message': 'Recording stopped' if stop_sent else 'Process killed but stop command may not have reached GoPro',
            'video_filename': video_filename,
            'video_exists': video_exists,
            'file_size': file_size,
            'gopro_ip': gopro_ip,
            'http_stop_sent': stop_sent,
            'warning': None if stop_sent and file_size > 0 else 'Recording may not have stopped properly on GoPro'
        })
        
    except Exception as e:
        print(f"✗ Error stopping recording: {str(e)}")
        import traceback
        traceback.print_exc()
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
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
            duration = data.get('duration', 1800)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
            video_path = os.path.expanduser(os.path.join(VIDEO_STORAGE_DIR, video_filename))
            
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
                'gopro_id': gopro_id
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
    
    # Get GoPro info to find interface
    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    
    try:
        process = recording_info['process']
        master_fd = recording_info.get('master_fd')
        video_path = recording_info['video_path']
        video_filename = recording_info['video_filename']
        
        print(f"Attempting to stop recording for {gopro_id}")
        
        # Find the GoPro's IP
        gopro_ip = get_gopro_wired_ip(gopro['interface']) if gopro else None
        
        if gopro_ip:
            try:
                print(f"Sending stop command to GoPro at {gopro_ip}")
                response = requests.get(
                    f'http://{gopro_ip}:8080/gopro/camera/shutter/stop',
                    timeout=5
                )
                if response.status_code == 200:
                    print("Successfully stopped recording on GoPro")
                    time.sleep(2)  # Wait for GoPro to stop
                else:
                    print(f"Stop command returned status {response.status_code}")
            except Exception as e:
                print(f"Error sending HTTP stop command: {e}")
        else:
            print("Could not find GoPro IP, killing process only")
        
        # Kill the gopro-video process
        try:
            pgid = os.getpgid(process.pid)
            os.killpg(pgid, signal.SIGTERM)
            process.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
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
        
        time.sleep(1)
        
        # Clean up
        with recording_lock:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
        
        # Check video file
        video_exists = os.path.exists(video_path)
        file_size = os.path.getsize(video_path) if video_exists else 0
        
        return jsonify({
            'success': True,
            'message': 'Recording stopped',
            'video_filename': video_filename,
            'video_exists': video_exists,
            'file_size': file_size,
            'gopro_ip': gopro_ip
        })
        
    except Exception as e:
        print(f"Error stopping recording: {str(e)}")
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

def get_gopro_wired_ip(interface):
    """Get the GoPro's IP address on the wired interface"""
    try:
        # GoPro typically uses 172.2X.110.51 on wired connections
        # We can try to ping common IPs or use ARP
        import subprocess
        
        # Try common GoPro wired IPs based on interface
        common_ips = [
            '172.20.110.51',
            '172.21.110.51', 
            '172.22.110.51',
            '172.23.110.51',
            '172.24.110.51',
            '172.25.110.51',
        ]
        
        for ip in common_ips:
            try:
                # Try to connect to GoPro's HTTP API
                response = requests.get(f'http://{ip}:8080/gopro/camera/state', timeout=1)
                if response.status_code == 200:
                    print(f"Found GoPro at {ip}")
                    return ip
            except:
                continue
        
        return None
    except Exception as e:
        print(f"Error finding GoPro IP: {e}")
        return None

if __name__ == '__main__':
    print("=" * 60)
    print("🎥 GoPro Controller API Service Starting...")
    print("=" * 60)
    print(f"📁 Video storage: {VIDEO_STORAGE_DIR}")
    print(f"🌐 API endpoint: http://0.0.0.0:5000")
    print(f"💡 Make sure GoPros are connected via USB")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
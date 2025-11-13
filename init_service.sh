#!/bin/bash
# Installation script for GoPro Controller Service
# Run this script to install the service to start automatically on boot

set -e

echo "========================================"
echo "GoPro Controller Service Installer"
echo "========================================"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Please run as root (use sudo)"
    exit 1
fi

# Get the actual user (not root)
ACTUAL_USER=${SUDO_USER:-$USER}
USER_HOME=$(eval echo ~$ACTUAL_USER)

echo "📋 Configuration:"
echo "   User: $ACTUAL_USER"
echo "   Home: $USER_HOME"
echo ""

# Install dependencies
echo "📦 Installing dependencies..."
pip3 install flask flask-cors

# Create service directory
SERVICE_DIR="/opt/gopro-controller"
echo "📁 Creating service directory: $SERVICE_DIR"
mkdir -p "$SERVICE_DIR"

# Copy the Python script
echo "📄 Installing GoPro Controller script..."
cat > "$SERVICE_DIR/gopro_controller.py" << 'PYTHON_SCRIPT_EOF'
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
        return jsonify({
            'success': False,
            'error': 'GoPro not found'
        }), 404
    
    with recording_lock:
        if gopro_id in recording_processes:
            return jsonify({
                'success': False,
                'error': 'Already recording on this GoPro'
            }), 400
        
        try:
            data = request.get_json() or {}
            duration = data.get('duration', 30)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
            video_path = os.path.join(VIDEO_STORAGE_DIR, video_filename)
            
            cmd = [
                'gopro-video', 
                '--wired', 
                '--wifi_interface', gopro['interface'],
                '-o', video_path, 
                str(duration)
            ]
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            
            recording_processes[gopro_id] = {
                'process': process,
                'video_path': video_path,
                'video_filename': video_filename,
                'start_time': datetime.now().isoformat(),
                'duration': duration
            }
            
            threading.Thread(
                target=monitor_recording, 
                args=(gopro_id,), 
                daemon=True
            ).start()
            
            return jsonify({
                'success': True,
                'message': f'Recording started for {duration}s',
                'video_filename': video_filename,
                'gopro_id': gopro_id
            })
            
        except Exception as e:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a specific GoPro"""
    global recording_processes
    
    with recording_lock:
        if gopro_id not in recording_processes:
            return jsonify({
                'success': False,
                'error': 'Not currently recording on this GoPro'
            }), 400
        
        try:
            recording_info = recording_processes[gopro_id]
            process = recording_info['process']
            
            os.killpg(os.getpgid(process.pid), signal.SIGINT)
            
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            
            video_filename = recording_info['video_filename']
            del recording_processes[gopro_id]
            
            return jsonify({
                'success': True,
                'message': 'Recording stopped',
                'video_filename': video_filename
            })
            
        except Exception as e:
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
PYTHON_SCRIPT_EOF

chmod +x "$SERVICE_DIR/gopro_controller.py"
chown -R $ACTUAL_USER:$ACTUAL_USER "$SERVICE_DIR"

# Create systemd service file
echo "⚙️  Creating systemd service..."
cat > /etc/systemd/system/gopro-controller.service << SERVICE_EOF
[Unit]
Description=GoPro Controller API Service
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$SERVICE_DIR
Environment="PATH=/usr/local/bin:/usr/bin:/bin"
ExecStart=/usr/bin/python3 $SERVICE_DIR/gopro_controller.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICE_EOF

# Create video storage directory
VIDEO_DIR="$USER_HOME/gopro_videos"
echo "📁 Creating video storage directory: $VIDEO_DIR"
mkdir -p "$VIDEO_DIR"
chown -R $ACTUAL_USER:$ACTUAL_USER "$VIDEO_DIR"

# Reload systemd
echo "🔄 Reloading systemd..."
systemctl daemon-reload

# Enable service
echo "✅ Enabling service to start on boot..."
systemctl enable gopro-controller.service

# Start service
echo "▶️  Starting service..."
systemctl start gopro-controller.service

# Wait a moment for service to start
sleep 2

# Check status
echo ""
echo "========================================"
echo "✅ Installation Complete!"
echo "========================================"
echo ""
echo "📊 Service Status:"
systemctl status gopro-controller.service --no-pager -l
echo ""
echo "🔧 Useful Commands:"
echo "   Check status:    sudo systemctl status gopro-controller"
echo "   Stop service:    sudo systemctl stop gopro-controller"
echo "   Start service:   sudo systemctl start gopro-controller"
echo "   Restart service: sudo systemctl restart gopro-controller"
echo "   View logs:       sudo journalctl -u gopro-controller -f"
echo "   Disable service: sudo systemctl disable gopro-controller"
echo ""
echo "🌐 API is now running on:"
echo "   http://$(hostname -I | awk '{print $1}'):5000"
echo ""
echo "📁 Videos will be stored in:"
echo "   $VIDEO_DIR"
echo ""
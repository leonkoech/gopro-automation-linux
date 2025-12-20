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

app = Flask(__name__)
CORS(app)

# Configuration
VIDEO_STORAGE_DIR = os.path.expanduser('~/gopro_videos')
SEGMENTS_DIR = os.path.join(VIDEO_STORAGE_DIR, 'segments')
os.makedirs(VIDEO_STORAGE_DIR, exist_ok=True)
os.makedirs(SEGMENTS_DIR, exist_ok=True)

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
    
    video_files = sorted(video_files, key=lambda x: os.path.getmtime(x))
    
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
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
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
    """Start recording on a specific GoPro with automatic segmentation"""
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
            segment_interval = data.get('segment_interval', 10)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            video_filename = f'gopro_{gopro["name"]}_{timestamp}.mp4'
            video_path = os.path.join(VIDEO_STORAGE_DIR, video_filename)
            
            session_id = f"{gopro_id}_{timestamp}"
            session_dir = os.path.join(SEGMENTS_DIR, session_id)
            os.makedirs(session_dir, exist_ok=True)
            
            gopro_ip = get_gopro_wired_ip(gopro_id)

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

            time.sleep(3)
            if process.poll() is not None:
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
                'recording_started': False,
                'is_stopping': False,
                'session_id': session_id,
                'session_dir': session_dir,
                'segments': [],
                'segment_interval': segment_interval,
                'gopro_ip': gopro_ip,
                'last_segment_time': time.time()
            }

            def monitor():
                output = []
                recording_confirmed = False

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
                                output.append(text)

                                if not recording_confirmed and ('recording' in text.lower() or 'started' in text.lower()):
                                    with recording_lock:
                                        if gopro_id in recording_processes:
                                            recording_processes[gopro_id]['recording_started'] = True
                                    recording_confirmed = True

                        if process.poll() is not None:
                            break
                    except OSError:
                        break
                    except Exception:
                        break

                try:
                    os.close(master)
                except OSError:
                    pass

                with recording_lock:
                    if gopro_id in recording_processes:
                        if not recording_processes[gopro_id].get('is_stopping'):
                            del recording_processes[gopro_id]

            def segment_downloader():
                """Download video segments periodically while recording"""
                processed_files = set()
                
                while True:
                    try:
                        with recording_lock:
                            if gopro_id not in recording_processes:
                                print(f"Segment downloader ending for {gopro_id}")
                                break
                            
                            rec_info = recording_processes[gopro_id]
                            if rec_info.get('is_stopping'):
                                print(f"Stop requested, segment downloader ending for {gopro_id}")
                                break
                            
                            elapsed = time.time() - rec_info['last_segment_time']
                            if elapsed < (rec_info['segment_interval'] * 60):
                                time.sleep(10)
                                continue
                            
                            gopro_ip = rec_info.get('gopro_ip')
                            session_dir = rec_info['session_dir']
                        
                        if not gopro_ip:
                            print(f"No GoPro IP for {gopro_id}, skipping segment download")
                            time.sleep(30)
                            continue
                        
                        print(f"Downloading segment for {gopro_id}...")
                        media_response = requests.get(
                            f'http://{gopro_ip}:8080/gopro/media/list',
                            timeout=10
                        )
                        
                        if media_response.status_code != 200:
                            print(f"Failed to get media list: {media_response.status_code}")
                            time.sleep(30)
                            continue
                        
                        media_list = media_response.json()
                        
                        for directory in reversed(media_list['media']):
                            for file_info in reversed(directory['fs']):
                                filename = file_info['n']
                                if filename.lower().endswith(('.mp4', '.mov')) and filename not in processed_files:
                                    segment_num = len(rec_info['segments']) + 1
                                    segment_path = os.path.join(session_dir, f'segment_{segment_num:03d}_{filename}')
                                    
                                    download_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory["d"]}/{filename}'
                                    print(f"Downloading segment {segment_num}: {filename}")
                                    
                                    response = requests.get(download_url, stream=True, timeout=300)
                                    with open(segment_path, 'wb') as f:
                                        for chunk in response.iter_content(chunk_size=8192):
                                            if chunk:
                                                f.write(chunk)
                                    
                                    with recording_lock:
                                        if gopro_id in recording_processes:
                                            recording_processes[gopro_id]['segments'].append(segment_path)
                                            recording_processes[gopro_id]['last_segment_time'] = time.time()
                                    
                                    processed_files.add(filename)
                                    print(f"✓ Downloaded segment {segment_num}")
                                    break
                            else:
                                continue
                            break
                        
                    except Exception as e:
                        print(f"Error in segment downloader for {gopro_id}: {e}")
                        time.sleep(30)
                
                print(f"Segment downloader stopped for {gopro_id}")

            threading.Thread(target=monitor, daemon=True).start()
            threading.Thread(target=segment_downloader, daemon=True).start()

            return jsonify({
                'success': True,
                'message': f'Recording started for {duration}s (segments every {segment_interval}min)',
                'video_filename': video_filename,
                'gopro_id': gopro_id,
                'video_path': video_path,
                'session_id': session_id
            })

        except Exception as e:
            if gopro_id in recording_processes:
                del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/gopros/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording and merge all downloaded segments"""
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
        segments = recording_info.get('segments', [])

        if process.poll() is None:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    process.wait(timeout=1)
            except Exception as e:
                print(f"Error stopping process: {e}")

        if master_fd:
            try:
                os.close(master_fd)
            except Exception:
                pass

        gopro_ip = get_gopro_wired_ip(gopro_id)
        if not gopro_ip:
            with recording_lock:
                if gopro_id in recording_processes:
                    del recording_processes[gopro_id]
            return jsonify({'success': False, 'error': 'Could not detect GoPro IP'}), 500

        try:
            requests.get(f'http://{gopro_ip}:8080/gopro/camera/shutter/stop', timeout=5)
        except Exception as e:
            print(f"Warning: Could not send stop command to GoPro: {e}")

        with recording_lock:
            if gopro_id in recording_processes:
                recording_processes[gopro_id]['downloading'] = True
                recording_processes[gopro_id]['download_progress'] = 0

        def download_final_and_merge():
            try:
                time.sleep(3)
                
                print(f"Downloading final segment for {gopro_id}...")
                try:
                    media_response = requests.get(
                        f'http://{gopro_ip}:8080/gopro/media/list',
                        timeout=10
                    )
                    media_list = media_response.json()
                    
                    last_dir = media_list['media'][-1]
                    last_file = last_dir['fs'][-1]
                    filename = last_file['n']
                    
                    segment_num = len(segments) + 1
                    segment_path = os.path.join(session_dir, f'segment_{segment_num:03d}_{filename}')
                    
                    download_url = f'http://{gopro_ip}:8080/videos/DCIM/{last_dir["d"]}/{filename}'
                    
                    def progress_callback(progress):
                        with recording_lock:
                            if gopro_id in recording_processes:
                                recording_processes[gopro_id]['download_progress'] = progress
                    
                    response = requests.get(download_url, stream=True, timeout=300)
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    
                    with open(segment_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0:
                                    progress = int((downloaded / total_size) * 100)
                                    progress_callback(progress)
                    
                    segments.append(segment_path)
                    print(f"✓ Downloaded final segment")
                except Exception as e:
                    print(f"Error downloading final segment: {e}")
                
                if segments:
                    print(f"Merging {len(segments)} segments into {video_path}...")
                    if merge_videos_ffmpeg(segments, video_path):
                        print(f"✓ Merged video saved to: {video_path}")
                        print(f"File size: {os.path.getsize(video_path)} bytes")
                        
                        try:
                            for seg in segments:
                                os.remove(seg)
                            os.rmdir(session_dir)
                            print(f"✓ Cleaned up {len(segments)} segment files")
                        except Exception as e:
                            print(f"Warning: Could not clean up segments: {e}")
                    else:
                        print(f"✗ Failed to merge segments")
                else:
                    print(f"No segments collected, downloading full video...")
                    
                    def progress_callback(progress):
                        with recording_lock:
                            if gopro_id in recording_processes:
                                recording_processes[gopro_id]['download_progress'] = progress
                    
                    download_gopro_video(gopro_ip, video_path, progress_callback)

                with recording_lock:
                    if gopro_id in recording_processes:
                        del recording_processes[gopro_id]
                        print(f"✓ Cleaned up recording_processes for {gopro_id}")

            except Exception as e:
                print(f"Error in download_final_and_merge for {gopro_id}: {e}")
                import traceback
                traceback.print_exc()
                with recording_lock:
                    if gopro_id in recording_processes:
                        recording_processes[gopro_id]['download_error'] = str(e)

        threading.Thread(target=download_final_and_merge, daemon=True).start()

        return jsonify({
            'success': True,
            'message': f'Recording stopped, merging {len(segments)} segments...',
            'video_filename': video_filename,
            'segments_downloaded': len(segments)
        })

    except Exception as e:
        print(f"Error in stop_recording: {str(e)}")
        import traceback
        traceback.print_exc()
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

if __name__ == '__main__':
    print("=" * 60)
    print("🎥 GoPro Controller API Service Starting...")
    print("=" * 60)
    print(f"📁 Video storage: {VIDEO_STORAGE_DIR}")
    print(f"📂 Segments storage: {SEGMENTS_DIR}")
    print(f"🌐 API endpoint: http://0.0.0.0:5000")
    print(f"💡 Make sure GoPros are connected via USB")
    print("=" * 60)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

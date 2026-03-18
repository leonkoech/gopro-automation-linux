"""
Health and System Routes

Endpoints:
- /health
- /api/system/info
- /api/system/ntp
- /api/debug/env
"""
from flask import Blueprint, jsonify, current_app
import subprocess
import os
from datetime import datetime

health_bp = Blueprint('health', __name__)


@health_bp.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'gopro-controller',
        'timestamp': datetime.now().isoformat(),
        'storage_path': current_app.config.get('VIDEO_STORAGE_DIR')
    })


@health_bp.route('/api/system/info', methods=['GET'])
def system_info():
    """Get system information"""
    try:
        # Get disk usage
        storage_path = current_app.config.get('VIDEO_STORAGE_DIR')
        stat = os.statvfs(storage_path)
        total_gb = (stat.f_blocks * stat.f_frsize) / (1024**3)
        free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
        used_gb = total_gb - free_gb

        # Get hostname
        hostname = os.uname().nodename

        return jsonify({
            'success': True,
            'system': {
                'hostname': hostname,
                'storage': {
                    'path': storage_path,
                    'total_gb': round(total_gb, 2),
                    'used_gb': round(used_gb, 2),
                    'free_gb': round(free_gb, 2),
                    'percent_used': round((used_gb / total_gb) * 100, 1)
                },
                'jetson_id': current_app.config.get('JETSON_ID', 'unknown')
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@health_bp.route('/api/system/ntp', methods=['GET'])
def get_ntp_status():
    """Get NTP synchronization status"""
    try:
        # Try timedatectl first (systemd)
        result = subprocess.run(
            ['timedatectl', 'show', '--property=NTPSynchronized'],
            capture_output=True, text=True, timeout=5
        )

        if result.returncode == 0:
            synced = 'yes' in result.stdout.lower()

            # Get offset from chrony or ntpstat
            offset_ms = None
            stratum = None
            source = None

            try:
                chrony = subprocess.run(
                    ['chronyc', 'tracking'],
                    capture_output=True, text=True, timeout=5
                )
                if chrony.returncode == 0:
                    for line in chrony.stdout.split('\n'):
                        if 'System time' in line:
                            # Parse offset
                            import re
                            match = re.search(r'([\d.]+)\s*seconds', line)
                            if match:
                                offset_ms = float(match.group(1)) * 1000
                        elif 'Stratum' in line:
                            match = re.search(r'(\d+)', line)
                            if match:
                                stratum = int(match.group(1))
                        elif 'Reference ID' in line:
                            source = line.split(':')[-1].strip() if ':' in line else None
            except:
                pass

            return jsonify({
                'success': True,
                'synced': synced,
                'offset_ms': offset_ms,
                'stratum': stratum,
                'source': source
            })

        return jsonify({
            'success': True,
            'synced': False,
            'warning': 'Could not determine NTP status'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@health_bp.route('/api/debug/env', methods=['GET'])
def debug_env():
    """Debug endpoint to show relevant environment variables"""
    env_vars = {
        'FLASK_ENV': os.getenv('FLASK_ENV'),
        'UPLOAD_ENABLED': os.getenv('UPLOAD_ENABLED'),
        'UPLOAD_BUCKET': os.getenv('UPLOAD_BUCKET'),
        'JETSON_ID': os.getenv('JETSON_ID'),
        'COURT_LOCATION': os.getenv('COURT_LOCATION'),
        'AWS_BATCH_JOB_QUEUE': os.getenv('AWS_BATCH_JOB_QUEUE'),
        'AWS_BATCH_JOB_DEFINITION_EXTRACT': os.getenv('AWS_BATCH_JOB_DEFINITION_EXTRACT'),
        'UBALL_BACKEND_URL': os.getenv('UBALL_BACKEND_URL'),
        'FIREBASE_CREDENTIALS_PATH': os.getenv('FIREBASE_CREDENTIALS_PATH'),
        'CAMERA_ANGLE_MAP': os.getenv('CAMERA_ANGLE_MAP'),
    }

    return jsonify({
        'success': True,
        'environment': env_vars,
        'config': {
            'video_storage_dir': current_app.config.get('VIDEO_STORAGE_DIR'),
            'segments_dir': current_app.config.get('SEGMENTS_DIR'),
        }
    })

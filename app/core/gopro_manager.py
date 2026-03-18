"""
GoPro Manager - Discovery, Connection, and Control

Handles GoPro camera discovery via USB-Ethernet adapters,
connection management, and basic camera control.
"""
import subprocess
import re
import json
import os
import requests
from . import gopro_ip_cache, recording_processes, recording_lock


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
                    print(f"Discovered GoPro at {gopro_ip} on {interface}")
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

                with recording_lock:
                    is_recording = current_interface in recording_processes

                gopro_info = {
                    'id': current_interface,
                    'name': f'GoPro-{current_interface[-4:]}',
                    'interface': current_interface,
                    'ip': current_ip,
                    'gopro_ip': gopro_ip,
                    'status': 'connected',
                    'is_recording': is_recording
                }
                gopros.append(gopro_info)
                current_interface = None
                current_ip = None

    except Exception as e:
        print(f"Error discovering GoPros: {e}")

    return gopros


def get_gopro_wired_ip(gopro_id):
    """Get the cached or discover GoPro IP for a specific interface"""
    global gopro_ip_cache

    if gopro_id in gopro_ip_cache:
        return gopro_ip_cache[gopro_id]

    gopros = get_connected_gopros()
    gopro = next((g for g in gopros if g['id'] == gopro_id), None)
    if gopro and gopro.get('gopro_ip'):
        return gopro['gopro_ip']

    return None


def enable_usb_control(gopro_ip):
    """Enable USB control mode on the GoPro - required before sending commands"""
    try:
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/camera/control/wired_usb?p=1',
            timeout=5
        )
        if response.status_code == 200:
            print(f"USB control enabled on {gopro_ip}")
            return True
        else:
            print(f"USB control response: {response.status_code}")
    except Exception as e:
        print(f"Failed to enable USB control: {e}")
    return False


def get_gopro_files(gopro_ip):
    """Get set of all current files on GoPro"""
    files = set()
    try:
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/media/list',
            timeout=10
        )
        if response.status_code == 200:
            media_list = response.json()
            for media in media_list.get('media', []):
                directory = media.get('d', '')
                for file_info in media.get('fs', []):
                    filename = file_info.get('n', '')
                    if filename:
                        files.add(f"{directory}/{filename}")
    except Exception as e:
        print(f"Error getting GoPro files: {e}")
    return files


def get_gopro_camera_name(gopro_ip):
    """Get the camera name (ap_ssid) from GoPro - used for angle identification"""
    try:
        response = requests.get(
            f'http://{gopro_ip}:8080/gopro/camera/state',
            timeout=5
        )
        if response.status_code == 200:
            state = response.json()
            # ap_ssid is in status key 30
            ap_ssid = state.get('status', {}).get('30', '')
            if ap_ssid:
                return ap_ssid
    except Exception as e:
        print(f"Error getting camera name: {e}")
    return None


def get_angle_code_from_camera_name(camera_name: str) -> str:
    """
    Extract angle code from camera name using CAMERA_ANGLE_MAP.

    Camera names like "GoPro FL", "GoPro FR", etc. map to angle codes.
    Falls back to 'UNK' (unknown) if no mapping found.
    """
    try:
        # Load camera angle map from environment
        camera_angle_map_str = os.getenv('CAMERA_ANGLE_MAP', '{}')
        camera_angle_map = json.loads(camera_angle_map_str)

        # Try exact match first
        if camera_name in camera_angle_map:
            return camera_angle_map[camera_name]

        # Try case-insensitive match
        camera_name_lower = camera_name.lower()
        for key, value in camera_angle_map.items():
            if key.lower() == camera_name_lower:
                return value

        # Try partial match (e.g., "FL" in "GoPro FL something")
        for key, value in camera_angle_map.items():
            if key.lower() in camera_name_lower or camera_name_lower in key.lower():
                return value

    except Exception as e:
        print(f"Error parsing CAMERA_ANGLE_MAP: {e}")

    return 'UNK'  # Unknown angle


def sanitize_filename(name):
    """Sanitize a string for use as a filename"""
    # Replace spaces and special characters with underscores
    sanitized = re.sub(r'[^\w\-_.]', '_', name)
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    return sanitized.strip('_')

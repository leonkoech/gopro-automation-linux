"""
Media Management Service
Provides functionality to list, preview, download, and delete videos
from both GoPro cameras and the Jetson local storage.
"""

import os
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any


class MediaService:
    """
    Service for managing media on GoPro cameras and local Jetson storage.
    """

    def __init__(self, local_storage_dir: str = None):
        """
        Initialize the media service.

        Args:
            local_storage_dir: Path to local video storage directory
        """
        self.local_storage_dir = local_storage_dir or os.path.expanduser('~/gopro_videos')
        os.makedirs(self.local_storage_dir, exist_ok=True)

    # ==================== GoPro Media Management ====================

    def get_gopro_media_list(self, gopro_ip: str) -> Dict[str, Any]:
        """
        Get list of all media files on a GoPro camera.

        Args:
            gopro_ip: IP address of the GoPro camera

        Returns:
            Dict with media list and metadata
        """
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/media/list',
                timeout=10
            )
            if response.status_code != 200:
                return {'success': False, 'error': f'Failed to get media list: {response.status_code}'}

            media_list = response.json()
            files = []
            total_size = 0

            for directory in media_list.get('media', []):
                dir_name = directory['d']
                for file_info in directory.get('fs', []):
                    filename = file_info['n']
                    size = file_info.get('s', 0)
                    total_size += size

                    # Build file info
                    file_data = {
                        'filename': filename,
                        'directory': dir_name,
                        'size_bytes': size,
                        'size_mb': round(size / (1024 * 1024), 2),
                        'created_timestamp': file_info.get('cre', ''),
                        'modified_timestamp': file_info.get('mod', ''),
                        'download_url': f'http://{gopro_ip}:8080/videos/DCIM/{dir_name}/{filename}',
                        'thumbnail_url': f'http://{gopro_ip}:8080/gopro/media/thumbnail?path={dir_name}/{filename}',
                        'preview_url': f'http://{gopro_ip}:8080/videos/DCIM/{dir_name}/{filename}',
                        'is_video': filename.lower().endswith(('.mp4', '.mov')),
                        'is_photo': filename.lower().endswith(('.jpg', '.jpeg', '.gpr', '.raw'))
                    }
                    files.append(file_data)

            # Sort by filename (newest files typically have higher numbers)
            files.sort(key=lambda x: x['filename'], reverse=True)

            return {
                'success': True,
                'gopro_ip': gopro_ip,
                'file_count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_size_gb': round(total_size / (1024 ** 3), 2),
                'files': files
            }

        except requests.exceptions.Timeout:
            return {'success': False, 'error': 'Connection to GoPro timed out'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'error': 'Could not connect to GoPro'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_gopro_file_info(self, gopro_ip: str, directory: str, filename: str) -> Dict[str, Any]:
        """
        Get detailed info about a specific file on GoPro.

        Args:
            gopro_ip: IP address of the GoPro
            directory: Directory name (e.g., '100GOPRO')
            filename: File name (e.g., 'GX010001.MP4')

        Returns:
            Dict with file info
        """
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/media/info?path={directory}/{filename}',
                timeout=10
            )
            if response.status_code == 200:
                return {'success': True, 'info': response.json()}
            else:
                return {'success': False, 'error': f'Failed to get file info: {response.status_code}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_gopro_file(self, gopro_ip: str, directory: str, filename: str) -> Dict[str, Any]:
        """
        Delete a file from GoPro.

        Args:
            gopro_ip: IP address of the GoPro
            directory: Directory name (e.g., '100GOPRO')
            filename: File name (e.g., 'GX010001.MP4')

        Returns:
            Dict with result
        """
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/media/delete/file?path={directory}/{filename}',
                timeout=30
            )
            if response.status_code == 200:
                return {
                    'success': True,
                    'message': f'Deleted {filename} from GoPro'
                }
            else:
                return {'success': False, 'error': f'Failed to delete: {response.status_code}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_gopro_all_files(self, gopro_ip: str) -> Dict[str, Any]:
        """
        Delete ALL files from GoPro (use with caution!).

        Args:
            gopro_ip: IP address of the GoPro

        Returns:
            Dict with result
        """
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/media/delete/all',
                timeout=60
            )
            if response.status_code == 200:
                return {
                    'success': True,
                    'message': 'All files deleted from GoPro'
                }
            else:
                return {'success': False, 'error': f'Failed to delete all: {response.status_code}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_gopro_storage_info(self, gopro_ip: str) -> Dict[str, Any]:
        """
        Get storage info from GoPro.

        Args:
            gopro_ip: IP address of the GoPro

        Returns:
            Dict with storage info
        """
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/camera/state',
                timeout=5
            )
            if response.status_code == 200:
                state = response.json()
                # Extract storage-related status
                status = state.get('status', {})
                return {
                    'success': True,
                    'remaining_photos': status.get('34', 0),
                    'remaining_video_seconds': status.get('35', 0),
                    'sd_card_present': status.get('33', 0) == 0,
                    'raw_state': state
                }
            else:
                return {'success': False, 'error': f'Failed to get state: {response.status_code}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # ==================== Jetson Local Media Management ====================

    def get_local_media_list(self) -> Dict[str, Any]:
        """
        Get list of all videos in local Jetson storage.

        Returns:
            Dict with media list
        """
        try:
            video_path = Path(self.local_storage_dir)
            files = []
            total_size = 0

            # Get all video files
            for video_file in video_path.glob('*.mp4'):
                try:
                    stat = video_file.stat()
                    size = stat.st_size
                    total_size += size

                    files.append({
                        'filename': video_file.name,
                        'path': str(video_file),
                        'size_bytes': size,
                        'size_mb': round(size / (1024 * 1024), 2),
                        'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        'is_video': True
                    })
                except Exception as e:
                    print(f"Error reading file {video_file}: {e}")

            # Sort by modification time (newest first)
            files.sort(key=lambda x: x['modified'], reverse=True)

            return {
                'success': True,
                'storage_path': self.local_storage_dir,
                'file_count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_size_gb': round(total_size / (1024 ** 3), 2),
                'files': files
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_local_file_path(self, filename: str) -> Optional[str]:
        """
        Get full path to a local file if it exists.

        Args:
            filename: Name of the file

        Returns:
            Full path or None if not found
        """
        file_path = os.path.join(self.local_storage_dir, filename)
        if os.path.exists(file_path) and file_path.startswith(self.local_storage_dir):
            return file_path
        return None

    def delete_local_file(self, filename: str) -> Dict[str, Any]:
        """
        Delete a local video file.

        Args:
            filename: Name of the file to delete

        Returns:
            Dict with result
        """
        try:
            file_path = os.path.join(self.local_storage_dir, filename)

            # Security check
            if not file_path.startswith(self.local_storage_dir):
                return {'success': False, 'error': 'Invalid file path'}

            if not os.path.exists(file_path):
                return {'success': False, 'error': 'File not found'}

            os.remove(file_path)
            return {
                'success': True,
                'message': f'Deleted {filename}'
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_local_storage_info(self) -> Dict[str, Any]:
        """
        Get local storage information.

        Returns:
            Dict with storage info
        """
        try:
            stat = os.statvfs(self.local_storage_dir)

            total_bytes = stat.f_blocks * stat.f_frsize
            free_bytes = stat.f_bavail * stat.f_frsize
            used_bytes = total_bytes - free_bytes

            return {
                'success': True,
                'storage_path': self.local_storage_dir,
                'total_gb': round(total_bytes / (1024 ** 3), 2),
                'free_gb': round(free_bytes / (1024 ** 3), 2),
                'used_gb': round(used_bytes / (1024 ** 3), 2),
                'usage_percent': round((used_bytes / total_bytes) * 100, 1)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}


# Singleton instance
_media_service = None

def get_media_service(local_storage_dir: str = None) -> MediaService:
    """Get or create the media service singleton."""
    global _media_service
    if _media_service is None:
        _media_service = MediaService(local_storage_dir)
    return _media_service

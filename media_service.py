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
        self.segments_dir = os.path.join(self.local_storage_dir, 'segments')
        os.makedirs(self.local_storage_dir, exist_ok=True)
        os.makedirs(self.segments_dir, exist_ok=True)

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

    # ==================== Segments Management ====================

    def get_segments_list(self) -> Dict[str, Any]:
        """
        Get list of all segment folders and their contents.

        Returns:
            Dict with segments list organized by recording session
        """
        try:
            segments_path = Path(self.segments_dir)
            sessions = []
            total_size = 0
            total_files = 0

            # Each subfolder is a recording session
            for session_dir in sorted(segments_path.iterdir(), reverse=True):
                if session_dir.is_dir():
                    session_files = []
                    session_size = 0

                    # Get all files in the session folder
                    for segment_file in session_dir.glob('*'):
                        if segment_file.is_file():
                            try:
                                stat = segment_file.stat()
                                size = stat.st_size
                                session_size += size
                                total_files += 1

                                session_files.append({
                                    'filename': segment_file.name,
                                    'size_bytes': size,
                                    'size_mb': round(size / (1024 * 1024), 2),
                                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                                    'is_video': segment_file.suffix.lower() in ['.mp4', '.mov']
                                })
                            except Exception as e:
                                print(f"Error reading segment file {segment_file}: {e}")

                    # Sort files by name
                    session_files.sort(key=lambda x: x['filename'])
                    total_size += session_size

                    # Parse session name for metadata (format: deviceid_YYYYMMDD_HHMMSS)
                    session_name = session_dir.name
                    session_date = None
                    try:
                        # Try to extract date from session name
                        parts = session_name.split('_')
                        if len(parts) >= 2:
                            date_str = parts[-2]  # YYYYMMDD
                            time_str = parts[-1]  # HHMMSS
                            session_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    except:
                        pass

                    sessions.append({
                        'session_name': session_name,
                        'session_date': session_date,
                        'path': str(session_dir),
                        'file_count': len(session_files),
                        'total_size_bytes': session_size,
                        'total_size_mb': round(session_size / (1024 * 1024), 2),
                        'files': session_files
                    })

            return {
                'success': True,
                'segments_path': self.segments_dir,
                'session_count': len(sessions),
                'total_file_count': total_files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_size_gb': round(total_size / (1024 ** 3), 2),
                'sessions': sessions
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_segment_session_files(self, session_name: str) -> Dict[str, Any]:
        """
        Get list of files in a specific segment session.

        Args:
            session_name: Name of the session folder

        Returns:
            Dict with session file list
        """
        try:
            session_path = Path(self.segments_dir) / session_name

            if not session_path.exists():
                return {'success': False, 'error': 'Session not found'}

            if not session_path.is_dir():
                return {'success': False, 'error': 'Invalid session path'}

            files = []
            total_size = 0

            for segment_file in session_path.glob('*'):
                if segment_file.is_file():
                    try:
                        stat = segment_file.stat()
                        size = stat.st_size
                        total_size += size

                        files.append({
                            'filename': segment_file.name,
                            'path': str(segment_file),
                            'size_bytes': size,
                            'size_mb': round(size / (1024 * 1024), 2),
                            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            'is_video': segment_file.suffix.lower() in ['.mp4', '.mov']
                        })
                    except Exception as e:
                        print(f"Error reading segment file {segment_file}: {e}")

            files.sort(key=lambda x: x['filename'])

            return {
                'success': True,
                'session_name': session_name,
                'path': str(session_path),
                'file_count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'files': files
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_segment_session(self, session_name: str) -> Dict[str, Any]:
        """
        Delete an entire segment session folder.

        Args:
            session_name: Name of the session folder to delete

        Returns:
            Dict with result
        """
        try:
            import shutil
            session_path = os.path.join(self.segments_dir, session_name)

            # Security check
            if not session_path.startswith(self.segments_dir):
                return {'success': False, 'error': 'Invalid session path'}

            if not os.path.exists(session_path):
                return {'success': False, 'error': 'Session not found'}

            if not os.path.isdir(session_path):
                return {'success': False, 'error': 'Not a valid session directory'}

            shutil.rmtree(session_path)
            return {
                'success': True,
                'message': f'Deleted session {session_name}'
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_segment_file(self, session_name: str, filename: str) -> Dict[str, Any]:
        """
        Delete a specific file from a segment session.

        Args:
            session_name: Name of the session folder
            filename: Name of the file to delete

        Returns:
            Dict with result
        """
        try:
            file_path = os.path.join(self.segments_dir, session_name, filename)

            # Security check
            if not file_path.startswith(self.segments_dir):
                return {'success': False, 'error': 'Invalid file path'}

            if not os.path.exists(file_path):
                return {'success': False, 'error': 'File not found'}

            os.remove(file_path)
            return {
                'success': True,
                'message': f'Deleted {filename} from session {session_name}'
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_segment_file_path(self, session_name: str, filename: str) -> Optional[str]:
        """
        Get full path to a segment file if it exists.

        Args:
            session_name: Name of the session folder
            filename: Name of the file

        Returns:
            Full path or None if not found
        """
        file_path = os.path.join(self.segments_dir, session_name, filename)
        if os.path.exists(file_path) and file_path.startswith(self.segments_dir):
            return file_path
        return None


# Singleton instance
_media_service = None

def get_media_service(local_storage_dir: str = None) -> MediaService:
    """Get or create the media service singleton."""
    global _media_service
    if _media_service is None:
        _media_service = MediaService(local_storage_dir)
    return _media_service

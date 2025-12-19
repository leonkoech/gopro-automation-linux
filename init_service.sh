#!/usr/bin/env python3
"""
GoPro Segmented Video Downloader
Downloads videos in segments and merges them into a final video
"""

import os
import time
import requests
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoProSegmentDownloader:
    def __init__(self, gopro_ip: str = "10.5.5.9", output_dir: str = "./gopro_downloads"):
        self.gopro_ip = gopro_ip
        self.base_url = f"http://{gopro_ip}:8080"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.segment_dir = self.output_dir / "segments"
        self.segment_dir.mkdir(exist_ok=True)
        
    def get_media_list(self) -> List[dict]:
        """Get list of media files from GoPro via WiFi API"""
        try:
            response = requests.get(f"{self.base_url}/gopro/media/list", timeout=5)
            if response.status_code == 200:
                data = response.json()
                media_list = []
                for directory in data.get('media', []):
                    for file in directory.get('fs', []):
                        # Parse the creation time
                        file['directory'] = directory['d']
                        file['timestamp'] = self._parse_gopro_time(file.get('mod', ''))
                        media_list.append(file)
                return sorted(media_list, key=lambda x: x.get('timestamp', 0))
            else:
                logger.error(f"Failed to get media list: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error getting media list: {e}")
            return []
    
    def _parse_gopro_time(self, time_str: str) -> float:
        """Parse GoPro timestamp to Unix timestamp"""
        try:
            # GoPro uses format like "1234567890"
            return float(time_str) if time_str else 0
        except:
            return 0
    
    def download_video(self, directory: str, filename: str, output_path: Path) -> bool:
        """Download a single video file from GoPro"""
        try:
            url = f"{self.base_url}/videos/DCIM/{directory}/{filename}"
            logger.info(f"Downloading {filename}...")
            
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size:
                                progress = (downloaded / total_size) * 100
                                print(f"\rProgress: {progress:.1f}%", end='')
                print()  # New line after progress
                logger.info(f"Downloaded: {output_path}")
                return True
            else:
                logger.error(f"Failed to download {filename}: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return False
    
    def continuous_download(self, interval_minutes: int = 10, stop_event=None) -> List[Path]:
        """
        Continuously download new videos every interval_minutes
        Returns list of downloaded video paths
        """
        downloaded_files = []
        last_check_time = time.time()
        processed_files = set()
        
        logger.info(f"Starting continuous download (checking every {interval_minutes} minutes)")
        
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stop signal received")
                break
            
            # Check every interval
            time.sleep(interval_minutes * 60)
            
            try:
                media_list = self.get_media_list()
                
                # Find new videos since last check
                for media in media_list:
                    if media['timestamp'] > last_check_time and media['n'] not in processed_files:
                        if media['n'].lower().endswith(('.mp4', '.mov')):
                            output_path = self.segment_dir / media['n']
                            if self.download_video(media['directory'], media['n'], output_path):
                                downloaded_files.append(output_path)
                                processed_files.add(media['n'])
                
                last_check_time = time.time()
                
            except Exception as e:
                logger.error(f"Error in continuous download: {e}")
                time.sleep(60)  # Wait a bit before retrying
        
        return downloaded_files
    
    def time_range_download(self, start_time: datetime, end_time: datetime) -> List[Path]:
        """
        Download all videos between start_time and end_time
        Returns list of downloaded video paths
        """
        downloaded_files = []
        
        logger.info(f"Downloading videos from {start_time} to {end_time}")
        
        try:
            media_list = self.get_media_list()
            
            start_ts = start_time.timestamp()
            end_ts = end_time.timestamp()
            
            for media in media_list:
                media_ts = media.get('timestamp', 0)
                
                # Check if video is in time range
                if start_ts <= media_ts <= end_ts:
                    if media['n'].lower().endswith(('.mp4', '.mov')):
                        output_path = self.segment_dir / media['n']
                        logger.info(f"Found video in range: {media['n']}")
                        if self.download_video(media['directory'], media['n'], output_path):
                            downloaded_files.append(output_path)
            
            logger.info(f"Downloaded {len(downloaded_files)} videos in time range")
            
        except Exception as e:
            logger.error(f"Error in time range download: {e}")
        
        return downloaded_files
    
    def merge_videos(self, video_files: List[Path], output_name: str = "merged_video.mp4") -> Optional[str]:
        """
        Merge multiple video files into one using ffmpeg
        Returns URL/path to merged video
        """
        if not video_files:
            logger.error("No videos to merge")
            return None
        
        # Sort files by modification time
        video_files = sorted(video_files, key=lambda x: x.stat().st_mtime)
        
        output_path = self.output_dir / output_name
        
        # Create file list for ffmpeg concat
        concat_file = self.segment_dir / "concat_list.txt"
        with open(concat_file, 'w') as f:
            for video in video_files:
                # Use absolute paths
                f.write(f"file '{video.absolute()}'\n")
        
        logger.info(f"Merging {len(video_files)} videos...")
        
        try:
            # Use ffmpeg to concatenate videos
            cmd = [
                'ffmpeg',
                '-f', 'concat',
                '-safe', '0',
                '-i', str(concat_file),
                '-c', 'copy',  # Copy streams without re-encoding (fast)
                '-y',  # Overwrite output
                str(output_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Successfully merged videos to: {output_path}")
                # Clean up concat file
                concat_file.unlink()
                return str(output_path)
            else:
                logger.error(f"FFmpeg error: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Error merging videos: {e}")
            return None
    
    def cleanup_segments(self):
        """Remove individual segment files after merging"""
        try:
            for file in self.segment_dir.glob("*.mp4"):
                file.unlink()
            for file in self.segment_dir.glob("*.mov"):
                file.unlink()
            logger.info("Cleaned up segment files")
        except Exception as e:
            logger.error(f"Error cleaning up segments: {e}")


# Example usage
if __name__ == "__main__":
    import threading
    import sys
    
    downloader = GoProSegmentDownloader()
    
    print("GoPro Segmented Video Downloader")
    print("=" * 50)
    print("1. Continuous mode (download every N minutes)")
    print("2. Time range mode (download videos in time range)")
    
    choice = input("Select mode (1 or 2): ").strip()
    
    if choice == "1":
        # Continuous mode
        interval = int(input("Check interval in minutes (default 10): ") or "10")
        
        stop_event = threading.Event()
        
        def signal_handler():
            input("\nPress Enter to stop and merge videos...\n")
            stop_event.set()
        
        # Start input listener in separate thread
        listener = threading.Thread(target=signal_handler, daemon=True)
        listener.start()
        
        try:
            downloaded = downloader.continuous_download(interval, stop_event)
            
            if downloaded:
                print(f"\nDownloaded {len(downloaded)} video segments")
                merged = downloader.merge_videos(downloaded)
                if merged:
                    print(f"\n✅ Merged video available at: {merged}")
                    cleanup = input("Delete segment files? (y/n): ").lower()
                    if cleanup == 'y':
                        downloader.cleanup_segments()
            else:
                print("No videos were downloaded")
                
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            
    elif choice == "2":
        # Time range mode
        print("\nEnter time range (format: YYYY-MM-DD HH:MM:SS)")
        start_str = input("Start time: ")
        end_str = input("End time: ")
        
        try:
            start_time = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
            
            downloaded = downloader.time_range_download(start_time, end_time)
            
            if downloaded:
                print(f"\nDownloaded {len(downloaded)} videos")
                merged = downloader.merge_videos(downloaded)
                if merged:
                    print(f"\n✅ Merged video available at: {merged}")
                    cleanup = input("Delete segment files? (y/n): ").lower()
                    if cleanup == 'y':
                        downloader.cleanup_segments()
            else:
                print("No videos found in specified time range")
                
        except ValueError as e:
            print(f"Error parsing dates: {e}")
    else:
        print("Invalid choice")

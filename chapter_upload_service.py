"""
Chapter Upload Service for GoPro to S3.

This module provides functionality to:
- Download video chapters from GoPro to Jetson local storage
- Upload chapters from Jetson to S3 (leveraging faster ethernet)
- Clean up local files after upload (keep on GoPro SD card)
- Track upload progress with callbacks

S3 Structure:
    raw-chapters/{segmentSession}/chapter_001_GX010038.MP4

Local Temp Structure:
    /tmp/chapters/{segmentSession}/GX010038.MP4

Flow:
    1. Download: GoPro WiFi → Jetson (/tmp/chapters/)
    2. Upload: Jetson Ethernet → S3 (faster!)
    3. Delete: Remove from Jetson (keep on GoPro SD card)

Key insight: GoPro cameras expose files via HTTP at:
    http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}
"""

import os
import requests
import time
import threading
from typing import List, Dict, Any, Optional, Callable
from logging_service import get_logger

logger = get_logger('gopro.chapter_upload')

# Download configuration (from robust download_chapters.py)
CHUNK_SIZE = 262144  # 256KB - smaller chunks for faster stall detection
S3_PART_SIZE = 25 * 1024 * 1024  # 25MB - S3 multipart part size
READ_TIMEOUT = 60  # seconds to wait for data between chunks
CONNECT_TIMEOUT = 10  # seconds to establish connection
MAX_DOWNLOAD_RETRIES = 20  # more retries with resume capability
KEEP_ALIVE_INTERVAL = 30  # send keep-alive every 30 seconds


class KeepAliveThread:
    """
    Background thread to send keep-alive to GoPro during download.
    
    Prevents GoPro from sleeping during long downloads.
    Based on proven logic from scripts/download_chapters.py.
    """
    
    def __init__(self, gopro_ip: str):
        self.gopro_ip = gopro_ip
        self.running = False
        self.thread = None
    
    def start(self):
        """Start the keep-alive background thread."""
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logger.debug(f"Keep-alive thread started for {self.gopro_ip}")
    
    def stop(self):
        """Stop the keep-alive background thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        logger.debug(f"Keep-alive thread stopped for {self.gopro_ip}")
    
    def _run(self):
        """Keep-alive loop - runs in background thread."""
        while self.running:
            try:
                requests.get(
                    f'http://{self.gopro_ip}:8080/gopro/camera/keep_alive',
                    timeout=2
                )
            except:
                pass  # Silently ignore errors
            time.sleep(KEEP_ALIVE_INTERVAL)


class ChapterUploadService:
    """
    Service for downloading GoPro chapters and uploading to S3.

    New Flow:
    1. Download chapter from GoPro to Jetson local storage
    2. Upload from Jetson to S3 (faster ethernet connection)
    3. Delete local file (keep on GoPro SD card as backup)
    
    Uses robust download logic with:
    - Resume capability (Range headers)
    - Keep-alive to prevent GoPro sleep
    - Exponential backoff retry
    - Never deletes partial files
    """

    def __init__(self, s3_client, bucket_name: str):
        """
        Initialize the chapter upload service.

        Args:
            s3_client: boto3 S3 client instance
            bucket_name: S3 bucket name for raw chapters
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.keep_alive = None  # Will be created per-session

    def download_chapter_to_local(
        self,
        gopro_ip: str,
        directory: str,
        filename: str,
        local_path: str,
        expected_size: int,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Download a single chapter from GoPro to local Jetson storage.
        
        Uses robust download logic from scripts/download_chapters.py:
        - Resume capability (Range headers)
        - Exponential backoff retry
        - Never deletes partial files
        - Progress reporting with speed calculation
        
        Args:
            gopro_ip: GoPro camera IP address
            directory: DCIM directory on GoPro (e.g., "100GOPRO")
            filename: Video filename (e.g., "GX010038.MP4")
            local_path: Full path where to save the file locally
            expected_size: Expected file size in bytes (for progress tracking)
            progress_callback: Optional callback(bytes_downloaded, total_bytes)
        
        Returns:
            Dict with download results:
                - success: bool
                - local_path: str
                - bytes_downloaded: int
                - error: str (if failed)
        """
        download_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}'
        
        logger.info(f"Downloading chapter from GoPro: {filename} -> {local_path}")
        logger.info(f"  Source: {download_url}")
        logger.info(f"  Expected size: {expected_size / (1024**3):.2f} GB")
        
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        for attempt in range(MAX_DOWNLOAD_RETRIES):
            try:
                # Check current file size for resume
                current_size = os.path.getsize(local_path) if os.path.exists(local_path) else 0
                
                # Skip if already complete
                if expected_size > 0 and current_size >= expected_size:
                    logger.info(f"  Already complete: {current_size:,} bytes")
                    return {
                        'success': True,
                        'local_path': local_path,
                        'bytes_downloaded': current_size
                    }
                
                # Prepare headers for resume
                headers = {}
                if current_size > 0:
                    headers['Range'] = f'bytes={current_size}-'
                    logger.info(f"  Resuming from byte {current_size:,} ({current_size/1024/1024/1024:.2f} GB)")
                
                # Start download with BOTH connect and read timeout
                response = requests.get(
                    download_url,
                    headers=headers,
                    stream=True,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)  # (connect, read) timeout tuple
                )
                
                # Handle response
                if current_size > 0 and response.status_code == 416:
                    # Range not satisfiable - file might be complete
                    logger.info(f"  Server returned 416 - file may be complete")
                    return {
                        'success': True,
                        'local_path': local_path,
                        'bytes_downloaded': current_size
                    }
                
                if current_size > 0 and response.status_code != 206:
                    logger.warning(f"  Server returned {response.status_code} instead of 206, will append anyway")
                
                # Determine write mode
                if current_size > 0 and response.status_code == 206:
                    mode = 'ab'  # Append
                elif current_size > 0:
                    mode = 'ab'  # Still try to append
                else:
                    mode = 'wb'  # Fresh start
                
                # Get content length
                content_length = int(response.headers.get('content-length', 0))
                total_expected = current_size + content_length if content_length else expected_size
                
                logger.info(f"  Content length: {content_length / (1024**3):.2f} GB, Total expected: {total_expected / (1024**3):.2f} GB")
                
                # Download with progress
                bytes_written = 0
                last_progress_time = time.time()
                last_progress_bytes = 0
                
                with open(local_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        
                        f.write(chunk)
                        bytes_written += len(chunk)
                        
                        # Report progress callback
                        if progress_callback:
                            try:
                                progress_callback(current_size + bytes_written, total_expected)
                            except Exception:
                                pass  # Don't let callback errors stop download
                        
                        # Log progress every 5 seconds
                        now = time.time()
                        if now - last_progress_time >= 5:
                            current_total = current_size + bytes_written
                            speed = (bytes_written - last_progress_bytes) / (now - last_progress_time) / 1024 / 1024
                            percent = (current_total / total_expected * 100) if total_expected else 0
                            logger.info(f"  Progress: {current_total/1024/1024/1024:.2f} GB / {total_expected/1024/1024/1024:.2f} GB ({percent:.1f}%) - {speed:.1f} MB/s")
                            last_progress_time = now
                            last_progress_bytes = bytes_written
                
                # Verify final size
                final_size = os.path.getsize(local_path)
                
                if expected_size > 0 and final_size >= expected_size:
                    logger.info(f"  Download complete: {final_size:,} bytes")
                    return {
                        'success': True,
                        'local_path': local_path,
                        'bytes_downloaded': final_size
                    }
                elif expected_size > 0:
                    logger.warning(f"  Incomplete: {final_size:,} / {expected_size:,} bytes - will retry")
                else:
                    logger.info(f"  Downloaded: {final_size:,} bytes (expected size unknown)")
                    return {
                        'success': True,
                        'local_path': local_path,
                        'bytes_downloaded': final_size
                    }
            
            except requests.exceptions.Timeout as e:
                logger.warning(f"  Timeout on attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES}: {e}")
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"  Connection error on attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES}: {e}")
            except Exception as e:
                logger.error(f"  Error on attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES}: {e}")
            
            # Wait before retry (exponential backoff, max 30 seconds)
            if attempt < MAX_DOWNLOAD_RETRIES - 1:
                wait_time = min(2 ** attempt, 30)
                logger.info(f"  Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
        
        logger.error(f"  FAILED after {MAX_DOWNLOAD_RETRIES} attempts")
        # Clean up partial file on final failure
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.info(f"  Cleaned up partial file: {local_path}")
            except:
                pass
        
        return {
            'success': False,
            'local_path': local_path,
            'bytes_downloaded': 0,
            'error': f'Failed after {MAX_DOWNLOAD_RETRIES} attempts'
        }

    def s3_object_exists(self, s3_key: str) -> bool:
        """
        Check if an object already exists in S3 (for dev: skip re-download/re-upload).
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except Exception:
            return False

    def upload_local_file_to_s3(
        self,
        local_path: str,
        s3_key: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Upload a local file to S3 using multipart upload.
        
        Args:
            local_path: Path to local file on Jetson
            s3_key: Full S3 key for the uploaded file
            progress_callback: Optional callback(bytes_uploaded, total_bytes)
        
        Returns:
            Dict with upload results:
                - success: bool
                - s3_key: str
                - bytes_uploaded: int
                - error: str (if failed)
        """
        if not os.path.exists(local_path):
            logger.error(f"Local file not found: {local_path}")
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': 0,
                'error': f'Local file not found: {local_path}'
            }
        
        file_size = os.path.getsize(local_path)
        logger.info(f"Uploading local file to S3: {local_path} -> s3://{self.bucket_name}/{s3_key}")
        logger.info(f"  File size: {file_size / (1024**3):.2f} GB")
        
        # Start S3 multipart upload
        try:
            mpu = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType='video/mp4'
            )
            upload_id = mpu['UploadId']
        except Exception as e:
            logger.error(f"Failed to create multipart upload: {e}")
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': 0,
                'error': f'Failed to create multipart upload: {e}'
            }
        
        parts = []
        part_number = 1
        total_bytes = 0
        
        try:
            with open(local_path, 'rb') as f:
                while True:
                    # Read S3_PART_SIZE chunk
                    chunk = f.read(S3_PART_SIZE)
                    if not chunk:
                        break
                    
                    # Upload part
                    part_response = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    parts.append({
                        'ETag': part_response['ETag'],
                        'PartNumber': part_number
                    })
                    
                    total_bytes += len(chunk)
                    
                    # Report progress
                    if progress_callback:
                        try:
                            progress_callback(total_bytes, file_size)
                        except Exception:
                            pass
                    
                    logger.info(f"  Uploaded part {part_number} ({total_bytes / (1024**3):.2f} GB / {file_size / (1024**3):.2f} GB)")
                    part_number += 1
            
            # Complete multipart upload
            if not parts:
                logger.error("No parts uploaded - empty file?")
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    UploadId=upload_id
                )
                return {
                    'success': False,
                    's3_key': s3_key,
                    'bytes_uploaded': 0,
                    'error': 'No data to upload (empty file?)'
                }
            
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            logger.info(f"  SUCCESS: Uploaded {total_bytes / (1024**3):.2f} GB to s3://{self.bucket_name}/{s3_key}")
            
            return {
                'success': True,
                's3_key': s3_key,
                'bytes_uploaded': total_bytes,
                'parts_count': len(parts)
            }
        
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            self._abort_multipart(s3_key, upload_id)
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': total_bytes,
                'error': str(e)
            }

    def stream_chapter_to_s3(
        self,
        gopro_ip: str,
        directory: str,
        filename: str,
        s3_key: str,
        expected_size: int,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Stream a single chapter directly from GoPro HTTP to S3 multipart upload.

        No temporary file is created - data flows directly from GoPro to S3.

        Args:
            gopro_ip: GoPro camera IP address
            directory: DCIM directory on GoPro (e.g., "100GOPRO")
            filename: Video filename (e.g., "GX010038.MP4")
            s3_key: Full S3 key for the uploaded file
            expected_size: Expected file size in bytes (for progress tracking)
            progress_callback: Optional callback(bytes_uploaded, total_bytes)

        Returns:
            Dict with upload results:
                - success: bool
                - s3_key: str
                - bytes_uploaded: int
                - error: str (if failed)
        """
        download_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}'

        logger.info(f"Streaming chapter to S3: {filename} -> s3://{self.bucket_name}/{s3_key}")
        logger.info(f"  Source: {download_url}")
        logger.info(f"  Expected size: {expected_size / (1024**3):.2f} GB")

        # Start S3 multipart upload
        try:
            mpu = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType='video/mp4'
            )
            upload_id = mpu['UploadId']
        except Exception as e:
            logger.error(f"Failed to create multipart upload: {e}")
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': 0,
                'error': f'Failed to create multipart upload: {e}'
            }

        parts = []
        part_number = 1
        total_bytes = 0
        buffer = b''

        try:
            # Stream from GoPro with timeouts
            response = requests.get(
                download_url,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )
            response.raise_for_status()

            # Get actual content length from response
            content_length = int(response.headers.get('content-length', expected_size))
            logger.info(f"  Actual content length: {content_length / (1024**3):.2f} GB")

            # Stream data in chunks, buffer to S3 part size
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue

                buffer += chunk
                total_bytes += len(chunk)

                # Report progress
                if progress_callback:
                    try:
                        progress_callback(total_bytes, content_length)
                    except Exception:
                        pass  # Don't let callback errors stop upload

                # Upload part when buffer reaches S3 part size
                if len(buffer) >= S3_PART_SIZE:
                    part_response = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer
                    )
                    parts.append({
                        'ETag': part_response['ETag'],
                        'PartNumber': part_number
                    })
                    logger.info(f"  Uploaded part {part_number} ({total_bytes / (1024**3):.2f} GB streamed)")
                    part_number += 1
                    buffer = b''

            # Upload remaining buffer (final part, can be < 5MB)
            if buffer:
                part_response = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer
                )
                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })
                logger.info(f"  Uploaded final part {part_number} ({total_bytes / (1024**3):.2f} GB total)")

            # Complete multipart upload
            if not parts:
                logger.error("No parts uploaded - empty file?")
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    UploadId=upload_id
                )
                return {
                    'success': False,
                    's3_key': s3_key,
                    'bytes_uploaded': 0,
                    'error': 'No data received from GoPro'
                }

            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            logger.info(f"  SUCCESS: Streamed {total_bytes / (1024**3):.2f} GB to s3://{self.bucket_name}/{s3_key}")

            return {
                'success': True,
                's3_key': s3_key,
                'bytes_uploaded': total_bytes,
                'parts_count': len(parts)
            }

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout streaming from GoPro: {e}")
            self._abort_multipart(s3_key, upload_id)
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': total_bytes,
                'error': f'Timeout streaming from GoPro: {e}'
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error streaming from GoPro: {e}")
            self._abort_multipart(s3_key, upload_id)
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': total_bytes,
                'error': f'HTTP error: {e}'
            }
        except Exception as e:
            logger.error(f"Error streaming chapter to S3: {e}")
            self._abort_multipart(s3_key, upload_id)
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': total_bytes,
                'error': str(e)
            }

    def _abort_multipart(self, s3_key: str, upload_id: str) -> None:
        """Abort a multipart upload on error."""
        try:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id
            )
            logger.info(f"  Aborted multipart upload: {s3_key}")
        except Exception as e:
            logger.warning(f"  Failed to abort multipart upload: {e}")

    def upload_session_chapters(
        self,
        session: Dict[str, Any],
        gopro_ip: str,
        chapters: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[str, int, int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Upload all chapters for a recording session to S3.
        
        NEW FLOW:
        1. Download chapter from GoPro to Jetson (/tmp/chapters/)
        2. Upload from Jetson to S3 (faster ethernet)
        3. Delete from Jetson local (keep on GoPro SD card)
        
        Uses robust download logic with:
        - Resume capability (Range headers)
        - Keep-alive to prevent GoPro sleep
        - Exponential backoff retry
        - Never deletes partial files during retry

        Args:
            session: Recording session document from Firebase
            gopro_ip: GoPro camera IP address
            chapters: List of chapter dicts with 'directory', 'filename', 'size' keys
            progress_callback: Optional callback(stage, chapter_num, total_chapters, bytes_uploaded)

        Returns:
            Dict with upload results:
                - success: bool
                - s3_prefix: str
                - chapters_uploaded: int
                - total_bytes: int
                - failed_chapters: list
                - errors: list
        """
        segment_session = session.get('segmentSession', '')
        if not segment_session:
            return {
                'success': False,
                's3_prefix': '',
                'chapters_uploaded': 0,
                'total_bytes': 0,
                'failed_chapters': [],
                'errors': ['Session missing segmentSession field']
            }

        # S3 prefix for this session's chapters
        s3_prefix = f"raw-chapters/{segment_session}/"
        
        # Local temp directory for this session
        local_temp_dir = f"/tmp/chapters/{segment_session}"
        os.makedirs(local_temp_dir, exist_ok=True)

        logger.info(f"Uploading {len(chapters)} chapters for session: {segment_session}")
        logger.info(f"  S3 prefix: s3://{self.bucket_name}/{s3_prefix}")
        logger.info(f"  Local temp dir: {local_temp_dir}")

        results = {
            'success': True,
            's3_prefix': s3_prefix,
            'chapters_uploaded': 0,
            'total_bytes': 0,
            'failed_chapters': [],
            'errors': [],
            'uploaded_chapters': []
        }

        total_chapters = len(chapters)
        
        # Start keep-alive thread for this session
        self.keep_alive = KeepAliveThread(gopro_ip)
        self.keep_alive.start()
        logger.info("Keep-alive thread started")

        try:
            for i, chapter in enumerate(chapters):
                chapter_num = i + 1
                filename = chapter['filename']
                directory = chapter['directory']
                expected_size = int(chapter.get('size', 0))

                # Generate S3 key: raw-chapters/{segmentSession}/chapter_001_GX010038.MP4
                s3_key = f"{s3_prefix}chapter_{chapter_num:03d}_{filename}"
                
                # Local path for this chapter
                local_path = os.path.join(local_temp_dir, filename)

                logger.info(f"[{chapter_num}/{total_chapters}] Processing: {filename}")

                # Skip if already in S3 (dev: avoid re-download/re-upload until cleanup)
                if self.s3_object_exists(s3_key):
                    results['chapters_uploaded'] += 1
                    results['total_bytes'] += expected_size
                    results['uploaded_chapters'].append({
                        'filename': filename,
                        's3_key': s3_key,
                        'bytes': expected_size,
                        'skipped': True
                    })
                    logger.info(f"[{chapter_num}/{total_chapters}] SKIPPED (already in S3): {s3_key}")
                    if progress_callback:
                        try:
                            progress_callback('uploading', chapter_num, total_chapters, results['total_bytes'])
                        except Exception:
                            pass
                    continue

                # Report progress: downloading
                if progress_callback:
                    try:
                        progress_callback('downloading', chapter_num, total_chapters, results['total_bytes'])
                    except Exception:
                        pass

                # STEP 1: Download from GoPro to Jetson
                def download_progress(bytes_downloaded, total_bytes):
                    if progress_callback:
                        try:
                            progress_callback('downloading', chapter_num, total_chapters,
                                             results['total_bytes'] + bytes_downloaded)
                        except Exception:
                            pass

                download_result = self.download_chapter_to_local(
                    gopro_ip=gopro_ip,
                    directory=directory,
                    filename=filename,
                    local_path=local_path,
                    expected_size=expected_size,
                    progress_callback=download_progress
                )

                if not download_result['success']:
                    results['success'] = False
                    results['failed_chapters'].append(filename)
                    results['errors'].append(f"Download failed: {download_result.get('error', 'Unknown error')}")
                    logger.error(f"[{chapter_num}/{total_chapters}] DOWNLOAD FAILED: {filename} - {download_result.get('error')}")
                    continue

                logger.info(f"[{chapter_num}/{total_chapters}] Downloaded: {filename} ({download_result['bytes_downloaded'] / (1024**3):.2f} GB)")

                # Report progress: uploading
                if progress_callback:
                    try:
                        progress_callback('uploading', chapter_num, total_chapters, results['total_bytes'])
                    except Exception:
                        pass

                # STEP 2: Upload from Jetson to S3
                def upload_progress(bytes_uploaded, total_bytes):
                    if progress_callback:
                        try:
                            progress_callback('uploading', chapter_num, total_chapters,
                                             results['total_bytes'] + bytes_uploaded)
                        except Exception:
                            pass

                upload_result = self.upload_local_file_to_s3(
                    local_path=local_path,
                    s3_key=s3_key,
                    progress_callback=upload_progress
                )

                if upload_result['success']:
                    results['chapters_uploaded'] += 1
                    results['total_bytes'] += upload_result['bytes_uploaded']
                    results['uploaded_chapters'].append({
                        'filename': filename,
                        's3_key': s3_key,
                        'bytes': upload_result['bytes_uploaded']
                    })
                    logger.info(f"[{chapter_num}/{total_chapters}] UPLOADED: {filename} -> s3://{self.bucket_name}/{s3_key}")
                else:
                    results['success'] = False
                    results['failed_chapters'].append(filename)
                    results['errors'].append(f"Upload failed: {upload_result.get('error', 'Unknown error')}")
                    logger.error(f"[{chapter_num}/{total_chapters}] UPLOAD FAILED: {filename} - {upload_result.get('error')}")

                # STEP 3: Delete from Jetson local (keep on GoPro SD card)
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                        logger.info(f"[{chapter_num}/{total_chapters}] Deleted local file: {local_path}")
                except Exception as e:
                    logger.warning(f"[{chapter_num}/{total_chapters}] Failed to delete local file: {e}")
                    # Don't fail the job for cleanup errors
        
        finally:
            # Stop keep-alive thread
            if self.keep_alive:
                self.keep_alive.stop()
                logger.info("Keep-alive thread stopped")
            
            # Cleanup: Remove session temp directory
            try:
                if os.path.exists(local_temp_dir):
                    os.rmdir(local_temp_dir)
                    logger.info(f"Cleaned up temp directory: {local_temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove temp directory: {e}")

        # Final summary
        logger.info(f"Upload complete: {results['chapters_uploaded']}/{total_chapters} chapters")
        logger.info(f"  Total uploaded: {results['total_bytes'] / (1024**3):.2f} GB")
        if results['failed_chapters']:
            logger.warning(f"  Failed: {results['failed_chapters']}")

        return results

    def get_gopro_media_list(self, gopro_ip: str) -> List[Dict[str, Any]]:
        """
        Get list of all media files on the GoPro.

        Args:
            gopro_ip: GoPro camera IP address

        Returns:
            List of file dicts with 'directory', 'filename', 'size' keys
        """
        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/media/list',
                timeout=15
            )
            response.raise_for_status()
            media_list = response.json()

            files = []
            for directory in media_list.get('media', []):
                dir_name = directory['d']
                for file_info in directory.get('fs', []):
                    filename = file_info['n']
                    if filename.lower().endswith('.mp4'):
                        files.append({
                            'directory': dir_name,
                            'filename': filename,
                            'size': int(file_info.get('s', 0))
                        })

            return files

        except Exception as e:
            logger.error(f"Error getting GoPro media list: {e}")
            return []

    def find_session_chapters_on_gopro(
        self,
        gopro_ip: str,
        session: Dict[str, Any],
        pre_record_files: Optional[set] = None
    ) -> List[Dict[str, Any]]:
        """
        Find chapters on GoPro that belong to a recording session.

        This matches files based on the session's pre-record file set
        (files that existed before recording started).

        Args:
            gopro_ip: GoPro camera IP address
            session: Recording session document
            pre_record_files: Set of filenames that existed before recording

        Returns:
            List of chapter dicts with 'directory', 'filename', 'size' keys
        """
        # Get all files currently on GoPro
        all_files = self.get_gopro_media_list(gopro_ip)

        if pre_record_files is None:
            # If no pre-record set, return all MP4 files
            # This is a fallback - in practice we should always have pre_record_files
            logger.warning("No pre_record_files provided, returning all files")
            return all_files

        # Filter to only new files (recorded during this session)
        new_files = [
            f for f in all_files
            if f['filename'] not in pre_record_files
        ]

        # Sort by GoPro naming convention (video number, then chapter number)
        def gopro_sort_key(chapter):
            filename = chapter['filename'].upper()
            if len(filename) >= 8 and filename.startswith('G'):
                # GXxxyyyy.MP4 where xx=chapter, yyyy=video
                chapter_num = filename[2:4]
                video_num = filename[4:8]
                return (video_num, chapter_num)
            return (filename, "00")

        new_files.sort(key=gopro_sort_key)

        logger.info(f"Found {len(new_files)} new chapters (out of {len(all_files)} total)")

        return new_files

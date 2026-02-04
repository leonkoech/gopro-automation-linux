"""
Chapter Upload Service for GoPro to S3 Streaming.

This module provides functionality to:
- Stream video chapters directly from GoPro HTTP to S3 (no temp files)
- Upload all chapters for a recording session
- Track upload progress with callbacks

S3 Structure:
    raw-chapters/{segmentSession}/chapter_001_GX010038.MP4

Key insight: GoPro cameras expose files via HTTP at:
    http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}

We stream directly from this URL to S3 multipart upload, avoiding
the 114GB Jetson disk overflow issue.
"""

import os
import requests
from typing import List, Dict, Any, Optional, Callable
from logging_service import get_logger

logger = get_logger('gopro.chapter_upload')

# Upload configuration
CHUNK_SIZE = 262144  # 256KB - read from GoPro in small chunks
S3_PART_SIZE = 25 * 1024 * 1024  # 25MB - S3 multipart part size
READ_TIMEOUT = 120  # seconds between chunks from GoPro
CONNECT_TIMEOUT = 30  # seconds to establish connection


class ChapterUploadService:
    """
    Service for streaming GoPro chapters directly to S3.

    Uses multipart upload to stream data from GoPro HTTP to S3
    without writing to local disk.
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

        logger.info(f"Uploading {len(chapters)} chapters for session: {segment_session}")
        logger.info(f"  S3 prefix: s3://{self.bucket_name}/{s3_prefix}")

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

        for i, chapter in enumerate(chapters):
            chapter_num = i + 1
            filename = chapter['filename']
            directory = chapter['directory']
            expected_size = int(chapter.get('size', 0))

            # Generate S3 key: raw-chapters/{segmentSession}/chapter_001_GX010038.MP4
            s3_key = f"{s3_prefix}chapter_{chapter_num:03d}_{filename}"

            logger.info(f"[{chapter_num}/{total_chapters}] Uploading: {filename}")

            # Report progress
            if progress_callback:
                try:
                    progress_callback('uploading', chapter_num, total_chapters, results['total_bytes'])
                except Exception:
                    pass

            # Create per-chapter progress callback
            def chapter_progress(bytes_uploaded, total_bytes):
                if progress_callback:
                    try:
                        progress_callback('streaming', chapter_num, total_chapters,
                                         results['total_bytes'] + bytes_uploaded)
                    except Exception:
                        pass

            # Stream chapter to S3
            upload_result = self.stream_chapter_to_s3(
                gopro_ip=gopro_ip,
                directory=directory,
                filename=filename,
                s3_key=s3_key,
                expected_size=expected_size,
                progress_callback=chapter_progress
            )

            if upload_result['success']:
                results['chapters_uploaded'] += 1
                results['total_bytes'] += upload_result['bytes_uploaded']
                results['uploaded_chapters'].append({
                    'filename': filename,
                    's3_key': s3_key,
                    'bytes': upload_result['bytes_uploaded']
                })
                logger.info(f"[{chapter_num}/{total_chapters}] SUCCESS: {filename}")
            else:
                results['success'] = False
                results['failed_chapters'].append(filename)
                results['errors'].append(upload_result.get('error', 'Unknown error'))
                logger.error(f"[{chapter_num}/{total_chapters}] FAILED: {filename} - {upload_result.get('error')}")

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

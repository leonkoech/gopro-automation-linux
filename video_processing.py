"""
Video Processing Module for Game Extraction.

This module provides functionality to:
- Extract game portions from continuous GoPro recordings
- Handle multi-chapter video files
- Use FFmpeg for video extraction and concatenation

S3 Key Format for game videos:
    {location}/{date}/{game_uuid}/{date}_{game_uuid}_{angle}.mp4

Where:
- {location} = court identifier (e.g., "court-a")
- {date} = game date YYYY-MM-DD
- {game_uuid} = shortened Uball game UUID (first 4 segments for uniqueness)
- {angle} = camera angle code (FL, FR, NL, NR)

Example:
    court-a/2026-01-20/95efaeaa-8475-4db4-8967/2026-01-20_95efaeaa-8475-4db4-8967_FL.mp4
"""

import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from logging_service import get_logger

logger = get_logger('gopro.video_processing')


class VideoProcessor:
    """Handles video extraction and processing for game clips."""

    def __init__(self, storage_dir: str, segments_dir: str):
        """
        Initialize VideoProcessor.

        Args:
            storage_dir: Base directory for video storage
            segments_dir: Directory containing segment session folders
        """
        self.storage_dir = storage_dir
        self.segments_dir = segments_dir
        self.output_dir = os.path.join(storage_dir, 'game_extracts')
        os.makedirs(self.output_dir, exist_ok=True)

    def get_session_chapters(self, session_name: str, check_corruption: bool = False) -> List[Dict[str, Any]]:
        """
        Get list of chapter files for a recording session.

        Args:
            session_name: Name of the segment session folder
            check_corruption: If True, check each file for corruption (slower)

        Returns:
            List of chapter info dicts with path, filename, size, duration, corruption status
        """
        session_path = os.path.join(self.segments_dir, session_name)
        if not os.path.exists(session_path):
            logger.warning(f"Session path not found: {session_path}")
            return []

        chapters = []
        for filename in sorted(os.listdir(session_path)):
            if filename.lower().endswith('.mp4'):
                filepath = os.path.join(session_path, filename)
                stat = os.stat(filepath)

                # Get video duration using ffprobe
                duration = self._get_video_duration(filepath)

                chapter_info = {
                    'filename': filename,
                    'path': filepath,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'duration_seconds': duration,
                    'duration_str': self._format_duration(duration) if duration else 'unknown',
                    'is_corrupted': False,
                    'corruption_error': None
                }

                # Check for corruption if duration is None or if explicitly requested
                if duration is None or check_corruption:
                    is_corrupted, error_msg = self._is_video_corrupted(filepath)
                    chapter_info['is_corrupted'] = is_corrupted
                    chapter_info['corruption_error'] = error_msg if is_corrupted else None
                    if is_corrupted:
                        logger.error(f"Corrupted chapter detected: {filename} - {error_msg}")

                chapters.append(chapter_info)

        return chapters

    def _get_video_duration(self, filepath: str) -> Optional[float]:
        """Get video duration in seconds using ffprobe."""
        try:
            result = subprocess.run([
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                filepath
            ], capture_output=True, text=True, timeout=120)

            if result.returncode == 0 and result.stdout.strip():
                return float(result.stdout.strip())

            # Check for corruption errors in stderr
            if 'moov atom not found' in result.stderr:
                logger.error(f"Video file corrupted (moov atom not found): {filepath}")
        except Exception as e:
            logger.warning(f"Could not get duration for {filepath}: {e}")

    def _is_video_corrupted(self, filepath: str) -> Tuple[bool, str]:
        """
        Check if a video file is corrupted.

        Returns:
            Tuple of (is_corrupted, error_message)
        """
        try:
            result = subprocess.run([
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                filepath
            ], capture_output=True, text=True, timeout=120)

            if result.returncode != 0:
                if 'moov atom not found' in result.stderr:
                    return True, 'Video file corrupted: moov atom not found (incomplete recording)'
                elif 'Invalid data' in result.stderr:
                    return True, 'Video file corrupted: invalid data'
                else:
                    return True, f'Video file error: {result.stderr.strip()}'

            # Check if duration was returned
            if not result.stdout.strip():
                return True, 'Video file corrupted: no duration metadata'

            return False, ''
        except subprocess.TimeoutExpired:
            logger.warning(f"ffprobe timeout checking {filepath} — assuming valid (large 4K file on loaded system)")
            return False, ''
        except Exception as e:
            return True, f'Error checking video: {str(e)}'
        return None

    def _get_video_height(self, filepath: str) -> Optional[int]:
        """Get video height (vertical resolution) in pixels using ffprobe."""
        try:
            result = subprocess.run([
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=height',
                '-of', 'csv=p=0',
                filepath
            ], capture_output=True, text=True, timeout=120)

            if result.returncode == 0 and result.stdout.strip():
                return int(result.stdout.strip())
        except Exception as e:
            logger.warning(f"Could not get height for {filepath}: {e}")
        return None

    def _get_video_codec(self, filepath: str) -> Optional[str]:
        """Get video codec name (e.g., 'hevc', 'h264') using ffprobe."""
        try:
            result = subprocess.run([
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=codec_name',
                '-of', 'csv=p=0',
                filepath
            ], capture_output=True, text=True, timeout=120)

            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip().lower()
        except Exception as e:
            logger.warning(f"Could not get codec for {filepath}: {e}")
        return None

    def _get_hw_decoder(self, filepath: str) -> Optional[str]:
        """Get the appropriate Jetson hardware decoder for the input file's codec.

        Jetson Orin Nano has NVDEC hardware decoder but NO NVENC encoder.
        Using HW decode frees CPU cores for libx264 software encoding.
        """
        codec = self._get_video_codec(filepath)
        hw_decoders = {
            'hevc': 'hevc_nvv4l2dec',
            'h265': 'hevc_nvv4l2dec',
            'h264': 'h264_nvv4l2dec',
        }
        decoder = hw_decoders.get(codec)
        if decoder:
            logger.info(f"  Using HW decoder {decoder} for {codec} input")
        else:
            logger.info(f"  No HW decoder for codec '{codec}', using software decode")
        return decoder

    def _needs_compression(self, filepath: str, target_height: int = 1080) -> bool:
        """Check if video needs compression (resolution > target_height)."""
        height = self._get_video_height(filepath)
        if height and height > target_height:
            logger.info(f"Video {filepath} is {height}p, needs compression to {target_height}p")
            return True
        return False

    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to HH:MM:SS."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    def calculate_extraction_params(
        self,
        game_start: datetime,
        game_end: datetime,
        recording_start: datetime,
        chapters: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate FFmpeg extraction parameters.

        Args:
            game_start: When the game started
            game_end: When the game ended
            recording_start: When the recording session started
            chapters: List of chapter files with durations

        Returns:
            Dict with:
                - offset_seconds: Seek position RELATIVE to first needed chapter
                - duration_seconds: Length of game clip
                - chapters_needed: List of chapter files needed
                - start_chapter_index: Index of first chapter
                - end_chapter_index: Index of last chapter
        """
        # Calculate offset from recording start to game start
        offset_from_recording_start = (game_start - recording_start).total_seconds()
        if offset_from_recording_start < 0:
            # Game started before recording - adjust
            offset_from_recording_start = 0
            game_start = recording_start

        # Calculate game duration
        duration = (game_end - game_start).total_seconds()

        # Find which chapters we need and calculate offset relative to first needed chapter
        current_time = 0
        start_chapter_idx = None
        end_chapter_idx = None
        chapters_needed = []
        first_chapter_start_time = 0  # When the first needed chapter starts in recording time

        for i, chapter in enumerate(chapters):
            chapter_duration = chapter.get('duration_seconds') or 0
            if not chapter_duration or chapter_duration <= 0:
                # Estimate ~15 min per 4GB chapter
                chapter_duration = 900  # 15 minutes

            chapter_end_time = current_time + chapter_duration

            # Check if this chapter contains any part of our game
            game_start_in_recording = offset_from_recording_start
            game_end_in_recording = offset_from_recording_start + duration

            # Chapter overlaps with game if:
            # chapter_start < game_end AND chapter_end > game_start
            if current_time < game_end_in_recording and chapter_end_time > game_start_in_recording:
                if start_chapter_idx is None:
                    start_chapter_idx = i
                    first_chapter_start_time = current_time
                end_chapter_idx = i
                chapters_needed.append(chapter)

            current_time = chapter_end_time

        # Calculate offset relative to the first needed chapter (not recording start)
        # This is the seek position within the concatenated needed chapters
        offset_relative_to_chapters = offset_from_recording_start - first_chapter_start_time
        if offset_relative_to_chapters < 0:
            offset_relative_to_chapters = 0

        logger.info(f"  Offset from recording start: {self._format_duration(offset_from_recording_start)}")
        logger.info(f"  First needed chapter starts at: {self._format_duration(first_chapter_start_time)}")
        logger.info(f"  Offset relative to chapters: {self._format_duration(offset_relative_to_chapters)}")

        return {
            'offset_seconds': offset_relative_to_chapters,  # FIXED: Use relative offset
            'duration_seconds': duration,
            'offset_str': self._format_duration(offset_relative_to_chapters),
            'duration_str': self._format_duration(duration),
            'chapters_needed': chapters_needed,
            'start_chapter_index': start_chapter_idx,
            'end_chapter_index': end_chapter_idx,
            'total_chapters': len(chapters),
            'chapters_to_process': len(chapters_needed),
            'offset_from_recording_start': offset_from_recording_start,  # Keep for reference
            'first_chapter_start_time': first_chapter_start_time  # For debugging
        }

    def extract_game_clip(
        self,
        chapters: List[Dict[str, Any]],
        offset_seconds: float,
        duration_seconds: float,
        output_filename: str,
        add_buffer: float = 30.0,
        compress_if_needed: bool = True,
        s3_upload_service=None,
        s3_key: str = None
    ) -> Optional[str]:
        """
        Extract a game clip from chapter files using FFmpeg.

        Args:
            chapters: List of chapter files to process
            offset_seconds: Seek position from start of first chapter
            duration_seconds: Length of clip to extract
            output_filename: Name for output file
            add_buffer: Extra seconds to add before/after game (default 30s)
            compress_if_needed: If True, compress to 1080p if source is >1080p (default True)
            s3_upload_service: If provided, pipe FFmpeg output directly to S3
            s3_key: S3 key for direct upload (required if s3_upload_service is set)

        Returns:
            Path to extracted video file, or None on failure.
            When streaming to S3, returns output_path for compatibility but file is in S3.
        """
        if not chapters:
            logger.error("No chapters provided for extraction")
            return None

        output_path = os.path.join(self.output_dir, output_filename)

        # Add buffer time (but don't go negative)
        buffered_offset = max(0, offset_seconds - add_buffer)
        buffered_duration = duration_seconds + (2 * add_buffer)

        actual_offset = buffered_offset

        try:
            if len(chapters) == 1:
                return self._extract_from_single_file(
                    chapters[0]['path'],
                    actual_offset,
                    buffered_duration,
                    output_path,
                    compress_if_needed=compress_if_needed,
                    s3_upload_service=s3_upload_service,
                    s3_key=s3_key
                )
            else:
                return self._extract_from_multiple_files(
                    [ch['path'] for ch in chapters],
                    actual_offset,
                    buffered_duration,
                    output_path,
                    compress_if_needed=compress_if_needed,
                    s3_upload_service=s3_upload_service,
                    s3_key=s3_key
                )

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _extract_from_single_file(
        self,
        input_path: str,
        offset: float,
        duration: float,
        output_path: str,
        compress_if_needed: bool = True,
        s3_upload_service=None,
        s3_key: str = None
    ) -> Optional[str]:
        """
        Extract clip from a single video file.

        If compress_if_needed is True and source is >1080p, will compress to 1080p
        using HW decode (hevc_nvv4l2dec) + libx264 ultrafast on Jetson Orin Nano.

        If s3_upload_service and s3_key are provided, pipes FFmpeg output directly
        to S3 via multipart upload (no temp file on disk).
        """
        logger.info(f"Extracting from single file: {input_path}")
        logger.info(f"  Offset: {self._format_duration(offset)}, Duration: {self._format_duration(duration)}")

        # Check if compression is needed
        needs_compress = compress_if_needed and self._needs_compression(input_path)

        # Build FFmpeg command with input-level seeking (-ss before -i)
        cmd = ['ffmpeg', '-y']

        # Input-level -ss for fast keyframe seeking (skips decoding unwanted frames)
        cmd.extend(['-ss', str(offset)])

        # Use hardware decoder if available (Jetson NVDEC)
        if needs_compress:
            hw_decoder = self._get_hw_decoder(input_path)
            if hw_decoder:
                cmd.extend(['-c:v', hw_decoder])

        cmd.extend(['-i', input_path])
        cmd.extend(['-t', str(duration)])

        if needs_compress:
            # Jetson Orin Nano has NO hardware encoder (NVENC).
            # Use libx264 ultrafast + HW decoder for best performance.
            logger.info(f"  Compressing to 1080p (HW decode + libx264 ultrafast, CRF 23)")
            cmd.extend([
                '-vf', 'scale=-2:1080',
                '-c:v', 'libx264',
                '-preset', 'ultrafast',
                '-crf', '23',
                '-c:a', 'aac', '-b:a', '128k',
            ])

            if s3_upload_service and s3_key:
                # Fragmented MP4 for pipe-compatible output (no seeking needed)
                cmd.extend(['-movflags', 'frag_keyframe+empty_moov'])
            else:
                cmd.extend(['-movflags', '+faststart'])
        else:
            # Stream copy (fast, no re-encoding)
            cmd.extend(['-c', 'copy'])

        # Pipe to S3 or write to disk
        if s3_upload_service and s3_key:
            cmd.extend(['-f', 'mp4', 'pipe:1'])
            return self._stream_ffmpeg_to_s3(cmd, s3_upload_service, s3_key, output_path, needs_compress)
        else:
            cmd.extend(['-avoid_negative_ts', 'make_zero', output_path])
            timeout = 7200 if needs_compress else 600
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

            if result.returncode != 0:
                logger.error(f"FFmpeg error: {result.stderr}")
                return None

            if os.path.exists(output_path):
                size_mb = os.path.getsize(output_path) / (1024 * 1024)
                logger.info(f"Extracted: {output_path} ({size_mb:.1f} MB)")
                return output_path

            return None

    def _extract_from_multiple_files(
        self,
        input_paths: List[str],
        offset: float,
        duration: float,
        output_path: str,
        compress_if_needed: bool = True,
        s3_upload_service=None,
        s3_key: str = None
    ) -> Optional[str]:
        """
        Extract clip from multiple concatenated video files.

        If compress_if_needed is True and source is >1080p, will compress to 1080p
        using HW decode (hevc_nvv4l2dec) + libx264 ultrafast on Jetson Orin Nano.

        If s3_upload_service and s3_key are provided, pipes FFmpeg output directly
        to S3 via multipart upload (no temp file on disk).
        """
        logger.info(f"Extracting from {len(input_paths)} files")
        logger.info(f"  Offset: {self._format_duration(offset)}, Duration: {self._format_duration(duration)}")

        # Check first file to determine if compression is needed (all chapters same resolution)
        needs_compress = compress_if_needed and input_paths and self._needs_compression(input_paths[0])

        # Create concat file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            concat_file = f.name
            for path in sorted(input_paths):
                escaped_path = path.replace("'", "'\\''")
                f.write(f"file '{escaped_path}'\n")

        try:
            cmd = ['ffmpeg', '-y']

            # Input-level -ss for fast keyframe seeking (before -i)
            cmd.extend(['-ss', str(offset)])

            cmd.extend(['-f', 'concat', '-safe', '0', '-i', concat_file])
            cmd.extend(['-t', str(duration)])

            if needs_compress:
                # Jetson Orin Nano has NO hardware encoder (NVENC).
                # Use libx264 ultrafast + HW decoder for best performance.
                # NOTE: HW decoder (-c:v hevc_nvv4l2dec) cannot be used with
                # concat demuxer — ffmpeg applies -c:v to concat input, not individual files.
                # The software decoder handles this automatically.
                logger.info(f"  Compressing to 1080p (libx264 ultrafast, CRF 23)")
                cmd.extend([
                    '-vf', 'scale=-2:1080',
                    '-c:v', 'libx264',
                    '-preset', 'ultrafast',
                    '-crf', '23',
                    '-c:a', 'aac', '-b:a', '128k',
                ])

                if s3_upload_service and s3_key:
                    cmd.extend(['-movflags', 'frag_keyframe+empty_moov'])
                else:
                    cmd.extend(['-movflags', '+faststart'])
            else:
                cmd.extend(['-c', 'copy'])

            # Pipe to S3 or write to disk
            if s3_upload_service and s3_key:
                cmd.extend(['-f', 'mp4', 'pipe:1'])
                return self._stream_ffmpeg_to_s3(cmd, s3_upload_service, s3_key, output_path, needs_compress)
            else:
                cmd.extend(['-avoid_negative_ts', 'make_zero', output_path])
                timeout = 7200 if needs_compress else 1200
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

                if result.returncode != 0:
                    logger.error(f"FFmpeg error: {result.stderr}")
                    return None

                if os.path.exists(output_path):
                    size_mb = os.path.getsize(output_path) / (1024 * 1024)
                    logger.info(f"Extracted: {output_path} ({size_mb:.1f} MB)")
                    return output_path

                return None

        finally:
            try:
                os.unlink(concat_file)
            except:
                pass

    def _stream_ffmpeg_to_s3(
        self,
        cmd: List[str],
        upload_service,
        s3_key: str,
        output_path: str,
        needs_compress: bool
    ) -> Optional[str]:
        """
        Run FFmpeg and pipe stdout directly to S3 via boto3 multipart upload.

        This avoids writing large temp files to disk and overlaps encoding
        with uploading for better throughput.

        Returns the output_path string on success (for compatibility with callers),
        even though the file is streamed to S3 and not saved locally.
        """
        import io
        import threading

        logger.info(f"  Streaming FFmpeg output directly to S3: {s3_key}")
        logger.info(f"  FFmpeg cmd: {' '.join(cmd)}")

        s3_client = upload_service.s3_client
        bucket = upload_service.bucket_name

        # Start multipart upload
        mpu = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=s3_key,
            ContentType='video/mp4'
        )
        upload_id = mpu['UploadId']
        parts = []
        part_number = 1
        # 25 MB part size (matches existing transfer config)
        PART_SIZE = 25 * 1024 * 1024
        total_bytes = 0

        try:
            # Start FFmpeg with stdout pipe
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=PART_SIZE
            )

            # Read stderr in background thread to prevent blocking
            stderr_output = []
            def read_stderr():
                stderr_output.append(process.stderr.read().decode('utf-8', errors='replace'))
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stderr_thread.start()

            buffer = b''
            while True:
                chunk = process.stdout.read(PART_SIZE - len(buffer))
                if not chunk:
                    break
                buffer += chunk

                if len(buffer) >= PART_SIZE:
                    # Upload this part
                    response = s3_client.upload_part(
                        Bucket=bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer
                    )
                    parts.append({
                        'ETag': response['ETag'],
                        'PartNumber': part_number
                    })
                    total_bytes += len(buffer)
                    logger.info(f"  Uploaded part {part_number} ({total_bytes / (1024*1024):.0f} MB streamed)")
                    part_number += 1
                    buffer = b''

            # Upload remaining buffer (final part, can be < 5MB)
            if buffer:
                response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer
                )
                parts.append({
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                })
                total_bytes += len(buffer)
                logger.info(f"  Uploaded final part {part_number} ({total_bytes / (1024*1024):.0f} MB total)")

            # Wait for FFmpeg to finish
            # For compression, FFmpeg may still be flushing - use longer timeout
            process.stdout.close()
            wait_timeout = 300 if needs_compress else 60  # 5 min for encoding, 1 min for copy
            return_code = process.wait(timeout=wait_timeout)
            stderr_thread.join(timeout=10)

            if return_code != 0:
                stderr_text = stderr_output[0] if stderr_output else 'unknown error'
                logger.error(f"FFmpeg failed (exit {return_code}): {stderr_text[-500:]}")
                # Abort multipart upload
                s3_client.abort_multipart_upload(
                    Bucket=bucket, Key=s3_key, UploadId=upload_id
                )
                return None

            if not parts:
                logger.error("FFmpeg produced no output")
                s3_client.abort_multipart_upload(
                    Bucket=bucket, Key=s3_key, UploadId=upload_id
                )
                return None

            # Complete multipart upload
            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            logger.info(f"  Streamed to S3: s3://{bucket}/{s3_key} ({total_bytes / (1024*1024):.1f} MB)")
            return output_path  # Return path for compatibility (file is in S3, not local)

        except Exception as e:
            logger.error(f"Stream-to-S3 failed: {e}")
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket, Key=s3_key, UploadId=upload_id
                )
            except:
                pass
            # Kill ffmpeg if still running
            try:
                process.kill()
            except:
                pass
            return None

    def extract_4k_stream_copy(
        self,
        chapters: List[Dict[str, Any]],
        offset_seconds: float,
        duration_seconds: float,
        output_path: str,
        add_buffer: float = 30.0
    ) -> Optional[str]:
        """
        Extract a clip with stream copy (no re-encoding). Ultra-fast for 4K extraction.

        This method is used for AWS GPU transcoding flow where we want to quickly
        extract the 4K video and offload encoding to AWS Batch.

        Args:
            chapters: List of chapter files to process
            offset_seconds: Seek position from start of first chapter
            duration_seconds: Length of clip to extract
            output_path: Full path for output file
            add_buffer: Extra seconds to add before/after game (default 30s)

        Returns:
            Path to extracted video file, or None on failure
        """
        if not chapters:
            logger.error("No chapters provided for 4K stream copy extraction")
            return None

        # Add buffer time (but don't go negative)
        buffered_offset = max(0, offset_seconds - add_buffer)
        buffered_duration = duration_seconds + (2 * add_buffer)

        logger.info(f"Extracting 4K with stream copy:")
        logger.info(f"  Chapters: {len(chapters)}")
        logger.info(f"  Offset: {self._format_duration(buffered_offset)}")
        logger.info(f"  Duration: {self._format_duration(buffered_duration)}")
        logger.info(f"  Output: {output_path}")

        try:
            if len(chapters) == 1:
                # Single file extraction with stream copy
                cmd = [
                    'ffmpeg', '-y',
                    '-ss', str(buffered_offset),
                    '-i', chapters[0]['path'],
                    '-t', str(buffered_duration),
                    '-c', 'copy',  # Stream copy - no encoding
                    '-avoid_negative_ts', 'make_zero',
                    output_path
                ]
            else:
                # Multiple files - create concat file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                    concat_file = f.name
                    for chapter in chapters:
                        escaped_path = chapter['path'].replace("'", "'\\''")
                        f.write(f"file '{escaped_path}'\n")

                cmd = [
                    'ffmpeg', '-y',
                    '-ss', str(buffered_offset),
                    '-f', 'concat', '-safe', '0',
                    '-i', concat_file,
                    '-t', str(buffered_duration),
                    '-c', 'copy',  # Stream copy - no encoding
                    '-avoid_negative_ts', 'make_zero',
                    output_path
                ]

            logger.info(f"  FFmpeg cmd: {' '.join(cmd)}")

            # Stream copy reads full 4K data from disk - allow 30 min for large multi-chapter extracts
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

            # Clean up concat file if created
            if len(chapters) > 1:
                try:
                    os.unlink(concat_file)
                except:
                    pass

            if result.returncode != 0:
                logger.error(f"FFmpeg stream copy error: {result.stderr}")
                return None

            if os.path.exists(output_path):
                size_mb = os.path.getsize(output_path) / (1024 * 1024)
                size_gb = size_mb / 1024
                logger.info(f"Extracted 4K stream copy: {output_path} ({size_gb:.2f} GB)")
                return output_path

            return None

        except subprocess.TimeoutExpired:
            logger.error("Stream copy extraction timed out (>5 min)")
            return None
        except Exception as e:
            logger.error(f"Stream copy extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return None


    def generate_game_filename(
        self,
        date: str,
        angle_code: str,
        uball_game_id: str = None
    ) -> str:
        """
        Generate filename for game video.

        Format: {date}_{game_uuid}_{angle}.mp4
        Example: 2026-01-20_95efaeaa-8475-4db4-8967_FL.mp4

        Args:
            date: Game date (YYYY-MM-DD)
            angle_code: Camera angle (FL, FR, NL, NR)
            uball_game_id: Full Uball game UUID (will be shortened to first 4 segments)
        """
        if uball_game_id:
            # Use first 4 segments of UUID for shorter but still unique identifier
            uuid_parts = uball_game_id.split('-')[:4]
            uuid_short = '-'.join(uuid_parts)
            return f"{date}_{uuid_short}_{angle_code}.mp4"
        else:
            return f"{date}_{angle_code}.mp4"

    def generate_s3_key(
        self,
        location: str,
        date: str,
        angle_code: str,
        uball_game_id: str = None
    ) -> str:
        """
        Generate S3 key for game video.

        Format: {location}/{date}/{game_uuid}/{date}_{game_uuid}_{angle}.mp4
        Example: court-a/2026-01-20/95efaeaa-8475-4db4-8967/2026-01-20_95efaeaa-8475-4db4-8967_FL.mp4

        Args:
            location: Court/location identifier
            date: Game date (YYYY-MM-DD)
            angle_code: Camera angle (FL, FR, NL, NR)
            uball_game_id: Uball game UUID for unique folder name
        """
        filename = self.generate_game_filename(date, angle_code, uball_game_id)
        if uball_game_id:
            # Use first 4 segments of UUID for shorter but still unique folder name
            uuid_parts = uball_game_id.split('-')[:4]
            folder = '-'.join(uuid_parts)
        else:
            # Fallback if no game ID (shouldn't happen in normal flow)
            folder = f"unknown-{date}"
        return f"{location}/{date}/{folder}/{filename}"

    def get_video_info(self, filepath: str) -> Dict[str, Any]:
        """Get detailed video information using ffprobe."""
        try:
            result = subprocess.run([
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                filepath
            ], capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)

                format_info = data.get('format', {})
                video_stream = next(
                    (s for s in data.get('streams', []) if s.get('codec_type') == 'video'),
                    {}
                )

                return {
                    'duration': float(format_info.get('duration', 0)),
                    'size_bytes': int(format_info.get('size', 0)),
                    'bitrate': int(format_info.get('bit_rate', 0)),
                    'width': video_stream.get('width'),
                    'height': video_stream.get('height'),
                    'codec': video_stream.get('codec_name'),
                    'fps': eval(video_stream.get('r_frame_rate', '0/1')) if video_stream.get('r_frame_rate') else None
                }
        except Exception as e:
            logger.warning(f"Could not get video info: {e}")

        return {}

    def get_session_chapters_from_s3(
        self,
        s3_prefix: str,
        s3_client,
        bucket: str,
        url_expiration: int = 7200
    ) -> List[Dict[str, Any]]:
        """
        Get list of chapter files from S3 with presigned URLs for FFmpeg.

        Args:
            s3_prefix: S3 prefix where chapters are stored (e.g., "raw-chapters/session123/")
            s3_client: boto3 S3 client instance
            bucket: S3 bucket name
            url_expiration: Presigned URL expiration in seconds (default 2 hours)

        Returns:
            List of chapter info dicts with path (presigned URL), filename, size, duration
        """
        chapters = []

        try:
            # List all objects with the given prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    key = obj['Key']
                    filename = key.split('/')[-1]

                    # Only include MP4 files
                    if not filename.lower().endswith('.mp4'):
                        continue

                    # Generate presigned URL for FFmpeg to read
                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': bucket, 'Key': key},
                        ExpiresIn=url_expiration
                    )

                    # Get video duration using ffprobe with presigned URL
                    duration = self._get_video_duration(presigned_url)

                    chapter_info = {
                        'filename': filename,
                        'path': presigned_url,  # FFmpeg can read this directly!
                        's3_key': key,
                        'size_bytes': obj['Size'],
                        'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                        'duration_seconds': duration,
                        'duration_str': self._format_duration(duration) if duration else 'unknown',
                        'is_corrupted': False,  # Assume S3 files are valid
                        'corruption_error': None,
                        'source': 's3'
                    }

                    chapters.append(chapter_info)

            # Sort by chapter number (extracted from filename like "chapter_001_GX010038.MP4")
            def chapter_sort_key(ch):
                fname = ch['filename']
                # Try to extract chapter number from filename
                if fname.startswith('chapter_'):
                    try:
                        return int(fname.split('_')[1])
                    except (IndexError, ValueError):
                        pass
                return fname

            chapters.sort(key=chapter_sort_key)

            logger.info(f"Found {len(chapters)} chapters in S3 at {s3_prefix}")

        except Exception as e:
            logger.error(f"Error listing S3 chapters: {e}")

        return chapters

    def download_s3_chapters_to_local(
        self,
        chapters: List[Dict[str, Any]],
        s3_client,
        bucket: str,
        session_id: str
    ) -> List[Dict[str, Any]]:
        """
        Download S3 chapters to local temp directory for FFmpeg processing.

        The Jetson's FFmpeg is compiled without HTTPS protocol support, so we must
        download S3 chapters to local disk before FFmpeg can process them.

        Args:
            chapters: List of chapter info dicts (must have 's3_key' for S3 chapters)
            s3_client: boto3 S3 client instance
            bucket: S3 bucket name
            session_id: Session ID for temp directory naming

        Returns:
            Updated list of chapters with 'path' pointing to local files
            and 'local_temp_path' for cleanup tracking
        """
        import shutil

        # Create temp directory for this session
        temp_dir = f"/tmp/game_extraction/{session_id}"
        os.makedirs(temp_dir, exist_ok=True)
        logger.info(f"Downloading S3 chapters to: {temp_dir}")

        updated_chapters = []
        for chapter in chapters:
            # Check if this is an S3 chapter
            s3_key = chapter.get('s3_key')
            if not s3_key:
                # Local chapter - keep as is
                updated_chapters.append(chapter)
                continue

            filename = chapter.get('filename', os.path.basename(s3_key))
            local_path = os.path.join(temp_dir, filename)

            try:
                # Download from S3 to local temp
                logger.info(f"  Downloading: {filename} ({chapter.get('size_mb', '?')} MB)")
                s3_client.download_file(bucket, s3_key, local_path)

                # Verify file was downloaded
                if not os.path.exists(local_path):
                    logger.error(f"  Download failed - file not created: {local_path}")
                    continue

                local_size = os.path.getsize(local_path)
                logger.info(f"  Downloaded: {filename} ({local_size / (1024*1024):.1f} MB)")

                # Update chapter with local path
                updated_chapter = chapter.copy()
                updated_chapter['path'] = local_path
                updated_chapter['local_temp_path'] = local_path  # For cleanup tracking
                updated_chapter['source'] = 's3_downloaded'

                # Get accurate duration from local file
                duration = self._get_video_duration(local_path)
                if duration:
                    updated_chapter['duration_seconds'] = duration
                    updated_chapter['duration_str'] = self._format_duration(duration)

                updated_chapters.append(updated_chapter)

            except Exception as e:
                logger.error(f"  Failed to download {filename}: {e}")
                # Keep original chapter info for error tracking
                updated_chapters.append(chapter)

        logger.info(f"Downloaded {len([c for c in updated_chapters if c.get('local_temp_path')])} of {len(chapters)} chapters")
        return updated_chapters

    def cleanup_temp_chapters(self, chapters: List[Dict[str, Any]]) -> None:
        """
        Clean up downloaded temp chapter files after extraction.

        Args:
            chapters: List of chapter info dicts (with 'local_temp_path' for temp files)
        """
        import shutil

        # Find unique temp directories to clean up
        temp_dirs = set()
        for chapter in chapters:
            temp_path = chapter.get('local_temp_path')
            if temp_path and os.path.exists(temp_path):
                temp_dir = os.path.dirname(temp_path)
                temp_dirs.add(temp_dir)
                try:
                    os.remove(temp_path)
                    logger.debug(f"Cleaned up temp file: {temp_path}")
                except Exception as e:
                    logger.warning(f"Failed to remove temp file {temp_path}: {e}")

        # Remove temp directories if empty
        for temp_dir in temp_dirs:
            try:
                if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                    os.rmdir(temp_dir)
                    logger.info(f"Removed empty temp directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove temp dir {temp_dir}: {e}")

    def get_session_chapters_auto(
        self,
        session: Dict[str, Any],
        s3_client=None,
        bucket: str = None,
        check_corruption: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Auto-select chapter source: S3 (if s3Prefix set) or local disk.

        This is the recommended method for getting chapters - it automatically
        uses the right source based on whether chapters have been uploaded to S3.

        Args:
            session: Recording session document from Firebase
            s3_client: boto3 S3 client (required if session has s3Prefix)
            bucket: S3 bucket name (required if session has s3Prefix)
            check_corruption: If True, check local files for corruption

        Returns:
            List of chapter info dicts with path (local or presigned URL)
        """
        s3_prefix = session.get('s3Prefix')
        session_name = session.get('segmentSession', '')

        if s3_prefix and s3_client and bucket:
            # Chapters are in S3 - use presigned URLs
            logger.info(f"[{session_name}] Using S3 chapters from: {s3_prefix}")
            return self.get_session_chapters_from_s3(s3_prefix, s3_client, bucket)
        else:
            # Fall back to local chapters
            logger.info(f"[{session_name}] Using local chapters from disk")
            return self.get_session_chapters(session_name, check_corruption=check_corruption)


def process_game_videos(
    firebase_game_id: str,
    game_number: int,
    firebase_service,
    upload_service,
    video_processor: VideoProcessor,
    location: str = 'default-location',
    uball_client=None,
    s3_bucket: str = 'uball-videos-production',
    progress_callback=None
) -> Dict[str, Any]:
    """
    Process videos for a specific game.

    This is the main entry point for game video processing.

    Args:
        firebase_game_id: Firebase game document ID
        game_number: Game number for the day (1, 2, 3...)
        firebase_service: Firebase service instance
        upload_service: Video upload service instance
        video_processor: VideoProcessor instance
        location: Location name for S3 path
        uball_client: Optional Uball client for registering FL/FR videos
        s3_bucket: S3 bucket name for Uball registration
        progress_callback: Optional callback(stage, detail, progress, current_angle)

    Returns:
        Dict with processing results
    """
    def report_progress(stage: str, detail: str = '', progress: float = 0, current_angle: str = ''):
        """Helper to report progress if callback is provided."""
        if progress_callback:
            try:
                progress_callback(stage, detail, progress, current_angle)
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")

    results = {
        'firebase_game_id': firebase_game_id,
        'game_number': game_number,
        'success': False,
        'processed_videos': [],
        'registered_videos': [],  # Videos registered in Uball (FL/FR only)
        'batch_jobs': [],  # AWS Batch transcode jobs
        'errors': [],
        'uball_game_id': None
    }

    report_progress('initializing', 'Loading game data...', 5)

    # Get Uball game ID for S3 folder structure
    # Auto-sync to Uball if not already synced
    uball_game_id = None
    if uball_client:
        uball_game = uball_client.get_game_by_firebase_id(firebase_game_id)
        if uball_game:
            uball_game_id = str(uball_game.get('id', ''))
            results['uball_game_id'] = uball_game_id
            logger.info(f"Found Uball game: {uball_game_id}")
        else:
            # Auto-sync: Game not in Uball, create it automatically
            logger.info(f"[AUTO-SYNC] Game not synced to Uball - syncing automatically...")

            try:
                # Get game data from Firebase for sync
                firebase_game = firebase_service.get_game(firebase_game_id)
                if not firebase_game:
                    results['errors'].append(f"Game not found in Firebase: {firebase_game_id}")
                    return results

                # Check if Firebase already has uballGameId (synced but not in Uball lookup)
                if firebase_game.get('uballGameId'):
                    uball_game_id = firebase_game['uballGameId']
                    results['uball_game_id'] = uball_game_id
                    logger.info(f"[AUTO-SYNC] Found uballGameId in Firebase: {uball_game_id}")
                else:
                    # Create teams in Uball
                    left_team = firebase_game.get('leftTeam', {})
                    right_team = firebase_game.get('rightTeam', {})
                    team1_name = left_team.get('name', 'Team 1')
                    team2_name = right_team.get('name', 'Team 2')

                    logger.info(f"[AUTO-SYNC] Creating teams: {team1_name} vs {team2_name}")

                    created_team1 = uball_client.create_team(team1_name)
                    if not created_team1:
                        results['errors'].append(f"Failed to create team: {team1_name}")
                        return results
                    team1_id = str(created_team1.get('id'))

                    created_team2 = uball_client.create_team(team2_name)
                    if not created_team2:
                        results['errors'].append(f"Failed to create team: {team2_name}")
                        return results
                    team2_id = str(created_team2.get('id'))

                    # Create game in Uball
                    created_at = firebase_game.get('createdAt', '')
                    ended_at = firebase_game.get('endedAt')
                    game_date_str = created_at[:10] if created_at else datetime.now().strftime('%Y-%m-%d')

                    uball_game_data = {
                        'firebase_game_id': firebase_game_id,
                        'date': game_date_str,
                        'team1_id': team1_id,
                        'team2_id': team2_id,
                        'start_time': created_at if created_at else None,
                        'end_time': ended_at if ended_at else None,
                        'source': 'firebase',
                        'video_name': f"{team1_name} vs {team2_name}"
                    }

                    # Add scores if available
                    if left_team.get('finalScore') is not None:
                        uball_game_data['team1_score'] = left_team['finalScore']
                    if right_team.get('finalScore') is not None:
                        uball_game_data['team2_score'] = right_team['finalScore']

                    logger.info(f"[AUTO-SYNC] Creating game in Uball...")
                    uball_game = uball_client.create_game(uball_game_data)

                    if not uball_game:
                        results['errors'].append("Failed to create game in Uball Backend")
                        return results

                    uball_game_id = str(uball_game.get('id', ''))
                    results['uball_game_id'] = uball_game_id

                    # Mark game as synced in Firebase
                    firebase_service.mark_game_synced(firebase_game_id, uball_game_id)
                    logger.info(f"[AUTO-SYNC] SUCCESS: Firebase {firebase_game_id} -> Uball {uball_game_id}")

            except Exception as e:
                logger.error(f"[AUTO-SYNC] Failed to auto-sync game: {e}")
                results['errors'].append(f"Auto-sync failed: {str(e)}")
                return results
    else:
        logger.error("[SYNC REQUIRED] Uball client not available - cannot determine game ID for S3 path")
        results['errors'].append("Uball client not configured - cannot process videos")
        return results

    try:
        # 1. Get game from Firebase
        game = firebase_service.get_game(firebase_game_id)
        if not game:
            results['errors'].append(f"Game not found: {firebase_game_id}")
            return results

        created_at = game.get('createdAt')
        ended_at = game.get('endedAt')

        if not created_at or not ended_at:
            results['errors'].append("Game missing start or end time")
            return results

        # Parse timestamps
        game_start = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        game_end = datetime.fromisoformat(ended_at.replace('Z', '+00:00'))
        game_date = created_at[:10]  # YYYY-MM-DD

        logger.info(f"Processing game {firebase_game_id}")
        logger.info(f"  Start: {game_start}, End: {game_end}")
        logger.info(f"  Duration: {(game_end - game_start).total_seconds() / 60:.1f} minutes")

        report_progress('finding_sessions', 'Finding overlapping recording sessions...', 10)

        # 2. Find overlapping recording sessions
        # IMPORTANT: Only query sessions for THIS Jetson to avoid processing sessions
        # that don't have local chapter files (sessions are stored on the Jetson that recorded them)
        jetson_id = os.getenv('JETSON_ID', 'unknown')
        logger.info(f"[ProcessGame] Filtering sessions for jetson_id: {jetson_id}")
        all_sessions = firebase_service.get_recording_sessions(jetson_id=jetson_id, limit=100)
        logger.info(f"[ProcessGame] Found {len(all_sessions)} sessions for {jetson_id}")
        overlapping_sessions = []

        for session in all_sessions:
            session_start_str = session.get('startedAt')
            session_end_str = session.get('endedAt')

            if not session_start_str:
                continue

            s_start = datetime.fromisoformat(session_start_str.replace('Z', '+00:00'))
            s_end = datetime.fromisoformat(session_end_str.replace('Z', '+00:00')) if session_end_str else datetime.now(s_start.tzinfo)

            # Check overlap
            if s_start < game_end and s_end > game_start:
                session['parsed_start'] = s_start
                session['parsed_end'] = s_end
                overlapping_sessions.append(session)

        logger.info(f"Found {len(overlapping_sessions)} overlapping recording sessions")

        # Sort sessions to process FL/FR first (priority angles), then NL/NR
        # This ensures the main game angles are processed before supplementary angles
        angle_priority = {'FL': 0, 'FR': 1, 'NL': 2, 'NR': 3}
        overlapping_sessions.sort(key=lambda s: angle_priority.get(s.get('angleCode', 'UNKNOWN'), 99))
        logger.info(f"Processing order: {[s.get('angleCode') for s in overlapping_sessions]}")

        if not overlapping_sessions:
            # No overlapping sessions on this Jetson is not an error - it just means this Jetson
            # doesn't have videos for this game timeframe. Mark as completed with "skipped" status.
            jetson_id = os.getenv('JETSON_ID', 'unknown')
            logger.info(f"No overlapping sessions found on {jetson_id} for game timeframe - skipping")
            results['success'] = True  # Not a failure, just no files to process
            results['skipped'] = True
            results['skip_reason'] = f"No recording sessions overlap with game timeframe on {jetson_id}"
            report_progress('completed', f'No videos found on {jetson_id} for this game timeframe (skipped)', 100)
            return results

        report_progress('processing', f'Processing {len(overlapping_sessions)} video angles...', 15)

        # 3. Process each session
        total_sessions = len(overlapping_sessions)
        for session_idx, session in enumerate(overlapping_sessions):
            session_name = session.get('segmentSession', '')
            angle_code = session.get('angleCode', 'UNKNOWN')
            recording_start = session['parsed_start']

            # Calculate progress for this session (15-90% range for processing)
            session_base_progress = 15 + (session_idx * 75 // total_sessions)

            # ============================================================
            # SKIP CHECK: Was this game already processed for this session?
            # ============================================================
            processed_games = session.get('processedGames', [])
            already_processed = any(
                pg.get('firebase_game_id') == firebase_game_id
                for pg in processed_games
            )
            if already_processed:
                logger.info(f"[SKIP] Game {firebase_game_id} already processed for session {session_name} ({angle_code})")
                # Add to results as skipped (not an error)
                results['processed_videos'].append({
                    'angle': angle_code,
                    'session_id': session['id'],
                    'status': 'skipped',
                    'skip_reason': 'already_processed'
                })
                continue

            # ============================================================
            # SKIP CHECK: Skip sessions with UNK/UNKNOWN angle codes
            # These sessions don't have proper camera angle mapping configured
            # ============================================================
            if angle_code.upper() in ('UNK', 'UNKNOWN', 'NONE', ''):
                logger.info(f"[SKIP] Session {session_name} has unknown angle ({angle_code}) - skipping")
                results['processed_videos'].append({
                    'angle': angle_code,
                    'session_id': session['id'],
                    'status': 'skipped',
                    'skip_reason': 'unknown_angle'
                })
                continue

            logger.info(f"Processing session: {session_name} (angle: {angle_code})")
            report_progress('extracting', f'Extracting {angle_code} video...', session_base_progress, angle_code)

            # Get chapter files - auto-selects S3 or local based on s3Prefix
            # Pass S3 client and bucket if upload_service is available
            s3_client = upload_service.s3_client if upload_service else None
            s3_bucket = upload_service.bucket_name if upload_service else None
            chapters = video_processor.get_session_chapters_auto(
                session,
                s3_client=s3_client,
                bucket=s3_bucket,
                check_corruption=True
            )
            if not chapters:
                logger.warning(f"No chapters found for session {session_name}")
                results['errors'].append(f"No chapters for session {session_name}")
                continue

            # Calculate extraction parameters FIRST to know which chapters we actually need
            params = video_processor.calculate_extraction_params(
                game_start, game_end, recording_start, chapters
            )

            logger.info(f"  Offset: {params['offset_str']}, Duration: {params['duration_str']}")
            logger.info(f"  Chapters needed: {params['chapters_to_process']}/{params['total_chapters']}")

            if not params['chapters_needed']:
                logger.warning(f"No chapters needed for this game timeframe")
                continue

            # Check for corrupted chapters ONLY among the chapters we actually need
            corrupted_chapters = [ch for ch in params['chapters_needed'] if ch.get('is_corrupted')]
            if corrupted_chapters:
                corruption_msg = corrupted_chapters[0].get('corruption_error', 'Unknown corruption')
                logger.error(f"Corrupted video files for {angle_code}: {corruption_msg}")
                results['errors'].append(f"CORRUPTED: {angle_code} video files are corrupted ({corruption_msg})")
                # Mark this specifically as a corruption error for the frontend
                if 'corrupted_sessions' not in results:
                    results['corrupted_sessions'] = []
                results['corrupted_sessions'].append({
                    'angle': angle_code,
                    'session': session_name,
                    'error': corruption_msg
                })
                continue

            # Generate output filename and S3 key upfront
            output_filename = video_processor.generate_game_filename(
                game_date, angle_code, uball_game_id
            )

            # Use COURT_LOCATION (e.g., "court-a") instead of jetson_id for S3 paths
            # This ensures all angles from all Jetsons go to the same court folder
            court_location = os.getenv('COURT_LOCATION', 'court-a')
            s3_key = video_processor.generate_s3_key(
                court_location, game_date, angle_code, uball_game_id
            ) if upload_service else None

            # ============================================================
            # AWS Batch Processing
            # ============================================================
            from aws_batch_transcode import AWSBatchTranscoder

            try:
                batch_transcoder = AWSBatchTranscoder(bucket=upload_service.bucket_name)
            except Exception as e:
                logger.error(f"Failed to initialize AWS Batch transcoder: {e}")
                results['errors'].append(f"AWS Batch init failed: {str(e)}")
                continue

            raw_s3_key = batch_transcoder.generate_raw_s3_key(
                court_location, game_date, uball_game_id, angle_code
            )

            # Check if chapters are from S3 (need cloud extraction via AWS Batch)
            chapters_from_s3 = any(
                ch.get('source') == 's3' for ch in params['chapters_needed']
            )

            # =======================================================
            # SKIP LOGIC: Check if files already exist in S3
            # =======================================================
            # Generate the final 1080p S3 key for skip check
            uuid_parts = uball_game_id.split('-')[:4]
            game_folder = '-'.join(uuid_parts)
            final_1080p_key = f"{court_location}/{game_date}/{game_folder}/{game_date}_{game_folder}_{angle_code}.mp4"

            # Check if 1080p file already exists (skip processing)
            try:
                import boto3
                from botocore.config import Config as BotoConfig
                boto_config = BotoConfig(retries={'max_attempts': 2, 'mode': 'adaptive'})
                s3_client = boto3.client('s3', config=boto_config, verify=False)
                bucket_name = upload_service.bucket_name

                # Check 1080p output first (if exists, skip everything)
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=final_1080p_key)
                    logger.info(f"[SKIP] 1080p file already exists: s3://{bucket_name}/{final_1080p_key}")

                    # For FL/FR angles, check if video is registered in Uball annotation tool
                    # If not registered, register it now before skipping
                    if angle_code in ['FL', 'FR'] and uball_client:
                        try:
                            existing_videos = uball_client.get_videos_for_game(uball_game_id)
                            uball_angle = 'LEFT' if angle_code == 'FL' else 'RIGHT'

                            already_registered = any(
                                v.get('angle') == uball_angle for v in existing_videos
                            )

                            if not already_registered:
                                logger.info(f"[AUTO-REGISTER] {angle_code} not registered in Uball, registering now...")
                                filename = final_1080p_key.split('/')[-1]

                                reg_result = uball_client.register_video(
                                    game_id=uball_game_id,
                                    s3_key=final_1080p_key,
                                    angle=uball_angle,
                                    filename=filename,
                                    duration=0.0  # Will be updated by frontend
                                )

                                if reg_result:
                                    logger.info(f"[AUTO-REGISTER] SUCCESS: {angle_code} registered for game {uball_game_id}")
                                    results.setdefault('registered_videos', []).append({
                                        'angle': angle_code,
                                        'uball_game_id': uball_game_id,
                                        's3_key': final_1080p_key
                                    })
                                else:
                                    logger.warning(f"[AUTO-REGISTER] FAILED: Could not register {angle_code}")
                            else:
                                logger.info(f"[SKIP] {angle_code} already registered in Uball for game {uball_game_id}")
                        except Exception as reg_err:
                            logger.error(f"[AUTO-REGISTER] Error checking/registering {angle_code}: {reg_err}")

                    results['processed_videos'].append({
                        'angle': angle_code,
                        'session_id': session['id'],
                        'status': 'skipped',
                        'skip_reason': '1080p_exists',
                        's3_key': final_1080p_key
                    })
                    continue  # Skip to next session
                except s3_client.exceptions.ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        raise  # Re-raise non-404 errors
                    # 1080p doesn't exist - check raw 4K next

                # Check if raw 4K file exists (can skip extract, just need Batch transcode)
                raw_4k_exists = False
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=raw_s3_key)
                    raw_4k_exists = True
                    logger.info(f"[SKIP] Raw 4K file already exists: s3://{bucket_name}/{raw_s3_key}")
                except s3_client.exceptions.ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        raise  # Re-raise non-404 errors
                    # Raw 4K doesn't exist - need to extract

            except Exception as e:
                logger.warning(f"[SKIP CHECK] Error checking S3: {e} - proceeding with processing")
                raw_4k_exists = False

            # =======================================================
            # SKIP PATH: Raw 4K exists in S3, submit Batch directly
            # =======================================================
            if raw_4k_exists:
                logger.info(f"[BATCH] Raw 4K exists - submitting Batch transcode directly")
                report_progress('encoding', f'Submitting Batch for {angle_code}...', session_base_progress, angle_code)

                try:
                    batch_job_result = batch_transcoder.submit_transcode_job(
                        input_s3_key=raw_s3_key,
                        output_s3_key=s3_key,
                        game_id=uball_game_id,
                        angle=angle_code
                    )

                    if batch_job_result:
                        batch_job_info = {
                            'job_id': batch_job_result['job_id'],
                            'job_name': batch_job_result.get('job_name', ''),
                            'job_queue': batch_transcoder.job_queue,
                            'angle': angle_code,
                            'game_id': uball_game_id,
                            'raw_s3_key': raw_s3_key,
                            'final_s3_key': s3_key,
                            'session_id': session['id'],
                            'filename': output_filename,
                            'status': 'SUBMITTED',
                            'submitted_by': 'direct_skip'
                        }
                        results['batch_jobs'].append(batch_job_info)

                        firebase_service.add_processed_game(session['id'], {
                            'firebase_game_id': firebase_game_id,
                            'game_number': game_number,
                            'extracted_filename': output_filename,
                            's3_key': s3_key,
                            'batch_job_id': batch_job_result['job_id'],
                            'batch_status': 'pending',
                            'extraction_method': 'skip_extract'
                        })
                        continue  # Skip to next session
                    else:
                        logger.error(f"[BATCH] Direct Batch submit failed for {angle_code}")
                except Exception as e:
                    logger.error(f"[BATCH] Direct Batch submit error: {e}")
                    results['errors'].append(f"Direct Batch submit failed for {angle_code}: {str(e)}")
                    continue

            # =======================================================
            # BATCH-ONLY PATH: Direct extraction + transcoding in one job
            # No intermediate 4K file - outputs directly to 1080p
            # =======================================================
            logger.info(f"[BATCH-ONLY] Submitting direct extract+transcode job for {angle_code}")
            report_progress('extracting', f'Batch extracting+transcoding {angle_code}...', session_base_progress, angle_code)

            try:
                batch_result = batch_transcoder.submit_extract_transcode_job(
                    chapters=params['chapters_needed'],
                    bucket=upload_service.bucket_name,
                    offset_seconds=params['offset_seconds'],
                    duration_seconds=params['duration_seconds'],
                    output_s3_key=s3_key,
                    game_id=uball_game_id,
                    angle=angle_code,
                    add_buffer_seconds=30.0
                )

                if batch_result and batch_result.get('job_id'):
                    logger.info(f"[BATCH-ONLY] Job submitted: {batch_result['job_id']}")

                    batch_job_info = {
                        'job_id': batch_result['job_id'],
                        'job_name': batch_result.get('jobName', ''),
                        'job_queue': batch_result.get('jobQueue', 'unknown'),
                        'angle': angle_code,
                        'game_id': uball_game_id,
                        'raw_s3_key': None,
                        'final_s3_key': s3_key,
                        'session_id': session['id'],
                        'filename': output_filename,
                        'duration': params['duration_seconds'],
                        'status': 'SUBMITTED',
                        'submitted_by': 'batch_only',
                        'pipeline': 'batch-only'
                    }
                    results['batch_jobs'].append(batch_job_info)

                    firebase_service.add_processed_game(session['id'], {
                        'firebase_game_id': firebase_game_id,
                        'game_number': game_number,
                        'extracted_filename': output_filename,
                        's3_key': s3_key,
                        'batch_job_id': batch_result['job_id'],
                        'batch_status': 'pending',
                        'extraction_method': 'batch_only'
                    })
                    continue
                else:
                    logger.error(f"[BATCH-ONLY] Job submission failed for {angle_code}")
                    results['errors'].append(f"Batch-only job submission failed for {angle_code}")
                    continue

            except Exception as e:
                logger.error(f"[BATCH-ONLY] Error submitting job for {angle_code}: {e}")
                results['errors'].append(f"Batch-only error for {angle_code}: {str(e)}")
                continue

        # Success if we processed videos directly OR submitted batch jobs
        batch_jobs_count = len(results.get('batch_jobs', []))
        processed_count = len(results['processed_videos'])
        results['success'] = processed_count > 0 or batch_jobs_count > 0

        # Build detailed status message
        corrupted_count = len(results.get('corrupted_sessions', []))
        error_count = len(results.get('errors', []))

        if results['success']:
            if batch_jobs_count > 0:
                # AWS GPU path - jobs submitted
                batch_angles = ', '.join([j['angle'] for j in results['batch_jobs']])
                if corrupted_count > 0:
                    corrupted_angles = ', '.join([c['angle'] for c in results['corrupted_sessions']])
                    status_msg = f"Submitted {batch_jobs_count} GPU transcode job(s) ({batch_angles}). {corrupted_count} corrupted ({corrupted_angles})"
                    results['status'] = 'batch_partial'
                else:
                    status_msg = f"Submitted {batch_jobs_count} GPU transcode job(s) ({batch_angles})"
                    results['status'] = 'batch_submitted'
                report_progress('batch_submitted', status_msg, 100)
            elif corrupted_count > 0:
                # Partial success with some corrupted files
                corrupted_angles = ', '.join([c['angle'] for c in results['corrupted_sessions']])
                status_msg = f"Processed {processed_count} video(s). {corrupted_count} corrupted ({corrupted_angles})"
                results['status'] = 'partial'
                report_progress('completed', status_msg, 100)
            else:
                status_msg = f"Processed {processed_count} video(s) successfully"
                results['status'] = 'success'
                report_progress('completed', status_msg, 100)
        else:
            if corrupted_count > 0:
                corrupted_angles = ', '.join([c['angle'] for c in results['corrupted_sessions']])
                status_msg = f"Failed: All video files corrupted ({corrupted_angles})"
                results['status'] = 'corrupted'
            else:
                status_msg = 'No videos were processed'
                results['status'] = 'failed'
            report_progress('failed', status_msg, 0)

        results['status_message'] = status_msg
        return results

    except Exception as e:
        logger.error(f"Error processing game videos: {e}")
        import traceback
        traceback.print_exc()
        results['errors'].append(str(e))
        return results


def poll_and_register_batch_jobs(
    batch_jobs: List[Dict[str, Any]],
    uball_client=None,
    poll_interval: int = 30,
    max_wait: int = 1800
) -> Dict[str, Any]:
    """
    Poll AWS Batch jobs and register videos in Uball when they complete.

    This function is called after submitting Batch jobs to:
    1. Wait for jobs to complete (or timeout)
    2. Register successful FL/FR videos in Uball backend

    Args:
        batch_jobs: List of batch job info dicts from process_game_videos()
        uball_client: UballClient instance for registration
        poll_interval: Seconds between status checks (default: 30)
        max_wait: Maximum wait time in seconds (default: 30 minutes)

    Returns:
        Dict with registered, failed, pending job counts
    """
    from aws_batch_transcode import AWSBatchTranscoder
    import time

    if not batch_jobs:
        return {'registered': 0, 'failed': 0, 'pending': 0}

    logger.info(f"[BatchPoller] Polling {len(batch_jobs)} Batch jobs...")

    try:
        batch_transcoder = AWSBatchTranscoder()
    except Exception as e:
        logger.error(f"[BatchPoller] Failed to init transcoder: {e}")
        return {'registered': 0, 'failed': len(batch_jobs), 'pending': 0, 'error': str(e)}

    # Track job status
    pending_jobs = {job['job_id']: job for job in batch_jobs if job.get('job_id')}
    completed_jobs = []
    failed_jobs = []

    start_time = time.time()

    while pending_jobs and (time.time() - start_time) < max_wait:
        for job_id, job_info in list(pending_jobs.items()):
            status = batch_transcoder.get_job_status(job_id)
            current_status = status.get('status', 'UNKNOWN')

            if current_status == 'SUCCEEDED':
                logger.info(f"[BatchPoller] Job {job_id} SUCCEEDED")
                job_info['final_status'] = 'SUCCEEDED'
                completed_jobs.append(job_info)
                del pending_jobs[job_id]

                # Register in Uball if FL or FR angle
                if uball_client and job_info.get('angle') in ['FL', 'FR']:
                    try:
                        uball_angle = 'LEFT' if job_info['angle'] == 'FL' else 'RIGHT'
                        s3_key = job_info.get('final_s3_key', '')
                        filename = job_info.get('filename', s3_key.split('/')[-1])

                        # Get game_id from job_info (set during submission)
                        game_id = job_info.get('game_id', '')

                        # Fallback: try to get from Batch job environment
                        if not game_id:
                            job_details = batch_transcoder.get_job_status(job_id)
                            if job_details:
                                env_vars = job_details.get('container', {}).get('environment', [])
                                for env_var in env_vars:
                                    if env_var.get('name') == 'GAME_ID':
                                        game_id = env_var.get('value', '')
                                        break

                        # Final fallback: parse from S3 path
                        if not game_id:
                            parts = s3_key.split('/')
                            if len(parts) >= 3:
                                game_id = parts[2]  # Partial UUID from path

                        if game_id:
                            result = uball_client.register_video(
                                game_id=game_id,
                                s3_key=s3_key,
                                angle=uball_angle,
                                filename=filename,
                                duration=0.0  # Will be updated by frontend
                            )

                            if result:
                                logger.info(f"[BatchPoller] Registered {job_info['angle']} for game {game_id} in Uball")
                            else:
                                logger.warning(f"[BatchPoller] Failed to register {job_info['angle']} for game {game_id}")
                        else:
                            logger.warning(f"[BatchPoller] No game_id found for job {job_id}")
                    except Exception as e:
                        logger.error(f"[BatchPoller] Registration error: {e}")

            elif current_status == 'FAILED':
                logger.error(f"[BatchPoller] Job {job_id} FAILED: {status.get('statusReason', 'Unknown')}")
                job_info['final_status'] = 'FAILED'
                job_info['failure_reason'] = status.get('statusReason', '')
                failed_jobs.append(job_info)
                del pending_jobs[job_id]

        if pending_jobs:
            logger.info(f"[BatchPoller] {len(pending_jobs)} jobs still pending...")
            time.sleep(poll_interval)

    # Mark remaining as timed out
    for job_info in pending_jobs.values():
        job_info['final_status'] = 'TIMEOUT'
        failed_jobs.append(job_info)

    result = {
        'registered': len(completed_jobs),
        'failed': len(failed_jobs),
        'pending': 0,
        'completed_jobs': completed_jobs,
        'failed_jobs': failed_jobs
    }

    logger.info(f"[BatchPoller] Done: {result['registered']} succeeded, {result['failed']} failed")
    return result

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

    def extract_4k_stream_to_s3(
        self,
        chapters: List[Dict[str, Any]],
        offset_seconds: float,
        duration_seconds: float,
        s3_upload_service,
        s3_key: str,
        add_buffer: float = 30.0
    ) -> Optional[str]:
        """
        Extract a 4K clip with stream copy and pipe directly to S3.
        No temp file on disk - streams FFmpeg output to S3 multipart upload.

        Args:
            chapters: List of chapter files to process
            offset_seconds: Seek position from start of first chapter
            duration_seconds: Length of clip to extract
            s3_upload_service: VideoUploadService instance with S3 client
            s3_key: S3 key (path) to upload to
            add_buffer: Extra seconds to add before/after game (default 30s)

        Returns:
            S3 URI on success, None on failure
        """
        import threading

        if not chapters:
            logger.error("No chapters provided for 4K stream to S3")
            return None

        # Add buffer time (but don't go negative)
        buffered_offset = max(0, offset_seconds - add_buffer)
        buffered_duration = duration_seconds + (2 * add_buffer)

        logger.info(f"Extracting 4K and streaming directly to S3:")
        logger.info(f"  Chapters: {len(chapters)}")
        logger.info(f"  Offset: {self._format_duration(buffered_offset)}")
        logger.info(f"  Duration: {self._format_duration(buffered_duration)}")
        logger.info(f"  S3 Key: {s3_key}")

        s3_client = s3_upload_service.s3_client
        bucket = s3_upload_service.bucket_name

        try:
            # Build FFmpeg command
            if len(chapters) == 1:
                # Single file extraction
                cmd = [
                    'ffmpeg', '-y',
                    '-ss', str(buffered_offset),
                    '-i', chapters[0]['path'],
                    '-t', str(buffered_duration),
                    '-c', 'copy',
                    '-avoid_negative_ts', 'make_zero',
                    '-movflags', 'frag_keyframe+empty_moov',
                    '-f', 'mp4',
                    'pipe:1'
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
                    '-c', 'copy',
                    '-avoid_negative_ts', 'make_zero',
                    '-movflags', 'frag_keyframe+empty_moov',
                    '-f', 'mp4',
                    'pipe:1'
                ]

            logger.info(f"  FFmpeg cmd: {' '.join(cmd)}")

            # Start multipart upload
            mpu = s3_client.create_multipart_upload(
                Bucket=bucket,
                Key=s3_key,
                ContentType='video/mp4'
            )
            upload_id = mpu['UploadId']
            parts = []
            part_number = 1
            PART_SIZE = 25 * 1024 * 1024  # 25 MB parts
            total_bytes = 0

            # Start FFmpeg process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=PART_SIZE
            )

            # Read stderr in background
            stderr_output = []
            def read_stderr():
                stderr_output.append(process.stderr.read().decode('utf-8', errors='replace'))
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stderr_thread.start()

            # Stream to S3
            buffer = b''
            while True:
                chunk = process.stdout.read(PART_SIZE - len(buffer))
                if not chunk:
                    break
                buffer += chunk

                if len(buffer) >= PART_SIZE:
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

            # Upload remaining buffer
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

            # Wait for FFmpeg
            process.stdout.close()
            return_code = process.wait(timeout=1800)
            stderr_thread.join(timeout=5)

            # Clean up concat file if created
            if len(chapters) > 1:
                try:
                    os.unlink(concat_file)
                except:
                    pass

            if return_code != 0:
                stderr_text = stderr_output[0] if stderr_output else "Unknown error"
                logger.error(f"FFmpeg failed: {stderr_text[-500:]}")
                # Abort multipart upload
                s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=s3_key,
                    UploadId=upload_id
                )
                return None

            # Complete multipart upload
            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            s3_uri = f"s3://{bucket}/{s3_key}"
            size_gb = total_bytes / (1024**3)
            logger.info(f"  Streamed 4K to S3: {s3_uri} ({size_gb:.2f} GB)")
            return s3_uri

        except Exception as e:
            logger.error(f"Stream to S3 failed: {e}")
            import traceback
            traceback.print_exc()
            # Try to abort multipart upload
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=s3_key,
                    UploadId=upload_id
                )
            except:
                pass
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


def process_game_videos(
    firebase_game_id: str,
    game_number: int,
    firebase_service,
    upload_service,
    video_processor: VideoProcessor,
    location: str = 'default-location',
    uball_client=None,
    s3_bucket: str = 'uball-videos-production',
    progress_callback=None,
    force_local_transcode: bool = False
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
        'batch_jobs': [],  # AWS Batch transcode jobs (when USE_AWS_GPU_TRANSCODE=true)
        'errors': [],
        'uball_game_id': None
    }

    # Check if AWS GPU transcoding is enabled (can be overridden by force_local_transcode)
    use_aws_gpu = os.getenv('USE_AWS_GPU_TRANSCODE', 'false').lower() == 'true'
    if force_local_transcode:
        use_aws_gpu = False
        logger.info("LOCAL TRANSCODE FORCED - will encode locally and stream to S3")
    elif use_aws_gpu:
        logger.info("AWS GPU transcoding ENABLED - will use stream copy + AWS Batch")
    else:
        logger.info("AWS GPU transcoding disabled - using local CPU encoding")

    report_progress('initializing', 'Loading game data...', 5)

    # Get Uball game ID for S3 folder structure
    uball_game_id = None
    if uball_client:
        uball_game = uball_client.get_game_by_firebase_id(firebase_game_id)
        if uball_game:
            uball_game_id = str(uball_game.get('id', ''))
            results['uball_game_id'] = uball_game_id
            logger.info(f"Found Uball game: {uball_game_id}")
        else:
            logger.warning(f"Uball game not found for Firebase ID: {firebase_game_id}")
            results['errors'].append("Game not synced to Uball - sync first before processing")

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

            logger.info(f"Processing session: {session_name} (angle: {angle_code})")
            report_progress('extracting', f'Extracting {angle_code} video...', session_base_progress, angle_code)

            # Get chapter files (with corruption check)
            chapters = video_processor.get_session_chapters(session_name, check_corruption=True)
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

            s3_key = video_processor.generate_s3_key(
                location, game_date, angle_code, uball_game_id
            ) if upload_service else None

            # ============================================================
            # BRANCHING: AWS GPU vs Local CPU encoding
            # ============================================================
            if use_aws_gpu and upload_service:
                # ===========================================================
                # FAST PATH: 4K stream copy + AWS Batch GPU transcoding
                # ===========================================================
                logger.info(f"[AWS GPU] Using stream copy + Batch for {angle_code}")
                report_progress('extracting', f'Stream copying 4K {angle_code}...', session_base_progress, angle_code)

                # Import AWS Batch transcoder
                from aws_batch_transcode import AWSBatchTranscoder

                try:
                    batch_transcoder = AWSBatchTranscoder(bucket=upload_service.bucket_name)
                except Exception as e:
                    logger.error(f"Failed to initialize AWS Batch transcoder: {e}")
                    results['errors'].append(f"AWS Batch init failed: {str(e)}")
                    continue

                # 1. Extract 4K and stream directly to S3 raw/ prefix
                # No temp file needed - streams FFmpeg output directly to S3
                raw_s3_key = batch_transcoder.generate_raw_s3_key(
                    location, game_date, uball_game_id, angle_code
                )

                logger.info(f"[AWS GPU] Streaming 4K directly to S3 (no temp file)")
                report_progress('extracting', f'Extracting & streaming 4K {angle_code} to S3...', session_base_progress, angle_code)

                raw_s3_uri = video_processor.extract_4k_stream_to_s3(
                    params['chapters_needed'],
                    params['offset_seconds'],
                    params['duration_seconds'],
                    upload_service,
                    raw_s3_key,
                    add_buffer=30.0
                )

                if not raw_s3_uri:
                    results['errors'].append(f"4K stream to S3 failed for {angle_code}")
                    continue

                logger.info(f"[AWS GPU] Streamed 4K to: {raw_s3_uri}")

                # 3. Submit AWS Batch transcode job
                report_progress('batch_submit', f'Submitting Batch job for {angle_code}...', session_base_progress + 30, angle_code)

                try:
                    job = batch_transcoder.submit_transcode_job(
                        input_s3_key=raw_s3_key,
                        output_s3_key=s3_key,
                        game_id=firebase_game_id,
                        angle=angle_code
                    )

                    logger.info(f"[AWS GPU] Batch job submitted: {job['jobId']}")

                    # 4. Track job info for later completion
                    batch_job_info = {
                        'job_id': job['jobId'],
                        'job_name': job['jobName'],
                        'angle': angle_code,
                        'raw_s3_key': raw_s3_key,
                        'final_s3_key': s3_key,
                        'session_id': session['id'],
                        'filename': output_filename,
                        'duration': params['duration_seconds'],
                        'status': 'SUBMITTED'
                    }
                    results['batch_jobs'].append(batch_job_info)

                    # Update recording session with pending Batch job info
                    firebase_service.add_processed_game(session['id'], {
                        'firebase_game_id': firebase_game_id,
                        'game_number': game_number,
                        'extracted_filename': output_filename,
                        's3_key': s3_key,
                        'batch_job_id': job['jobId'],
                        'batch_status': 'pending'
                    })

                except Exception as e:
                    logger.error(f"[AWS GPU] Failed to submit Batch job for {angle_code}: {e}")
                    results['errors'].append(f"Batch job submission failed for {angle_code}: {str(e)}")
                    # Clean up raw file from S3 if job submission failed
                    batch_transcoder.delete_raw_file(raw_s3_key)

                # No local cleanup needed - 4K was streamed directly to S3

            else:
                # ===========================================================
                # SLOW PATH: Existing Jetson CPU encoding (libx264)
                # ===========================================================

                # Extract the clip — streams directly to S3 if upload_service is available
                report_progress('extracting', f'Encoding & streaming {angle_code} to S3...', session_base_progress, angle_code)

                extracted_path = video_processor.extract_game_clip(
                    params['chapters_needed'],
                    params['offset_seconds'],
                    params['duration_seconds'],
                    output_filename,
                    add_buffer=30.0,
                    s3_upload_service=upload_service,
                    s3_key=s3_key
                )

                if not extracted_path:
                    results['errors'].append(f"Extraction failed for {angle_code}")
                    continue

                # When streamed to S3, file is not on disk — get info from S3 instead
                streamed_to_s3 = upload_service and s3_key
                if streamed_to_s3:
                    # Get file size from S3
                    try:
                        s3_head = upload_service.s3_client.head_object(
                            Bucket=upload_service.bucket_name, Key=s3_key
                        )
                        video_info = {
                            'size_bytes': s3_head.get('ContentLength', 0),
                            'duration': params['duration_seconds'],
                        }
                    except Exception as e:
                        logger.warning(f"Could not get S3 object info: {e}")
                        video_info = {'size_bytes': 0, 'duration': params['duration_seconds']}

                    s3_uri = f"s3://{upload_service.bucket_name}/{s3_key}"
                    logger.info(f"Streamed to S3: {s3_uri}")
                else:
                    video_info = video_processor.get_video_info(extracted_path)

                # 4. Upload to S3 (only if NOT already streamed)
                if upload_service:
                    try:
                        upload_progress = session_base_progress + 25

                        if not streamed_to_s3:
                            # Fallback: separate upload (only if streaming was not used)
                            report_progress('uploading', f'Uploading {angle_code} to S3...', upload_progress, angle_code)
                            s3_uri = upload_service.upload_video_with_key(
                                video_path=extracted_path,
                                s3_key=s3_key
                            )
                            logger.info(f"Uploaded to S3: {s3_uri}")

                        report_progress('uploaded', f'{angle_code} uploaded successfully', upload_progress + 10, angle_code)

                        # Update recording session with processed game info
                        firebase_service.add_processed_game(session['id'], {
                            'firebase_game_id': firebase_game_id,
                            'game_number': game_number,
                            'extracted_filename': output_filename,
                            's3_key': s3_key
                        })

                        video_result = {
                            'angle': angle_code,
                            'session_id': session['id'],
                            'filename': output_filename,
                            's3_key': s3_key,
                            's3_uri': s3_uri,
                            'duration': video_info.get('duration'),
                            'size_bytes': video_info.get('size_bytes', 0),
                            'size_mb': round(video_info.get('size_bytes', 0) / (1024 * 1024), 2)
                        }

                        results['processed_videos'].append(video_result)

                        # 5. Register FL/FR videos in Uball Backend
                        if uball_client and angle_code in ['FL', 'FR']:
                            report_progress('registering', f'Registering {angle_code} in Uball...', upload_progress + 15, angle_code)
                            try:
                                uball_result = uball_client.register_game_video(
                                    firebase_game_id=firebase_game_id,
                                    s3_key=s3_key,
                                    angle_code=angle_code,
                                    filename=output_filename,
                                    duration=video_info.get('duration'),
                                    file_size=video_info.get('size_bytes'),
                                    s3_bucket=s3_bucket
                                )

                                if uball_result:
                                    logger.info(f"Registered {angle_code} video in Uball: {uball_result.get('id')}")
                                    results['registered_videos'].append({
                                        'angle': angle_code,
                                        'uball_video_id': uball_result.get('id'),
                                        's3_key': s3_key
                                    })
                                else:
                                    logger.warning(f"Failed to register {angle_code} video in Uball")
                                    results['errors'].append(f"Uball registration failed for {angle_code}")

                            except Exception as e:
                                logger.error(f"Uball registration error for {angle_code}: {e}")
                                results['errors'].append(f"Uball registration error for {angle_code}: {str(e)}")

                        # Clean up local file after upload (skip if streamed to S3)
                        if not streamed_to_s3:
                            try:
                                os.remove(extracted_path)
                            except:
                                pass

                    except Exception as e:
                        logger.error(f"Upload failed for {angle_code}: {e}")
                        results['errors'].append(f"Upload failed for {angle_code}: {str(e)}")
                else:
                    # No upload service, just record local file
                    results['processed_videos'].append({
                        'angle': angle_code,
                        'session_id': session['id'],
                        'filename': output_filename,
                        'local_path': extracted_path,
                        'duration': video_info.get('duration'),
                        'size_mb': round(video_info.get('size_bytes', 0) / (1024 * 1024), 2)
                    })

        # Success if we processed videos directly OR submitted batch jobs
        batch_jobs_count = len(results.get('batch_jobs', []))
        processed_count = len(results['processed_videos'])
        results['success'] = processed_count > 0 or batch_jobs_count > 0

        # Build detailed status message
        corrupted_count = len(results.get('corrupted_sessions', []))
        error_count = len(results.get('errors', []))

        # Add GPU transcode flag to results
        results['gpu_transcode_enabled'] = use_aws_gpu

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

"""
Video Processing Module for Game Extraction.

This module provides functionality to:
- Extract game portions from continuous GoPro recordings
- Handle multi-chapter video files
- Use FFmpeg for video extraction and concatenation

S3 Key Format for game videos:
    {location}/{date}/game{N}-{UUID}/{date}_game{N}_{angle}.mp4

Where:
- {location} = court identifier (e.g., "court-a")
- {date} = game date YYYY-MM-DD
- {N} = game number for that day (1, 2, 3...)
- {UUID} = first 4 segments of Uball game UUID (for uniqueness)
- {angle} = camera angle code (FL, FR, NL, NR)

Example:
    court-a/2026-01-20/game1-95efaeaa-8475-4db4/2026-01-20_game1_FL.mp4
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

    def get_session_chapters(self, session_name: str) -> List[Dict[str, Any]]:
        """
        Get list of chapter files for a recording session.

        Args:
            session_name: Name of the segment session folder

        Returns:
            List of chapter info dicts with path, filename, size, duration
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

                chapters.append({
                    'filename': filename,
                    'path': filepath,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'duration_seconds': duration,
                    'duration_str': self._format_duration(duration) if duration else 'unknown'
                })

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
            ], capture_output=True, text=True, timeout=30)

            if result.returncode == 0 and result.stdout.strip():
                return float(result.stdout.strip())
        except Exception as e:
            logger.warning(f"Could not get duration for {filepath}: {e}")
        return None

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
                - offset_seconds: Seek position from start of recording
                - duration_seconds: Length of game clip
                - chapters_needed: List of chapter files needed
                - start_chapter_index: Index of first chapter
                - end_chapter_index: Index of last chapter
        """
        # Calculate offset from recording start to game start
        offset = (game_start - recording_start).total_seconds()
        if offset < 0:
            # Game started before recording - adjust
            offset = 0
            game_start = recording_start

        # Calculate game duration
        duration = (game_end - game_start).total_seconds()

        # Find which chapters we need
        current_time = 0
        start_chapter_idx = None
        end_chapter_idx = None
        chapters_needed = []

        for i, chapter in enumerate(chapters):
            chapter_duration = chapter.get('duration_seconds', 0)
            if chapter_duration <= 0:
                # Estimate ~15 min per 4GB chapter
                chapter_duration = 900  # 15 minutes

            chapter_end_time = current_time + chapter_duration

            # Check if this chapter contains any part of our game
            game_start_in_recording = offset
            game_end_in_recording = offset + duration

            # Chapter overlaps with game if:
            # chapter_start < game_end AND chapter_end > game_start
            if current_time < game_end_in_recording and chapter_end_time > game_start_in_recording:
                if start_chapter_idx is None:
                    start_chapter_idx = i
                end_chapter_idx = i
                chapters_needed.append(chapter)

            current_time = chapter_end_time

        return {
            'offset_seconds': offset,
            'duration_seconds': duration,
            'offset_str': self._format_duration(offset),
            'duration_str': self._format_duration(duration),
            'chapters_needed': chapters_needed,
            'start_chapter_index': start_chapter_idx,
            'end_chapter_index': end_chapter_idx,
            'total_chapters': len(chapters),
            'chapters_to_process': len(chapters_needed)
        }

    def extract_game_clip(
        self,
        chapters: List[Dict[str, Any]],
        offset_seconds: float,
        duration_seconds: float,
        output_filename: str,
        add_buffer: float = 30.0
    ) -> Optional[str]:
        """
        Extract a game clip from chapter files using FFmpeg.

        Args:
            chapters: List of chapter files to process
            offset_seconds: Seek position from start of first chapter
            duration_seconds: Length of clip to extract
            output_filename: Name for output file
            add_buffer: Extra seconds to add before/after game (default 30s)

        Returns:
            Path to extracted video file, or None on failure
        """
        if not chapters:
            logger.error("No chapters provided for extraction")
            return None

        output_path = os.path.join(self.output_dir, output_filename)

        # Add buffer time (but don't go negative)
        buffered_offset = max(0, offset_seconds - add_buffer)
        buffered_duration = duration_seconds + (2 * add_buffer)

        # Adjust offset since we're starting earlier
        actual_offset = buffered_offset

        try:
            if len(chapters) == 1:
                # Single chapter - direct extraction
                return self._extract_from_single_file(
                    chapters[0]['path'],
                    actual_offset,
                    buffered_duration,
                    output_path
                )
            else:
                # Multiple chapters - concat then extract
                return self._extract_from_multiple_files(
                    [ch['path'] for ch in chapters],
                    actual_offset,
                    buffered_duration,
                    output_path
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
        output_path: str
    ) -> Optional[str]:
        """Extract clip from a single video file."""
        logger.info(f"Extracting from single file: {input_path}")
        logger.info(f"  Offset: {self._format_duration(offset)}, Duration: {self._format_duration(duration)}")

        cmd = [
            'ffmpeg',
            '-y',  # Overwrite output
            '-ss', str(offset),  # Seek position (before -i for fast seek)
            '-i', input_path,
            '-t', str(duration),  # Duration
            '-c', 'copy',  # Copy streams without re-encoding
            '-avoid_negative_ts', 'make_zero',
            output_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

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
        output_path: str
    ) -> Optional[str]:
        """Extract clip from multiple concatenated video files."""
        logger.info(f"Extracting from {len(input_paths)} files")
        logger.info(f"  Offset: {self._format_duration(offset)}, Duration: {self._format_duration(duration)}")

        # Create concat file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            concat_file = f.name
            for path in sorted(input_paths):
                # Escape single quotes in paths
                escaped_path = path.replace("'", "'\\''")
                f.write(f"file '{escaped_path}'\n")

        try:
            cmd = [
                'ffmpeg',
                '-y',
                '-f', 'concat',
                '-safe', '0',
                '-i', concat_file,
                '-ss', str(offset),
                '-t', str(duration),
                '-c', 'copy',
                '-avoid_negative_ts', 'make_zero',
                output_path
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)

            if result.returncode != 0:
                logger.error(f"FFmpeg error: {result.stderr}")
                return None

            if os.path.exists(output_path):
                size_mb = os.path.getsize(output_path) / (1024 * 1024)
                logger.info(f"Extracted: {output_path} ({size_mb:.1f} MB)")
                return output_path

            return None

        finally:
            # Clean up concat file
            try:
                os.unlink(concat_file)
            except:
                pass

    def generate_game_filename(
        self,
        date: str,
        game_number: int,
        angle_code: str
    ) -> str:
        """
        Generate filename for game video.

        Format: {date}_game{N}_{angle}.mp4
        Example: 2025-01-20_game1_FL.mp4
        """
        return f"{date}_game{game_number}_{angle_code}.mp4"

    def generate_s3_key(
        self,
        location: str,
        date: str,
        game_number: int,
        angle_code: str,
        uball_game_id: str = None
    ) -> str:
        """
        Generate S3 key for game video.

        Format: {location}/{date}/game{N}-{UUID}/{date}_game{N}_{angle}.mp4
        Example: court-a/2026-01-20/game1-95efaeaa-8475-4db4/2026-01-20_game1_FL.mp4

        Args:
            location: Court/location identifier
            date: Game date (YYYY-MM-DD)
            game_number: Game number for the day
            angle_code: Camera angle (FL, FR, NL, NR)
            uball_game_id: Uball game UUID for unique folder name
        """
        filename = self.generate_game_filename(date, game_number, angle_code)
        if uball_game_id:
            # Use first 4 segments of UUID for shorter but still unique folder name
            uuid_parts = uball_game_id.split('-')[:4]
            uuid_short = '-'.join(uuid_parts)
            folder = f"game{game_number}-{uuid_short}"
        else:
            folder = f"game{game_number}"
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
            ], capture_output=True, text=True, timeout=30)

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
    s3_bucket: str = 'jetson-videos-uball',
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
        'errors': [],
        'uball_game_id': None
    }

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
        all_sessions = firebase_service.get_recording_sessions(limit=100)
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
            results['errors'].append("No overlapping recording sessions found")
            report_progress('error', 'No overlapping recording sessions found', 0)
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

            # Get chapter files
            chapters = video_processor.get_session_chapters(session_name)
            if not chapters:
                logger.warning(f"No chapters found for session {session_name}")
                results['errors'].append(f"No chapters for session {session_name}")
                continue

            # Calculate extraction parameters
            params = video_processor.calculate_extraction_params(
                game_start, game_end, recording_start, chapters
            )

            logger.info(f"  Offset: {params['offset_str']}, Duration: {params['duration_str']}")
            logger.info(f"  Chapters needed: {params['chapters_to_process']}/{params['total_chapters']}")

            if not params['chapters_needed']:
                logger.warning(f"No chapters needed for this game timeframe")
                continue

            # Generate output filename
            output_filename = video_processor.generate_game_filename(
                game_date, game_number, angle_code
            )

            # Extract the clip
            extracted_path = video_processor.extract_game_clip(
                params['chapters_needed'],
                params['offset_seconds'],
                params['duration_seconds'],
                output_filename,
                add_buffer=30.0  # 30 second buffer
            )

            if not extracted_path:
                results['errors'].append(f"Extraction failed for {angle_code}")
                continue

            # Get video info
            video_info = video_processor.get_video_info(extracted_path)

            # 4. Upload to S3
            if upload_service:
                try:
                    upload_progress = session_base_progress + 25
                    report_progress('uploading', f'Uploading {angle_code} to S3...', upload_progress, angle_code)

                    s3_key = video_processor.generate_s3_key(
                        location, game_date, game_number, angle_code, uball_game_id
                    )

                    # Upload using the service
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

                    # Clean up local file after upload
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

        results['success'] = len(results['processed_videos']) > 0
        if results['success']:
            report_progress('completed', f"Processed {len(results['processed_videos'])} videos successfully", 100)
        else:
            report_progress('failed', 'No videos were processed', 0)
        return results

    except Exception as e:
        logger.error(f"Error processing game videos: {e}")
        import traceback
        traceback.print_exc()
        results['errors'].append(str(e))
        return results

"""
Pipeline Orchestrator - Automated Video Processing Pipeline.

This module orchestrates the complete pipeline:
1. Upload chapters from GoPro to S3 (streaming, no local temp files)
2. Detect games from Firebase based on recording timerange
3. Process each game (extract, upload 4K to S3)
4. Submit AWS Batch jobs for encoding (4K -> 1080p)
5. Delete GoPro SD card files after success

The pipeline is triggered automatically after recording stops.
"""

import os
import threading
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from enum import Enum

from logging_service import get_logger

logger = get_logger('gopro.pipeline_orchestrator')

# Sessions must display as one of these four only (no UNKNOWN in UI).
VALID_ANGLE_CODES = ('FL', 'FR', 'NL', 'NR')


def _normalize_angle_code(angle_code: Optional[str]) -> str:
    """Return angle code as one of FL, FR, NL, NR for consistent UI display."""
    if angle_code and str(angle_code).upper() in ('FL', 'FR', 'NL', 'NR'):
        return str(angle_code).upper()
    return 'UNK'


def _is_valid_angle(angle_code: Optional[str]) -> bool:
    """Check if angle code is a valid camera angle (FL, FR, NL, NR)."""
    if not angle_code:
        return False
    return str(angle_code).upper() in ('FL', 'FR', 'NL', 'NR')


def _session_display_date(session: Dict[str, Any]) -> str:
    """Return session date as MM/DD/YYYY for UI label (e.g. 02/02/2026)."""
    started_at = session.get('startedAt') or ''
    if started_at:
        try:
            dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
            return dt.strftime('%m/%d/%Y')
        except (ValueError, TypeError):
            pass
    # Fallback: parse from segmentSession (e.g. enx..._NR_20260202_193947)
    segment = session.get('segmentSession') or ''
    for part in segment.split('_'):
        if len(part) == 8 and part.isdigit() and part.startswith('20'):
            try:
                y, m, d = int(part[:4]), int(part[4:6]), int(part[6:8])
                return f'{m:02d}/{d:02d}/{y}'
            except (ValueError, TypeError):
                break
    return ''


class SessionPipelineState:
    """State for a single session (one angle) in the pipeline."""

    def __init__(
        self,
        session_id: str,
        segment_session: str,
        angle_code: str,
        interface_id: str,
        session_date: str = '',
        display_label: str = ''
    ):
        self.session_id = session_id
        self.segment_session = segment_session
        self.angle_code = angle_code
        self.interface_id = interface_id
        self.session_date = session_date  # MM/DD/YYYY for UI
        self.display_label = display_label  # e.g. "02/02/2026 NR" for Sessions list
        self.status = 'pending'
        self.chapters_total = 0
        self.chapters_uploaded = 0
        self.bytes_uploaded = 0
        self.s3_prefix = None
        self.error = None


def _make_session_state(session: Dict[str, Any]) -> SessionPipelineState:
    """Build SessionPipelineState from Firebase session doc; includes display_label for UI."""
    angle = _normalize_angle_code(session.get('angleCode'))
    session_date = _session_display_date(session)
    display_label = f'{session_date} {angle}'.strip() if session_date else angle
    return SessionPipelineState(
        session_id=session['id'],
        segment_session=session.get('segmentSession', ''),
        angle_code=angle,
        interface_id=session.get('interfaceId', ''),
        session_date=session_date,
        display_label=display_label
    )


class PipelineStage(str, Enum):
    """Pipeline execution stages."""
    INITIALIZING = 'initializing'
    UPLOADING_CHAPTERS = 'uploading_chapters'
    DETECTING_GAMES = 'detecting_games'
    PROCESSING_GAMES = 'processing_games'
    WAITING_BATCH = 'waiting_batch'
    CLEANUP = 'cleanup'
    COMPLETED = 'completed'
    FAILED = 'failed'


class GameProcessingState:
    """State for a single game being processed."""

    def __init__(
        self,
        firebase_game_id: str,
        game_number: int,
        team_a_name: str = '',
        team_b_name: str = '',
        video_name: str = ''
    ):
        self.firebase_game_id = firebase_game_id
        self.game_number = game_number
        self.team_a_name = team_a_name
        self.team_b_name = team_b_name
        self.video_name = video_name or f'{team_a_name} vs {team_b_name}'.strip() or f'Game {game_number}'
        self.status = 'pending'  # pending, extracting, batch_submitted, completed, failed
        self.angles_processed = {}  # angle_code -> { status, batch_job_id, s3_key }
        self.batch_jobs = []  # List of AWS Batch job IDs
        self.error = None


class PipelineOrchestrator:
    """
    Orchestrates the complete video processing pipeline.

    Usage:
        orchestrator = PipelineOrchestrator(
            jetson_id='jetson-1',
            firebase_service=firebase_service,
            upload_service=upload_service,
            video_processor=video_processor
        )

        # Start pipeline for sessions
        pipeline_id = orchestrator.start_pipeline(sessions, gopro_connections)

        # Check status
        status = orchestrator.get_pipeline_status(pipeline_id)
    """

    def __init__(
        self,
        jetson_id: str,
        firebase_service,
        upload_service,
        video_processor,
        uball_client=None,
        batch_enabled: bool = True
    ):
        self.jetson_id = jetson_id
        self.firebase_service = firebase_service
        self.upload_service = upload_service
        self.video_processor = video_processor
        self.uball_client = uball_client
        self.batch_enabled = batch_enabled

        # Pipeline tracking
        self._pipelines: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def start_pipeline(
        self,
        sessions: List[Dict[str, Any]],
        gopro_connections: Dict[str, str],  # interface_id -> gopro_ip
        auto_delete_sd: bool = True,
        progress_callback: Optional[Callable] = None
    ) -> str:
        """
        Start the automated pipeline for a set of sessions.

        Args:
            sessions: List of Firebase session documents
            gopro_connections: Map of interface_id to GoPro IP address
            auto_delete_sd: Whether to delete GoPro files after success
            progress_callback: Optional callback for progress updates

        Returns:
            pipeline_id: Unique identifier for this pipeline run
        """
        pipeline_id = str(uuid.uuid4())[:8]

        # Calculate total recording timerange
        earliest_start = None
        latest_end = None

        # Use startedAt/endedAt from Firebase (ISO 8601 UTC). All timestamps must be UTC for matching.
        for session in sessions:
            start = session.get('startedAt')
            end = session.get('endedAt')
            if start:
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                if earliest_start is None or start_dt < earliest_start:
                    earliest_start = start_dt
            if end:
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                if latest_end is None or end_dt > latest_end:
                    latest_end = end_dt

        # Filter out sessions with UNK/unknown angles early
        valid_sessions = [s for s in sessions if _is_valid_angle(s.get('angleCode'))]
        skipped_unk_sessions = [s for s in sessions if not _is_valid_angle(s.get('angleCode'))]

        if skipped_unk_sessions:
            skipped_angles = [s.get('angleCode', 'NONE') for s in skipped_unk_sessions]
            logger.info(f"[Pipeline {pipeline_id}] Skipping {len(skipped_unk_sessions)} sessions with unknown angles: {skipped_angles}")

        # Use valid_sessions for the rest of the pipeline
        sessions = valid_sessions

        # Initialize pipeline state
        with self._lock:
            self._pipelines[pipeline_id] = {
                'pipeline_id': pipeline_id,
                'jetson_id': self.jetson_id,
                'status': 'running',
                'stage': PipelineStage.INITIALIZING,
                'stage_message': 'Initializing pipeline...',
                'progress': 0,

                # Sessions (angle_code only FL, FR, NL, NR; display_label = "MM/DD/YYYY ANGLE")
                'sessions': {
                    s['id']: _make_session_state(s).__dict__
                    for s in sessions
                },
                'sessions_total': len(sessions),
                'sessions_skipped_unk': len(skipped_unk_sessions),  # Track skipped UNK sessions
                'sessions_uploaded': 0,

                # Games
                'games': {},
                'games_total': 0,
                'games_completed': 0,

                # Timerange
                'recording_start': earliest_start.isoformat() if earliest_start else None,
                'recording_end': latest_end.isoformat() if latest_end else None,

                # Batch jobs
                'batch_jobs_submitted': 0,
                'batch_jobs_completed': 0,

                # Config
                'auto_delete_sd': auto_delete_sd,
                'gopro_connections': gopro_connections,

                # Timestamps
                'started_at': datetime.now().isoformat(),
                'completed_at': None,

                # Errors
                'errors': []
            }

        # Run pipeline in background thread
        def run_pipeline():
            try:
                self._run_pipeline(pipeline_id, sessions, gopro_connections, progress_callback)
            except Exception as e:
                logger.error(f"[Pipeline {pipeline_id}] Fatal error: {e}")
                import traceback
                traceback.print_exc()
                self._update_pipeline(pipeline_id, {
                    'status': 'failed',
                    'stage': PipelineStage.FAILED,
                    'stage_message': f'Pipeline failed: {str(e)}',
                    'errors': [str(e)]
                })

        threading.Thread(target=run_pipeline, daemon=True).start()

        logger.info(f"[Pipeline {pipeline_id}] Started with {len(sessions)} sessions")
        return pipeline_id

    def _run_pipeline(
        self,
        pipeline_id: str,
        sessions: List[Dict[str, Any]],
        gopro_connections: Dict[str, str],
        progress_callback: Optional[Callable]
    ):
        """Execute the full pipeline."""

        # ==================== PHASE 1: Upload Chapters to S3 ====================
        self._update_pipeline(pipeline_id, {
            'stage': PipelineStage.UPLOADING_CHAPTERS,
            'stage_message': f'Uploading chapters from {len(sessions)} sessions...',
            'progress': 5
        })

        from chapter_upload_service import ChapterUploadService
        chapter_service = ChapterUploadService(
            s3_client=self.upload_service.s3_client,
            bucket_name=self.upload_service.bucket_name
        )

        upload_errors = []

        for i, session in enumerate(sessions):
            session_id = session['id']
            interface_id = session.get('interfaceId', '')
            existing_s3_prefix = session.get('s3Prefix')

            # Skip upload if session already has chapters in S3
            if existing_s3_prefix:
                logger.info(f"[Pipeline {pipeline_id}] Session {session_id} already uploaded to {existing_s3_prefix}, skipping upload")
                self._update_session_state(pipeline_id, session_id, {
                    'status': 'completed',
                    's3_prefix': existing_s3_prefix,
                    'chapters_uploaded': session.get('totalChapters', 0),
                    'skipped': True
                })
                with self._lock:
                    self._pipelines[pipeline_id]['sessions_uploaded'] += 1
                continue

            gopro_ip = gopro_connections.get(interface_id)

            if not gopro_ip:
                error = f"No GoPro IP for interface {interface_id}"
                upload_errors.append(error)
                self._update_session_state(pipeline_id, session_id, {
                    'status': 'failed',
                    'error': error
                })
                continue

            self._update_session_state(pipeline_id, session_id, {
                'status': 'uploading'
            })

            self._update_pipeline(pipeline_id, {
                'stage_message': f'Uploading {_normalize_angle_code(session.get("angleCode"))} ({i+1}/{len(sessions)})...'
            })

            try:
                # Get chapters from GoPro
                all_chapters = chapter_service.get_gopro_media_list(gopro_ip)
                expected_count = session.get('totalChapters', 0)

                # Take the chapters for this session (last N chapters)
                chapters_to_upload = all_chapters[-expected_count:] if expected_count > 0 else all_chapters

                self._update_session_state(pipeline_id, session_id, {
                    'chapters_total': len(chapters_to_upload)
                })

                # Progress callback for this session
                def session_progress(stage, chapter_num, total, bytes_uploaded):
                    self._update_session_state(pipeline_id, session_id, {
                        'chapters_uploaded': chapter_num if stage != 'streaming' else chapter_num - 1,
                        'bytes_uploaded': bytes_uploaded
                    })
                    # Update overall progress
                    base_progress = 5 + int((i / len(sessions)) * 30)
                    session_progress_pct = (chapter_num / total) * 30 / len(sessions) if total > 0 else 0
                    self._update_pipeline(pipeline_id, {
                        'progress': int(base_progress + session_progress_pct)
                    })

                # Upload chapters
                result = chapter_service.upload_session_chapters(
                    session=session,
                    gopro_ip=gopro_ip,
                    chapters=chapters_to_upload,
                    progress_callback=session_progress
                )

                if result['success']:
                    # Update Firebase with s3Prefix
                    self.firebase_service.update_session_s3_prefix(session_id, result['s3_prefix'])

                    self._update_session_state(pipeline_id, session_id, {
                        'status': 'completed',
                        's3_prefix': result['s3_prefix'],
                        'chapters_uploaded': result['chapters_uploaded'],
                        'bytes_uploaded': result['total_bytes']
                    })

                    with self._lock:
                        self._pipelines[pipeline_id]['sessions_uploaded'] += 1

                    logger.info(f"[Pipeline {pipeline_id}] Session {session_id} uploaded to {result['s3_prefix']}")
                else:
                    error = '; '.join(result.get('errors', ['Unknown error']))
                    upload_errors.append(f"{_normalize_angle_code(session.get('angleCode'))}: {error}")
                    self._update_session_state(pipeline_id, session_id, {
                        'status': 'failed',
                        'error': error
                    })

            except Exception as e:
                error = str(e)
                upload_errors.append(f"{_normalize_angle_code(session.get('angleCode'))}: {error}")
                self._update_session_state(pipeline_id, session_id, {
                    'status': 'failed',
                    'error': error
                })
                logger.error(f"[Pipeline {pipeline_id}] Session {session_id} upload failed: {e}")

        if upload_errors:
            with self._lock:
                self._pipelines[pipeline_id]['errors'].extend(upload_errors)

        # ==================== PHASE 2: Detect Games ====================
        self._update_pipeline(pipeline_id, {
            'stage': PipelineStage.DETECTING_GAMES,
            'stage_message': 'Detecting games from Firebase...',
            'progress': 40
        })

        # Get pipeline state for timerange (stored as ISO strings; parse to UTC datetime for query)
        with self._lock:
            pipeline = self._pipelines[pipeline_id]
            recording_start_str = pipeline.get('recording_start')
            recording_end_str = pipeline.get('recording_end')

        games = []
        if recording_start_str and recording_end_str:
            try:
                start_dt = datetime.fromisoformat(recording_start_str.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(recording_end_str.replace('Z', '+00:00'))
                games = self.firebase_service.get_games_in_timerange(start_dt, end_dt)
                logger.info(f"[Pipeline {pipeline_id}] Found {len(games)} games in timerange")
            except Exception as e:
                logger.warning(f"[Pipeline {pipeline_id}] Failed to detect games: {e}")

        if not games:
            # No games found - pipeline complete (chapters uploaded successfully)
            self._update_pipeline(pipeline_id, {
                'status': 'completed',
                'stage': PipelineStage.COMPLETED,
                'stage_message': 'Chapters uploaded. No games found to process.',
                'progress': 100,
                'completed_at': datetime.now().isoformat()
            })
            logger.info(f"[Pipeline {pipeline_id}] Completed - no games to process")
            return

        # Initialize game states
        with self._lock:
            self._pipelines[pipeline_id]['games_total'] = len(games)
            for i, game in enumerate(games):
                game_id = game['id']
                # Extract team names from Firebase game data
                left_team = game.get('leftTeam', {}) or {}
                right_team = game.get('rightTeam', {}) or {}
                team_a_name = left_team.get('name', '') or ''
                team_b_name = right_team.get('name', '') or ''

                self._pipelines[pipeline_id]['games'][game_id] = GameProcessingState(
                    firebase_game_id=game_id,
                    game_number=i + 1,
                    team_a_name=team_a_name,
                    team_b_name=team_b_name
                ).__dict__

        self._update_pipeline(pipeline_id, {
            'stage_message': f'Found {len(games)} games to process',
            'progress': 45
        })

        # ==================== PHASE 3: Process Games ====================
        self._update_pipeline(pipeline_id, {
            'stage': PipelineStage.PROCESSING_GAMES,
            'stage_message': f'Processing 0/{len(games)} games...',
            'progress': 50
        })

        from video_processing import process_game_videos

        processing_errors = []

        for i, game in enumerate(games):
            game_id = game['id']
            game_number = i + 1

            self._update_game_state(pipeline_id, game_id, {
                'status': 'extracting'
            })

            self._update_pipeline(pipeline_id, {
                'stage_message': f'Processing game {game_number}/{len(games)}...',
                'progress': 50 + int((i / len(games)) * 40)
            })

            try:
                # Process game videos
                result = process_game_videos(
                    firebase_game_id=game_id,
                    game_number=game_number,
                    location=self.jetson_id,
                    video_processor=self.video_processor,
                    firebase_service=self.firebase_service,
                    upload_service=self.upload_service,
                    uball_client=self.uball_client
                )

                if result.get('success'):
                    # Track batch jobs if GPU transcoding is enabled
                    batch_jobs = result.get('batch_jobs', [])

                    self._update_game_state(pipeline_id, game_id, {
                        'status': 'batch_submitted' if batch_jobs else 'completed',
                        'batch_jobs': [j.get('job_id') for j in batch_jobs],
                        'batch_jobs_info': batch_jobs,  # Full info for polling/registration
                        'angles_processed': {
                            v.get('angle'): {
                                'status': 'batch_submitted' if batch_jobs else 'completed',
                                's3_key': v.get('s3_key')
                            }
                            for v in result.get('processed_videos', [])
                        }
                    })

                    with self._lock:
                        self._pipelines[pipeline_id]['batch_jobs_submitted'] += len(batch_jobs)
                        if not batch_jobs:
                            self._pipelines[pipeline_id]['games_completed'] += 1

                    logger.info(f"[Pipeline {pipeline_id}] Game {game_number} processed ({len(batch_jobs)} batch jobs)")
                else:
                    error = result.get('error', 'Unknown error')
                    processing_errors.append(f"Game {game_number}: {error}")
                    self._update_game_state(pipeline_id, game_id, {
                        'status': 'failed',
                        'error': error
                    })

            except Exception as e:
                error = str(e)
                processing_errors.append(f"Game {game_number}: {error}")
                self._update_game_state(pipeline_id, game_id, {
                    'status': 'failed',
                    'error': error
                })
                logger.error(f"[Pipeline {pipeline_id}] Game {game_number} processing failed: {e}")

        if processing_errors:
            with self._lock:
                self._pipelines[pipeline_id]['errors'].extend(processing_errors)

        # ==================== PHASE 4: Wait for Batch Jobs (if any) ====================
        with self._lock:
            batch_jobs_count = self._pipelines[pipeline_id]['batch_jobs_submitted']
            # Collect all batch job info for polling
            all_batch_jobs = []
            for game_state in self._pipelines[pipeline_id].get('games', {}).values():
                all_batch_jobs.extend(game_state.get('batch_jobs_info', []))

        if batch_jobs_count > 0:
            self._update_pipeline(pipeline_id, {
                'stage': PipelineStage.WAITING_BATCH,
                'stage_message': f'Waiting for {batch_jobs_count} encoding jobs...',
                'progress': 90
            })

            logger.info(f"[Pipeline {pipeline_id}] Starting background poller for {batch_jobs_count} batch jobs")

            # Start background thread to poll batch jobs and auto-register videos
            if all_batch_jobs and self.uball_client:
                from video_processing import poll_and_register_batch_jobs

                def poll_and_register():
                    try:
                        result = poll_and_register_batch_jobs(
                            batch_jobs=all_batch_jobs,
                            uball_client=self.uball_client,
                            poll_interval=30,
                            max_wait=3600  # 1 hour max wait
                        )
                        logger.info(f"[Pipeline {pipeline_id}] Batch poller complete: {result.get('registered', 0)} registered, {result.get('failed', 0)} failed")
                    except Exception as e:
                        logger.error(f"[Pipeline {pipeline_id}] Batch poller error: {e}")

                threading.Thread(target=poll_and_register, daemon=True).start()
            else:
                logger.info(f"[Pipeline {pipeline_id}] Waiting for {batch_jobs_count} batch jobs (no auto-registration)")

        # ==================== PHASE 5: Cleanup ====================
        with self._lock:
            auto_delete = self._pipelines[pipeline_id].get('auto_delete_sd', False)
            all_games_done = self._pipelines[pipeline_id]['games_completed'] == len(games)
            no_batch_pending = batch_jobs_count == 0

        if auto_delete and all_games_done and no_batch_pending:
            self._update_pipeline(pipeline_id, {
                'stage': PipelineStage.CLEANUP,
                'stage_message': 'Deleting GoPro files...',
                'progress': 95
            })

            # Delete files from all connected GoPros
            for interface_id, gopro_ip in gopro_connections.items():
                try:
                    self._delete_gopro_files(gopro_ip)
                    logger.info(f"[Pipeline {pipeline_id}] Deleted files from GoPro at {gopro_ip}")
                except Exception as e:
                    logger.warning(f"[Pipeline {pipeline_id}] Failed to delete files from {gopro_ip}: {e}")

        # ==================== Complete ====================
        self._update_pipeline(pipeline_id, {
            'status': 'completed' if not processing_errors else 'completed_with_errors',
            'stage': PipelineStage.COMPLETED,
            'stage_message': f'Pipeline complete. {len(games)} games processed.',
            'progress': 100,
            'completed_at': datetime.now().isoformat()
        })

        logger.info(f"[Pipeline {pipeline_id}] Completed successfully")

    def _delete_gopro_files(self, gopro_ip: str):
        """Delete all files from GoPro SD card."""
        import requests

        try:
            response = requests.get(
                f'http://{gopro_ip}:8080/gopro/media/delete/all',
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Deleted all files from GoPro at {gopro_ip}")
        except Exception as e:
            logger.warning(f"Failed to delete files from GoPro at {gopro_ip}: {e}")
            raise

    def _update_pipeline(self, pipeline_id: str, updates: Dict[str, Any]):
        """Update pipeline state."""
        with self._lock:
            if pipeline_id in self._pipelines:
                self._pipelines[pipeline_id].update(updates)

    def _update_session_state(self, pipeline_id: str, session_id: str, updates: Dict[str, Any]):
        """Update session state within pipeline."""
        with self._lock:
            if pipeline_id in self._pipelines:
                sessions = self._pipelines[pipeline_id].get('sessions', {})
                if session_id in sessions:
                    sessions[session_id].update(updates)

    def _update_game_state(self, pipeline_id: str, game_id: str, updates: Dict[str, Any]):
        """Update game state within pipeline."""
        with self._lock:
            if pipeline_id in self._pipelines:
                games = self._pipelines[pipeline_id].get('games', {})
                if game_id in games:
                    games[game_id].update(updates)

    def get_pipeline_status(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a pipeline."""
        with self._lock:
            return self._pipelines.get(pipeline_id)

    def list_pipelines(self, status: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """List pipelines, optionally filtered by status."""
        with self._lock:
            pipelines = list(self._pipelines.values())

        if status:
            pipelines = [p for p in pipelines if p.get('status') == status]

        # Sort by started_at descending
        pipelines.sort(key=lambda p: p.get('started_at', ''), reverse=True)

        return pipelines[:limit]


# Global orchestrator instance (initialized in main.py)
_orchestrator: Optional[PipelineOrchestrator] = None


def get_orchestrator() -> Optional[PipelineOrchestrator]:
    """Get the global pipeline orchestrator instance."""
    return _orchestrator


def init_orchestrator(
    jetson_id: str,
    firebase_service,
    upload_service,
    video_processor,
    uball_client=None,
    batch_enabled: bool = True
) -> PipelineOrchestrator:
    """Initialize the global pipeline orchestrator."""
    global _orchestrator
    _orchestrator = PipelineOrchestrator(
        jetson_id=jetson_id,
        firebase_service=firebase_service,
        upload_service=upload_service,
        video_processor=video_processor,
        uball_client=uball_client,
        batch_enabled=batch_enabled
    )
    return _orchestrator

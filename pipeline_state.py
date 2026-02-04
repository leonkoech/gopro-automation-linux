"""
Pipeline State Management for Chapter Upload Pipeline.

This module provides:
- PipelineState dataclass for tracking pipeline execution
- PipelineStateManager for thread-safe state updates and JSON persistence

State is persisted to /tmp/pipeline_states/{pipeline_id}.json for resume capability.
"""

import os
import json
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum

from logging_service import get_logger

logger = get_logger('gopro.pipeline_state')

# Default state directory
STATE_DIR = '/tmp/pipeline_states'


class UploadStatus(str, Enum):
    """Status for session upload."""
    PENDING = 'pending'
    UPLOADING = 'uploading'
    COMPLETED = 'completed'
    FAILED = 'failed'


class GameStatus(str, Enum):
    """Status for game processing."""
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'
    SKIPPED = 'skipped'


class AngleStatus(str, Enum):
    """Status for individual angle processing."""
    PENDING = 'pending'
    EXTRACTING = 'extracting'
    UPLOADING = 'uploading'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CORRUPTED = 'corrupted'


@dataclass
class SessionUploadState:
    """State for a single session upload."""
    session_id: str
    segment_session: str
    angle_code: str
    status: str = UploadStatus.PENDING
    total_chapters: int = 0
    chapters_uploaded: int = 0
    total_bytes: int = 0
    bytes_uploaded: int = 0
    s3_prefix: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class AngleState:
    """State for a single angle within a game."""
    angle_code: str
    session_id: str
    status: str = AngleStatus.PENDING
    s3_key: Optional[str] = None
    batch_job_id: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class GameState:
    """State for a single game processing."""
    firebase_game_id: str
    uball_game_id: Optional[str] = None
    status: str = GameStatus.PENDING
    angles: Dict[str, dict] = field(default_factory=dict)  # angle_code -> AngleState dict
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class PipelineState:
    """
    Complete state for a pipeline execution.

    Tracks:
    - Session uploads (GoPro -> S3)
    - Game processing (extraction, transcoding)
    - Per-angle status within games
    """
    pipeline_id: str
    jetson_id: str
    status: str = 'initialized'
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')

    # Session uploads
    session_uploads: Dict[str, dict] = field(default_factory=dict)  # session_id -> SessionUploadState dict

    # Game processing
    games: Dict[str, dict] = field(default_factory=dict)  # firebase_game_id -> GameState dict

    # Summary stats
    total_sessions: int = 0
    sessions_completed: int = 0
    total_games: int = 0
    games_completed: int = 0

    # Error tracking
    errors: List[str] = field(default_factory=list)


class PipelineStateManager:
    """
    Thread-safe manager for pipeline state with JSON persistence.

    Usage:
        manager = PipelineStateManager(pipeline_id, jetson_id)
        manager.add_session_upload(session_id, segment_session, angle_code, total_chapters)
        manager.update_session_progress(session_id, chapters_uploaded, bytes_uploaded)
        manager.complete_session_upload(session_id, s3_prefix)

        state = manager.get_state()
    """

    def __init__(self, pipeline_id: str, jetson_id: str, state_dir: str = STATE_DIR):
        """
        Initialize pipeline state manager.

        Args:
            pipeline_id: Unique identifier for this pipeline run
            jetson_id: Jetson device identifier
            state_dir: Directory for state persistence
        """
        self.pipeline_id = pipeline_id
        self.jetson_id = jetson_id
        self.state_dir = state_dir
        self.state_file = os.path.join(state_dir, f'{pipeline_id}.json')
        self._lock = threading.RLock()

        # Ensure state directory exists
        os.makedirs(state_dir, exist_ok=True)

        # Load existing state or create new
        self._state = self._load_or_create_state()

    def _load_or_create_state(self) -> PipelineState:
        """Load existing state from disk or create new."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                logger.info(f"Loaded existing pipeline state: {self.pipeline_id}")
                return PipelineState(**data)
            except Exception as e:
                logger.warning(f"Failed to load state file, creating new: {e}")

        return PipelineState(
            pipeline_id=self.pipeline_id,
            jetson_id=self.jetson_id
        )

    def _save_state(self) -> None:
        """Persist state to disk."""
        try:
            self._state.updated_at = datetime.utcnow().isoformat() + 'Z'
            with open(self.state_file, 'w') as f:
                json.dump(asdict(self._state), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save pipeline state: {e}")

    def get_state(self) -> Dict[str, Any]:
        """Get current state as dictionary."""
        with self._lock:
            return asdict(self._state)

    def get_state_summary(self) -> Dict[str, Any]:
        """Get a summary of current state for API responses."""
        with self._lock:
            return {
                'pipeline_id': self._state.pipeline_id,
                'jetson_id': self._state.jetson_id,
                'status': self._state.status,
                'created_at': self._state.created_at,
                'updated_at': self._state.updated_at,
                'sessions': {
                    'total': self._state.total_sessions,
                    'completed': self._state.sessions_completed,
                    'pending': self._state.total_sessions - self._state.sessions_completed
                },
                'games': {
                    'total': self._state.total_games,
                    'completed': self._state.games_completed,
                    'pending': self._state.total_games - self._state.games_completed
                },
                'errors': self._state.errors[-5:]  # Last 5 errors
            }

    # ==================== Pipeline Status ====================

    def set_status(self, status: str) -> None:
        """Set overall pipeline status."""
        with self._lock:
            self._state.status = status
            self._save_state()

    def add_error(self, error: str) -> None:
        """Add an error to the pipeline."""
        with self._lock:
            self._state.errors.append(error)
            self._save_state()

    # ==================== Session Upload Methods ====================

    def add_session_upload(
        self,
        session_id: str,
        segment_session: str,
        angle_code: str,
        total_chapters: int,
        total_bytes: int = 0
    ) -> None:
        """Add a session to be uploaded."""
        with self._lock:
            self._state.session_uploads[session_id] = asdict(SessionUploadState(
                session_id=session_id,
                segment_session=segment_session,
                angle_code=angle_code,
                total_chapters=total_chapters,
                total_bytes=total_bytes
            ))
            self._state.total_sessions += 1
            self._save_state()

    def start_session_upload(self, session_id: str) -> None:
        """Mark a session upload as started."""
        with self._lock:
            if session_id in self._state.session_uploads:
                self._state.session_uploads[session_id]['status'] = UploadStatus.UPLOADING
                self._state.session_uploads[session_id]['started_at'] = datetime.utcnow().isoformat() + 'Z'
                self._save_state()

    def update_session_progress(
        self,
        session_id: str,
        chapters_uploaded: int = None,
        bytes_uploaded: int = None
    ) -> None:
        """Update upload progress for a session."""
        with self._lock:
            if session_id in self._state.session_uploads:
                if chapters_uploaded is not None:
                    self._state.session_uploads[session_id]['chapters_uploaded'] = chapters_uploaded
                if bytes_uploaded is not None:
                    self._state.session_uploads[session_id]['bytes_uploaded'] = bytes_uploaded
                self._save_state()

    def complete_session_upload(self, session_id: str, s3_prefix: str) -> None:
        """Mark a session upload as completed."""
        with self._lock:
            if session_id in self._state.session_uploads:
                self._state.session_uploads[session_id]['status'] = UploadStatus.COMPLETED
                self._state.session_uploads[session_id]['s3_prefix'] = s3_prefix
                self._state.session_uploads[session_id]['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                self._state.sessions_completed += 1
                self._save_state()

    def fail_session_upload(self, session_id: str, error: str) -> None:
        """Mark a session upload as failed."""
        with self._lock:
            if session_id in self._state.session_uploads:
                self._state.session_uploads[session_id]['status'] = UploadStatus.FAILED
                self._state.session_uploads[session_id]['error'] = error
                self._state.session_uploads[session_id]['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                self._state.errors.append(f"Session {session_id}: {error}")
                self._save_state()

    def get_session_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get state for a specific session."""
        with self._lock:
            return self._state.session_uploads.get(session_id)

    # ==================== Game Processing Methods ====================

    def add_game(self, firebase_game_id: str, uball_game_id: str = None) -> None:
        """Add a game to be processed."""
        with self._lock:
            self._state.games[firebase_game_id] = asdict(GameState(
                firebase_game_id=firebase_game_id,
                uball_game_id=uball_game_id
            ))
            self._state.total_games += 1
            self._save_state()

    def start_game_processing(self, firebase_game_id: str) -> None:
        """Mark a game as started processing."""
        with self._lock:
            if firebase_game_id in self._state.games:
                self._state.games[firebase_game_id]['status'] = GameStatus.PROCESSING
                self._state.games[firebase_game_id]['started_at'] = datetime.utcnow().isoformat() + 'Z'
                self._save_state()

    def add_game_angle(
        self,
        firebase_game_id: str,
        angle_code: str,
        session_id: str
    ) -> None:
        """Add an angle to a game."""
        with self._lock:
            if firebase_game_id in self._state.games:
                self._state.games[firebase_game_id]['angles'][angle_code] = asdict(AngleState(
                    angle_code=angle_code,
                    session_id=session_id
                ))
                self._save_state()

    def update_angle_status(
        self,
        firebase_game_id: str,
        angle_code: str,
        status: str,
        s3_key: str = None,
        batch_job_id: str = None,
        error: str = None
    ) -> None:
        """Update status for a game angle."""
        with self._lock:
            if firebase_game_id in self._state.games:
                angles = self._state.games[firebase_game_id].get('angles', {})
                if angle_code in angles:
                    angles[angle_code]['status'] = status
                    if s3_key:
                        angles[angle_code]['s3_key'] = s3_key
                    if batch_job_id:
                        angles[angle_code]['batch_job_id'] = batch_job_id
                    if error:
                        angles[angle_code]['error'] = error
                    if status in [AngleStatus.COMPLETED, AngleStatus.FAILED, AngleStatus.CORRUPTED]:
                        angles[angle_code]['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                    self._save_state()

    def complete_game(self, firebase_game_id: str) -> None:
        """Mark a game as completed."""
        with self._lock:
            if firebase_game_id in self._state.games:
                self._state.games[firebase_game_id]['status'] = GameStatus.COMPLETED
                self._state.games[firebase_game_id]['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                self._state.games_completed += 1
                self._save_state()

    def fail_game(self, firebase_game_id: str, error: str) -> None:
        """Mark a game as failed."""
        with self._lock:
            if firebase_game_id in self._state.games:
                self._state.games[firebase_game_id]['status'] = GameStatus.FAILED
                self._state.games[firebase_game_id]['error'] = error
                self._state.games[firebase_game_id]['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                self._state.errors.append(f"Game {firebase_game_id}: {error}")
                self._save_state()

    def skip_game(self, firebase_game_id: str, reason: str) -> None:
        """Mark a game as skipped."""
        with self._lock:
            if firebase_game_id in self._state.games:
                self._state.games[firebase_game_id]['status'] = GameStatus.SKIPPED
                self._state.games[firebase_game_id]['error'] = reason
                self._state.games[firebase_game_id]['completed_at'] = datetime.utcnow().isoformat() + 'Z'
                self._save_state()

    def get_game_state(self, firebase_game_id: str) -> Optional[Dict[str, Any]]:
        """Get state for a specific game."""
        with self._lock:
            return self._state.games.get(firebase_game_id)

    # ==================== Cleanup ====================

    def delete_state_file(self) -> None:
        """Delete the state file (cleanup after completion)."""
        try:
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
                logger.info(f"Deleted pipeline state file: {self.state_file}")
        except Exception as e:
            logger.warning(f"Failed to delete state file: {e}")


def get_active_pipelines(state_dir: str = STATE_DIR) -> List[Dict[str, Any]]:
    """
    Get list of active pipeline states.

    Returns:
        List of pipeline state summaries
    """
    pipelines = []

    if not os.path.exists(state_dir):
        return pipelines

    for filename in os.listdir(state_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(state_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                pipelines.append({
                    'pipeline_id': data.get('pipeline_id'),
                    'jetson_id': data.get('jetson_id'),
                    'status': data.get('status'),
                    'created_at': data.get('created_at'),
                    'updated_at': data.get('updated_at'),
                    'total_sessions': data.get('total_sessions', 0),
                    'sessions_completed': data.get('sessions_completed', 0),
                    'total_games': data.get('total_games', 0),
                    'games_completed': data.get('games_completed', 0)
                })
            except Exception as e:
                logger.warning(f"Failed to read pipeline state {filename}: {e}")

    # Sort by created_at descending
    pipelines.sort(key=lambda p: p.get('created_at', ''), reverse=True)

    return pipelines

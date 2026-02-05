"""
Firebase Admin SDK integration for recording session management.

This module provides functionality to:
- Register recording sessions when GoPro recording starts/stops
- Fetch basketball games from Firebase that overlap with recording time ranges
- Manage the recording-sessions collection

Collection: recording-sessions
Document Structure:
{
    jetsonId: string,           // "jetson-1" or "jetson-2"
    cameraName: string,         // From GoPro ap_ssid: "GoPro FL"
    angleCode: string,          // FL, FR, NL, NR
    startedAt: string,          // ISO 8601 UTC timestamp
    endedAt: string | null,     // ISO 8601 UTC timestamp (null while recording)
    segmentSession: string,     // e.g., "enxd43260ef4d38_20250120_140530"
    interfaceId: string,        // USB interface ID
    totalChapters: number,
    totalSizeBytes: number,
    status: "recording" | "stopped" | "processing" | "uploaded",
    processedGames: [{
        firebaseGameId: string,
        gameNumber: number,
        extractedFilename: string,
        s3Key: string,
        uploadedAt: string
    }]
}
"""

import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

import firebase_admin
from firebase_admin import credentials, firestore


class FirebaseService:
    """Firebase Admin SDK wrapper for recording session management."""

    RECORDING_SESSIONS_COLLECTION = 'recording-sessions'
    BASKETBALL_GAMES_COLLECTION = 'basketball-games'

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize Firebase Admin SDK.

        Args:
            credentials_path: Path to Firebase Admin SDK JSON credentials file.
                            If None, uses FIREBASE_CREDENTIALS_PATH env var.
        """
        self.credentials_path = credentials_path or os.getenv('FIREBASE_CREDENTIALS_PATH')
        self.jetson_id = os.getenv('JETSON_ID', 'jetson-1')

        # Parse camera angle map from environment
        camera_angle_map_str = os.getenv('CAMERA_ANGLE_MAP', '{}')
        try:
            self.camera_angle_map = json.loads(camera_angle_map_str)
        except json.JSONDecodeError:
            self.camera_angle_map = {}

        if not self.credentials_path:
            raise ValueError(
                "Firebase credentials path not provided. "
                "Set FIREBASE_CREDENTIALS_PATH environment variable or pass credentials_path."
            )

        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"Firebase credentials file not found: {self.credentials_path}")

        # Initialize Firebase Admin SDK (only once)
        if not firebase_admin._apps:
            cred = credentials.Certificate(self.credentials_path)
            firebase_admin.initialize_app(cred)

        self.db = firestore.client()

    # Only these four session/angle types are supported; no UNKNOWN in UI.
    VALID_ANGLE_CODES = ('FL', 'FR', 'NL', 'NR')

    def _get_angle_code(self, camera_name: str) -> str:
        """
        Extract angle code from camera name. Returns only FL, FR, NL, or NR.

        Args:
            camera_name: GoPro camera name (e.g., "GoPro FL")

        Returns:
            Angle code: one of FL, FR, NL, NR (never UNKNOWN)
        """
        # Check explicit mapping first
        if camera_name in self.camera_angle_map:
            code = self.camera_angle_map[camera_name]
            return code if code in self.VALID_ANGLE_CODES else 'NL'

        # Try to extract from camera name (e.g., "GoPro FL" -> "FL")
        if camera_name and ' ' in camera_name:
            suffix = camera_name.split(' ')[-1].upper()
            if suffix in self.VALID_ANGLE_CODES:
                return suffix

        # Fallback so we never persist UNKNOWN; sessions stay one of 4 types.
        return 'NL'

    def register_recording_start(self, session_data: dict) -> str:
        """
        Create a recording-sessions document when recording starts.

        Args:
            session_data: Dictionary containing:
                - camera_name: GoPro camera name (from ap_ssid)
                - segment_session: Session identifier (e.g., "enxd43260ef4d38_20250120_140530")
                - interface_id: USB interface ID

        Returns:
            Document ID of the created recording session
        """
        camera_name = session_data.get('camera_name', 'Unknown Camera')
        segment_session = session_data.get('segment_session', '')
        interface_id = session_data.get('interface_id', '')

        doc_data = {
            'jetsonId': self.jetson_id,
            'cameraName': camera_name,
            'angleCode': self._get_angle_code(camera_name),
            'startedAt': datetime.utcnow().isoformat() + 'Z',
            'endedAt': None,
            'segmentSession': segment_session,
            'interfaceId': interface_id,
            'totalChapters': 0,
            'totalSizeBytes': 0,
            'status': 'recording',
            'processedGames': []
        }

        doc_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION).add(doc_data)
        # .add() returns a tuple of (update_time, doc_ref)
        return doc_ref[1].id

    def register_recording_stop(self, session_id: str, stop_data: dict) -> None:
        """
        Update a recording-sessions document when recording stops.

        Args:
            session_id: Document ID of the recording session
            stop_data: Dictionary containing:
                - total_chapters: Number of video chapters recorded
                - total_size_bytes: Total size of recorded video in bytes
        """
        doc_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION).document(session_id)

        update_data = {
            'endedAt': datetime.utcnow().isoformat() + 'Z',
            'status': 'stopped',
            'totalChapters': stop_data.get('total_chapters', 0),
            'totalSizeBytes': stop_data.get('total_size_bytes', 0)
        }

        doc_ref.update(update_data)

    def update_session_status(self, session_id: str, status: str) -> None:
        """
        Update the status of a recording session.

        Args:
            session_id: Document ID of the recording session
            status: New status ("recording", "stopped", "processing", "uploaded")
        """
        doc_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION).document(session_id)
        doc_ref.update({'status': status})

    def add_processed_game(self, session_id: str, game_data: dict) -> None:
        """
        Add a processed game to a recording session.

        Args:
            session_id: Document ID of the recording session
            game_data: Dictionary containing:
                - firebase_game_id: Firebase game document ID
                - game_number: Game number in session
                - extracted_filename: Name of extracted video file
                - s3_key: S3 key where video is uploaded
        """
        doc_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION).document(session_id)

        processed_game = {
            'firebaseGameId': game_data.get('firebase_game_id', ''),
            'gameNumber': game_data.get('game_number', 0),
            'extractedFilename': game_data.get('extracted_filename', ''),
            's3Key': game_data.get('s3_key', ''),
            'uploadedAt': datetime.utcnow().isoformat() + 'Z'
        }

        doc_ref.update({
            'processedGames': firestore.ArrayUnion([processed_game])
        })

    def get_games_in_timerange(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """
        Fetch games from basketball-games that overlap with the given time range.

        Games are considered overlapping if:
        - Game started before recording ended AND
        - Game ended after recording started (or game hasn't ended yet)

        Args:
            start: Start of time range (recording start time)
            end: End of time range (recording end time)

        Returns:
            List of game documents that overlap with the time range
        """
        games_ref = self.db.collection(self.BASKETBALL_GAMES_COLLECTION)

        # Convert to ISO format strings for Firestore comparison
        start_iso = start.isoformat() + 'Z' if not start.isoformat().endswith('Z') else start.isoformat()
        end_iso = end.isoformat() + 'Z' if not end.isoformat().endswith('Z') else end.isoformat()

        # Query games that started before our recording ended
        # We'll filter further in Python for games that ended after our recording started
        query = games_ref.where('createdAt', '<=', end_iso)

        games = []
        for doc in query.stream():
            game_data = doc.to_dict()
            game_data['id'] = doc.id

            # Check if game overlaps with our recording
            game_ended_at = game_data.get('endedAt')

            # If game hasn't ended, it overlaps
            if not game_ended_at:
                games.append(game_data)
                continue

            # If game ended after our recording started, it overlaps
            if game_ended_at >= start_iso:
                games.append(game_data)

        return games

    def get_recording_sessions(self, jetson_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recording sessions, optionally filtered by Jetson ID.

        Args:
            jetson_id: Filter by specific Jetson ID (defaults to current Jetson)
            limit: Maximum number of sessions to return

        Returns:
            List of recording session documents
        """
        sessions_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION)

        query = sessions_ref.order_by('startedAt', direction=firestore.Query.DESCENDING).limit(limit)

        if jetson_id:
            query = query.where('jetsonId', '==', jetson_id)

        sessions = []
        for doc in query.stream():
            session_data = doc.to_dict()
            session_data['id'] = doc.id
            sessions.append(session_data)

        return sessions

    def get_recording_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific recording session by ID.

        Args:
            session_id: Document ID of the recording session

        Returns:
            Recording session document or None if not found
        """
        doc_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION).document(session_id)
        doc = doc_ref.get()

        if doc.exists:
            data = doc.to_dict()
            data['id'] = doc.id
            return data

        return None

    def find_session_by_segment(self, segment_session: str) -> Optional[Dict[str, Any]]:
        """
        Find a recording session by its segment session name.

        Args:
            segment_session: Segment session identifier

        Returns:
            Recording session document or None if not found
        """
        sessions_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION)
        query = sessions_ref.where('segmentSession', '==', segment_session).limit(1)

        for doc in query.stream():
            data = doc.to_dict()
            data['id'] = doc.id
            return data

        return None

    # ==================== Basketball Games Methods ====================

    def get_game(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific basketball game by ID.

        Args:
            game_id: Firebase document ID of the game

        Returns:
            Game document or None if not found
        """
        doc_ref = self.db.collection(self.BASKETBALL_GAMES_COLLECTION).document(game_id)
        doc = doc_ref.get()

        if doc.exists:
            data = doc.to_dict()
            data['id'] = doc.id
            return data

        return None

    def list_games(
        self,
        limit: int = 50,
        status: Optional[str] = None,
        date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List basketball games from Firebase.

        Args:
            limit: Maximum number of games to return
            status: Optional filter by status (e.g., "ended", "active")
            date: Optional filter by date (YYYY-MM-DD format)

        Returns:
            List of game documents
        """
        games_ref = self.db.collection(self.BASKETBALL_GAMES_COLLECTION)

        # Start with ordering by createdAt descending
        query = games_ref.order_by('createdAt', direction=firestore.Query.DESCENDING)

        if status:
            query = query.where('status', '==', status)

        query = query.limit(limit)

        games = []
        for doc in query.stream():
            game_data = doc.to_dict()
            game_data['id'] = doc.id

            # Filter by date if specified (done in Python since Firestore doesn't support date extraction)
            if date:
                created_at = game_data.get('createdAt', '')
                if not created_at.startswith(date):
                    continue

            games.append(game_data)

        return games

    def get_games_for_sync(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get games that are ready to be synced to Uball Backend.

        Returns games that:
        - Have ended (have endedAt timestamp)
        - Are not yet synced (no uballGameId field)

        Args:
            limit: Maximum number of games to return

        Returns:
            List of game documents ready for sync
        """
        games_ref = self.db.collection(self.BASKETBALL_GAMES_COLLECTION)

        # Query for games that have ended
        # Note: Firestore doesn't support "field does not exist" queries well,
        # so we'll filter in Python
        query = games_ref.order_by('endedAt', direction=firestore.Query.DESCENDING).limit(limit * 2)

        games = []
        for doc in query.stream():
            game_data = doc.to_dict()

            # Skip if endedAt is missing or null
            if not game_data.get('endedAt'):
                continue

            # Skip if already synced to Uball
            if game_data.get('uballGameId'):
                continue

            game_data['id'] = doc.id
            games.append(game_data)

            if len(games) >= limit:
                break

        return games

    def mark_game_synced(self, game_id: str, uball_game_id: str) -> None:
        """
        Mark a game as synced to Uball Backend.

        Args:
            game_id: Firebase game document ID
            uball_game_id: The game ID in Uball Backend (Supabase)
        """
        doc_ref = self.db.collection(self.BASKETBALL_GAMES_COLLECTION).document(game_id)
        doc_ref.update({
            'uballGameId': uball_game_id,
            'syncedAt': datetime.utcnow().isoformat() + 'Z'
        })

    # ==================== Chapter Upload Pipeline Methods ====================

    def update_session_s3_prefix(self, session_id: str, s3_prefix: str) -> None:
        """
        Set the s3Prefix field on a recording session after chapters are uploaded to S3.

        Args:
            session_id: Document ID of the recording session
            s3_prefix: S3 prefix where chapters are stored (e.g., "raw-chapters/enxd43260ef4d38_20250120_140530/")
        """
        doc_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION).document(session_id)
        doc_ref.update({
            's3Prefix': s3_prefix,
            's3UploadedAt': datetime.utcnow().isoformat() + 'Z'
        })

    def get_sessions_pending_upload(self, jetson_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get recording sessions that need chapter upload to S3.

        Sessions are pending upload if:
        - status is 'stopped' (recording finished, chapters available)
        - s3Prefix is not set (chapters not yet uploaded to S3)

        Args:
            jetson_id: Optional filter by Jetson ID (defaults to current Jetson)

        Returns:
            List of session documents pending upload
        """
        sessions_ref = self.db.collection(self.RECORDING_SESSIONS_COLLECTION)

        # Query for stopped sessions
        query = sessions_ref.where('status', '==', 'stopped')

        if jetson_id:
            query = query.where('jetsonId', '==', jetson_id)

        # Filter in Python for missing s3Prefix (Firestore doesn't support "field not exists" well)
        pending_sessions = []
        for doc in query.stream():
            session_data = doc.to_dict()
            session_data['id'] = doc.id

            # Skip if already has s3Prefix (already uploaded)
            if session_data.get('s3Prefix'):
                continue

            # Skip if no chapters (nothing to upload)
            if session_data.get('totalChapters', 0) == 0:
                continue

            pending_sessions.append(session_data)

        # Sort by startedAt descending (most recent first)
        pending_sessions.sort(key=lambda s: s.get('startedAt', ''), reverse=True)

        return pending_sessions


# Singleton instance
_firebase_service: Optional[FirebaseService] = None


def get_firebase_service() -> Optional[FirebaseService]:
    """
    Get the singleton FirebaseService instance.

    Returns:
        FirebaseService instance or None if initialization fails
    """
    global _firebase_service

    if _firebase_service is not None:
        return _firebase_service

    try:
        _firebase_service = FirebaseService()
        return _firebase_service
    except Exception as e:
        print(f"⚠ Failed to initialize Firebase service: {e}")
        return None

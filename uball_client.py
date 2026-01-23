"""
Uball Backend API Client for game synchronization.

This module provides functionality to:
- Authenticate with Uball Backend
- Create games with Firebase game ID linkage
- Register videos for games

Environment Variables:
    UBALL_BACKEND_URL: Base URL of Uball Backend API
    UBALL_AUTH_EMAIL: Authentication email
    UBALL_AUTH_PASSWORD: Authentication password
"""

import os
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from logging_service import get_logger

logger = get_logger('gopro.uball_client')


class UballClient:
    """Client for interacting with Uball Backend API."""

    def __init__(
        self,
        backend_url: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize Uball Backend client.

        Args:
            backend_url: Base URL of Uball Backend (e.g., "https://api.uball.example.com")
            email: Authentication email
            password: Authentication password
        """
        self.backend_url = (backend_url or os.getenv('UBALL_BACKEND_URL', '')).rstrip('/')
        self.email = email or os.getenv('UBALL_AUTH_EMAIL', '')
        self.password = password or os.getenv('UBALL_AUTH_PASSWORD', '')

        if not self.backend_url:
            raise ValueError("UBALL_BACKEND_URL not configured")
        if not self.email or not self.password:
            raise ValueError("UBALL_AUTH_EMAIL and UBALL_AUTH_PASSWORD must be configured")

        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._user_id: Optional[str] = None

    def _is_token_valid(self) -> bool:
        """Check if current access token is still valid."""
        if not self._access_token or not self._token_expires_at:
            return False
        # Add 60 second buffer before expiry
        return datetime.now() < (self._token_expires_at - timedelta(seconds=60))

    def _authenticate(self) -> bool:
        """
        Authenticate with Uball Backend to obtain access token.

        Uses Backend Auth API: POST /api/auth/login

        Returns:
            True if authentication successful, False otherwise
        """
        try:
            response = requests.post(
                f"{self.backend_url}/api/auth/login",
                headers={
                    "Content-Type": "application/json"
                },
                json={
                    "email": self.email,
                    "password": self.password
                },
                timeout=10
            )

            if response.status_code != 200:
                logger.error(f"[UballClient] Auth failed: {response.status_code} - {response.text}")
                return False

            data = response.json()

            # Extract tokens from response
            self._access_token = data.get('access_token')
            self._refresh_token = data.get('refresh_token')
            self._user_id = data.get('user_id')

            # Calculate expiry time
            expires_in = data.get('expires_in', 3600)  # Default 1 hour
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

            logger.info(f"[UballClient] Authenticated (user: {self._user_id})")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"[UballClient] Auth request failed: {e}")
            return False
        except Exception as e:
            logger.error(f"[UballClient] Auth error: {e}")
            return False

    def _ensure_authenticated(self) -> bool:
        """Ensure we have a valid access token."""
        if self._is_token_valid():
            return True
        return self._authenticate()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    def create_game(self, game_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a game in Uball Backend.

        Args:
            game_data: Game data containing:
                - firebase_game_id: Firebase game document ID
                - date: Game date (YYYY-MM-DD)
                - team1_id: UUID of team 1
                - team2_id: UUID of team 2
                - start_time: Optional start timestamp (ISO 8601)
                - end_time: Optional end timestamp (ISO 8601)
                - team1_score: Optional score for team 1
                - team2_score: Optional score for team 2
                - source: "firebase" (indicates synced from Firebase)

        Returns:
            Created game data with Uball game ID, or None on failure
        """
        if not self._ensure_authenticated():
            logger.error("[UballClient] Failed to authenticate")
            return None

        try:
            # Build request payload
            payload = {
                "date": game_data.get("date"),
                "team1_id": game_data.get("team1_id"),
                "team2_id": game_data.get("team2_id"),
                "firebase_game_id": game_data.get("firebase_game_id"),
                "source": game_data.get("source", "firebase")
            }

            # Add optional fields if present
            if game_data.get("start_time"):
                payload["start_time"] = game_data["start_time"]
            if game_data.get("end_time"):
                payload["end_time"] = game_data["end_time"]
            if game_data.get("team1_score") is not None:
                payload["team1_score"] = game_data["team1_score"]
            if game_data.get("team2_score") is not None:
                payload["team2_score"] = game_data["team2_score"]

            response = requests.post(
                f"{self.backend_url}/api/games",
                json=payload,
                headers=self._get_headers(),
                timeout=15
            )

            if response.status_code in [200, 201]:
                result = response.json()
                logger.info(f"[UballClient] Game created: {result.get('id')}")
                return result
            else:
                logger.error(f"[UballClient] Create game failed: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"[UballClient] Create game request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"[UballClient] Create game error: {e}")
            return None

    def get_game_by_firebase_id(self, firebase_game_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a game by its Firebase game ID.

        Args:
            firebase_game_id: Firebase game document ID

        Returns:
            Game data or None if not found
        """
        if not self._ensure_authenticated():
            return None

        try:
            response = requests.get(
                f"{self.backend_url}/api/games",
                params={"firebase_game_id": firebase_game_id},
                headers=self._get_headers(),
                timeout=10
            )

            if response.status_code == 200:
                games = response.json()
                # Return first matching game
                if isinstance(games, list) and len(games) > 0:
                    return games[0]
                elif isinstance(games, dict) and games.get('games'):
                    return games['games'][0] if games['games'] else None
            return None

        except Exception as e:
            logger.error(f"[UballClient] Get game by Firebase ID failed: {e}")
            return None

    def list_teams(self) -> List[Dict[str, Any]]:
        """
        List all teams from Uball Backend.

        Returns:
            List of team dictionaries with id and name
        """
        if not self._ensure_authenticated():
            return []

        try:
            response = requests.get(
                f"{self.backend_url}/api/teams",
                headers=self._get_headers(),
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                # Handle both list and paginated response formats
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'teams' in data:
                    return data['teams']
                return []
            return []

        except Exception as e:
            logger.error(f"[UballClient] List teams failed: {e}")
            return []

    def health_check(self) -> bool:
        """
        Check if Uball Backend is reachable and authentication works.

        Returns:
            True if backend is healthy and auth works
        """
        try:
            # First check if backend is reachable
            response = requests.get(
                f"{self.backend_url}/health",
                timeout=5
            )
            if response.status_code != 200:
                return False

            # Then verify authentication
            return self._ensure_authenticated()

        except Exception as e:
            logger.error(f"[UballClient] Health check failed: {e}")
            return False

    # ==================== Video Registration Methods ====================

    @staticmethod
    def angle_code_to_uball_angle(angle_code: str) -> Optional[str]:
        """
        Convert GoPro angle code to Uball angle name.

        Only FL and FR are registered in Uball Backend.
        NL and NR are uploaded to S3 but not registered.

        Args:
            angle_code: GoPro angle code (FL, FR, NL, NR)

        Returns:
            Uball angle name (LEFT, RIGHT) or None if not applicable
        """
        mapping = {
            'FL': 'LEFT',   # Far Left -> LEFT
            'FR': 'RIGHT',  # Far Right -> RIGHT
        }
        return mapping.get(angle_code.upper())

    def register_video(
        self,
        game_id: str,
        s3_key: str,
        angle: str,
        filename: str,
        duration: Optional[float] = None,
        file_size: Optional[int] = None,
        s3_bucket: str = 'jetson-videos-uball'
    ) -> Optional[Dict[str, Any]]:
        """
        Register a video in Uball Backend's video_metadata.

        Args:
            game_id: Uball game UUID
            s3_key: S3 object key for the video
            angle: Uball angle name (LEFT or RIGHT)
            filename: Video filename
            duration: Video duration in seconds
            file_size: Video file size in bytes
            s3_bucket: S3 bucket name

        Returns:
            Created video metadata or None on failure
        """
        if not self._ensure_authenticated():
            logger.error("[UballClient] Failed to authenticate for video registration")
            return None

        if angle not in ['LEFT', 'RIGHT']:
            logger.warning(f"[UballClient] Invalid angle '{angle}' - only LEFT/RIGHT supported")
            return None

        try:
            payload = {
                "game_id": game_id,
                "s3_key": s3_key,
                "s3_bucket": s3_bucket,
                "angle": angle,
                "filename": filename,
                "status": "uploaded"
            }

            if duration is not None:
                payload["duration"] = duration
            if file_size is not None:
                payload["file_size"] = file_size

            response = requests.post(
                f"{self.backend_url}/api/videos",
                json=payload,
                headers=self._get_headers(),
                timeout=15
            )

            if response.status_code in [200, 201]:
                result = response.json()
                logger.info(f"[UballClient] Video registered: {result.get('id')} for game {game_id}")
                return result
            else:
                logger.error(f"[UballClient] Register video failed: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"[UballClient] Register video request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"[UballClient] Register video error: {e}")
            return None

    def get_videos_for_game(self, game_id: str) -> List[Dict[str, Any]]:
        """
        Get all registered videos for a game.

        Args:
            game_id: Uball game UUID

        Returns:
            List of video metadata objects
        """
        if not self._ensure_authenticated():
            return []

        try:
            response = requests.get(
                f"{self.backend_url}/api/games/{game_id}/videos",
                headers=self._get_headers(),
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'videos' in data:
                    return data['videos']
            return []

        except Exception as e:
            logger.error(f"[UballClient] Get videos for game failed: {e}")
            return []

    def register_game_video(
        self,
        firebase_game_id: str,
        s3_key: str,
        angle_code: str,
        filename: str,
        duration: Optional[float] = None,
        file_size: Optional[int] = None,
        s3_bucket: str = 'jetson-videos-uball'
    ) -> Optional[Dict[str, Any]]:
        """
        Register a video for a game, looking up game by Firebase ID.

        This is a convenience method that:
        1. Converts angle code (FL/FR) to Uball angle (LEFT/RIGHT)
        2. Looks up the Uball game ID from Firebase game ID
        3. Registers the video

        Args:
            firebase_game_id: Firebase game document ID
            s3_key: S3 object key
            angle_code: GoPro angle code (FL, FR)
            filename: Video filename
            duration: Video duration in seconds
            file_size: Video file size in bytes
            s3_bucket: S3 bucket name

        Returns:
            Created video metadata or None on failure
        """
        # Convert angle code
        uball_angle = self.angle_code_to_uball_angle(angle_code)
        if not uball_angle:
            logger.info(f"[UballClient] Angle {angle_code} not registered in Uball (only FL/FR)")
            return None

        # Look up Uball game ID
        game = self.get_game_by_firebase_id(firebase_game_id)
        if not game:
            logger.error(f"[UballClient] Game not found for Firebase ID: {firebase_game_id}")
            return None

        uball_game_id = str(game.get('id', ''))
        if not uball_game_id:
            logger.error(f"[UballClient] Game has no ID")
            return None

        # Register the video
        return self.register_video(
            game_id=uball_game_id,
            s3_key=s3_key,
            angle=uball_angle,
            filename=filename,
            duration=duration,
            file_size=file_size,
            s3_bucket=s3_bucket
        )


# Singleton instance
_uball_client: Optional[UballClient] = None


def get_uball_client() -> Optional[UballClient]:
    """
    Get the singleton UballClient instance.

    Returns:
        UballClient instance or None if initialization fails
    """
    global _uball_client

    if _uball_client is not None:
        return _uball_client

    try:
        _uball_client = UballClient()
        return _uball_client
    except Exception as e:
        logger.warning(f"Failed to initialize Uball client: {e}")
        return None

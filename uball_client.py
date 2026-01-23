"""
Uball Backend API Client for game synchronization.

This module provides functionality to:
- Authenticate with Uball Backend (Supabase-based)
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
        Authenticate with Uball Backend and obtain access token.

        Returns:
            True if authentication successful, False otherwise
        """
        try:
            response = requests.post(
                f"{self.backend_url}/api/v1/auth/login",
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
            # Supabase returns session with access_token, refresh_token, expires_in
            session = data.get('session', data)
            self._access_token = session.get('access_token')
            self._refresh_token = session.get('refresh_token')

            # Calculate expiry time
            expires_in = session.get('expires_in', 3600)  # Default 1 hour
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

            # Get user ID
            user = data.get('user', {})
            self._user_id = user.get('id')

            logger.info(f"[UballClient] Authenticated successfully (user: {self._user_id})")
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
                f"{self.backend_url}/api/v1/games",
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
                f"{self.backend_url}/api/v1/games",
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
                f"{self.backend_url}/api/v1/teams",
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

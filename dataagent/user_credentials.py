"""
User OAuth credential management for Data Assistant.

In Cloud Run (IAP mode), IAP handles authentication (identity) but doesn't
provide OAuth tokens for data API access. This module manages a secondary
OAuth consent flow where users grant BigQuery/Sheets/Drive/GCS permissions.

Token storage: In-memory only (most secure — tokens never hit disk).
Tokens are lost on container restart; users re-consent as needed.
"""
import os
import logging
import threading
from typing import Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest

logger = logging.getLogger(__name__)

# OAuth scopes for data access
DATA_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/devstorage.read_write",
]

# OAuth client configuration (set via env vars or defaults)
OAUTH_DATA_CLIENT_ID = os.environ.get(
    "OAUTH_DATA_CLIENT_ID", ""
)
OAUTH_DATA_CLIENT_SECRET = os.environ.get(
    "OAUTH_DATA_CLIENT_SECRET", ""
)


class UserCredentialStore:
    """Thread-safe in-memory store for per-user OAuth credentials.
    
    Tokens live only in process memory and are never serialized to disk.
    This is the most secure option: the only risk is process memory access,
    which is isolated per Cloud Run container.
    """

    def __init__(self):
        self._store: dict[str, Credentials] = {}
        self._lock = threading.Lock()

    def store(self, email: str, credentials: Credentials) -> None:
        """Store OAuth credentials for a user."""
        with self._lock:
            self._store[email] = credentials
            logger.info(f"[CredStore] Stored credentials for {email}")

    def get(self, email: str) -> Optional[Credentials]:
        """Get credentials for a user, refreshing if expired."""
        with self._lock:
            creds = self._store.get(email)
            if not creds:
                return None
            # Auto-refresh if expired
            if creds.expired and creds.refresh_token:
                try:
                    creds.refresh(GoogleAuthRequest())
                    logger.info(f"[CredStore] Refreshed credentials for {email}")
                except Exception as e:
                    logger.warning(f"[CredStore] Failed to refresh for {email}: {e}")
                    # Remove stale credentials
                    del self._store[email]
                    return None
            return creds

    def has(self, email: str) -> bool:
        """Check if a user has stored credentials (may be expired but refreshable)."""
        with self._lock:
            return email in self._store

    def remove(self, email: str) -> None:
        """Remove credentials for a user."""
        with self._lock:
            self._store.pop(email, None)
            logger.info(f"[CredStore] Removed credentials for {email}")


# Module-level singleton
credential_store = UserCredentialStore()


def build_oauth_authorize_url(redirect_uri: str, state: str = "") -> str:
    """Build the Google OAuth authorization URL for data access consent."""
    from urllib.parse import urlencode
    params = {
        "client_id": OAUTH_DATA_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(DATA_SCOPES),
        "access_type": "offline",       # Get a refresh token
        "prompt": "consent",            # Always show consent to get refresh token
        "state": state,
    }
    return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"


def exchange_code_for_credentials(code: str, redirect_uri: str) -> Credentials:
    """Exchange an authorization code for OAuth credentials."""
    import requests
    
    token_response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": code,
            "client_id": OAUTH_DATA_CLIENT_ID,
            "client_secret": OAUTH_DATA_CLIENT_SECRET,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
    )
    token_response.raise_for_status()
    token_data = token_response.json()

    credentials = Credentials(
        token=token_data["access_token"],
        refresh_token=token_data.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=OAUTH_DATA_CLIENT_ID,
        client_secret=OAUTH_DATA_CLIENT_SECRET,
        scopes=DATA_SCOPES,
    )
    return credentials


def get_user_bq_client(email: str):
    """Return a BigQuery client using the user's OAuth credentials, or None."""
    creds = credential_store.get(email)
    if not creds:
        return None
    from google.cloud import bigquery
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(credentials=creds, project=project)


def get_user_sheets_service(email: str):
    """Return a Sheets API service using the user's OAuth credentials, or None."""
    creds = credential_store.get(email)
    if not creds:
        return None
    from googleapiclient.discovery import build
    return build("sheets", "v4", credentials=creds)


def get_user_gcs_client(email: str):
    """Return a GCS client using the user's OAuth credentials, or None."""
    creds = credential_store.get(email)
    if not creds:
        return None
    from google.cloud import storage
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    return storage.Client(credentials=creds, project=project)

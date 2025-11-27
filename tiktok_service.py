"""Helper utilities for integrating with TikTok v2 APIs.
These functions intentionally avoid importing the Flask app to prevent circular imports.
"""
from __future__ import annotations

import os
import secrets
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urlencode

import base64
import hashlib
import requests

# TikTok OAuth / API configuration (falls back to env defaults)
TIKTOK_CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY", "")
TIKTOK_CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET", "")
TIKTOK_OAUTH_REDIRECT_URI = os.getenv(
    "TIKTOK_OAUTH_REDIRECT_URI", "http://localhost:5000/oauth/tiktok/callback"
)
TIKTOK_OAUTH_SCOPE = os.getenv(
    "TIKTOK_OAUTH_SCOPE",
    "user.info.basic,video.list,video.upload,video.publish,insights.video.read",
)
REQUIRED_TIKTOK_PUBLISH_SCOPES = {"video.upload", "video.publish"}

# OAuth endpoints
TIKTOK_OAUTH_AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TIKTOK_OAUTH_TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"

# Core API endpoints
TIKTOK_USER_INFO_URL = "https://open.tiktokapis.com/v2/user/info/"
TIKTOK_VIDEO_LIST_URL = "https://open.tiktokapis.com/v2/video/list/"
TIKTOK_VIDEO_QUERY_URL = "https://open.tiktokapis.com/v2/video/query/"
TIKTOK_VIDEO_UPLOAD_URL = "https://open.tiktokapis.com/v2/video/upload/"
TIKTOK_VIDEO_PUBLISH_URL = "https://open.tiktokapis.com/v2/video/publish/"


class TikTokApiError(RuntimeError):
    """Raised when TikTok responds with an error payload."""

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}


def _generate_pkce_pair() -> Tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _scope_set() -> Set[str]:
    """Return the configured TikTok scopes as a normalized set."""

    scopes = set()
    for raw in TIKTOK_OAUTH_SCOPE.split(","):
        scope = raw.strip()
        if scope:
            scopes.add(scope)
    return scopes


def missing_tiktok_publish_scopes() -> Set[str]:
    """Return the subset of required publish scopes that are not configured."""

    scopes = _scope_set()
    return {scope for scope in REQUIRED_TIKTOK_PUBLISH_SCOPES if scope not in scopes}


def tiktok_can_publish() -> bool:
    """True if the configured scope list includes the publish/upload scopes."""

    return not missing_tiktok_publish_scopes()


def build_tiktok_oauth_url(state: Optional[str] = None) -> Tuple[str, str, str]:
    """Return (oauth_url, state, code_verifier) for initiating TikTok OAuth."""

    if not TIKTOK_CLIENT_KEY:
        raise TikTokApiError("Missing TIKTOK_CLIENT_KEY env var")

    if not state:
        state = secrets.token_urlsafe(32)

    code_verifier, code_challenge = _generate_pkce_pair()

    params = {
        "client_key": TIKTOK_CLIENT_KEY,
        "scope": TIKTOK_OAUTH_SCOPE,
        "response_type": "code",
        "redirect_uri": TIKTOK_OAUTH_REDIRECT_URI,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{TIKTOK_OAUTH_AUTH_URL}?{urlencode(params)}", state, code_verifier


def exchange_tiktok_code_for_token(code: str, code_verifier: Optional[str] = None) -> Optional[dict]:
    """Exchange authorization code for an access token."""

    if not TIKTOK_CLIENT_KEY or not TIKTOK_CLIENT_SECRET:
        raise TikTokApiError("TikTok OAuth credentials missing")

    data = {
        "client_key": TIKTOK_CLIENT_KEY,
        "client_secret": TIKTOK_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": TIKTOK_OAUTH_REDIRECT_URI,
    }
    if code_verifier:
        data["code_verifier"] = code_verifier
    try:
        response = requests.post(TIKTOK_OAUTH_TOKEN_URL, data=data, timeout=20)
        response.raise_for_status()
        payload = response.json()
        if payload.get("error_code") not in (None, 0):
            raise TikTokApiError("TikTok token exchange failed", payload)
        return payload.get("data") or payload
    except requests.RequestException as exc:
        raise TikTokApiError("TikTok token exchange error", {"error": str(exc)}) from exc


def get_tiktok_accounts(access_token: str) -> List[dict]:
    """Fetch TikTok user info for the authenticated account."""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    body = {
        "fields": [
            "open_id",
            "union_id",
            "display_name",
            "username",
            "avatar_url",
            "avatar_url_100",
            "bio_description",
        ]
    }

    try:
        response = requests.post(TIKTOK_USER_INFO_URL, headers=headers, json=body, timeout=15)
        response.raise_for_status()
        payload = response.json()
        if payload.get("error_code") not in (None, 0):
            raise TikTokApiError("user.info returned error", payload)

        user_data = (payload.get("data") or {}).get("user")
        if not user_data:
            return []

        user_data.setdefault("id", user_data.get("open_id"))
        return [user_data]
    except requests.RequestException as exc:
        raise TikTokApiError("Failed to fetch TikTok user info", {"error": str(exc)}) from exc


def list_tiktok_posts(open_id: str, access_token: str, max_pages: int = 5) -> List[dict]:
    """Fetch a list of the most recent TikTok videos for the given account."""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    cursor = 0
    videos: List[dict] = []

    for _ in range(max_pages):
        body = {
            "open_id": open_id,
            "max_count": 20,
            "cursor": cursor,
        }
        try:
            resp = requests.post(TIKTOK_VIDEO_LIST_URL, headers=headers, json=body, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("error_code") not in (None, 0):
                raise TikTokApiError("video.list returned error", payload)

            data = payload.get("data") or {}
            batch = data.get("videos", [])
            videos.extend(batch)

            if not data.get("has_more"):
                break
            cursor = data.get("cursor", 0)
        except requests.RequestException as exc:
            raise TikTokApiError("Failed fetching TikTok posts", {"error": str(exc)}) from exc

    return videos


def publish_tiktok_video(open_id: str, caption: str, media_path: str, access_token: str) -> Optional[str]:
    """Upload and publish a TikTok video, returning the TikTok video ID on success."""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Step 1: request an upload endpoint
    try:
        init_resp = requests.post(
            TIKTOK_VIDEO_UPLOAD_URL,
            headers=headers,
            json={"open_id": open_id},
            timeout=20,
        )
        init_resp.raise_for_status()
        init_payload = init_resp.json()
        if init_payload.get("error_code") not in (None, 0):
            raise TikTokApiError("video.upload returned error", init_payload)
    except requests.RequestException as exc:
        raise TikTokApiError("Failed to initialize TikTok upload", {"error": str(exc)}) from exc

    upload_data = init_payload.get("data") or {}
    upload_url = upload_data.get("upload_url")
    video_id = upload_data.get("video_id")
    if not upload_url or not video_id:
        raise TikTokApiError("TikTok upload payload missing upload_url/video_id", upload_data)

    # Step 2: upload the binary
    try:
        with open(media_path, "rb") as video_fp:
            upload_resp = requests.put(
                upload_url,
                data=video_fp,
                headers={"Content-Type": "application/octet-stream"},
                timeout=60,
            )
        upload_resp.raise_for_status()
    except FileNotFoundError:
        raise TikTokApiError("Video file not found for TikTok upload", {"file": media_path})
    except requests.RequestException as exc:
        raise TikTokApiError("TikTok video binary upload failed", {"error": str(exc)}) from exc

    # Step 3: publish the uploaded video
    publish_body = {
        "open_id": open_id,
        "video_id": video_id,
        "post_info": {
            "caption": caption[:2200] if caption else "Posted via Postly",
        },
    }
    try:
        publish_resp = requests.post(
            TIKTOK_VIDEO_PUBLISH_URL,
            headers=headers,
            json=publish_body,
            timeout=30,
        )
        publish_resp.raise_for_status()
        payload = publish_resp.json()
        if payload.get("error_code") not in (None, 0):
            raise TikTokApiError("video.publish returned error", payload)
        data = payload.get("data") or {}
        return data.get("id") or data.get("video_id")
    except requests.RequestException as exc:
        raise TikTokApiError("Failed to publish TikTok video", {"error": str(exc)}) from exc


def fetch_tiktok_post_stats(open_id: str, video_id: str, access_token: str) -> Dict[str, int]:
    """Fetch analytics for a TikTok video."""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    body = {"open_id": open_id, "video_ids": [video_id]}

    try:
        resp = requests.post(TIKTOK_VIDEO_QUERY_URL, headers=headers, json=body, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("error_code") not in (None, 0):
            raise TikTokApiError("video.query returned error", payload)

        videos = (payload.get("data") or {}).get("videos", [])
        if not videos:
            return {}

        stats = videos[0].get("statistics", {})
        return {
            "views": stats.get("view_count", 0),
            "likes": stats.get("like_count", 0),
            "comments": stats.get("comment_count", 0),
            "shares": stats.get("share_count", 0),
            "favorites": stats.get("favorite_count", 0),
        }
    except requests.RequestException as exc:
        raise TikTokApiError("Failed to fetch TikTok video analytics", {"error": str(exc)}) from exc

"""
Reap Video API client.

Provides access to the Reap automation API for clip generation:
upload video files, create clips jobs, poll job status, retrieve clips, and download.
"""

import logging
import os

import requests


DEFAULT_BASE_URL = "https://public.reap.video/api/v1/automation"

_DEFAULT_CLIP_PARAMS = {
    "exportOrientation": "portrait",
    "reframeClips": True,
    "captionsPreset": "system_beasty",
    "genre": "talking",
    "clipDurations": [[30, 60]],
}


class ReapCreditsExhausted(Exception):
    """Raised when Reap API returns a credit/quota exhaustion error."""


class ReapApiClient:
    def __init__(self, api_key: str = None, base_url: str = None):
        self._api_key = api_key or os.environ.get("REAP_API_KEY")
        self._base_url = (base_url or os.environ.get("REAP_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    def _check_credits_error(self, response) -> None:
        """Raise ReapCreditsExhausted if response indicates credit exhaustion."""
        if response.status_code not in (402, 429, 403):
            return
        body_str = response.text.lower()
        credit_keywords = ["credit", "balance", "quota", "billing", "insufficient"]
        if any(kw in body_str for kw in credit_keywords):
            raise ReapCreditsExhausted(f"Reap credits exhausted: {response.text}")

    def get_upload_url(self, filename: str) -> dict:
        """POST /get-upload-url → {uploadUrl, upload_id, ...}"""
        url = f"{self._base_url}/get-upload-url"
        response = requests.post(url, json={"filename": filename}, headers=self._headers)
        self._check_credits_error(response)
        response.raise_for_status()
        data = response.json()
        data.setdefault("upload_id", data.get("uploadId") or data.get("id"))
        return data

    def upload_file(self, upload_url: str, file_path: str) -> None:
        """PUT presigned S3 URL — no auth header on this request."""
        with open(file_path, "rb") as f:
            response = requests.put(upload_url, data=f, headers={"Content-Type": "video/mp4"})
        response.raise_for_status()

    def create_clips_job(self, upload_id: str, **kwargs) -> dict:
        """POST /create-clips → project dict with project_id and status."""
        payload = {**_DEFAULT_CLIP_PARAMS, "uploadId": upload_id, **kwargs}
        url = f"{self._base_url}/create-clips"
        response = requests.post(url, json=payload, headers=self._headers)
        self._check_credits_error(response)
        response.raise_for_status()
        data = response.json()
        data.setdefault("project_id", data.get("projectId") or data.get("id"))
        return data

    def get_project_status(self, project_id: str) -> dict:
        """GET /get-project-status?projectId=... → {projectId, status, ...}"""
        url = f"{self._base_url}/get-project-status"
        response = requests.get(url, params={"projectId": project_id}, headers=self._headers)
        response.raise_for_status()
        return response.json()

    def _normalize_clip(self, clip: dict) -> dict:
        clip.setdefault("clip_id", clip.get("clipId") or clip.get("id"))
        clip.setdefault("clip_url", clip.get("clipUrl") or clip.get("url"))
        clip.setdefault("virality_score", clip.get("viralityScore") or clip.get("virality_score") or 0.0)
        return clip

    def get_project_clips(self, project_id: str) -> list:
        """GET /get-project-clips?projectId=... — handles pagination.

        Returns a flat list of ALL clips across all pages.
        """
        url = f"{self._base_url}/get-project-clips"
        all_clips = []
        page = 1
        page_size = 50

        while True:
            params = {"projectId": project_id, "page": page, "pageSize": page_size}
            response = requests.get(url, params=params, headers=self._headers)
            response.raise_for_status()
            data = response.json()

            clips = data if isinstance(data, list) else data.get("clips", data.get("items", []))
            if not clips:
                break

            all_clips.extend(self._normalize_clip(c) for c in clips)

            next_cursor = data.get("nextCursor") if isinstance(data, dict) else None
            if next_cursor is None and len(clips) < page_size:
                break

            page += 1

        logging.info(f"Retrieved {len(all_clips)} clips for project {project_id}")
        return all_clips

    def download_clip(self, clip_url: str, dest_path: str) -> None:
        """Download clip MP4 to dest_path using streaming requests."""
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        response = requests.get(clip_url, stream=True)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logging.info(f"Downloaded clip to {dest_path}")

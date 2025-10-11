"""
Whisper transcription utility for audio processing.

This module provides functions to transcribe audio files using a Whisper API server.
Designed for use in Airflow DAGs but can be used in any Python project.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


def transcribe_audio(
    audio_file_path: str,
    whisper_api_url: str = "http://whisper-api:9000",
    language: Optional[str] = None,
    task: str = "transcribe",
    timeout: int = 300,
) -> Dict[str, any]:
    """
    Transcribe audio file using Whisper API.

    Args:
        audio_file_path: Path to audio file (supports mp3, wav, webm, m4a, etc.)
        whisper_api_url: Base URL of Whisper API server (default: http://whisper-api:9000)
        language: Language code (e.g., 'es', 'en', 'ca'). Auto-detect if None
        task: 'transcribe' or 'translate' (translate to English)
        timeout: Request timeout in seconds (default: 300s for large files)

    Returns:
        Dictionary with transcription result:
        {
            "success": bool,
            "text": str,  # Full transcription text
            "language": str,  # Detected/specified language
            "segments": list,  # Detailed segments with timestamps
            "duration": float,  # Audio duration in seconds
            "error": str  # Error message if failed
        }

    Raises:
        FileNotFoundError: If audio file doesn't exist
        requests.RequestException: If API call fails

    Example:
        >>> result = transcribe_audio("/app/audio/video_audio.webm")
        >>> if result["success"]:
        ...     print(f"Transcription: {result['text']}")
        ...     print(f"Language: {result['language']}")
    """
    # Validate file exists
    audio_path = Path(audio_file_path)
    if not audio_path.exists():
        error_msg = f"Audio file not found: {audio_file_path}"
        logger.error(error_msg)
        return {
            "success": False,
            "text": None,
            "language": None,
            "segments": None,
            "duration": None,
            "error": error_msg,
        }

    result = {
        "success": False,
        "text": None,
        "language": None,
        "segments": None,
        "duration": None,
        "error": None,
    }

    try:
        logger.info(f"Transcribing audio: {audio_path.name}")
        logger.info(f"File size: {audio_path.stat().st_size / (1024*1024):.2f} MB")
        logger.info(f"Whisper API: {whisper_api_url}")

        # Prepare request
        endpoint = f"{whisper_api_url}/v1/audio/transcriptions"

        # Open file and prepare multipart form data
        with open(audio_file_path, "rb") as audio_file:
            files = {"file": (audio_path.name, audio_file, "audio/webm")}

            data = {
                "response_format": "verbose_json",  # Get detailed response with segments
                "task": task,
            }

            if language:
                data["language"] = language

            # Make API request
            logger.info(f"Sending request to Whisper API...")
            response = requests.post(
                endpoint, files=files, data=data, timeout=timeout
            )

            response.raise_for_status()

        # Parse response
        response_data = response.json()

        result["success"] = True
        result["text"] = response_data.get("text", "").strip()
        result["language"] = response_data.get("language")
        result["segments"] = response_data.get("segments", [])
        result["duration"] = response_data.get("duration")

        logger.info(f"✅ Transcription complete!")
        logger.info(f"   Language: {result['language']}")
        logger.info(f"   Duration: {result['duration']:.2f}s")
        logger.info(f"   Text length: {len(result['text'])} characters")
        logger.info(f"   Segments: {len(result['segments'])}")

    except FileNotFoundError as e:
        result["error"] = f"File error: {str(e)}"
        logger.error(result["error"], exc_info=True)
    except requests.RequestException as e:
        result["error"] = f"API request failed: {str(e)}"
        logger.error(result["error"], exc_info=True)
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result


def transcribe_audio_simple(
    audio_file_path: str,
    whisper_api_url: str = "http://whisper-api:9000",
    language: Optional[str] = None,
) -> str:
    """
    Simplified transcribe function that returns only the text.

    Args:
        audio_file_path: Path to audio file
        whisper_api_url: Base URL of Whisper API server
        language: Language code (e.g., 'es', 'en'). Auto-detect if None

    Returns:
        Transcription text as string (empty string if failed)

    Example:
        >>> text = transcribe_audio_simple("/app/audio/video_audio.webm", language="es")
        >>> print(text)
    """
    result = transcribe_audio(audio_file_path, whisper_api_url, language=language)

    if result["success"]:
        return result["text"]

    logger.error(f"Transcription failed: {result['error']}")
    return ""


def transcribe_and_save(
    audio_file_path: str,
    output_file_path: str,
    whisper_api_url: str = "http://whisper-api:9000",
    language: Optional[str] = None,
    include_timestamps: bool = False,
) -> Dict[str, any]:
    """
    Transcribe audio and save result to a text file.

    Args:
        audio_file_path: Path to audio file
        output_file_path: Path to save transcription text file
        whisper_api_url: Base URL of Whisper API server
        language: Language code. Auto-detect if None
        include_timestamps: If True, include timestamps for each segment

    Returns:
        Dictionary with transcription result and file path

    Example:
        >>> result = transcribe_and_save(
        ...     "/app/audio/video.webm",
        ...     "/app/output/transcription.txt"
        ... )
        >>> print(f"Saved to: {result['output_file']}")
    """
    result = transcribe_audio(audio_file_path, whisper_api_url, language=language)

    if result["success"]:
        try:
            output_path = Path(output_file_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file_path, "w", encoding="utf-8") as f:
                if include_timestamps and result["segments"]:
                    # Write with timestamps
                    for segment in result["segments"]:
                        start = segment.get("start", 0)
                        end = segment.get("end", 0)
                        text = segment.get("text", "").strip()
                        f.write(f"[{start:.2f}s -> {end:.2f}s] {text}\n")
                else:
                    # Write plain text
                    f.write(result["text"])

            result["output_file"] = str(output_path)
            logger.info(f"✅ Transcription saved to: {output_file_path}")

        except Exception as e:
            result["error"] = f"Failed to save file: {str(e)}"
            logger.error(result["error"], exc_info=True)

    return result


def check_whisper_health(whisper_api_url: str = "http://whisper-api:9000") -> bool:
    """
    Check if Whisper API server is running and healthy.

    Args:
        whisper_api_url: Base URL of Whisper API server

    Returns:
        True if server is healthy, False otherwise

    Example:
        >>> if check_whisper_health():
        ...     print("Whisper API is ready!")
    """
    try:
        response = requests.get(f"{whisper_api_url}/health", timeout=5)
        return response.status_code == 200
    except requests.RequestException as e:
        logger.warning(f"Whisper API health check failed: {e}")
        return False

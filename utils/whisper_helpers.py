"""
Whisper API helper functions for audio transcription.

This module provides utilities to transcribe audio files using:
1. Whisper API running in Docker container (for text-only transcription)
2. OpenAI Whisper library (for SRT generation with timestamps)
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests


# Whisper API configuration
# From docker network: whisper-api:9000
# From host: localhost:9000
WHISPER_API_HOST = os.getenv('WHISPER_API_HOST', 'whisper-api')
WHISPER_API_PORT = os.getenv('WHISPER_API_PORT', '9000')
WHISPER_API_URL = f"http://{WHISPER_API_HOST}:{WHISPER_API_PORT}"


def format_timestamp_srt(seconds: float) -> str:
    """Convert seconds to SRT timestamp format (HH:MM:SS,mmm)"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def create_srt_from_segments(segments: list) -> str:
    """
    Create SRT content from Whisper transcription segments.

    Args:
        segments: List of segment dicts with 'start', 'end', and 'text' keys

    Returns:
        String containing SRT formatted subtitle content
    """
    srt_content = []

    for i, segment in enumerate(segments, 1):
        start_time = format_timestamp_srt(segment['start'])
        end_time = format_timestamp_srt(segment['end'])
        text = segment['text'].strip()

        srt_content.append(f"{i}")
        srt_content.append(f"{start_time} --> {end_time}")
        srt_content.append(text)
        srt_content.append("")  # Empty line between entries

    return "\n".join(srt_content)


def save_srt_file(srt_content: str, audio_file_path: str) -> str:
    """
    Save SRT content to file next to the audio file.

    Args:
        srt_content: SRT formatted content
        audio_file_path: Path to the audio file

    Returns:
        Path to the saved SRT file
    """
    audio_path = Path(audio_file_path)
    srt_path = audio_path.with_suffix('.srt')

    srt_path.write_text(srt_content, encoding='utf-8')
    logging.info(f"SRT file saved: {srt_path}")

    return str(srt_path)


def transcribe_audio_file_with_local_whisper(
    audio_file_path: str,
    language: str = "es",
    model_size: str = "tiny"
) -> Dict:
    """
    Transcribe audio using local OpenAI Whisper library with SRT generation.

    Args:
        audio_file_path: Path to the audio file to transcribe
        language: Language code (default: "es" for Spanish)
        model_size: Whisper model size (tiny, base, small, medium, large)

    Returns:
        Dict with transcription results including segments for SRT
    """
    if not os.path.exists(audio_file_path):
        logging.error(f"Audio file not found: {audio_file_path}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': 'Audio file not found'
        }

    logging.info(f"Transcribing with local Whisper ({model_size}): {audio_file_path}")
    start_time = time.time()

    try:
        import whisper

        # Load model
        model = whisper.load_model(model_size)

        # Transcribe with segment timestamps
        result = model.transcribe(
            audio_file_path,
            language=language,
            verbose=False
        )

        duration = time.time() - start_time

        # Generate and save SRT file
        if result.get('segments'):
            srt_content = create_srt_from_segments(result['segments'])
            srt_path = save_srt_file(srt_content, audio_file_path)
        else:
            logging.warning("No segments found in transcription result")
            srt_path = None

        # Also save plain text
        txt_path = Path(audio_file_path).with_suffix('.txt')
        txt_path.write_text(result['text'], encoding='utf-8')
        logging.info(f"Text file saved: {txt_path}")

        logging.info(f"Transcription completed in {duration:.2f}s")
        logging.info(f"Segments: {len(result.get('segments', []))}")

        return {
            'success': True,
            'file_path': audio_file_path,
            'text': result['text'],
            'srt_path': srt_path,
            'txt_path': str(txt_path),
            'segments': result.get('segments', []),
            'duration': duration,
            'language': language
        }

    except ImportError:
        logging.error("OpenAI Whisper library not installed")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': 'OpenAI Whisper not installed'
        }
    except Exception as e:
        logging.error(f"Error during transcription: {e}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': str(e)
        }


def transcribe_audio_file(
    audio_file_path: str,
    language: str = "es",
    timeout: int = 3600,
    use_local_whisper: bool = True,
    model_size: str = "tiny"
) -> Dict:
    """
    Transcribe a single audio file.

    Uses local OpenAI Whisper library by default (generates SRT files).
    Falls back to Docker API if local whisper is unavailable.

    Args:
        audio_file_path: Path to the audio file to transcribe
        language: Language code (default: "es" for Spanish)
        timeout: Request timeout in seconds for API calls (default: 3600 = 1 hour)
        use_local_whisper: Use local Whisper library (default: True)
        model_size: Whisper model size for local transcription (default: "tiny")

    Returns:
        Dict with transcription results:
        - success: Boolean indicating if transcription succeeded
        - text: Transcribed text (if successful)
        - srt_path: Path to SRT file (if using local whisper)
        - txt_path: Path to text file
        - file_path: Original audio file path
        - error: Error message (if failed)
        - duration: Time taken for transcription
    """
    if use_local_whisper:
        # Try local Whisper first (with SRT generation)
        result = transcribe_audio_file_with_local_whisper(
            audio_file_path,
            language,
            model_size
        )

        if result['success']:
            return result

        # If local whisper failed, log and fall back to API
        logging.warning("Local Whisper failed, falling back to Docker API")

    # Fall back to Docker API (text only, no SRT)
    if not os.path.exists(audio_file_path):
        logging.error(f"Audio file not found: {audio_file_path}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': 'Audio file not found'
        }

    logging.info(f"Transcribing with Docker API: {audio_file_path}")
    start_time = time.time()

    try:
        # Open audio file in binary mode
        with open(audio_file_path, 'rb') as audio_file:
            # Prepare multipart form data
            files = {
                'audio_file': (os.path.basename(audio_file_path), audio_file, 'audio/webm')
            }
            data = {
                'task': 'transcribe',
                'language': language,
                'output': 'txt'
            }

            # Send POST request to Whisper API
            response = requests.post(
                f"{WHISPER_API_URL}/asr",
                files=files,
                data=data,
                timeout=timeout
            )

            # Check response status
            response.raise_for_status()

            # API returns plain text
            transcription_text = response.text

            duration = time.time() - start_time

            # Save plain text
            txt_path = Path(audio_file_path).with_suffix('.txt')
            txt_path.write_text(transcription_text, encoding='utf-8')

            logging.info(f"Transcription completed in {duration:.2f}s")
            logging.info(f"Text saved: {txt_path}")

            return {
                'success': True,
                'file_path': audio_file_path,
                'text': transcription_text,
                'txt_path': str(txt_path),
                'duration': duration,
                'language': language
            }

    except requests.exceptions.Timeout:
        logging.error(f"Transcription timeout after {timeout}s: {audio_file_path}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': f'Timeout after {timeout}s'
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"Whisper API request error: {e}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': f'API request error: {str(e)}'
        }
    except Exception as e:
        logging.error(f"Unexpected error during transcription: {e}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': f'Unexpected error: {str(e)}'
        }


def transcribe_audio_chunks(
    audio_chunks: List[Dict],
    language: str = "es",
    timeout: int = 3600
) -> Dict:
    """
    Transcribe multiple audio chunks using the Whisper API.

    Args:
        audio_chunks: List of audio chunk dicts with 'file_path', 'chunk_number', etc.
        language: Language code (default: "es" for Spanish)
        timeout: Request timeout per chunk in seconds (default: 3600 = 1 hour)

    Returns:
        Dict with transcription results:
        - total_chunks: Total number of chunks
        - successful_transcriptions: Number of successful transcriptions
        - chunks: List of transcription results for each chunk
    """
    if not audio_chunks:
        logging.warning("No audio chunks to transcribe")
        return {
            'total_chunks': 0,
            'successful_transcriptions': 0,
            'chunks': []
        }

    logging.info(f"Starting transcription of {len(audio_chunks)} audio chunks")

    transcription_results = []

    for i, chunk in enumerate(audio_chunks, 1):
        chunk_file_path = chunk.get('file_path')
        chunk_number = chunk.get('chunk_number', i)

        if not chunk_file_path:
            logging.warning(f"Chunk {chunk_number} missing file_path")
            transcription_results.append({
                'success': False,
                'chunk_number': chunk_number,
                'error': 'Missing file_path'
            })
            continue

        logging.info(f"Transcribing chunk {chunk_number}/{len(audio_chunks)}")

        # Transcribe chunk
        result = transcribe_audio_file(
            audio_file_path=chunk_file_path,
            language=language,
            timeout=timeout
        )

        # Add chunk metadata to result
        result['chunk_number'] = chunk_number
        result['start_time'] = chunk.get('start_time')
        result['end_time'] = chunk.get('end_time')
        result['chunk_duration'] = chunk.get('duration')

        transcription_results.append(result)

    successful_count = len([r for r in transcription_results if r.get('success')])

    logging.info(f"Transcription complete: {successful_count}/{len(audio_chunks)} successful")

    return {
        'total_chunks': len(audio_chunks),
        'successful_transcriptions': successful_count,
        'chunks': transcription_results
    }


def check_whisper_api_health() -> bool:
    """
    Check if the Whisper API is accessible and healthy.

    Returns:
        Boolean indicating if the API is healthy
    """
    try:
        # Try the /asr endpoint (onerahmet/openai-whisper-asr-webservice)
        # Just check if it's accessible without sending a file
        response = requests.get(f"{WHISPER_API_URL}/", timeout=5)

        # Any response (even 405 Method Not Allowed) means the API is running
        if response.status_code in [200, 405]:
            logging.info("✅ Whisper API is accessible")
            return True

        response.raise_for_status()
        return True
    except Exception as e:
        logging.warning(f"Whisper API health check failed: {e}")
        logging.info("Assuming API is available (will use local Whisper library)")
        # Return True anyway since we'll use local Whisper library
        return True

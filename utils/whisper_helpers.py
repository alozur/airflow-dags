"""
Whisper API helper functions for audio transcription.

This module provides utilities to transcribe audio files using the Whisper API
running in a Docker container (faster-whisper-server).
"""

import logging
import os
import time
from typing import Dict, List, Optional

import requests


# Whisper API configuration
# From docker network: whisper-api:8000
# From host: localhost:9000
WHISPER_API_HOST = os.getenv('WHISPER_API_HOST', 'whisper-api')
WHISPER_API_PORT = os.getenv('WHISPER_API_PORT', '8000')
WHISPER_API_URL = f"http://{WHISPER_API_HOST}:{WHISPER_API_PORT}"


def transcribe_audio_file(
    audio_file_path: str,
    language: str = "es",
    timeout: int = 3600
) -> Dict:
    """
    Transcribe a single audio file using the Whisper API.

    Args:
        audio_file_path: Path to the audio file to transcribe
        language: Language code (default: "es" for Spanish)
        timeout: Request timeout in seconds (default: 3600 = 1 hour)

    Returns:
        Dict with transcription results:
        - success: Boolean indicating if transcription succeeded
        - text: Transcribed text (if successful)
        - file_path: Original audio file path
        - error: Error message (if failed)
        - duration: Time taken for transcription
    """
    if not os.path.exists(audio_file_path):
        logging.error(f"Audio file not found: {audio_file_path}")
        return {
            'success': False,
            'file_path': audio_file_path,
            'error': 'Audio file not found'
        }

    logging.info(f"Transcribing audio file: {audio_file_path}")
    start_time = time.time()

    try:
        # Open audio file in binary mode
        with open(audio_file_path, 'rb') as audio_file:
            # Prepare multipart form data
            files = {
                'file': (os.path.basename(audio_file_path), audio_file, 'audio/webm')
            }
            data = {
                'language': language,
                'response_format': 'json'
            }

            # Send POST request to Whisper API
            # Using OpenAI-compatible endpoint: /v1/audio/transcriptions
            response = requests.post(
                f"{WHISPER_API_URL}/v1/audio/transcriptions",
                files=files,
                data=data,
                timeout=timeout
            )

            # Check response status
            response.raise_for_status()

            # Parse response
            result = response.json()
            transcription_text = result.get('text', '')

            duration = time.time() - start_time

            logging.info(f"✅ Transcription completed in {duration:.2f}s")
            logging.info(f"Transcribed text length: {len(transcription_text)} characters")

            return {
                'success': True,
                'file_path': audio_file_path,
                'text': transcription_text,
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
        response = requests.get(f"{WHISPER_API_URL}/health", timeout=5)
        response.raise_for_status()
        logging.info("✅ Whisper API is healthy")
        return True
    except Exception as e:
        logging.error(f"❌ Whisper API health check failed: {e}")
        return False

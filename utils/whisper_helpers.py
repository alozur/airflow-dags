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
    Save SRT content to srt_files folder within the video directory.

    Structure: .../downloads/{date}/{video_id}/srt_files/{filename}.srt

    Args:
        srt_content: SRT formatted content
        audio_file_path: Path to the audio file
                        Expected: .../downloads/{date}/{video_id}/audio_chunks/{filename}.webm

    Returns:
        Path to the saved SRT file
    """
    try:
        audio_path = Path(audio_file_path)

        # Get the video directory (2 levels up from audio file)
        # .../downloads/{date}/{video_id}/audio_chunks/{filename}.webm
        video_dir = audio_path.parent.parent

        # Create srt_files folder in video directory
        srt_dir = video_dir / "srt_files"
        srt_dir.mkdir(parents=True, exist_ok=True)

        # Get SRT filename (same as audio but .srt extension)
        srt_filename = audio_path.stem + '.srt'
        srt_path = srt_dir / srt_filename

        # Save SRT file
        srt_path.write_text(srt_content, encoding='utf-8')
        logging.info(f"SRT file saved: {srt_path}")

        return str(srt_path)

    except Exception as e:
        logging.error(f"Error saving SRT file: {e}")
        # Fallback: save next to audio file
        audio_path = Path(audio_file_path)
        srt_path = audio_path.with_suffix('.srt')
        srt_path.write_text(srt_content, encoding='utf-8')
        logging.warning(f"SRT saved to fallback location: {srt_path}")
        return str(srt_path)


def transcribe_audio_file_with_local_whisper(
    audio_file_path: str,
    language: str = "es",
    model_size: str = "tiny"
) -> Dict:
    """
    Transcribe audio using local OpenAI Whisper library with SRT generation.

    SRT files are saved to srt_files folder within the video directory:
    .../downloads/{date}/{video_id}/srt_files/{chunk_name}.srt

    Args:
        audio_file_path: Path to the audio file to transcribe
                        Expected: .../downloads/{date}/{video_id}/audio_chunks/{chunk_name}.webm
        language: Language code (default: "es" for Spanish)
        model_size: Whisper model size (tiny, base, small, medium, large)

    Returns:
        Dict with transcription results:
        - success: Boolean
        - text: Full transcription text
        - srt_path: Path to saved SRT file in video's srt_files folder
        - segments: List of segments with timestamps
        - duration: Processing time
        - language: Language code used
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

        # Generate and save SRT file (only SRT, no text file)
        if result.get('segments'):
            srt_content = create_srt_from_segments(result['segments'])
            srt_path = save_srt_file(srt_content, audio_file_path)
        else:
            logging.warning("No segments found in transcription result")
            srt_path = None

        logging.info(f"Transcription completed in {duration:.2f}s")
        logging.info(f"Segments: {len(result.get('segments', []))}")

        return {
            'success': True,
            'file_path': audio_file_path,
            'text': result['text'],
            'srt_path': srt_path,
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
    Falls back to Docker API if local whisper is unavailable (text only, no SRT).

    SRT files are saved to: .../downloads/{date}/{video_id}/srt_files/

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
        - srt_path: Path to SRT file in video's srt_files folder (if using local whisper)
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

            logging.info(f"Transcription completed in {duration:.2f}s (Docker API fallback - no SRT)")
            logging.warning("Docker API used - no SRT file generated (only text)")

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


def merge_srt_files(srt_files: List[str], output_path: str) -> Dict:
    """
    Merge multiple SRT files into one, simplifying the format.

    Removes entry numbers and simplifies timestamps to HH:MM:SS format.

    Args:
        srt_files: List of SRT file paths to merge (should be in order)
        output_path: Path where the merged SRT file should be saved

    Returns:
        Dict with merge results:
        - success: Boolean
        - output_path: Path to merged file
        - total_entries: Number of subtitle entries
        - error: Error message if failed
    """
    result = {
        'success': False,
        'output_path': None,
        'total_entries': 0,
        'error': None
    }

    if not srt_files:
        result['error'] = 'No SRT files provided'
        logging.error(result['error'])
        return result

    try:
        merged_content = []
        entry_count = 0

        for srt_file in sorted(srt_files):
            if not os.path.exists(srt_file):
                logging.warning(f"SRT file not found: {srt_file}")
                continue

            with open(srt_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Parse SRT entries
            # SRT format: number\ntimestamp\ntext\n\n
            entries = content.strip().split('\n\n')

            for entry in entries:
                if not entry.strip():
                    continue

                lines = entry.strip().split('\n')
                if len(lines) < 3:
                    continue

                # Skip the number (line 0)
                # Get timestamp (line 1)
                timestamp = lines[1]
                # Get text (line 2 onwards)
                text = '\n'.join(lines[2:])

                # Simplify timestamp: remove milliseconds
                # From: 00:09:30,440 --> 00:09:38,120
                # To:   00:09:30 --> 00:09:38
                timestamp_simplified = timestamp.replace(',000', '').replace(',', '').split(',')[0]
                if ' --> ' in timestamp:
                    start, end = timestamp.split(' --> ')
                    # Remove milliseconds (everything after comma)
                    start_clean = start.split(',')[0]
                    end_clean = end.split(',')[0]
                    timestamp_simplified = f"{start_clean} --> {end_clean}"

                # Add to merged content (without entry numbers)
                merged_content.append(f"{timestamp_simplified}\n{text}")
                entry_count += 1

        # Join all entries with double newline
        final_content = '\n\n'.join(merged_content)

        # Save merged file
        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)
        output_path_obj.write_text(final_content, encoding='utf-8')

        result['success'] = True
        result['output_path'] = output_path
        result['total_entries'] = entry_count

        logging.info(f"✅ Merged {len(srt_files)} SRT files into {output_path}")
        logging.info(f"   Total subtitle entries: {entry_count}")

    except Exception as e:
        result['error'] = f"Error merging SRT files: {str(e)}"
        logging.error(result['error'], exc_info=True)

    return result


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

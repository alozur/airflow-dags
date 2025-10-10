"""
YouTube video transcription using OpenAI Whisper API.

This module provides functions to transcribe audio files with timestamps,
enabling video segmentation and content analysis.
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def transcribe_audio_with_timestamps(
    audio_file_path: str,
    language: str = "es",
    response_format: str = "verbose_json",
) -> Dict:
    """
    Transcribe audio file using OpenAI Whisper API with word-level timestamps.

    Args:
        audio_file_path: Path to audio file (mp3, mp4, wav, etc.)
        language: Language code (default: "es" for Spanish)
        response_format: Response format ("verbose_json" for timestamps, "text" for plain text)

    Returns:
        Dict with transcription results:
        {
            "success": bool,
            "text": str,  # Full transcript
            "segments": [  # Time-stamped segments
                {
                    "id": int,
                    "start": float,  # Start time in seconds
                    "end": float,    # End time in seconds
                    "text": str,     # Segment text
                },
                ...
            ],
            "words": [  # Word-level timestamps (if available)
                {
                    "word": str,
                    "start": float,
                    "end": float,
                },
                ...
            ],
            "language": str,
            "duration": float,
            "error": str (if failed)
        }

    Example:
        >>> result = transcribe_audio_with_timestamps("audio.mp3")
        >>> if result["success"]:
        >>>     for segment in result["segments"]:
        >>>         print(f"{segment['start']:.2f}s - {segment['end']:.2f}s: {segment['text']}")
    """
    try:
        import openai
        from openai import OpenAI
    except ImportError:
        return {
            "success": False,
            "error": "OpenAI library not installed. Install with: pip install openai"
        }

    # Verify file exists
    if not os.path.exists(audio_file_path):
        return {
            "success": False,
            "error": f"Audio file not found: {audio_file_path}"
        }

    # Get OpenAI API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        return {
            "success": False,
            "error": "OPENAI_API_KEY environment variable not set"
        }

    result = {
        "success": False,
        "text": None,
        "segments": [],
        "words": [],
        "language": language,
        "duration": None,
        "error": None
    }

    try:
        client = OpenAI(api_key=api_key)

        logger.info(f"Transcribing audio: {audio_file_path}")
        logger.info(f"Language: {language}, Format: {response_format}")

        # Open audio file and send to Whisper API
        with open(audio_file_path, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language=language,
                response_format=response_format,
                timestamp_granularities=["segment", "word"] if response_format == "verbose_json" else None
            )

        # Process response based on format
        if response_format == "verbose_json":
            result["success"] = True
            result["text"] = transcript.text
            result["language"] = transcript.language
            result["duration"] = transcript.duration if hasattr(transcript, 'duration') else None

            # Extract segments with timestamps
            if hasattr(transcript, 'segments') and transcript.segments:
                result["segments"] = [
                    {
                        "id": seg.id,
                        "start": seg.start,
                        "end": seg.end,
                        "text": seg.text.strip(),
                    }
                    for seg in transcript.segments
                ]
                logger.info(f"Extracted {len(result['segments'])} segments")

            # Extract word-level timestamps (if available)
            if hasattr(transcript, 'words') and transcript.words:
                result["words"] = [
                    {
                        "word": word.word,
                        "start": word.start,
                        "end": word.end,
                    }
                    for word in transcript.words
                ]
                logger.info(f"Extracted {len(result['words'])} word timestamps")

        else:
            # Plain text response
            result["success"] = True
            result["text"] = transcript

        logger.info(f"✅ Transcription complete: {len(result['text'])} characters")
        return result

    except Exception as e:
        error_msg = f"Error transcribing audio: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result["error"] = error_msg
        return result


def transcribe_video_audio(video_details, target_date: str):
    """
    Transcribe audio from extracted YouTube videos.

    This function integrates with extract_audio_from_youtube results
    to automatically transcribe all extracted audio files.

    Args:
        video_details: Results from extract_audio_from_youtube
        target_date: Target date in YYYY-MM-DD format

    Returns:
        Dict with transcription results for each video:
        {
            "total_transcribed": int,
            "successful_transcriptions": int,
            "failed_transcriptions": int,
            "videos": [
                {
                    "video_id": str,
                    "audio_file_path": str,
                    "transcript": str,
                    "segments": [...],
                    "words": [...],
                    "transcription_success": bool,
                    "error": str (if failed)
                },
                ...
            ]
        }
    """
    if not video_details or not video_details.get('videos'):
        logger.warning("No video details to transcribe")
        return {
            'total_transcribed': 0,
            'successful_transcriptions': 0,
            'failed_transcriptions': 0,
            'videos': []
        }

    transcription_results = {
        'total_transcribed': 0,
        'successful_transcriptions': 0,
        'failed_transcriptions': 0,
        'videos': []
    }

    for video in video_details['videos']:
        video_id = video.get('video_id')
        audio_file_path = video.get('audio_file_path')

        if not audio_file_path:
            logger.warning(f"No audio file path for {video_id}")
            continue

        transcription_results['total_transcribed'] += 1

        logger.info(f"Transcribing audio for video {video_id}")

        # Transcribe with timestamps
        transcript_result = transcribe_audio_with_timestamps(
            audio_file_path=audio_file_path,
            language="es"  # Spanish for Congress videos
        )

        video_transcript = {
            'video_id': video_id,
            'video_title': video.get('video_title'),
            'target_date': target_date,
            'audio_file_path': audio_file_path,
            'transcription_success': transcript_result.get('success', False),
        }

        if transcript_result['success']:
            video_transcript.update({
                'transcript': transcript_result['text'],
                'segments': transcript_result['segments'],
                'words': transcript_result.get('words', []),
                'language': transcript_result['language'],
                'duration': transcript_result.get('duration'),
                'segment_count': len(transcript_result['segments']),
                'word_count': len(transcript_result.get('words', [])),
            })
            transcription_results['successful_transcriptions'] += 1
            logger.info(f"✅ Transcribed {video_id}: {len(transcript_result['segments'])} segments, {len(transcript_result['text'])} chars")
        else:
            video_transcript['error'] = transcript_result.get('error', 'Unknown error')
            transcription_results['failed_transcriptions'] += 1
            logger.error(f"Failed to transcribe {video_id}: {video_transcript['error']}")

        transcription_results['videos'].append(video_transcript)

    logger.info(
        f"Transcription complete: {transcription_results['successful_transcriptions']}/{transcription_results['total_transcribed']} successful"
    )
    return transcription_results


def find_segments_by_keyword(transcription_results, keywords: List[str], context_seconds: float = 5.0) -> Dict:
    """
    Find video segments containing specific keywords with surrounding context.

    Useful for identifying moments to cut or highlight in the video.

    Args:
        transcription_results: Results from transcribe_video_audio
        keywords: List of keywords to search for
        context_seconds: Seconds of context before/after keyword (default: 5.0)

    Returns:
        Dict with matching segments:
        {
            "total_matches": int,
            "videos": [
                {
                    "video_id": str,
                    "matches": [
                        {
                            "keyword": str,
                            "segment_id": int,
                            "start_time": float,
                            "end_time": float,
                            "text": str,
                            "context_start": float,  # start - context_seconds
                            "context_end": float,    # end + context_seconds
                        },
                        ...
                    ]
                },
                ...
            ]
        }
    """
    results = {
        "total_matches": 0,
        "videos": []
    }

    if not transcription_results or not transcription_results.get('videos'):
        return results

    # Normalize keywords for case-insensitive search
    keywords_lower = [k.lower() for k in keywords]

    for video in transcription_results['videos']:
        if not video.get('segments'):
            continue

        video_id = video['video_id']
        matches = []

        for segment in video['segments']:
            segment_text_lower = segment['text'].lower()

            # Check if any keyword appears in this segment
            for keyword in keywords_lower:
                if keyword in segment_text_lower:
                    matches.append({
                        'keyword': keyword,
                        'segment_id': segment['id'],
                        'start_time': segment['start'],
                        'end_time': segment['end'],
                        'text': segment['text'],
                        'context_start': max(0, segment['start'] - context_seconds),
                        'context_end': segment['end'] + context_seconds,
                    })
                    results['total_matches'] += 1

        if matches:
            results['videos'].append({
                'video_id': video_id,
                'video_title': video.get('video_title'),
                'matches': matches
            })

    logger.info(f"Found {results['total_matches']} keyword matches across {len(results['videos'])} videos")
    return results


def export_transcript_to_srt(transcription_results, output_dir: str) -> Dict:
    """
    Export transcription to SRT subtitle format.

    Args:
        transcription_results: Results from transcribe_video_audio
        output_dir: Directory to save SRT files

    Returns:
        Dict with export results:
        {
            "total_exported": int,
            "files": [
                {
                    "video_id": str,
                    "srt_file_path": str,
                    "success": bool
                }
            ]
        }
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    results = {
        "total_exported": 0,
        "files": []
    }

    if not transcription_results or not transcription_results.get('videos'):
        return results

    for video in transcription_results['videos']:
        video_id = video.get('video_id')
        segments = video.get('segments', [])

        if not segments:
            continue

        srt_path = os.path.join(output_dir, f"{video_id}.srt")

        try:
            with open(srt_path, 'w', encoding='utf-8') as f:
                for i, segment in enumerate(segments, 1):
                    # Format timestamps for SRT (HH:MM:SS,mmm)
                    start_time = _format_srt_timestamp(segment['start'])
                    end_time = _format_srt_timestamp(segment['end'])

                    # Write SRT entry
                    f.write(f"{i}\n")
                    f.write(f"{start_time} --> {end_time}\n")
                    f.write(f"{segment['text']}\n")
                    f.write("\n")

            results['files'].append({
                'video_id': video_id,
                'srt_file_path': srt_path,
                'success': True
            })
            results['total_exported'] += 1
            logger.info(f"✅ Exported SRT: {srt_path}")

        except Exception as e:
            logger.error(f"Error exporting SRT for {video_id}: {e}")
            results['files'].append({
                'video_id': video_id,
                'srt_file_path': srt_path,
                'success': False,
                'error': str(e)
            })

    return results


def _format_srt_timestamp(seconds: float) -> str:
    """
    Format seconds to SRT timestamp format (HH:MM:SS,mmm).

    Args:
        seconds: Time in seconds

    Returns:
        Formatted timestamp string
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    milliseconds = int((seconds % 1) * 1000)

    return f"{hours:02d}:{minutes:02d}:{secs:02d},{milliseconds:03d}"

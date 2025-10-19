"""
YouTube video and audio download operations.

This module handles downloading videos and extracting audio from YouTube using yt-dlp.
"""

import logging

from congress_videos.config.paths import (
    get_download_video_path,
    ensure_directory_exists
)
from utils.youtube_downloader import (
    download_youtube_video_for_upload,
    download_audio_only,
    download_audio_in_chunks
)


def create_test_video_data(test_video_url: str = "https://www.youtube.com/watch?v=ZBU0bVpYXM4"):
    """
    Create mock video data for testing with a specified YouTube URL.

    Args:
        test_video_url: YouTube video URL to use for testing
                       (default: https://www.youtube.com/watch?v=ZBU0bVpYXM4)

    Returns:
        Dict with test video in the same structure as filter_plenary_session_videos
        (so it can be processed by get_video_details and get_video_descriptions)
    """
    # Extract video ID from URL
    # Supports formats: youtube.com/watch?v=ID, youtu.be/ID
    import re

    video_id_match = re.search(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})', test_video_url)
    if video_id_match:
        test_video_id = video_id_match.group(1)
    else:
        # Fallback to default if URL parsing fails
        logging.warning(f"Could not extract video ID from URL: {test_video_url}, using default")
        test_video_id = 'ZBU0bVpYXM4'
        test_video_url = f'https://www.youtube.com/watch?v={test_video_id}'

    # Match the structure returned by filter_plenary_session_videos
    mock_plenary_videos = {
        'total_matches': 1,
        'videos': [{
            'video_id': test_video_id,
            'title': 'Test Video - Sesión Plenaria',
            'url': test_video_url,
            'published_at': '2025-01-01T10:00:00Z',  # Mock date
            'is_live': False,
            'is_upcoming': False
        }]
    }

    logging.info(f"Created test video data for video ID: {test_video_id}")
    logging.info(f"Test video URL: {test_video_url}")

    return mock_plenary_videos


def download_video_from_youtube(video_details, target_date: str):
    """
    Download video from YouTube in best quality.

    Uses yt-dlp to download the full video file.

    Args:
        video_details: Results from get_video_details (contains youtube_url)
        target_date: Target date in YYYY-MM-DD format (for organizing downloads)

    Returns:
        Dict with download results:
        - total_downloaded: Number of videos downloaded
        - videos: List with video_id, file_path, file_size_mb, duration
    """
    if not video_details or not video_details.get('videos'):
        logging.warning("No video details to download")
        return {'total_downloaded': 0, 'videos': []}

    downloaded_videos = []

    for video in video_details['videos']:
        video_id = video['video_id']
        youtube_url = video.get('youtube_url')

        if not youtube_url:
            logging.warning(f"No YouTube URL for {video_id}")
            downloaded_videos.append({
                'video_id': video_id,
                'error': 'No YouTube URL available'
            })
            continue

        try:
            # Create download directory: downloads/{date}/{video_id}/
            video_download_dir = get_download_video_path(target_date, video_id)
            ensure_directory_exists(video_download_dir)

            logging.info(f"Downloading video from YouTube: {youtube_url}")

            # Download using youtube_downloader utility (best quality)
            result = download_youtube_video_for_upload(
                youtube_url=youtube_url,
                output_dir=video_download_dir,
                quality="best"  # Download best available quality
            )

            if result['success']:
                download_data = {
                    'video_id': video_id,
                    'video_title': video.get('title'),
                    'target_date': target_date,
                    'youtube_url': youtube_url,
                    'file_path': result['file_path'],
                    'file_size_mb': result['file_size_mb'],
                    'duration': result['duration'],
                    'download_title': result['title']
                }
                logging.info(f"✅ Video downloaded: {result['file_path']} ({result['file_size_mb']} MB)")
                downloaded_videos.append(download_data)
            else:
                logging.error(f"Failed to download video {video_id}: {result.get('error')}")
                downloaded_videos.append({
                    'video_id': video_id,
                    'video_title': video.get('title'),
                    'youtube_url': youtube_url,
                    'error': result.get('error')
                })

        except Exception as e:
            logging.error(f"Error downloading video {video_id}: {e}")
            downloaded_videos.append({
                'video_id': video_id,
                'youtube_url': youtube_url,
                'error': str(e)
            })

    logging.info(f"Total videos downloaded: {len([v for v in downloaded_videos if 'file_path' in v])}")
    return {
        'total_downloaded': len([v for v in downloaded_videos if 'file_path' in v]),
        'videos': downloaded_videos
    }


def try_download_subtitles_from_youtube(video_details, target_date: str):
    """
    Try to download existing SRT subtitles directly from YouTube.

    This is much faster than transcribing audio and should be tried first.
    If subtitles are not available, the DAG will fall back to audio transcription.

    Args:
        video_details: Results from get_video_details (contains youtube_url)
        target_date: Target date in YYYY-MM-DD format (for organizing downloads)

    Returns:
        Dict with subtitle download results:
        - total_downloaded: Number of videos with subtitles downloaded
        - videos: List with video_id, has_subtitles, merged_srt_path, or error
    """
    from utils.youtube_downloader import download_youtube_subtitles

    if not video_details or not video_details.get('videos'):
        logging.warning("No video details to download subtitles")
        return {'total_downloaded': 0, 'videos': []}

    subtitle_results = []

    for video in video_details['videos']:
        video_id = video['video_id']
        youtube_url = video.get('youtube_url')

        if not youtube_url:
            logging.warning(f"No YouTube URL for {video_id}")
            subtitle_results.append({
                'video_id': video_id,
                'has_subtitles': False,
                'error': 'No YouTube URL available'
            })
            continue

        try:
            # Create download directory: downloads/{date}/{video_id}/
            video_download_dir = get_download_video_path(target_date, video_id)
            ensure_directory_exists(video_download_dir)

            logging.info(f"Attempting to download subtitles from YouTube: {youtube_url}")

            # Try to download subtitles
            result = download_youtube_subtitles(
                youtube_url=youtube_url,
                output_dir=video_download_dir,
                languages=['es', 'es-ES', 'en', 'auto']  # Prefer Spanish, fallback to English or auto
            )

            if result['success']:
                subtitle_results.append({
                    'video_id': video_id,
                    'video_title': video.get('title'),
                    'target_date': target_date,
                    'youtube_url': youtube_url,
                    'has_subtitles': True,
                    'merged_srt_path': result['merged_srt_path'],
                    'subtitle_files': result['subtitle_files'],
                    'downloaded_from_youtube': True  # Flag to skip transcription
                })
                logging.info(f"✅ Downloaded subtitles for {video_id} from YouTube!")
            else:
                subtitle_results.append({
                    'video_id': video_id,
                    'video_title': video.get('title'),
                    'youtube_url': youtube_url,
                    'has_subtitles': False,
                    'error': result.get('error'),
                    'downloaded_from_youtube': False  # Will need transcription
                })
                logging.info(f"No subtitles available for {video_id}, will need transcription")

        except Exception as e:
            logging.error(f"Error downloading subtitles for {video_id}: {e}")
            subtitle_results.append({
                'video_id': video_id,
                'youtube_url': youtube_url,
                'has_subtitles': False,
                'error': str(e),
                'downloaded_from_youtube': False
            })

    successful_count = len([v for v in subtitle_results if v.get('has_subtitles')])
    logging.info(f"Total videos with subtitles downloaded: {successful_count}/{len(subtitle_results)}")

    return {
        'total_downloaded': successful_count,
        'videos': subtitle_results
    }


def extract_audio_from_youtube(video_details, target_date: str, chunk_duration_minutes: int = None):
    """
    Extract audio from YouTube video, optionally split into chunks.

    Uses yt-dlp to download audio-only stream. If chunk_duration_minutes is provided,
    downloads audio in time-based chunks directly from YouTube (no ffmpeg needed).

    Args:
        video_details: Results from get_video_details (contains youtube_url)
        target_date: Target date in YYYY-MM-DD format (for organizing downloads)
        chunk_duration_minutes: If provided, split audio into chunks of this duration

    Returns:
        Dict with extraction results:
        - total_extracted: Number of audio files extracted
        - videos: List with video_id, audio_file_path OR chunks info, file_size_mb
    """
    if not video_details or not video_details.get('videos'):
        logging.warning("No video details to extract audio")
        return {'total_extracted': 0, 'videos': []}

    extracted_audios = []

    for video in video_details['videos']:
        video_id = video['video_id']
        youtube_url = video.get('youtube_url')

        if not youtube_url:
            logging.warning(f"No YouTube URL for {video_id}")
            extracted_audios.append({
                'video_id': video_id,
                'error': 'No YouTube URL available'
            })
            continue

        try:
            # Create download directory: downloads/{date}/{video_id}/
            video_download_dir = get_download_video_path(target_date, video_id)
            ensure_directory_exists(video_download_dir)

            # Check if chunking is requested
            if chunk_duration_minutes:
                logging.info(f"Extracting audio in chunks from YouTube: {youtube_url}")
                logging.info(f"Chunk duration: {chunk_duration_minutes} minutes")

                # Download audio in chunks
                result = download_audio_in_chunks(
                    youtube_url=youtube_url,
                    output_dir=video_download_dir,
                    chunk_duration_minutes=chunk_duration_minutes,
                    audio_format="webm"
                )

                if result['success']:
                    audio_data = {
                        'video_id': video_id,
                        'video_title': video.get('title'),
                        'target_date': target_date,
                        'youtube_url': youtube_url,
                        'chunked': True,
                        'total_chunks': result['total_chunks'],
                        'total_duration': result['total_duration'],
                        'chunks': result['chunks'],
                        'file_size_mb': sum(c['file_size_mb'] for c in result['chunks'])
                    }
                    logging.info(f"✅ Audio extracted in {result['total_chunks']} chunks")
                    extracted_audios.append(audio_data)
                else:
                    logging.error(f"Failed to extract audio chunks {video_id}: {result.get('error')}")
                    extracted_audios.append({
                        'video_id': video_id,
                        'video_title': video.get('title'),
                        'youtube_url': youtube_url,
                        'error': result.get('error')
                    })
            else:
                logging.info(f"Extracting audio from YouTube: {youtube_url}")

                # Extract audio as single file using youtube_downloader utility
                result = download_audio_only(
                    youtube_url=youtube_url,
                    output_dir=video_download_dir,
                    convert_to_mp3=False,  # Don't convert to MP3
                    audio_format="webm"  # Use webm (lighter format)
                )

                if result['success']:
                    audio_data = {
                        'video_id': video_id,
                        'video_title': video.get('title'),
                        'target_date': target_date,
                        'youtube_url': youtube_url,
                        'chunked': False,
                        'audio_file_path': result['file_path'],
                        'file_size_mb': result['file_size_mb'],
                        'duration': result['duration']
                    }
                    logging.info(f"✅ Audio extracted: {result['file_path']} ({result['file_size_mb']} MB)")
                    extracted_audios.append(audio_data)
                else:
                    logging.error(f"Failed to extract audio {video_id}: {result.get('error')}")
                    extracted_audios.append({
                        'video_id': video_id,
                        'video_title': video.get('title'),
                        'youtube_url': youtube_url,
                        'error': result.get('error')
                    })

        except Exception as e:
            logging.error(f"Error extracting audio {video_id}: {e}")
            extracted_audios.append({
                'video_id': video_id,
                'youtube_url': youtube_url,
                'error': str(e)
            })

    logging.info(f"Total audio files extracted: {len([v for v in extracted_audios if 'audio_file_path' in v])}")
    return {
        'total_extracted': len([v for v in extracted_audios if 'audio_file_path' in v]),
        'videos': extracted_audios
    }


def transcribe_audio_with_whisper(extracted_audio, language: str = "es", timeout: int = 3600):
    """
    Transcribe extracted audio using Whisper API.

    Handles both single audio files and chunked audio files.

    Args:
        extracted_audio: Results from extract_audio_from_youtube
                        (contains audio files or chunks to transcribe)
        language: Language code for transcription (default: "es" for Spanish)
        timeout: Request timeout per audio file/chunk in seconds (default: 3600 = 1 hour)

    Returns:
        Dict with transcription results:
        - total_transcribed: Number of videos transcribed
        - videos: List with video_id, transcription text or chunks
    """
    from utils.whisper_helpers import (
        transcribe_audio_file,
        transcribe_audio_chunks,
        check_whisper_api_health
    )

    if not extracted_audio or not extracted_audio.get('videos'):
        logging.warning("No extracted audio to transcribe")
        return {'total_transcribed': 0, 'videos': []}

    # Check if Whisper API is available
    if not check_whisper_api_health():
        logging.error("Whisper API is not available. Aborting transcription.")
        return {
            'total_transcribed': 0,
            'videos': [],
            'error': 'Whisper API unavailable'
        }

    transcribed_videos = []

    for video in extracted_audio['videos']:
        video_id = video['video_id']
        is_chunked = video.get('chunked', False)

        try:
            if is_chunked:
                # Transcribe audio chunks
                chunks = video.get('chunks', [])
                logging.info(f"Transcribing {len(chunks)} audio chunks for video {video_id}")

                transcription_result = transcribe_audio_chunks(
                    audio_chunks=chunks,
                    language=language,
                    timeout=timeout
                )

                transcribed_videos.append({
                    'video_id': video_id,
                    'video_title': video.get('video_title'),
                    'chunked': True,
                    'total_chunks': transcription_result['total_chunks'],
                    'successful_transcriptions': transcription_result['successful_transcriptions'],
                    'chunks': transcription_result['chunks']
                })

            else:
                # Transcribe single audio file
                audio_file_path = video.get('audio_file_path')

                if not audio_file_path:
                    logging.warning(f"No audio file path for video {video_id}")
                    transcribed_videos.append({
                        'video_id': video_id,
                        'error': 'No audio file path'
                    })
                    continue

                logging.info(f"Transcribing audio file for video {video_id}")

                transcription_result = transcribe_audio_file(
                    audio_file_path=audio_file_path,
                    language=language,
                    timeout=timeout
                )

                transcribed_videos.append({
                    'video_id': video_id,
                    'video_title': video.get('video_title'),
                    'chunked': False,
                    'audio_file_path': audio_file_path,
                    'transcription': transcription_result.get('text', ''),
                    'transcription_success': transcription_result.get('success', False),
                    'transcription_duration': transcription_result.get('duration'),
                    'error': transcription_result.get('error')
                })

        except Exception as e:
            logging.error(f"Error transcribing video {video_id}: {e}")
            transcribed_videos.append({
                'video_id': video_id,
                'error': str(e)
            })

    successful_count = len([v for v in transcribed_videos if not v.get('error')])
    logging.info(f"Total videos transcribed: {successful_count}/{len(transcribed_videos)}")

    return {
        'total_transcribed': successful_count,
        'videos': transcribed_videos
    }


def merge_transcription_srt_files(transcriptions, target_date: str):
    """
    Merge individual SRT chunk files into a single simplified SRT file per video.

    For each video with chunked transcriptions, finds all SRT files in the
    srt_files folder and merges them into a single file named {video_id}_merged.srt.

    The merged file has a simplified format:
    - No entry numbers
    - Timestamps without milliseconds (HH:MM:SS format)

    Args:
        transcriptions: Results from transcribe_audio_with_whisper
        target_date: Target date in YYYY-MM-DD format (for locating files)

    Returns:
        Dict with merge results:
        - total_merged: Number of videos with merged SRT files
        - videos: List with video_id, merged_srt_path, total_entries
    """
    from pathlib import Path
    from utils.whisper_helpers import merge_srt_files
    from congress_videos.config.paths import get_download_video_path

    if not transcriptions or not transcriptions.get('videos'):
        logging.warning("No transcriptions to merge")
        return {'total_merged': 0, 'videos': []}

    merged_videos = []

    for video in transcriptions['videos']:
        video_id = video['video_id']
        is_chunked = video.get('chunked', False)

        # Only process chunked videos (single files don't need merging)
        if not is_chunked:
            logging.info(f"Video {video_id} is not chunked, skipping merge")
            continue

        try:
            # Get the video directory
            video_dir = Path(get_download_video_path(target_date, video_id))
            srt_dir = video_dir / "srt_files"

            if not srt_dir.exists():
                logging.warning(f"SRT directory not found: {srt_dir}")
                merged_videos.append({
                    'video_id': video_id,
                    'error': 'SRT directory not found'
                })
                continue

            # Get all SRT files (sorted by name to maintain order)
            srt_files = sorted(srt_dir.glob("*.srt"))

            if not srt_files:
                logging.warning(f"No SRT files found in {srt_dir}")
                merged_videos.append({
                    'video_id': video_id,
                    'error': 'No SRT files found'
                })
                continue

            logging.info(f"Merging {len(srt_files)} SRT files for video {video_id}")

            # Merge SRT files
            merged_output_path = srt_dir / f"{video_id}_merged.srt"
            result = merge_srt_files(
                srt_files=[str(f) for f in srt_files],
                output_path=str(merged_output_path)
            )

            if result['success']:
                merged_videos.append({
                    'video_id': video_id,
                    'video_title': video.get('video_title'),
                    'merged_srt_path': result['output_path'],
                    'total_entries': result['total_entries'],
                    'source_files': len(srt_files)
                })
            else:
                merged_videos.append({
                    'video_id': video_id,
                    'error': result.get('error')
                })

        except Exception as e:
            logging.error(f"Error merging SRT files for video {video_id}: {e}")
            merged_videos.append({
                'video_id': video_id,
                'error': str(e)
            })

    successful_count = len([v for v in merged_videos if not v.get('error')])
    logging.info(f"Total videos with merged SRT files: {successful_count}/{len(merged_videos)}")

    return {
        'total_merged': successful_count,
        'videos': merged_videos
    }


def split_srt_by_silence(srt_data, target_date: str, min_silence_seconds: int = 15, min_chunk_duration_minutes: int = 20, max_chunk_duration_minutes: int = 30):
    """
    TASK 1: Split SRT content into chunks based on silence gaps.

    This task splits the SRT transcript at natural breaks (15+ second silences).
    Small chunks (< min_chunk_duration_minutes) are automatically merged with adjacent chunks.

    Args:
        srt_data: Results from merge_transcription_srt_files OR try_download_subtitles_from_youtube
        target_date: Target date in YYYY-MM-DD format (for locating files)
        min_silence_seconds: Minimum silence duration to use as split point (default: 15)
        min_chunk_duration_minutes: Minimum duration for each chunk (default: 20)
        max_chunk_duration_minutes: Maximum duration for each chunk (default: 30)

    Returns:
        Dict with chunking results:
        - total_videos: Number of videos processed
        - videos: List with video_id, chunks (with SRT content)
    """
    from pathlib import Path
    from utils.ai_chapter_analyzer import chunk_by_silence
    from congress_videos.config.paths import get_download_video_path

    if not srt_data or not srt_data.get('videos'):
        logging.warning("No SRT data to split into chunks")
        return {'total_videos': 0, 'videos': []}

    chunked_videos = []

    for srt_video in srt_data['videos']:
        video_id = srt_video['video_id']

        # Get merged SRT path
        merged_srt_path = srt_video.get('merged_srt_path')

        if not merged_srt_path:
            # Try to construct path
            video_dir = Path(get_download_video_path(target_date, video_id))
            srt_dir = video_dir / "srt_files"
            potential_path = srt_dir / f"{video_id}_merged.srt"
            if potential_path.exists():
                merged_srt_path = str(potential_path)

        if not merged_srt_path or srt_video.get('error'):
            logging.warning(f"Skipping video {video_id}: no merged SRT file")
            chunked_videos.append({
                'video_id': video_id,
                'error': 'No merged SRT file available'
            })
            continue

        try:
            # Read the merged SRT file
            srt_content = Path(merged_srt_path).read_text(encoding='utf-8')
            logging.info(f"Loaded SRT file for video {video_id}: {len(srt_content)} characters")

            # Split content by silence gaps
            logging.info(f"Splitting SRT by silence gaps (min: {min_chunk_duration_minutes} min)...")
            chunks = chunk_by_silence(
                srt_content=srt_content,
                min_silence_seconds=min_silence_seconds,
                min_chunk_duration_minutes=min_chunk_duration_minutes,
                max_chunk_duration_minutes=max_chunk_duration_minutes
            )

            logging.info(f"✅ Created {len(chunks)} chunks for video {video_id}")

            chunked_videos.append({
                'video_id': video_id,
                'video_title': srt_video.get('video_title'),
                'total_chunks': len(chunks),
                'chunks': chunks,  # Contains: chunk_number, start_time, end_time, duration, content
                'merged_srt_path': merged_srt_path
            })

        except Exception as e:
            logging.error(f"Error splitting video {video_id}: {e}", exc_info=True)
            chunked_videos.append({
                'video_id': video_id,
                'error': str(e)
            })

    successful_count = len([v for v in chunked_videos if not v.get('error')])
    logging.info(f"SRT splitting complete: {successful_count}/{len(chunked_videos)} videos processed")

    return {
        'total_videos': successful_count,
        'videos': chunked_videos
    }


def summarize_silence_chunks(chunked_srt_data, target_date: str):
    """
    TASK 2: Summarize each chunk with speakers, topics, and timestamps.

    Analyzes each chunk to extract:
    - Speakers (who spoke, when they started/ended)
    - Topics discussed
    - Mini SRT-style timeline in JSON format

    Args:
        chunked_srt_data: Results from split_srt_by_silence (contains chunks with SRT content)
        target_date: Target date in YYYY-MM-DD format

    Returns:
        Dict with summarization results:
        - total_videos: Number of videos processed
        - videos: List with video_id, summarized_chunks (speakers, topics, timeline)
    """
    from congress_videos.config.ai_prompts import CHUNK_SUMMARY_SYSTEM_PROMPT, CHUNK_SUMMARY_USER_PROMPT_TEMPLATE
    import openai
    import json

    if not chunked_srt_data or not chunked_srt_data.get('videos'):
        logging.warning("No chunked SRT data to summarize")
        return {'total_videos': 0, 'videos': []}

    summarized_videos = []

    for video_data in chunked_srt_data['videos']:
        video_id = video_data['video_id']
        chunks = video_data.get('chunks', [])

        if not chunks or video_data.get('error'):
            logging.warning(f"Skipping video {video_id}: no chunks available")
            summarized_videos.append({
                'video_id': video_id,
                'error': video_data.get('error', 'No chunks available')
            })
            continue

        try:
            logging.info(f"Summarizing {len(chunks)} chunks for video {video_id}...")
            summarized_chunks = []

            for chunk in chunks:
                try:
                    # Call OpenAI to extract speakers, topics, and timeline
                    client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": CHUNK_SUMMARY_SYSTEM_PROMPT},
                            {"role": "user", "content": CHUNK_SUMMARY_USER_PROMPT_TEMPLATE.format(
                                chunk_number=chunk['chunk_number'],
                                start_time=chunk['start_time'],
                                end_time=chunk['end_time'],
                                duration_minutes=chunk['duration_minutes'],
                                chunk_content=chunk['content'][:15000]  # Limit to 15k chars
                            )}
                        ],
                        temperature=0.2,
                        max_tokens=1000,
                        response_format={"type": "json_object"}  # Request JSON response
                    )

                    # Parse AI response as JSON
                    ai_response = response.choices[0].message.content.strip()
                    summary_data = json.loads(ai_response)

                    summarized_chunks.append({
                        "chunk_number": chunk['chunk_number'],
                        "start_time": chunk['start_time'],
                        "end_time": chunk['end_time'],
                        "duration_seconds": chunk['duration_seconds'],
                        "duration_minutes": chunk['duration_minutes'],
                        "speakers": summary_data.get('speakers', []),  # List of {name, start_time, end_time}
                        "topics": summary_data.get('topics', []),  # List of topics discussed
                        "timeline": summary_data.get('timeline', []),  # Mini SRT: [{time, speaker, content}]
                        "summary": summary_data.get('summary', '')  # Brief overall summary
                    })

                    logging.info(f"  ✅ Chunk {chunk['chunk_number']}: {len(summary_data.get('speakers', []))} speakers, {len(summary_data.get('topics', []))} topics")

                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON for chunk {chunk['chunk_number']}: {e}")
                    summarized_chunks.append({
                        "chunk_number": chunk['chunk_number'],
                        "start_time": chunk['start_time'],
                        "end_time": chunk['end_time'],
                        "duration_seconds": chunk['duration_seconds'],
                        "duration_minutes": chunk['duration_minutes'],
                        "error": f"JSON parse error: {str(e)}",
                        "raw_response": ai_response[:500]
                    })
                except Exception as e:
                    logging.error(f"Failed to summarize chunk {chunk['chunk_number']}: {e}")
                    summarized_chunks.append({
                        "chunk_number": chunk['chunk_number'],
                        "start_time": chunk['start_time'],
                        "end_time": chunk['end_time'],
                        "duration_seconds": chunk['duration_seconds'],
                        "duration_minutes": chunk['duration_minutes'],
                        "error": str(e)
                    })

            summarized_videos.append({
                'video_id': video_id,
                'video_title': video_data.get('video_title'),
                'total_chunks': len(summarized_chunks),
                'summarized_chunks': summarized_chunks
            })

            logging.info(f"✅ Summarized {len(summarized_chunks)} chunks for video {video_id}")

        except Exception as e:
            logging.error(f"Error summarizing video {video_id}: {e}", exc_info=True)
            summarized_videos.append({
                'video_id': video_id,
                'error': str(e)
            })

    successful_count = len([v for v in summarized_videos if not v.get('error')])
    logging.info(f"Chunk summarization complete: {successful_count}/{len(summarized_videos)} videos processed")

    return {
        'total_videos': successful_count,
        'videos': summarized_videos
    }


def identify_interesting_chapters(chunk_summaries, agenda_sections, target_date: str):
    """
    Use AI to identify interesting chapters from chunk summaries and agenda.

    Takes the summarized chunks (with speakers, topics, timeline) and combines
    them with the agenda to create meaningful chapters for video extraction.

    Args:
        chunk_summaries: Results from summarize_silence_chunks (contains summarized chunks)
        agenda_sections: Results from extract_agenda_section
        target_date: Target date in YYYY-MM-DD format (for locating files)

    Returns:
        Dict with chapter identification results:
        - total_videos: Number of videos analyzed
        - videos: List with video_id, chapters data
    """
    from pathlib import Path
    from utils.ai_chapter_analyzer import analyze_chapters_with_ai
    from congress_videos.config.paths import get_download_video_path

    if not chunk_summaries or not chunk_summaries.get('videos'):
        logging.warning("No chunk summaries to analyze")
        return {'total_videos': 0, 'videos': []}

    analyzed_videos = []

    for video_data in chunk_summaries['videos']:
        video_id = video_data['video_id']
        summarized_chunks = video_data.get('summarized_chunks', [])

        if not summarized_chunks or video_data.get('error'):
            logging.warning(f"Skipping video {video_id}: no summarized chunks")
            analyzed_videos.append({
                'video_id': video_id,
                'error': video_data.get('error', 'No summarized chunks available')
            })
            continue

        try:
            # Find matching agenda section
            agenda_content = ""
            if agenda_sections and agenda_sections.get('videos'):
                for agenda_video in agenda_sections['videos']:
                    if agenda_video.get('video_id') == video_id:
                        agenda_content = agenda_video.get('agenda_section_text', '')
                        break

            if not agenda_content:
                logging.warning(f"No agenda found for video {video_id}, using generic prompt")
                agenda_content = "No agenda available. Please analyze based on chunk summaries."

            logging.info(f"Analyzing video {video_id} with AI to identify interesting chapters from {len(summarized_chunks)} chunks...")

            # Prepare combined summary for AI analysis
            # Convert chunk summaries to a readable format for the AI
            combined_summary = f"VIDEO STRUCTURE ({len(summarized_chunks)} chunks):\n\n"

            for chunk in summarized_chunks:
                combined_summary += f"--- CHUNK {chunk['chunk_number']} ({chunk['start_time']} - {chunk['end_time']}) ---\n"
                combined_summary += f"Duration: {chunk['duration_minutes']} minutes\n\n"

                # Speakers
                if chunk.get('speakers'):
                    combined_summary += "Intervinientes:\n"
                    for speaker in chunk['speakers']:
                        combined_summary += f"  - {speaker.get('name', 'Unknown')} ({speaker.get('role', '')}): {speaker.get('start_time', '')} - {speaker.get('end_time', '')}\n"
                    combined_summary += "\n"

                # Topics
                if chunk.get('topics'):
                    combined_summary += f"Temas: {', '.join(chunk['topics'])}\n\n"

                # Summary
                if chunk.get('summary'):
                    combined_summary += f"Resumen: {chunk['summary']}\n\n"

                combined_summary += "\n"

            # Use AI to analyze and identify chapters from summaries
            result = analyze_chapters_with_ai(
                srt_content=combined_summary,  # Pass combined summaries instead of full SRT
                agenda_content=agenda_content,
                min_duration_minutes=15,
                max_duration_minutes=30,
                model="gpt-4o-mini"
            )

            if result['success']:
                analyzed_videos.append({
                    'video_id': video_id,
                    'video_title': video_data.get('video_title'),
                    'total_chapters': result['total_chapters'],
                    'total_duration_seconds': result['total_duration_seconds'],
                    'chapters': result['chapters'],
                    'chunk_summaries': summarized_chunks  # Include original summaries
                })
                logging.info(f"✅ Identified {result['total_chapters']} chapters for video {video_id}")
            else:
                analyzed_videos.append({
                    'video_id': video_id,
                    'error': result.get('error')
                })
                logging.error(f"Failed to analyze video {video_id}: {result.get('error')}")

        except Exception as e:
            logging.error(f"Error analyzing video {video_id}: {e}", exc_info=True)
            analyzed_videos.append({
                'video_id': video_id,
                'error': str(e)
            })

    successful_count = len([v for v in analyzed_videos if not v.get('error')])
    logging.info(f"Chapter analysis complete: {successful_count}/{len(analyzed_videos)} videos analyzed")

    return {
        'total_videos': successful_count,
        'videos': analyzed_videos
    }

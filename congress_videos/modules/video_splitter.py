"""
Video splitting module using ffmpeg.

Splits downloaded YouTube videos into chapters based on start_time and end_time.
"""

import logging
import os
import subprocess
from pathlib import Path


def convert_srt_time_to_seconds(srt_time):
    """
    Convert SRT timestamp format (HH:MM:SS,mmm) to seconds.

    Args:
        srt_time: Time string in SRT format (e.g., "00:10:15,500")

    Returns:
        Float representing total seconds
    """
    try:
        # Split time and milliseconds
        time_part, ms_part = srt_time.split(',')
        hours, minutes, seconds = map(int, time_part.split(':'))
        milliseconds = int(ms_part)

        total_seconds = hours * 3600 + minutes * 60 + seconds + milliseconds / 1000.0
        return total_seconds
    except Exception as e:
        logging.error(f"Error converting SRT time '{srt_time}': {e}")
        return 0.0


def split_video_chapter(source_video_path, output_path, start_time, end_time):
    """
    Split a video segment using ffmpeg.

    Uses ffmpeg to extract a specific time range from the source video.
    Fast processing with copy codec (no re-encoding).

    Args:
        source_video_path: Path to source video file
        output_path: Path where the extracted chapter will be saved
        start_time: Start time in SRT format (HH:MM:SS,mmm)
        end_time: End time in SRT format (HH:MM:SS,mmm)

    Returns:
        Dict with success status and file info
    """
    try:
        # Ensure source video exists
        if not os.path.exists(source_video_path):
            raise FileNotFoundError(f"Source video not found: {source_video_path}")

        # Convert SRT timestamps to seconds
        start_seconds = convert_srt_time_to_seconds(start_time)
        end_seconds = convert_srt_time_to_seconds(end_time)
        duration_seconds = end_seconds - start_seconds

        if duration_seconds <= 0:
            raise ValueError(f"Invalid time range: {start_time} to {end_time}")

        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Build ffmpeg command
        # -ss: start time (seek to position)
        # -t: duration to extract
        # -i: input file
        # -c copy: copy codec (fast, no re-encoding)
        # -avoid_negative_ts make_zero: fix timestamp issues
        ffmpeg_command = [
            'ffmpeg',
            '-y',  # Overwrite output file if exists
            '-ss', str(start_seconds),
            '-t', str(duration_seconds),
            '-i', source_video_path,
            '-c', 'copy',
            '-avoid_negative_ts', 'make_zero',
            output_path
        ]

        logging.info(f"Extracting chapter: {start_time} to {end_time} ({duration_seconds:.2f}s)")
        logging.info(f"Command: {' '.join(ffmpeg_command)}")

        # Run ffmpeg
        result = subprocess.run(
            ffmpeg_command,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )

        if result.returncode != 0:
            raise Exception(f"ffmpeg failed: {result.stderr}")

        # Get file size
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 * 1024)

        logging.info(f"✅ Chapter extracted successfully: {output_path}")
        logging.info(f"   File size: {file_size_mb:.2f} MB")

        return {
            "success": True,
            "output_path": output_path,
            "file_size_bytes": file_size,
            "file_size_mb": file_size_mb,
            "duration_seconds": duration_seconds,
            "start_time": start_time,
            "end_time": end_time,
            "error": None
        }

    except subprocess.TimeoutExpired:
        error_msg = f"ffmpeg timeout while processing {source_video_path}"
        logging.error(error_msg)
        return {
            "success": False,
            "output_path": None,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Error splitting video: {str(e)}"
        logging.error(error_msg)
        return {
            "success": False,
            "output_path": None,
            "error": error_msg
        }


def extract_chapters_from_video(uploadable_chapters, data_directory):
    """
    Extract chapter videos from source YouTube videos.

    For each chapter:
    1. Locate source video file (data_directory/video_id/)
    2. Extract chapter segment using start_time and end_time
    3. Save to: data_directory/video_id/chapter_id/chapter_video.mp4

    Args:
        uploadable_chapters: List of chapter records from uploadable_chapters view
                            Required fields: chapter_id, video_id, start_time, end_time,
                                           source_video_title
        data_directory: Base data directory (congress_videos folder)

    Returns:
        Dict with extraction results:
        {
            'total_chapters': int,
            'successful_extractions': int,
            'failed_extractions': int,
            'results': [
                {
                    'chapter_id': int,
                    'video_id': str,
                    'success': bool,
                    'output_path': str,
                    'file_size_mb': float,
                    'duration_seconds': float,
                    'error': str or None
                }
            ]
        }
    """
    if not uploadable_chapters:
        logging.warning("No chapters provided for extraction")
        return {
            'total_chapters': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'results': []
        }

    extraction_results = {
        'total_chapters': len(uploadable_chapters),
        'successful_extractions': 0,
        'failed_extractions': 0,
        'results': []
    }

    for chapter in uploadable_chapters:
        chapter_id = chapter.get('chapter_id')
        video_id = chapter.get('video_id')
        start_time = chapter.get('start_time')
        end_time = chapter.get('end_time')
        source_video_title = chapter.get('source_video_title', 'video')

        logging.info(f"Processing chapter {chapter_id} from video {video_id}")

        try:
            # Find source video file in data_directory/video_id/
            video_folder = os.path.join(data_directory, str(video_id))

            if not os.path.exists(video_folder):
                raise FileNotFoundError(f"Source video folder not found: {video_folder}")

            # Look for video files (mp4, mkv, webm)
            video_extensions = ['.mp4', '.mkv', '.webm']
            source_video_path = None

            for file in os.listdir(video_folder):
                if any(file.endswith(ext) for ext in video_extensions):
                    # Skip chapter videos (to avoid using extracted chapters as source)
                    if 'chapter_video' not in file:
                        source_video_path = os.path.join(video_folder, file)
                        break

            if not source_video_path:
                raise FileNotFoundError(f"No source video found in {video_folder}")

            logging.info(f"Found source video: {source_video_path}")

            # Create output path: data_directory/video_id/chapter_id/chapter_video.mp4
            chapter_folder = os.path.join(data_directory, str(video_id), str(chapter_id))
            os.makedirs(chapter_folder, exist_ok=True)
            output_path = os.path.join(chapter_folder, 'chapter_video.mp4')

            # Extract chapter using ffmpeg
            result = split_video_chapter(
                source_video_path=source_video_path,
                output_path=output_path,
                start_time=start_time,
                end_time=end_time
            )

            result['chapter_id'] = chapter_id
            result['video_id'] = video_id
            extraction_results['results'].append(result)

            if result['success']:
                extraction_results['successful_extractions'] += 1
                logging.info(f"✅ Chapter {chapter_id} extracted successfully")
            else:
                extraction_results['failed_extractions'] += 1
                logging.error(f"❌ Chapter {chapter_id} extraction failed: {result.get('error')}")

        except Exception as e:
            error_msg = f"Error processing chapter {chapter_id}: {str(e)}"
            logging.error(error_msg)
            extraction_results['results'].append({
                'chapter_id': chapter_id,
                'video_id': video_id,
                'success': False,
                'output_path': None,
                'error': error_msg
            })
            extraction_results['failed_extractions'] += 1

    logging.info(
        f"Chapter extraction complete: {extraction_results['successful_extractions']}/{extraction_results['total_chapters']} "
        f"chapters extracted successfully"
    )

    return extraction_results

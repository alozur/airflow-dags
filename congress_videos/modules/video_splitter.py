"""
Video splitting module using ffmpeg.

Splits downloaded YouTube videos into chapters based on start_time and end_time.
"""

import logging
import os
import subprocess

from utils.time_utils import parse_timestamp


def build_ffmpeg_cut_cmd(
    src: str,
    out: str,
    start: float,
    duration: float,
    reencode: bool = True,
) -> list[str]:
    """Build an ffmpeg cut command.

    Default mode re-encodes (``-c:v libx264`` / ``-c:a aac``) with
    *input-seeking* (``-ss`` placed AFTER ``-i``), so cuts land on the exact
    requested boundary instead of snapping to the nearest keyframe. The source
    is now downloaded as H264 (see ``utils.youtube_downloader``), so decoding is
    clean and does not hit the AV1 corruption that previously crashed re-encode.
    The veryfast preset bounds CPU cost; chapter/short clips are short.

    Set ``reencode=False`` for a stream copy (``-c copy``) with output-seeking
    (``-ss`` before ``-i``): near-instant and never decodes, but cuts snap to the
    nearest keyframe (few-second drift). Useful as a fallback on a suspect source.

    Args:
        src: Path to the source video.
        out: Path where the cut clip is written.
        start: Start offset in seconds from the beginning of ``src``.
        duration: Clip duration in seconds. Must be > 0.
        reencode: When ``True`` (default), re-encode with libx264/aac for frame
            accuracy (input-seek). When ``False``, stream-copy with keyframe
            snapping (no decode).

    Returns:
        The ffmpeg argument list, ready for :func:`subprocess.run`.

    Raises:
        ValueError: If ``duration <= 0``.
    """
    if duration <= 0:
        raise ValueError("clip duration is 0 seconds")

    if reencode:
        # Input-seek (-ss after -i) → frame-accurate, but decodes every frame.
        return [
            'ffmpeg',
            '-y',
            '-i', src,
            '-ss', str(start),
            '-t', str(duration),
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-crf', '20',
            '-c:a', 'aac',
            '-avoid_negative_ts', 'make_zero',
            out,
        ]

    # Output-seek (-ss before -i) + stream copy → no decode, keyframe-snapped.
    return [
        'ffmpeg',
        '-y',
        '-ss', str(start),
        '-i', src,
        '-t', str(duration),
        '-c', 'copy',
        '-avoid_negative_ts', 'make_zero',
        out,
    ]


def compute_ffmpeg_timeout(
    duration_seconds: float,
    base: int = 120,
    factor: float = 8.0,
    max_timeout: int = 3600,
) -> int:
    """Compute an adaptive subprocess timeout for an ffmpeg cut.

    The timeout scales with clip duration because a full re-encode takes longer
    for longer clips. Defaults (base=120, factor=8) are conservative for the
    veryfast re-encode used by :func:`build_ffmpeg_cut_cmd`.

    Args:
        duration_seconds: Clip duration in seconds. Non-positive values yield
            just ``base``.
        base: Fixed overhead in seconds (default 120).
        factor: Seconds of timeout budget per second of clip (default 8.0).
        max_timeout: Hard cap in seconds (default 3600).

    Returns:
        Timeout in whole seconds, never exceeding ``max_timeout``.
    """
    if duration_seconds <= 0:
        return base
    return int(min(max_timeout, base + factor * duration_seconds))


def convert_srt_time_to_seconds(srt_time: str) -> float:
    """Convert an SRT timestamp to total seconds.

    Delegates to ``utils.time_utils.parse_timestamp``, which handles
    ``HH:MM:SS``, ``HH:MM:SS,mmm``, and ``HH:MM:SS.mmm`` formats
    with millisecond precision.

    Args:
        srt_time: Timestamp string, e.g. ``"00:10:15,500"``.

    Returns:
        Total seconds as float, or ``0.0`` on parse error.
    """
    try:
        return parse_timestamp(srt_time)
    except (ValueError, AttributeError) as e:
        logging.error("Error converting SRT time %r: %s", srt_time, e)
        return 0.0


def split_video_chapter(source_video_path, output_path, start_time, end_time):
    """
    Split a video segment using ffmpeg.

    Uses ffmpeg to extract a specific time range from the source video.
    Frame-accurate extraction via input-seeking + re-encode (libx264/aac);
    see :func:`build_ffmpeg_cut_cmd`. Relies on the source being H264 (the
    downloader now forces it) so decoding is clean. The source video is never
    modified — the segment is written to ``output_path`` as a new file.

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

        # Build frame-accurate ffmpeg command (input-seek + re-encode).
        ffmpeg_command = build_ffmpeg_cut_cmd(
            src=source_video_path,
            out=output_path,
            start=start_seconds,
            duration=duration_seconds,
        )
        timeout = compute_ffmpeg_timeout(duration_seconds)

        logging.info(f"Extracting chapter: {start_time} to {end_time} ({duration_seconds:.2f}s)")
        logging.info(f"Command: {' '.join(ffmpeg_command)} (timeout={timeout}s)")

        # Run ffmpeg
        result = subprocess.run(
            ffmpeg_command,
            capture_output=True,
            text=True,
            timeout=timeout
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

        logging.info(f"Processing chapter {chapter_id} from video {video_id}")

        try:
            # Source videos are in: data_directory/downloads/{date}/{video_id}/
            # We need to search for the video across all dates in downloads folder
            downloads_folder = os.path.join(data_directory, 'downloads')

            source_video_path = None

            if os.path.exists(downloads_folder):
                # Search through all date folders for this video_id
                for date_folder in os.listdir(downloads_folder):
                    video_folder = os.path.join(downloads_folder, date_folder, str(video_id))

                    if os.path.exists(video_folder):
                        # Look for video files (mp4, mkv, webm)
                        video_extensions = ['.mp4', '.mkv', '.webm']

                        for file in os.listdir(video_folder):
                            if any(file.endswith(ext) for ext in video_extensions):
                                # Skip chapter videos (to avoid using extracted chapters as source)
                                if 'chapter_video' not in file:
                                    source_video_path = os.path.join(video_folder, file)
                                    break

                        if source_video_path:
                            break  # Found the video, stop searching

            if not source_video_path:
                raise FileNotFoundError(
                    f"No source video found for video_id {video_id} in {downloads_folder}. "
                    f"Make sure youtube_channel_monitor_dag has downloaded the video first."
                )

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

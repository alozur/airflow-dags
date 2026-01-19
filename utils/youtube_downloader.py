"""
YouTube video downloader utility for congress videos.

This module provides functions to download videos from YouTube in formats
ready for processing and uploading.
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import yt_dlp

logger = logging.getLogger(__name__)


def download_with_pytubefix(
    youtube_url: str,
    output_dir: str,
    min_resolution: int = 720,
) -> Dict[str, any]:
    """
    Download YouTube video using pytubefix (alternative to yt-dlp).

    This often works when yt-dlp fails due to YouTube restrictions.
    Downloads adaptive streams (video+audio separately) and merges with ffmpeg.

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save video
        min_resolution: Minimum resolution to download (default 720)

    Returns:
        Dictionary with download info
    """
    try:
        from pytubefix import YouTube
        from pytubefix.cli import on_progress
    except ImportError:
        return {
            "success": False,
            "error": "pytubefix not installed. Install with: pip install pytubefix"
        }

    result = {
        "success": False,
        "file_path": None,
        "file_size_mb": None,
        "duration": None,
        "title": None,
        "resolution": None,
        "error": None,
    }

    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        logger.info(f"[pytubefix] Downloading: {youtube_url}")
        yt = YouTube(youtube_url, on_progress_callback=on_progress)

        video_id = yt.video_id
        safe_title = "".join(c for c in yt.title if c.isalnum() or c in (' ', '-', '_')).strip()[:50]

        # First try: adaptive video stream at min_resolution or higher (720p+)
        video_stream = yt.streams.filter(
            adaptive=True,
            only_video=True,
            file_extension='mp4'
        ).filter(lambda s: s.resolution and int(s.resolution[:-1]) >= min_resolution).order_by('resolution').desc().first()

        if video_stream:
            logger.info(f"[pytubefix] Found adaptive video stream: {video_stream.resolution}")

            # Get best audio stream
            audio_stream = yt.streams.filter(
                adaptive=True,
                only_audio=True
            ).order_by('abr').desc().first()

            if audio_stream:
                logger.info(f"[pytubefix] Found audio stream: {audio_stream.abr}")

                # Download video and audio separately
                video_file = f"{video_id}_video_temp.mp4"
                audio_file = f"{video_id}_audio_temp.{audio_stream.subtype}"
                final_file = f"{video_id}_{safe_title}.mp4"

                logger.info("[pytubefix] Downloading video stream...")
                video_path = video_stream.download(output_path=output_dir, filename=video_file)

                logger.info("[pytubefix] Downloading audio stream...")
                audio_path = audio_stream.download(output_path=output_dir, filename=audio_file)

                # Merge with ffmpeg
                final_path = Path(output_dir) / final_file
                logger.info("[pytubefix] Merging video and audio with ffmpeg...")

                import subprocess
                merge_cmd = [
                    "ffmpeg", "-y",
                    "-i", video_path,
                    "-i", audio_path,
                    "-c:v", "copy",
                    "-c:a", "aac",
                    str(final_path)
                ]

                merge_result = subprocess.run(merge_cmd, capture_output=True, text=True)

                if merge_result.returncode == 0 and final_path.exists():
                    # Clean up temp files
                    Path(video_path).unlink(missing_ok=True)
                    Path(audio_path).unlink(missing_ok=True)

                    file_size_mb = final_path.stat().st_size / (1024 * 1024)

                    result["success"] = True
                    result["file_path"] = str(final_path)
                    result["file_size_mb"] = round(file_size_mb, 2)
                    result["duration"] = yt.length
                    result["title"] = yt.title
                    result["resolution"] = video_stream.resolution

                    logger.info(f"[pytubefix] Download complete: {final_file}")
                    logger.info(f"[pytubefix] Size: {file_size_mb:.2f} MB, Resolution: {video_stream.resolution}")
                    return result
                else:
                    logger.error(f"[pytubefix] ffmpeg merge failed: {merge_result.stderr}")
                    # Clean up on failure
                    Path(video_path).unlink(missing_ok=True)
                    Path(audio_path).unlink(missing_ok=True)

        # Fallback: progressive stream (video+audio combined, usually lower quality)
        logger.info(f"[pytubefix] No {min_resolution}p+ adaptive stream, trying progressive...")
        stream = yt.streams.filter(
            progressive=True,
            file_extension='mp4'
        ).order_by('resolution').desc().first()

        if stream:
            logger.info(f"[pytubefix] Using progressive stream: {stream.resolution}")
            filename = f"{video_id}_{safe_title}.mp4"
            output_path = stream.download(output_path=output_dir, filename=filename)

            if Path(output_path).exists():
                file_size_mb = Path(output_path).stat().st_size / (1024 * 1024)

                result["success"] = True
                result["file_path"] = output_path
                result["file_size_mb"] = round(file_size_mb, 2)
                result["duration"] = yt.length
                result["title"] = yt.title
                result["resolution"] = stream.resolution

                logger.info(f"[pytubefix] Download complete: {filename}")
                logger.info(f"[pytubefix] Size: {file_size_mb:.2f} MB, Resolution: {stream.resolution}")
                return result

        result["error"] = "No suitable stream found"
        logger.error(result["error"])

    except Exception as e:
        result["error"] = f"pytubefix error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result


def download_youtube_video_for_upload(
    youtube_url: str,
    output_dir: str,
    quality: str = "720p",
    cookies_file: str = "/opt/airflow/data/congress_videos/youtube_cookies.txt",
    use_pytubefix_first: bool = True,
) -> Dict[str, any]:
    """
    Download YouTube video in format ready for re-upload to YouTube.

    This downloads a pre-merged video+audio file (no ffmpeg needed).
    Tries pytubefix first (more reliable), then falls back to yt-dlp.

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save video
        quality: Video quality (720p, 1080p, best)
        cookies_file: Path to YouTube cookies.txt file (for bypassing restrictions)
        use_pytubefix_first: Try pytubefix before yt-dlp (default True)

    Returns:
        Dictionary with download info:
        {
            "success": bool,
            "file_path": str,
            "file_size_mb": float,
            "duration": int (seconds),
            "title": str,
            "error": str (if failed)
        }

    Example:
        >>> result = download_youtube_video_for_upload(
        ...     "https://www.youtube.com/watch?v=VIDEO_ID",
        ...     "/path/to/output"
        ... )
        >>> if result["success"]:
        ...     print(f"Downloaded: {result['file_path']}")
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Map quality string to minimum resolution
    quality_to_resolution = {"720p": 720, "1080p": 1080, "best": 720}
    min_resolution = quality_to_resolution.get(quality, 720)

    # Try pytubefix first (often more reliable for YouTube restrictions)
    if use_pytubefix_first:
        logger.info("Trying pytubefix first...")
        result = download_with_pytubefix(youtube_url, output_dir, min_resolution)
        if result["success"]:
            logger.info(f"pytubefix succeeded! Resolution: {result.get('resolution')}")
            return result
        else:
            logger.warning(f"pytubefix failed: {result.get('error')}. Falling back to yt-dlp...")

    # Fall back to yt-dlp
    logger.info("Trying yt-dlp...")

    # Format selection based on quality
    # Use bestvideo+bestaudio with merge to avoid SABR streaming issues
    # height>=720 ensures MINIMUM 720p quality
    format_map = {
        "720p": "bestvideo[height>=720]+bestaudio/bestvideo+bestaudio/best",
        "1080p": "bestvideo[height>=1080]+bestaudio/bestvideo[height>=720]+bestaudio/best",
        "best": "bestvideo[height>=720]+bestaudio/bestvideo+bestaudio/best",
    }

    format_string = format_map.get(quality, format_map["720p"])

    ydl_opts = {
        # Format with fallback: try 720p+ first, then any quality
        "format": f"{format_string}/best",
        "outtmpl": f"{output_dir}/%(id)s_%(title)s.%(ext)s",
        "quiet": False,
        "no_warnings": False,
        # Merge video+audio into mp4
        "merge_output_format": "mp4",
    }

    # Use cookies file if it exists (bypasses YouTube restrictions for 720p+)
    if cookies_file and Path(cookies_file).exists():
        ydl_opts["cookiefile"] = cookies_file
        logger.info(f"Using cookies file: {cookies_file}")

    result = {
        "success": False,
        "file_path": None,
        "file_size_mb": None,
        "duration": None,
        "title": None,
        "error": None,
    }

    try:
        logger.info(f"Downloading video from YouTube: {youtube_url}")
        logger.info(f"Quality: {quality}, Format: {format_string}")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(youtube_url, download=True)
            output_file = ydl.prepare_filename(info)

            # Verify file exists
            file_path = Path(output_file)
            if file_path.exists():
                file_size_mb = file_path.stat().st_size / (1024 * 1024)

                result["success"] = True
                result["file_path"] = str(file_path)
                result["file_size_mb"] = round(file_size_mb, 2)
                result["duration"] = info.get("duration")
                result["title"] = info.get("title")

                logger.info(f"✅ Download complete: {file_path.name}")
                logger.info(f"   Size: {file_size_mb:.2f} MB")
                logger.info(f"   Duration: {info.get('duration_string')}")
                logger.info(f"   Ready for YouTube upload!")
            else:
                result["error"] = f"File not found: {output_file}"
                logger.error(result["error"])

    except yt_dlp.utils.DownloadError as e:
        result["error"] = f"Download error: {str(e)}"
        logger.error(result["error"], exc_info=True)
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result


def download_audio_only(
    youtube_url: str,
    output_dir: str,
    convert_to_mp3: bool = False,
    audio_format: str = "webm",
) -> Dict[str, any]:
    """
    Download audio only from YouTube video.

    Useful for transcription or audio processing.

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save audio
        convert_to_mp3: If True, convert to MP3 (requires ffmpeg)
        audio_format: Audio format to use ("webm" for lighter, "mp3" for compatibility)

    Returns:
        Dictionary with download info
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": f"{output_dir}/%(id)s_%(title)s_audio.%(ext)s",
        "quiet": False,
    }

    # Add format conversion if requested AND ffmpeg is available
    if convert_to_mp3 or audio_format == "mp3":
        ydl_opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ]
    elif audio_format == "webm":
        # Prefer webm audio (lighter and faster)
        # Download raw webm audio without post-processing to avoid ffmpeg dependency
        ydl_opts["format"] = "bestaudio[ext=webm]/bestaudio"
        # Don't add postprocessors - just download the raw audio file

    result = {
        "success": False,
        "file_path": None,
        "file_size_mb": None,
        "duration": None,
        "error": None,
    }

    try:
        logger.info(f"Downloading audio from: {youtube_url}")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(youtube_url, download=True)
            output_file = ydl.prepare_filename(info)

            # If converted to MP3, update extension
            if convert_to_mp3:
                output_file = output_file.replace(f".{info['ext']}", ".mp3")

            file_path = Path(output_file)
            if file_path.exists():
                file_size_mb = file_path.stat().st_size / (1024 * 1024)

                result["success"] = True
                result["file_path"] = str(file_path)
                result["file_size_mb"] = round(file_size_mb, 2)
                result["duration"] = info.get("duration")

                logger.info(f"✅ Audio downloaded: {file_path.name}")
                logger.info(f"   Size: {file_size_mb:.2f} MB")
            else:
                # File might have been downloaded with a different name
                # Try to find the downloaded file in the output directory
                logger.warning(f"Expected file not found: {output_file}")
                logger.info("Searching for downloaded audio file...")

                video_id = info.get('id')
                audio_files = list(Path(output_dir).glob(f"{video_id}*_audio.*"))

                if audio_files:
                    file_path = audio_files[0]
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)

                    result["success"] = True
                    result["file_path"] = str(file_path)
                    result["file_size_mb"] = round(file_size_mb, 2)
                    result["duration"] = info.get("duration")

                    logger.info(f"✅ Found downloaded audio: {file_path.name}")
                    logger.info(f"   Size: {file_size_mb:.2f} MB")
                else:
                    result["error"] = f"Audio file not found after download: {output_file}"
                    logger.error(result["error"])

    except Exception as e:
        result["error"] = f"Error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result


def download_audio_in_chunks(
    youtube_url: str,
    output_dir: str,
    chunk_duration_minutes: int = 10,
    audio_format: str = "webm",
) -> Dict[str, any]:
    """
    Download audio from YouTube and split into time-based chunks.

    This uses yt-dlp to download the full audio, then gets duration info
    to create separate chunk downloads. Works without ffmpeg.

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save audio chunks
        chunk_duration_minutes: Duration of each chunk in minutes (default: 10)
        audio_format: Audio format to use (default: "webm")

    Returns:
        Dictionary with chunk download info:
        {
            "success": bool,
            "total_chunks": int,
            "total_duration": int (seconds),
            "chunks": [
                {
                    "chunk_index": int,
                    "file_path": str,
                    "start_time": int (seconds),
                    "end_time": int (seconds),
                    "duration": int (seconds),
                    "file_size_mb": float
                },
                ...
            ],
            "error": str (if failed)
        }
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    result = {
        "success": False,
        "total_chunks": 0,
        "total_duration": 0,
        "chunks": [],
        "error": None,
    }

    try:
        # First, get video info without downloading
        logger.info(f"Getting video info from: {youtube_url}")

        ydl_opts_info = {
            "quiet": True,
            "no_warnings": True,
        }

        with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            video_id = info.get('id')
            video_title = info.get('title', 'unknown')
            total_duration = info.get('duration', 0)

            if not total_duration:
                result["error"] = "Could not get video duration"
                logger.error(result["error"])
                return result

            result["total_duration"] = total_duration
            logger.info(f"Video duration: {total_duration}s ({total_duration/60:.2f} minutes)")

        # Calculate chunks
        chunk_duration_seconds = chunk_duration_minutes * 60
        num_chunks = (total_duration + chunk_duration_seconds - 1) // chunk_duration_seconds

        logger.info(f"Will download {num_chunks} chunks of ~{chunk_duration_minutes} minutes each")

        # Create audio_chunks subdirectory
        chunks_dir = Path(output_dir) / "audio_chunks"
        chunks_dir.mkdir(parents=True, exist_ok=True)

        # Download each chunk
        for i in range(num_chunks):
            start_time = i * chunk_duration_seconds
            end_time = min((i + 1) * chunk_duration_seconds, total_duration)

            # Format: HH:MM:SS
            start_str = f"{start_time//3600:02d}:{(start_time%3600)//60:02d}:{start_time%60:02d}"
            end_str = f"{end_time//3600:02d}:{(end_time%3600)//60:02d}:{end_time%60:02d}"

            chunk_filename = f"{video_id}_chunk_{i:03d}.{audio_format}"
            chunk_path = chunks_dir / chunk_filename

            logger.info(f"Downloading chunk {i+1}/{num_chunks}: {start_str} - {end_str}")

            ydl_opts_chunk = {
                "format": "bestaudio[ext=webm]/bestaudio",
                "outtmpl": str(chunk_path),
                "quiet": False,
                "download_ranges": yt_dlp.utils.download_range_func(None, [(start_time, end_time)]),
                "force_keyframes_at_cuts": True,
            }

            try:
                with yt_dlp.YoutubeDL(ydl_opts_chunk) as ydl:
                    ydl.download([youtube_url])

                if chunk_path.exists():
                    file_size_mb = chunk_path.stat().st_size / (1024 * 1024)

                    chunk_info = {
                        "chunk_index": i,
                        "file_path": str(chunk_path),
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration": end_time - start_time,
                        "file_size_mb": round(file_size_mb, 2),
                    }

                    result["chunks"].append(chunk_info)
                    logger.info(f"✅ Chunk {i+1} downloaded: {chunk_filename} ({file_size_mb:.2f} MB)")
                else:
                    logger.warning(f"Chunk {i+1} file not found: {chunk_path}")

            except Exception as e:
                logger.error(f"Error downloading chunk {i+1}: {e}")
                # Continue with next chunk

        result["total_chunks"] = len(result["chunks"])
        result["success"] = result["total_chunks"] > 0

        if result["success"]:
            logger.info(f"✅ Successfully downloaded {result['total_chunks']}/{num_chunks} chunks")
        else:
            result["error"] = "No chunks were downloaded successfully"
            logger.error(result["error"])

    except Exception as e:
        result["error"] = f"Error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result


def merge_video_audio_ffmpeg(
    video_path: str,
    audio_path: str,
    output_path: str,
) -> Dict[str, any]:
    """
    Merge separate video and audio files using ffmpeg.

    Requires ffmpeg installed on system.

    Args:
        video_path: Path to video file (no audio)
        audio_path: Path to audio file
        output_path: Path for output merged file

    Returns:
        Dictionary with merge result
    """
    import subprocess

    result = {
        "success": False,
        "output_path": None,
        "error": None,
    }

    try:
        logger.info(f"Merging video and audio with ffmpeg...")
        logger.info(f"  Video: {video_path}")
        logger.info(f"  Audio: {audio_path}")
        logger.info(f"  Output: {output_path}")

        # FFmpeg command: copy video, convert audio to AAC (compatible everywhere)
        cmd = [
            "ffmpeg",
            "-i",
            video_path,
            "-i",
            audio_path,
            "-c:v",
            "copy",  # Copy video stream (no re-encoding, fast!)
            "-c:a",
            "aac",  # Convert audio to AAC (compatible with all players & YouTube)
            "-y",  # Overwrite output file
            output_path,
        ]

        # Run ffmpeg
        subprocess.run(cmd, check=True, capture_output=True)

        if Path(output_path).exists():
            result["success"] = True
            result["output_path"] = output_path
            logger.info(f"✅ Merge complete: {output_path}")
        else:
            result["error"] = "Output file not created"

    except FileNotFoundError:
        result["error"] = "ffmpeg not found. Install with: conda install -c conda-forge ffmpeg"
        logger.error(result["error"])
    except subprocess.CalledProcessError as e:
        result["error"] = f"FFmpeg error: {e.stderr.decode()}"
        logger.error(result["error"])
    except Exception as e:
        result["error"] = f"Error: {str(e)}"
        logger.error(result["error"])

    return result


def merge_video_audio_moviepy(
    video_path: str,
    audio_path: str,
    output_path: str,
) -> Dict[str, any]:
    """
    Merge separate video and audio files using MoviePy (no ffmpeg binary needed).

    WARNING: This is MUCH slower than ffmpeg (re-encodes everything).
    Use only if ffmpeg is not available.

    Args:
        video_path: Path to video file (no audio)
        audio_path: Path to audio file
        output_path: Path for output merged file

    Returns:
        Dictionary with merge result
    """
    try:
        from moviepy.editor import AudioFileClip, VideoFileClip
    except ImportError:
        return {
            "success": False,
            "error": "MoviePy not installed. Install with: pip install moviepy",
        }

    result = {
        "success": False,
        "output_path": None,
        "error": None,
    }

    try:
        logger.info(f"Merging video and audio with MoviePy (this may take a while)...")
        logger.info(f"  Video: {video_path}")
        logger.info(f"  Audio: {audio_path}")
        logger.info(f"  Output: {output_path}")

        video = VideoFileClip(video_path)
        audio = AudioFileClip(audio_path)

        final = video.set_audio(audio)
        final.write_videofile(output_path, codec="libx264", audio_codec="aac")

        # Clean up
        video.close()
        audio.close()
        final.close()

        if Path(output_path).exists():
            result["success"] = True
            result["output_path"] = output_path
            logger.info(f"✅ Merge complete: {output_path}")

    except Exception as e:
        result["error"] = f"Error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result

def download_youtube_subtitles(
    youtube_url: str,
    output_dir: str,
    languages: List[str] = None
) -> Dict:
    """
    Download SRT subtitles directly from YouTube if available.

    This function tries to download existing subtitles from YouTube in the specified
    languages. This is much faster than transcribing and should be tried first.

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save subtitle files
        languages: List of language codes to try (default: ['es', 'es-ES', 'en', 'auto'])

    Returns:
        Dictionary with download info:
        {
            "success": bool,
            "has_subtitles": bool,
            "subtitle_files": [{language, file_path, file_size_mb, is_auto_generated}],
            "merged_srt_path": str (if successful),
            "error": str (if failed)
        }
    """
    if languages is None:
        languages = ['es', 'es-ES', 'en', 'auto']

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    result = {
        "success": False,
        "has_subtitles": False,
        "subtitle_files": [],
        "merged_srt_path": None,
        "error": None
    }

    try:
        logger.info(f"Checking for available subtitles: {youtube_url}")

        # First, get video info to check available subtitles
        ydl_opts_info = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True
        }

        with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            video_id = info.get('id')
            available_subtitles = info.get('subtitles', {})
            automatic_captions = info.get('automatic_captions', {})

            # Check if any subtitles are available
            if not available_subtitles and not automatic_captions:
                logger.info("No subtitles available for this video")
                result['error'] = "No subtitles available"
                return result

            result['has_subtitles'] = True
            logger.info(f"Found subtitles! Manual: {list(available_subtitles.keys())}, Auto: {list(automatic_captions.keys())}")

        # Try to download subtitles in order of preference
        downloaded_files = []

        for lang in languages:
            try:
                logger.info(f"Attempting to download subtitles for language: {lang}")

                # Create srt_files directory
                srt_dir = Path(output_dir) / "srt_files"
                srt_dir.mkdir(parents=True, exist_ok=True)

                ydl_opts = {
                    'skip_download': True,  # Don't download the video
                    'writesubtitles': True,  # Download subtitles
                    'writeautomaticsub': True,  # Include auto-generated subtitles
                    'subtitleslangs': [lang],  # Language to download
                    'subtitlesformat': 'srt',  # SRT format
                    'outtmpl': str(srt_dir / f'{video_id}_%(lang)s'),
                    'quiet': False,
                    'no_warnings': False,
                }

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([youtube_url])

                # Check if file was downloaded
                possible_files = list(srt_dir.glob(f'{video_id}*.srt'))

                if possible_files:
                    for srt_file in possible_files:
                        file_size_mb = srt_file.stat().st_size / (1024 * 1024)

                        # Determine if it's auto-generated
                        is_auto = 'auto' in srt_file.name.lower() or lang == 'auto'

                        downloaded_files.append({
                            'language': lang,
                            'file_path': str(srt_file),
                            'file_size_mb': round(file_size_mb, 2),
                            'is_auto_generated': is_auto
                        })

                        logger.info(f"✅ Downloaded {lang} subtitles: {srt_file.name} ({file_size_mb:.2f} MB)")

                    # If we found subtitles, we can stop trying other languages
                    break

            except Exception as e:
                logger.debug(f"Could not download {lang} subtitles: {e}")
                continue

        if not downloaded_files:
            result['error'] = "Failed to download subtitles in any language"
            logger.warning(result['error'])
            return result

        # If we downloaded subtitles, create a merged/simplified version
        result['subtitle_files'] = downloaded_files
        result['success'] = True

        # Use the first downloaded file as the main subtitle file
        main_subtitle = downloaded_files[0]['file_path']

        # Copy subtitle content to merged file (ensure merged file has content)
        try:
            # Create merged/simplified version
            srt_dir = Path(output_dir) / "srt_files"
            merged_path = srt_dir / f"{video_id}_merged.srt"

            # Simply copy the content from the downloaded subtitle to merged file
            logger.info(f"Creating merged subtitle file from: {main_subtitle}")

            with open(main_subtitle, 'r', encoding='utf-8') as src:
                subtitle_content = src.read()

            with open(merged_path, 'w', encoding='utf-8') as dst:
                dst.write(subtitle_content)

            # Verify the file was written correctly
            if merged_path.exists() and merged_path.stat().st_size > 0:
                result['merged_srt_path'] = str(merged_path)
                logger.info(f"✅ Created merged subtitle file: {merged_path} ({merged_path.stat().st_size} bytes)")
            else:
                # If copy fails, use the original
                result['merged_srt_path'] = main_subtitle
                logger.warning(f"Merged file is empty, using original: {main_subtitle}")

        except Exception as e:
            logger.warning(f"Could not copy subtitles to merged file: {e}")
            result['merged_srt_path'] = main_subtitle

        logger.info(f"✅ Successfully downloaded subtitles from YouTube!")
        logger.info(f"   Languages: {[f['language'] for f in downloaded_files]}")
        logger.info(f"   Main subtitle: {result['merged_srt_path']}")

    except Exception as e:
        result['error'] = f"Error: {str(e)}"
        logger.error(result['error'], exc_info=True)

    return result

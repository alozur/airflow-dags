"""
YouTube video downloader utility for congress videos.

This module provides functions to download videos from YouTube in formats
ready for processing and uploading.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import yt_dlp

logger = logging.getLogger(__name__)


def download_youtube_video_for_upload(
    youtube_url: str,
    output_dir: str,
    quality: str = "720p",
) -> Dict[str, any]:
    """
    Download YouTube video in format ready for re-upload to YouTube.

    This downloads a pre-merged video+audio file (no ffmpeg needed).

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save video
        quality: Video quality (720p, 1080p, best)

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

    # Format selection based on quality
    format_map = {
        "720p": "best[ext=mp4][height<=720]/best[height<=720]",
        "1080p": "best[ext=mp4][height<=1080]/best[height<=1080]",
        "best": "best[ext=mp4]/best",
    }

    format_string = format_map.get(quality, format_map["720p"])

    ydl_opts = {
        "format": format_string,  # Pre-merged video+audio
        "outtmpl": f"{output_dir}/%(id)s_%(title)s.%(ext)s",
        "quiet": False,
        "no_warnings": False,
    }

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

        # Create chunks subdirectory
        chunks_dir = Path(output_dir) / "chunks"
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

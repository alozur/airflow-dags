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
) -> Dict[str, any]:
    """
    Download audio only from YouTube video.

    Useful for transcription or audio processing.

    Args:
        youtube_url: YouTube video URL
        output_dir: Directory to save audio
        convert_to_mp3: If True, convert to MP3 (requires ffmpeg)

    Returns:
        Dictionary with download info
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": f"{output_dir}/%(id)s_%(title)s_audio.%(ext)s",
        "quiet": False,
    }

    # Add MP3 conversion if requested
    if convert_to_mp3:
        ydl_opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ]

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

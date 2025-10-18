"""
AI-powered chapter analysis for video transcriptions.

This module uses OpenAI to analyze video transcriptions and agendas
to identify interesting chapters/segments for content extraction.
"""

import json
import logging
import os
from typing import Dict, List, Optional

import openai

from congress_videos.config.ai_prompts import (
    CHAPTER_ANALYSIS_SYSTEM_PROMPT_TEMPLATE,
    CHAPTER_ANALYSIS_USER_PROMPT_TEMPLATE
)

logger = logging.getLogger(__name__)

# OpenAI API configuration
openai.api_key = os.getenv('OPENAI_API_KEY')


def parse_timestamp_to_seconds(timestamp: str) -> int:
    """
    Convert SRT timestamp (HH:MM:SS) to total seconds.

    Args:
        timestamp: Time in format "HH:MM:SS" or "H:MM:SS"

    Returns:
        Total seconds as integer
    """
    parts = timestamp.strip().split(':')
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    return 0


def format_seconds_to_timestamp(seconds: int) -> str:
    """
    Convert seconds to HH:MM:SS format.

    Args:
        seconds: Total seconds

    Returns:
        Formatted timestamp string
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def analyze_chapters_with_ai(
    srt_content: str,
    agenda_content: str,
    min_duration_minutes: int = 15,
    max_duration_minutes: int = 30,
    model: str = "gpt-4o-mini"
) -> Dict:
    """
    Use AI to identify topic changes in transcription based on content similarity.

    Simplified approach:
    1. Read the full transcription (SRT format with timestamps)
    2. Read the session agenda for context
    3. Identify when topics change based on content similarity
    4. Create chapter boundaries at topic changes
    5. Return structured chapter data with timestamps

    Args:
        srt_content: Full transcription in SRT format with timestamps
        agenda_content: Session agenda text (for context)
        min_duration_minutes: Minimum chapter duration (default: 15)
        max_duration_minutes: Maximum chapter duration (default: 30)
        model: OpenAI model to use (default: "gpt-3.5-turbo")

    Returns:
        Dict with chapter analysis results:
        {
            "success": bool,
            "total_chapters": int,
            "total_duration_seconds": int,
            "chapters": [
                {
                    "chapter_number": int,
                    "title": str,
                    "start_time": str (HH:MM:SS),
                    "end_time": str (HH:MM:SS),
                    "duration_seconds": int,
                    "topics": [str]
                }
            ],
            "error": str (if failed)
        }
    """
    result = {
        "success": False,
        "total_chapters": 0,
        "total_duration_seconds": 0,
        "chapters": [],
        "error": None
    }

    if not openai.api_key:
        result["error"] = "OpenAI API key not configured"
        logger.error(result["error"])
        return result

    try:
        logger.info("Identifying topic changes in transcription using AI...")
        logger.info(f"Target chapter duration: {min_duration_minutes}-{max_duration_minutes} minutes")

        # Use prompts from configuration file
        system_prompt = CHAPTER_ANALYSIS_SYSTEM_PROMPT_TEMPLATE.format(
            min_duration=min_duration_minutes,
            max_duration=max_duration_minutes
        )

        # Use full SRT content without truncation
        user_prompt = CHAPTER_ANALYSIS_USER_PROMPT_TEMPLATE.format(
            agenda_content=agenda_content,
            srt_content=srt_content,
            min_duration_minutes=min_duration_minutes,
            max_duration_minutes=max_duration_minutes
        )

        # Call OpenAI API
        logger.info(f"Calling OpenAI API with model: {model}")

        client = openai.OpenAI(api_key=openai.api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,  # Low temperature for consistent output
            max_tokens=2000
        )

        # Extract and parse the response
        ai_response = response.choices[0].message.content.strip()
        logger.info(f"Received AI response ({len(ai_response)} characters)")

        # Remove markdown code blocks if present
        if ai_response.startswith('```'):
            ai_response = ai_response.split('\n', 1)[1]
            ai_response = ai_response.rsplit('```', 1)[0]

        # Parse JSON response
        try:
            chapters_data = json.loads(ai_response.strip())
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            logger.error(f"AI response was: {ai_response[:500]}")
            result["error"] = f"AI returned invalid JSON: {str(e)}"
            return result

        # Process chapters
        chapters = chapters_data.get('chapters', [])

        for chapter in chapters:
            # Calculate duration
            start_seconds = parse_timestamp_to_seconds(chapter['start_time'])
            end_seconds = parse_timestamp_to_seconds(chapter['end_time'])
            duration_seconds = end_seconds - start_seconds

            chapter['duration_seconds'] = duration_seconds
            chapter['duration_minutes'] = round(duration_seconds / 60, 1)

        # Sort chapters by start time
        chapters.sort(key=lambda x: parse_timestamp_to_seconds(x['start_time']))

        # Calculate total duration
        if chapters:
            first_start = parse_timestamp_to_seconds(chapters[0]['start_time'])
            last_end = parse_timestamp_to_seconds(chapters[-1]['end_time'])
            total_duration = last_end - first_start
        else:
            total_duration = 0

        result['success'] = True
        result['total_chapters'] = len(chapters)
        result['total_duration_seconds'] = total_duration
        result['chapters'] = chapters

        logger.info(f"✅ Identified {len(chapters)} topic changes")
        for i, chapter in enumerate(chapters, 1):
            logger.info(f"  Chapter {i}: {chapter['title']} ({chapter['start_time']} - {chapter['end_time']}, {chapter.get('duration_minutes', 0):.1f} min)")

    except openai.OpenAIError as e:
        result['error'] = f"OpenAI API error: {str(e)}"
        logger.error(result['error'], exc_info=True)
    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        logger.error(result['error'], exc_info=True)

    return result

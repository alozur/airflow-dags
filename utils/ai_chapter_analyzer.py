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
    model: str = "gpt-3.5-turbo"
) -> Dict:
    """
    Use AI to analyze transcription and agenda to identify interesting chapters.

    The AI will:
    1. Read the full transcription (SRT format with timestamps)
    2. Read the session agenda
    3. Identify interesting topics/discussions
    4. Group related content into chapters (15-30 minutes each)
    5. Ensure chapters don't cut off mid-discussion or mid-speech
    6. Return structured chapter data with timestamps

    Args:
        srt_content: Full transcription in SRT format with timestamps
        agenda_content: Session agenda text
        min_duration_minutes: Minimum chapter duration (default: 15)
        max_duration_minutes: Maximum chapter duration (default: 30)
        model: OpenAI model to use (default: "gpt-4o-mini")

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
                    "description": str,
                    "start_time": str (HH:MM:SS),
                    "end_time": str (HH:MM:SS),
                    "duration_seconds": int,
                    "topics": [str],
                    "speakers": [str] (if identifiable),
                    "interest_score": int (1-10)
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
        logger.info("Analyzing transcription and agenda with AI to identify chapters...")
        logger.info(f"Target chapter duration: {min_duration_minutes}-{max_duration_minutes} minutes")

        # Prepare the prompt for the AI
        system_prompt = """You are an expert video content analyzer specializing in parliamentary sessions.
Your task is to analyze transcriptions and agendas to identify interesting chapters/segments for video content.

IMPORTANT RULES:
1. Each chapter must be between {min_duration} and {max_duration} minutes long
2. NEVER cut off in the middle of a discussion, topic, or speaker's intervention
3. Find natural breaking points (end of topics, speaker changes, procedural breaks)
4. Group related topics together when they form a coherent narrative
5. Prioritize content that is interesting, controversial, or newsworthy
6. Each chapter should have a clear theme or focus

OUTPUT FORMAT:
Return ONLY a valid JSON object (no markdown, no code blocks) with this structure:
{{
  "chapters": [
    {{
      "chapter_number": 1,
      "title": "Short descriptive title",
      "description": "Brief description of what happens in this chapter",
      "start_time": "HH:MM:SS",
      "end_time": "HH:MM:SS",
      "topics": ["topic1", "topic2"],
      "interest_score": 8
    }}
  ]
}}

The interest_score should be 1-10 based on how interesting/newsworthy the content is.""".format(
            min_duration=min_duration_minutes,
            max_duration=max_duration_minutes
        )

        user_prompt = f"""Analyze this parliamentary session and identify interesting chapters for video content.

=== SESSION AGENDA ===
{agenda_content}

=== FULL TRANSCRIPTION (with timestamps) ===
{srt_content[:30000]}

{'...(transcription truncated for context length)...' if len(srt_content) > 30000 else ''}

TASK: Identify 3-8 interesting chapters that:
- Are {min_duration_minutes}-{max_duration_minutes} minutes long
- Don't cut off mid-discussion or mid-speech
- Group related topics together
- Focus on newsworthy or interesting content

Return ONLY the JSON object, no other text."""

        # Call OpenAI API
        logger.info(f"Calling OpenAI API with model: {model}")

        client = openai.OpenAI(api_key=openai.api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,  # Lower temperature for more consistent structured output
            max_tokens=4000
        )

        # Extract and parse the response
        ai_response = response.choices[0].message.content.strip()
        logger.info(f"Received AI response ({len(ai_response)} characters)")

        # Remove markdown code blocks if present
        if ai_response.startswith('```'):
            # Remove opening ```json or ```
            ai_response = ai_response.split('\n', 1)[1]
            # Remove closing ```
            ai_response = ai_response.rsplit('```', 1)[0]

        # Parse JSON response
        try:
            chapters_data = json.loads(ai_response.strip())
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            logger.error(f"AI response was: {ai_response[:500]}")
            result["error"] = f"AI returned invalid JSON: {str(e)}"
            return result

        # Process and validate chapters
        chapters = chapters_data.get('chapters', [])

        for chapter in chapters:
            # Calculate duration
            start_seconds = parse_timestamp_to_seconds(chapter['start_time'])
            end_seconds = parse_timestamp_to_seconds(chapter['end_time'])
            duration_seconds = end_seconds - start_seconds

            chapter['duration_seconds'] = duration_seconds
            chapter['duration_minutes'] = round(duration_seconds / 60, 1)

            # Validate duration is within acceptable range
            duration_minutes = duration_seconds / 60
            if duration_minutes < min_duration_minutes * 0.8:  # Allow 20% tolerance
                logger.warning(f"Chapter {chapter['chapter_number']} is shorter than minimum ({duration_minutes:.1f} min)")
            if duration_minutes > max_duration_minutes * 1.2:  # Allow 20% tolerance
                logger.warning(f"Chapter {chapter['chapter_number']} is longer than maximum ({duration_minutes:.1f} min)")

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

        logger.info(f"✅ Successfully identified {len(chapters)} interesting chapters")
        for i, chapter in enumerate(chapters, 1):
            logger.info(f"  Chapter {i}: {chapter['title']} ({chapter['start_time']} - {chapter['end_time']}, {chapter.get('duration_minutes', 0):.1f} min)")

    except openai.OpenAIError as e:
        result['error'] = f"OpenAI API error: {str(e)}"
        logger.error(result['error'], exc_info=True)
    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        logger.error(result['error'], exc_info=True)

    return result

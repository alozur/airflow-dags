"""
Thumbnail generation module for Congress videos.

Generates eye-catching YouTube thumbnails with:
- Congress chamber background
- AI-generated impactful text (3-6 words)
- Channel logo
- Session number
- Gold border
- Professional styling with shadows and effects
"""

import logging
import os
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageEnhance
from openai import OpenAI
from congress_videos.config.ai_prompts import (
    THUMBNAIL_TEXT_SYSTEM_PROMPT,
    THUMBNAIL_TEXT_USER_PROMPT_TEMPLATE
)
from congress_videos.config.paths import (
    FONT_BOLD,
    FONT_REGULAR,
    BACKGROUND_IMAGE,
    CHANNEL_LOGO
)


def truncate_to_complete_words(text, max_length):
    """
    Truncate text to fit within max_length without cutting words.

    Args:
        text: Text to truncate
        max_length: Maximum character length

    Returns:
        Truncated text with complete words only
    """
    if len(text) <= max_length:
        return text

    # Find the last space before max_length
    truncated = text[:max_length]
    last_space = truncated.rfind(' ')

    if last_space > 0:
        # Truncate at the last complete word
        return truncated[:last_space].strip()
    else:
        # No space found, return first word only
        first_word = text.split()[0] if text.split() else text
        return first_word[:max_length].strip()


def generate_thumbnail_text(video_title, video_description, max_length=40, max_attempts=5):
    """
    Generate short, impactful text for thumbnail using AI (3-6 words max).

    Focuses on creating attention-grabbing phrases that encourage clicks.
    Retries if AI generates text longer than max_length.

    Args:
        video_title: Original video title
        video_description: Video description for context
        max_length: Maximum character length (default 40)
        max_attempts: Maximum number of AI generation attempts (default 5)

    Returns:
        Dict with thumbnail_text and metadata
    """
    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Use prompts from config file
        user_prompt = THUMBNAIL_TEXT_USER_PROMPT_TEMPLATE.format(
            video_title=video_title,
            video_description=video_description[:200],  # Truncate description for context
            max_length=max_length
        )

        thumbnail_text = None

        # Retry loop: ask AI again if text is too long
        for attempt in range(1, max_attempts + 1):
            logging.info(f"Generating thumbnail text (attempt {attempt}/{max_attempts})...")

            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": THUMBNAIL_TEXT_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.9,  # Higher creativity for attention-grabbing
                max_tokens=30
            )

            thumbnail_text = response.choices[0].message.content.strip()

            # Remove quotes if AI added them
            thumbnail_text = thumbnail_text.strip('"').strip("'").strip()

            # Check if it fits within max_length
            if len(thumbnail_text) <= max_length:
                logging.info(f"✓ Generated valid thumbnail text on attempt {attempt}: '{thumbnail_text}' ({len(thumbnail_text)} chars)")
                break
            else:
                logging.warning(f"✗ Attempt {attempt} too long: '{thumbnail_text}' ({len(thumbnail_text)} chars > {max_length}). Retrying...")

                # Update prompt to be more strict for next attempt
                user_prompt = THUMBNAIL_TEXT_USER_PROMPT_TEMPLATE.format(
                    video_title=video_title,
                    video_description=video_description[:200],
                    max_length=max_length
                ) + f"\n\nINTENTO {attempt+1}: Tu intento anterior fue demasiado largo ({len(thumbnail_text)} caracteres). ¡Hazlo MÁS CORTO!"

        # If all attempts failed, truncate smartly
        if len(thumbnail_text) > max_length:
            logging.warning(f"All {max_attempts} attempts exceeded {max_length} chars. Truncating: '{thumbnail_text}'")
            thumbnail_text = truncate_to_complete_words(thumbnail_text, max_length)
            logging.info(f"Truncated to: '{thumbnail_text}' ({len(thumbnail_text)} chars)")

        word_count = len(thumbnail_text.split())

        return {
            "thumbnail_text": thumbnail_text.upper(),  # Uppercase for impact
            "word_count": word_count,
            "char_count": len(thumbnail_text),
            "success": True,
            "error": None
        }

    except Exception as e:
        logging.error(f"Error generating thumbnail text: {e}")

        # Fallback: extract first 3-5 words from title
        words = video_title.split()[:5]
        fallback_text = " ".join(words).upper()

        # Smart truncation for fallback too
        if len(fallback_text) > max_length:
            fallback_text = truncate_to_complete_words(fallback_text, max_length)

        return {
            "thumbnail_text": fallback_text,
            "word_count": len(fallback_text.split()),
            "char_count": len(fallback_text),
            "success": False,
            "error": str(e)
        }


def split_text_into_lines(text, font, max_width):
    """Split text into multiple lines if it exceeds max width."""
    words = text.split()
    lines = []
    current_line = []

    temp_image = Image.new('RGB', (1, 1))
    temp_draw = ImageDraw.Draw(temp_image)

    for word in words:
        test_line = ' '.join(current_line + [word])
        bbox = temp_draw.textbbox((0, 0), test_line, font=font)
        line_width = bbox[2] - bbox[0]

        if line_width <= max_width:
            current_line.append(word)
        else:
            if current_line:
                lines.append(' '.join(current_line))
                current_line = [word]
            else:
                lines.append(word)

    if current_line:
        lines.append(' '.join(current_line))

    return lines if lines else [text]


def calculate_font_size_multiline(text, image_width, image_height, max_width_ratio=0.85, max_height_ratio=0.55):
    """Calculate optimal font size for text with multi-line support."""
    max_text_width = int(image_width * max_width_ratio)
    max_text_height = int(image_height * max_height_ratio)

    font_size = 200
    min_font_size = 40

    # Font paths for Linux (Docker environment)
    font_paths = [
        FONT_BOLD,  # Project fonts folder (centralized config)
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",  # System fallback
        "C:/Windows/Fonts/ariblk.ttf",  # Windows fallback (for local testing)
    ]

    font_path = None
    for path in font_paths:
        if os.path.exists(path):
            font_path = path
            break

    while font_size > min_font_size:
        try:
            if font_path:
                font = ImageFont.truetype(font_path, font_size)
            else:
                font = ImageFont.load_default()
                break

            lines = split_text_into_lines(text, font, max_text_width)

            temp_image = Image.new('RGB', (1, 1))
            temp_draw = ImageDraw.Draw(temp_image)

            line_height = 0
            max_line_width = 0
            for line in lines:
                bbox = temp_draw.textbbox((0, 0), line, font=font)
                line_width = bbox[2] - bbox[0]
                line_h = bbox[3] - bbox[1]
                max_line_width = max(max_line_width, line_width)
                line_height = max(line_height, line_h)

            line_spacing = int(line_height * 0.2)
            total_height = len(lines) * line_height + (len(lines) - 1) * line_spacing

            if max_line_width <= max_text_width and total_height <= max_text_height:
                return font_size, font, lines, line_spacing

            font_size -= 5

        except Exception as e:
            logging.warning(f"Error with font size {font_size}: {e}")
            font_size -= 10

    if font_path:
        font = ImageFont.truetype(font_path, min_font_size)
    else:
        font = ImageFont.load_default()

    lines = split_text_into_lines(text, font, max_text_width)
    return min_font_size, font, lines, 10


def darken_image(image, factor=0.70):
    """Darken the image to make text more readable."""
    enhancer = ImageEnhance.Brightness(image)
    return enhancer.enhance(factor)


def create_thumbnail(
    background_image_path,
    text,
    output_path,
    session_number,
    logo_path=None,
    youtube_size=(1280, 720)
):
    """
    Create a YouTube thumbnail with impactful text overlay and gold border.

    Args:
        background_image_path: Path to Congress chamber background image
        text: Impactful text to display (3-6 words)
        output_path: Path to save thumbnail
        session_number: Congress session number
        logo_path: Path to channel logo (optional)
        youtube_size: Thumbnail dimensions (default 1280x720)

    Returns:
        Dict with success status and thumbnail info
    """
    try:
        # Load and resize background
        bg_image = Image.open(background_image_path)
        bg_image = bg_image.resize(youtube_size, Image.Resampling.LANCZOS)

        # Darken background for better text contrast
        bg_image = darken_image(bg_image, factor=0.70)

        width, height = bg_image.size

        # Add bright gold border
        border_width = 8
        border_color = (255, 215, 0)  # Bright gold
        draw_border = ImageDraw.Draw(bg_image)
        for i in range(border_width):
            draw_border.rectangle(
                [i, i, width - 1 - i, height - 1 - i],
                outline=border_color,
                width=1
            )

        # Calculate font size and split text
        font_size, font, lines, line_spacing = calculate_font_size_multiline(text, width, height)

        draw = ImageDraw.Draw(bg_image)

        # Calculate text dimensions
        line_heights = []
        line_widths = []
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font)
            line_widths.append(bbox[2] - bbox[0])
            line_heights.append(bbox[3] - bbox[1])

        max_line_width = max(line_widths)
        total_text_height = sum(line_heights) + line_spacing * (len(lines) - 1)

        # Position text in center (42% from top)
        start_y = int(height * 0.42) - (total_text_height // 2)

        # Text effects settings
        shadow_color = (0, 0, 0)
        shadow_offset = 8
        shadow_width = 6
        outline_color = (0, 0, 0)
        outline_width = 5
        text_color = (255, 255, 255)

        # Draw each line with effects
        current_y = start_y
        for i, line in enumerate(lines):
            line_width = line_widths[i]
            line_x = (width - line_width) // 2

            # Shadow
            for offset_x in range(-shadow_width, shadow_width + 1):
                for offset_y in range(-shadow_width, shadow_width + 1):
                    draw.text(
                        (line_x + shadow_offset + offset_x, current_y + shadow_offset + offset_y),
                        line,
                        font=font,
                        fill=shadow_color
                    )

            # Outline
            for offset_x in range(-outline_width, outline_width + 1):
                for offset_y in range(-outline_width, outline_width + 1):
                    if offset_x != 0 or offset_y != 0:
                        draw.text(
                            (line_x + offset_x, current_y + offset_y),
                            line,
                            font=font,
                            fill=outline_color
                        )

            # Main text
            draw.text((line_x, current_y), line, font=font, fill=text_color)

            current_y += line_heights[i] + line_spacing

        # Add session number at bottom (different style)
        session_text = f"Sesión {session_number}"
        session_font_size = int(font_size * 0.35)

        # Session font (lighter, regular weight for Linux)
        session_font_paths = [
            FONT_REGULAR,  # Project fonts folder (centralized config)
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",  # System fallback
            "C:/Windows/Fonts/calibri.ttf",  # Windows fallback (for local testing)
        ]

        session_font = None
        for path in session_font_paths:
            if os.path.exists(path):
                session_font = ImageFont.truetype(path, session_font_size)
                break

        if not session_font:
            session_font = font

        # Position session text
        session_bbox = draw.textbbox((0, 0), session_text, font=session_font)
        session_width = session_bbox[2] - session_bbox[0]
        session_height = session_bbox[3] - session_bbox[1]
        session_x = (width - session_width) // 2
        session_y = int(height * 0.85) - (session_height // 2)

        # Session text effects (lighter style)
        session_text_color = (255, 215, 0)  # Gold
        session_shadow_width = 3
        session_shadow_offset = 4
        session_outline_width = 2

        # Draw session shadow
        for offset_x in range(-session_shadow_width, session_shadow_width + 1):
            for offset_y in range(-session_shadow_width, session_shadow_width + 1):
                draw.text(
                    (session_x + session_shadow_offset + offset_x, session_y + session_shadow_offset + offset_y),
                    session_text,
                    font=session_font,
                    fill=(0, 0, 0)
                )

        # Draw session outline
        for offset_x in range(-session_outline_width, session_outline_width + 1):
            for offset_y in range(-session_outline_width, session_outline_width + 1):
                if offset_x != 0 or offset_y != 0:
                    draw.text(
                        (session_x + offset_x, session_y + offset_y),
                        session_text,
                        font=session_font,
                        fill=(0, 0, 0)
                    )

        # Draw session text
        draw.text((session_x, session_y), session_text, font=session_font, fill=session_text_color)

        # Add circular logo if provided
        if logo_path and os.path.exists(logo_path):
            try:
                logo_size = int(width * 0.06)
                logo = Image.open(logo_path).convert('RGBA')
                logo = logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)

                logo_x = int(width * 0.03)
                logo_y = int(height * 0.05)

                bg_image = bg_image.convert('RGBA')
                bg_image.paste(logo, (logo_x, logo_y), logo)
                bg_image = bg_image.convert('RGB')
            except Exception as e:
                logging.warning(f"Error adding logo: {e}")

        # Save thumbnail
        bg_image.save(output_path, quality=95, optimize=True)

        logging.info(f"Thumbnail created successfully: {output_path}")

        return {
            "success": True,
            "output_path": output_path,
            "thumbnail_size": youtube_size,
            "text": text,
            "lines": lines,
            "line_count": len(lines),
            "font_size": font_size,
            "session_number": session_number,
            "error": None
        }

    except Exception as e:
        logging.error(f"Failed to create thumbnail: {e}")
        return {
            "success": False,
            "output_path": None,
            "error": str(e)
        }


def generate_thumbnail_text_for_videos(queued_videos, youtube_metadata_results):
    """
    Generate short, attention-grabbing text for thumbnails using AI.

    Args:
        queued_videos: List of chapters from uploadable_chapters view
        youtube_metadata_results: Results dict from generate_youtube_metadata_for_selected_videos
                                  containing 'topic_metadata' list

    Returns:
        Dict with thumbnail text results
    """
    if not queued_videos or not youtube_metadata_results:
        logging.warning("No chapters or metadata for thumbnail text generation")
        return {"results": []}

    # Extract the topic_metadata list from the results dict
    topic_metadata = youtube_metadata_results.get('topic_metadata', [])
    if not topic_metadata:
        logging.warning("No topic_metadata in youtube_metadata_results")
        return {"results": []}

    results = []
    for video in queued_videos:
        chapter_id = video.get('chapter_id')

        # Find metadata for this chapter by chapter_id
        metadata = next((m for m in topic_metadata if m.get('chapter_id') == chapter_id), None)

        if metadata:
            # Extract title and description from nested dicts
            title_data = metadata.get('title', {})
            description_data = metadata.get('description', {})

            title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
            description = description_data.get('description', '') if isinstance(description_data, dict) else str(description_data)

            # Generate impactful thumbnail text (3-6 words, max 40 chars)
            result = generate_thumbnail_text(title, description, max_length=40)
            result['chapter_id'] = chapter_id
            result['video_id'] = video.get('video_id')
            results.append(result)
        else:
            logging.warning(f"No metadata found for chapter {chapter_id}")

    return {"results": results}


def generate_video_thumbnails(queued_videos, thumbnail_texts, download_results, data_dir):
    """
    Generate thumbnail images for chapters.

    Creates folder structure: data_dir/video_id/chapter_id/thumbnail.png

    Args:
        queued_videos: List of chapters from uploadable_chapters view
        thumbnail_texts: Generated thumbnail text results
        download_results: Not used for chapters (kept for signature compatibility)
        data_dir: Data directory path

    Returns:
        Dict with thumbnail generation results
    """
    if not queued_videos:
        logging.warning("No chapters for thumbnail generation")
        return {"results": []}

    # Merge thumbnail text with chapter info
    videos_with_text = []
    for video in queued_videos:
        video_copy = video.copy()
        chapter_id = video.get('chapter_id')

        # Find thumbnail text for this chapter
        text_result = next(
            (t for t in thumbnail_texts.get('results', [])
             if t.get('chapter_id') == chapter_id),
            None
        )

        if text_result:
            video_copy['thumbnail_text'] = text_result.get('thumbnail_text', '')
        else:
            # Fallback to chapter title
            video_copy['thumbnail_text'] = video.get('chapter_title', '')[:40]

        videos_with_text.append(video_copy)

    # Generate thumbnails
    return generate_thumbnails_for_videos(videos_with_text, data_dir, download_results)


def generate_thumbnails_for_videos(videos, data_directory, download_results=None):
    """
    Generate thumbnails for chapters.

    Creates folder structure: data_directory/video_id/chapter_id/thumbnail.png

    Args:
        videos: List of chapter dicts with metadata
        data_directory: Base data directory (congress_videos folder)
        download_results: Not used for chapters (kept for signature compatibility)

    Returns:
        Dict with thumbnail generation results
    """
    # Use centralized asset paths from config
    background_path = BACKGROUND_IMAGE
    logo_path = CHANNEL_LOGO

    results = []

    for video in videos:
        try:
            chapter_id = video.get('chapter_id')
            video_id = video.get('video_id')
            session_number = video.get('session_number', 133)
            thumbnail_text = video.get('thumbnail_text', video.get('chapter_title', ''))[:40]

            # Create video_id/chapter_id/ folder structure
            chapter_folder = os.path.join(data_directory, str(video_id), str(chapter_id))
            os.makedirs(chapter_folder, exist_ok=True)
            output_filename = "thumbnail.png"
            output_path = os.path.join(chapter_folder, output_filename)
            logging.info(f"Creating chapter thumbnail in: {chapter_folder}")

            # Generate thumbnail
            result = create_thumbnail(
                background_image_path=background_path,
                text=thumbnail_text,
                output_path=output_path,
                session_number=session_number,
                logo_path=logo_path
            )

            result['video_id'] = video_id
            result['chapter_id'] = chapter_id
            results.append(result)

        except Exception as e:
            logging.error(f"Error generating thumbnail for chapter {video.get('chapter_id')}: {e}")
            results.append({
                "video_id": video.get('video_id'),
                "chapter_id": video.get('chapter_id'),
                "success": False,
                "error": str(e)
            })

    success_count = sum(1 for r in results if r.get('success'))
    logging.info(f"Generated {success_count}/{len(results)} thumbnails successfully")

    return {
        "results": results,
        "total": len(results),
        "success_count": success_count,
        "failed_count": len(results) - success_count
    }

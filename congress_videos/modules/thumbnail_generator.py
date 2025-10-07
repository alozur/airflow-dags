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


def generate_thumbnail_text(video_title, video_description, max_length=40):
    """
    Generate short, impactful text for thumbnail using AI (3-6 words max).

    Focuses on creating attention-grabbing phrases that encourage clicks.

    Args:
        video_title: Original video title
        video_description: Video description for context
        max_length: Maximum character length (default 40)

    Returns:
        Dict with thumbnail_text and metadata
    """
    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        system_prompt = """Eres un experto en crear texto impactante para miniaturas de YouTube.
Tu objetivo es crear frases MUY CORTAS (3-6 palabras) que capten atención y generen clics."""

        user_prompt = f"""Crea una frase ULTRA CORTA para miniatura de YouTube de un vídeo del Congreso.

Título: {video_title}
Contexto: {video_description[:200]}

REQUISITOS CRÍTICOS:
- MÁXIMO 3-6 palabras
- Máximo {max_length} caracteres en total
- Lenguaje directo e impactante
- Generar curiosidad o urgencia
- Sin signos de interrogación ni comillas
- Palabras clave que llamen la atención

Ejemplos de buen estilo:
- "REFORMA PENSIONES: ¡DEBATE EXPLOSIVO!"
- "CRISIS ENERGÉTICA REVELADA"
- "GOBIERNO: POLÍTICAS SECRETAS"

Devuelve SOLO la frase, sin explicaciones."""

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.9,  # Higher creativity for attention-grabbing
            max_tokens=30
        )

        thumbnail_text = response.choices[0].message.content.strip()

        # Remove quotes if AI added them
        thumbnail_text = thumbnail_text.strip('"').strip("'").strip()

        # Truncate if too long
        if len(thumbnail_text) > max_length:
            thumbnail_text = thumbnail_text[:max_length].strip()

        word_count = len(thumbnail_text.split())

        logging.info(f"Generated thumbnail text: '{thumbnail_text}' ({word_count} words, {len(thumbnail_text)} chars)")

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

        if len(fallback_text) > max_length:
            fallback_text = fallback_text[:max_length].strip()

        return {
            "thumbnail_text": fallback_text,
            "word_count": len(words),
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
        "/opt/airflow/data/congress_videos/fonts/LiberationSans-Bold.ttf",  # Project fonts folder
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
            "/opt/airflow/data/congress_videos/fonts/LiberationSans-Regular.ttf",  # Project fonts folder
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
        queued_videos: List of videos from queue
        youtube_metadata_results: Results dict from generate_youtube_metadata_for_selected_videos
                                  containing 'topic_metadata' list

    Returns:
        Dict with thumbnail text results
    """
    if not queued_videos or not youtube_metadata_results:
        logging.warning("No videos or metadata for thumbnail text generation")
        return {"results": []}

    # Extract the topic_metadata list from the results dict
    topic_metadata = youtube_metadata_results.get('topic_metadata', [])
    if not topic_metadata:
        logging.warning("No topic_metadata in youtube_metadata_results")
        return {"results": []}

    results = []
    for video in queued_videos:
        entry_id = video.get('entry_id')

        # Find metadata for this video by topic_entry_id
        metadata = next((m for m in topic_metadata if m.get('topic_entry_id') == entry_id), None)

        if metadata:
            # Extract title and description from nested dicts
            title_data = metadata.get('title', {})
            description_data = metadata.get('description', {})

            title = title_data.get('title', '') if isinstance(title_data, dict) else str(title_data)
            description = description_data.get('description', '') if isinstance(description_data, dict) else str(description_data)

            # Generate impactful thumbnail text
            result = generate_thumbnail_text(title, description, max_length=40)
            result['entry_id'] = entry_id
            results.append(result)
        else:
            logging.warning(f"No metadata found for video {entry_id}")

    return {"results": results}


def generate_video_thumbnails(queued_videos, thumbnail_texts, download_results, data_dir):
    """
    Generate thumbnail images for videos in the same folder as downloaded videos.

    Args:
        queued_videos: List of videos from queue
        thumbnail_texts: Generated thumbnail text results
        download_results: Download results with file paths
        data_dir: Data directory path

    Returns:
        Dict with thumbnail generation results
    """
    if not queued_videos:
        logging.warning("No videos for thumbnail generation")
        return {"results": []}

    # Get video folder from download results (first downloaded video's folder)
    video_folder = None
    if download_results and download_results.get('download_details'):
        # Find first successfully downloaded video
        successful_videos = [v for v in download_results['download_details'] if v.get('success')]
        if successful_videos:
            first_video = successful_videos[0]
            video_path = first_video.get('file_path')
            if video_path:
                video_folder = os.path.dirname(video_path)
                logging.info(f"Thumbnails will be saved in video folder: {video_folder}")

    if not video_folder:
        # Fallback to data directory
        video_folder = data_dir
        logging.warning(f"No video folder found, using data directory: {video_folder}")

    # Merge thumbnail text with video info
    videos_with_text = []
    for video in queued_videos:
        video_copy = video.copy()

        # Find thumbnail text for this video
        text_result = next(
            (t for t in thumbnail_texts.get('results', []) if t.get('video_id') == video.get('video_id')),
            None
        )

        if text_result:
            video_copy['thumbnail_text'] = text_result.get('thumbnail_text', '')
        else:
            # Fallback to title
            video_copy['thumbnail_text'] = video.get('youtube_title', '')[:40]

        videos_with_text.append(video_copy)

    # Generate thumbnails in the video folder
    return generate_thumbnails_for_videos(videos_with_text, data_dir, video_folder)


def generate_thumbnails_for_videos(videos, data_directory, video_folder=None):
    """
    Generate thumbnails for a list of videos.

    Args:
        videos: List of video dicts with metadata
        data_directory: Base data directory for assets
        video_folder: Folder where videos are saved (thumbnails will be saved here)

    Returns:
        Dict with thumbnail generation results
    """
    assets_dir = os.path.join(data_directory, 'congress_videos')
    background_path = os.path.join(assets_dir, 'congress_chamber_background.png')
    logo_path = os.path.join(assets_dir, 'congress_channel_logo.png')

    # Use video_folder if provided, otherwise use data_directory
    thumbnail_output_dir = video_folder if video_folder else data_directory

    results = []

    for video in videos:
        try:
            video_id = video.get('video_id')
            entry_id = video.get('entry_id')
            session_number = video.get('session_number', 133)
            thumbnail_text = video.get('thumbnail_text', video.get('youtube_title', ''))[:40]

            # Output path for thumbnail (in the same folder as videos)
            output_filename = f"thumbnail_{video_id}.jpg"
            output_path = os.path.join(thumbnail_output_dir, output_filename)

            # Generate thumbnail
            result = create_thumbnail(
                background_image_path=background_path,
                text=thumbnail_text,
                output_path=output_path,
                session_number=session_number,
                logo_path=logo_path
            )

            result['video_id'] = video_id
            result['entry_id'] = entry_id
            results.append(result)

        except Exception as e:
            logging.error(f"Error generating thumbnail for video {video.get('video_id')}: {e}")
            results.append({
                "video_id": video.get('video_id'),
                "entry_id": video.get('entry_id'),
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

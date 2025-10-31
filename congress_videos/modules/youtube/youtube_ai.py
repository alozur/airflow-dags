"""
YouTube AI-powered metadata generation and video interest evaluation.

This module uses OpenAI to generate YouTube titles and descriptions,
and to evaluate video interest scores for upload prioritization.
"""

import logging
import os

from congress_videos.config.ai_prompts import (
    CHAPTER_RELEVANCE_SCORING_SYSTEM_PROMPT,
    CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE,
    VIDEO_INTEREST_INTERVENTION_SYSTEM_PROMPT,
    VIDEO_INTEREST_INTERVENTION_USER_PROMPT_TEMPLATE,
    VIDEO_INTEREST_MAIN_TOPIC_SYSTEM_PROMPT,
    VIDEO_INTEREST_MAIN_TOPIC_USER_PROMPT_TEMPLATE,
    YOUTUBE_DESCRIPTION_SYSTEM_PROMPT,
    YOUTUBE_DESCRIPTION_USER_PROMPT_TEMPLATE,
    YOUTUBE_TITLE_SYSTEM_PROMPT,
    YOUTUBE_TITLE_USER_PROMPT_TEMPLATE,
)
from congress_videos.modules.speaker_helpers import (
    format_speaker_context,
    format_speaker_list,
)
from congress_videos.modules.web_scraping import construct_session_link
from utils.ai_helpers import (
    clamp_value,
    generate_chat_completion,
    generate_json_completion,
    truncate_text,
)


def generate_youtube_title(main_topic_content, speakers_info, max_length=100):
    """
    Generates a YouTube-optimized title for a congressional video using OpenAI.

    Args:
        main_topic_content: The main topic/question content
        speakers_info: List of speaker information including names and roles
        max_length: Maximum title length (YouTube recommends under 100 characters)

    Returns:
        Dict with generated title and metadata containing:
        - title: Generated YouTube title
        - character_count: Length of generated title
        - within_limit: Boolean indicating if title is within max_length
        - error: Error message if generation failed
    """
    try:
        # Prepare speaker context
        speaker_context = format_speaker_context(speakers_info, max_speakers=3)

        # Format user prompt
        user_prompt = YOUTUBE_TITLE_USER_PROMPT_TEMPLATE.format(
            max_length=max_length,
            main_topic_content=main_topic_content,
            speaker_context=speaker_context,
        )

        # Generate title
        result = generate_chat_completion(
            system_prompt=YOUTUBE_TITLE_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model="gpt-3.5-turbo",
            temperature=0.7,
            max_tokens=150,
        )

        if result["error"]:
            raise Exception(result["error"])

        generated_title = result["content"]

        # Strip any surrounding quotes (single or double)
        generated_title = generated_title.strip().strip('"').strip("'").strip()

        # Ensure title doesn't exceed max_length
        if len(generated_title) > max_length:
            generated_title = truncate_text(generated_title, max_length)

        logging.info(
            f"Generated YouTube title ({len(generated_title)} chars): {generated_title}"
        )

        return {
            "title": generated_title,
            "character_count": len(generated_title),
            "within_limit": len(generated_title) <= max_length,
            "error": None,
        }

    except Exception as e:
        error_msg = f"Error generating YouTube title: {str(e)}"
        logging.error(error_msg)

        # Fallback title
        fallback_title = "Debate en el Congreso de España"
        if main_topic_content and "PREGUNTA" in main_topic_content:
            fallback_title = "Sesión de Control al Gobierno - Congreso"

        return {
            "title": fallback_title,
            "character_count": len(fallback_title),
            "within_limit": True,
            "error": error_msg,
        }


def generate_youtube_description(
    main_topic_content, speakers_info, video_metadata, session_number, target_date=None
):
    """
    Generates a YouTube-optimized description for a congressional video using OpenAI.

    Args:
        main_topic_content: The main topic/question content
        speakers_info: List of speaker information including names and roles
        video_metadata: Video file metadata (duration, size, etc.)
        session_number: Congressional session number
        target_date: Date of the session for generating session link

    Returns:
        Dict with generated description and metadata containing:
        - description: Generated YouTube description
        - character_count: Length of description
        - word_count: Number of words in description
        - error: Error message if generation failed
    """
    try:
        # Generate session link
        session_link = ""
        if session_number and target_date:
            try:
                session_link = construct_session_link(session_number, target_date)
            except Exception as e:
                logging.warning(f"Could not generate session link: {e}")
                session_link = "https://www.congreso.es"

        # Prepare speaker context
        speaker_context = format_speaker_context(
            speakers_info, max_speakers=4, prefix="Participantes principales"
        )

        # Prepare video info
        duration = video_metadata.get("duration_estimated", "N/A")
        duration_info = f"Duración: {duration}" if duration != "N/A" else ""

        # Format user prompt
        user_prompt = YOUTUBE_DESCRIPTION_USER_PROMPT_TEMPLATE.format(
            main_topic_content=main_topic_content,
            session_number=session_number,
            speaker_context=speaker_context,
            duration_info=duration_info,
        )

        # Generate description
        result = generate_chat_completion(
            system_prompt=YOUTUBE_DESCRIPTION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model="gpt-3.5-turbo",
            temperature=0.7,
            max_tokens=1000,
        )

        if result["error"]:
            raise Exception(result["error"])

        ai_generated_content = result["content"]

        # Build final structured description
        final_description = ai_generated_content

        # Add technical information section with proper formatting
        final_description += "\n\n" + "─" * 40
        final_description += "\n📺 INFORMACIÓN DE LA SESIÓN\n"

        if duration != "N/A":
            final_description += f"⏱️ Duración: {duration}\n"

        final_description += f"🏛️ Sesión Plenaria Nº {session_number}\n"

        if session_link and session_link != "https://www.congreso.es":
            final_description += f"🔗 Ver sesión completa: {session_link}\n"

        final_description += "📜 Fuente oficial: Congreso de los Diputados\n"
        final_description += "🌐 www.congreso.es"

        logging.info(
            f"Generated structured YouTube description ({len(final_description)} chars)"
        )

        return {
            "description": final_description,
            "character_count": len(final_description),
            "word_count": len(final_description.split()),
            "error": None,
        }

    except Exception as e:
        error_msg = f"Error generating YouTube description: {str(e)}"
        logging.error(error_msg)

        # Fallback description with better structure
        fallback_description = f"""🏛️ Debate en el Congreso de los Diputados

{main_topic_content[:200] if main_topic_content else 'En esta sesión parlamentaria se abordan temas de actualidad política nacional.'}

Este vídeo forma parte de las sesiones de control al Gobierno, donde los diputados formulan preguntas y el Ejecutivo responde sobre diversos asuntos de interés público.

📺 INFORMACIÓN DE LA SESIÓN
🏛️ Sesión Plenaria Nº {session_number}
📜 Fuente oficial: Congreso de los Diputados
🌐 www.congreso.es

#CongresoEspaña #Política #Debate #Democracia #SesiónPlenaria"""

        return {
            "description": fallback_description,
            "character_count": len(fallback_description),
            "word_count": len(fallback_description.split()),
            "error": error_msg,
        }


def generate_youtube_metadata_from_enriched_groups(
    enriched_video_groups, session_number, target_date=None
):
    """
    Generates YouTube metadata (titles and descriptions) directly from enriched video groups.

    This function works without requiring downloads to be completed first.

    Args:
        enriched_video_groups: Enriched video groups from enrich_with_metadata function
        session_number: Congressional session number
        target_date: Date of the session for generating session links

    Returns:
        Dict with metadata for each topic containing:
        - total_topics: Total number of topics processed
        - successful_generations: Number of successful metadata generations
        - failed_generations: Number of failed generations
        - topic_metadata: List of metadata for each topic
    """
    metadata_results = {
        "total_topics": 0,
        "successful_generations": 0,
        "failed_generations": 0,
        "topic_metadata": [],
    }

    for group in enriched_video_groups:
        if group.get("type") == "topic_group":
            metadata_results["total_topics"] += 1

            main_topic = group.get("main_topic", {})
            topic_entry_id = main_topic.get("entry_id")
            main_topic_content = main_topic.get("content", "")

            # Extract speakers info from interventions
            speakers_info = []
            for intervention in group.get("interventions", []):
                speakers_info.append(
                    {
                        "speaker_name": intervention.get("speaker_name", ""),
                        "role": intervention.get("role", ""),
                    }
                )

            # Get video metadata from the enriched data
            video_metadata = main_topic.get("metadata_url", {})

            # Generate title and description
            logging.info(f"Generating YouTube metadata for topic {topic_entry_id}")
            title_result = generate_youtube_title(main_topic_content, speakers_info)
            description_result = generate_youtube_description(
                main_topic_content,
                speakers_info,
                video_metadata,
                session_number,
                target_date,
            )

            topic_metadata = {
                "topic_entry_id": topic_entry_id,
                "video_file_path": None,  # Will be set later after download
                "info_file_path": None,  # Will be set later after download
                "title": title_result,
                "description": description_result,
                "main_topic_content": main_topic_content,
                "video_url": main_topic.get("video_url"),
                "video_metadata": video_metadata,
                "speakers_info": speakers_info,
                "generation_success": title_result.get("error") is None
                and description_result.get("error") is None,
            }

            metadata_results["topic_metadata"].append(topic_metadata)

            if topic_metadata["generation_success"]:
                metadata_results["successful_generations"] += 1
            else:
                metadata_results["failed_generations"] += 1

    logging.info(
        f"YouTube metadata generation complete: {metadata_results['successful_generations']}/{metadata_results['total_topics']} topics processed successfully"
    )
    return metadata_results


def generate_youtube_metadata_for_topics(
    download_results, session_number, target_date=None
):
    """
    Generates YouTube metadata (titles and descriptions) for all downloaded topics.

    DEPRECATED: Use generate_youtube_metadata_from_enriched_groups instead.
    This function is kept for backward compatibility.

    Args:
        download_results: Results from download_main_topic_videos function
        session_number: Congressional session number
        target_date: Date of the session for generating session links

    Returns:
        Dict with metadata for each topic
    """
    metadata_results = {
        "total_topics": len(download_results.get("download_details", [])),
        "successful_generations": 0,
        "failed_generations": 0,
        "topic_metadata": [],
    }

    for detail in download_results.get("download_details", []):
        if not detail.get("success"):
            # Skip topics that weren't downloaded successfully
            continue

        topic_entry_id = detail.get("topic_entry_id")

        # Read the info file to get topic and speaker data
        info_file_path = detail.get("info_file_path")
        main_topic_content = ""
        speakers_info = []

        if info_file_path and os.path.exists(info_file_path):
            try:
                with open(info_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    # Extract main topic (basic parsing)
                    if "MAIN TOPIC:" in content:
                        topic_start = content.find("MAIN TOPIC:") + len("MAIN TOPIC:")
                        topic_end = content.find("SPEAKERS AND INTERVENTIONS:")
                        if topic_end == -1:
                            topic_end = len(content)
                        main_topic_content = (
                            content[topic_start:topic_end]
                            .strip()
                            .replace("-----------", "")
                            .strip()
                        )

            except Exception as e:
                logging.warning(f"Could not read info file {info_file_path}: {e}")

        # Get video metadata if available
        video_metadata = {}
        if "metadata" in detail:
            video_metadata = detail["metadata"]

        # Generate title
        logging.info(f"Generating YouTube metadata for topic {topic_entry_id}")
        title_result = generate_youtube_title(main_topic_content, speakers_info)
        description_result = generate_youtube_description(
            main_topic_content, speakers_info, video_metadata, session_number, target_date
        )

        topic_metadata = {
            "topic_entry_id": topic_entry_id,
            "video_file_path": detail.get("output_path"),
            "info_file_path": info_file_path,
            "title": title_result,
            "description": description_result,
            "main_topic_content": main_topic_content,
            "generation_success": title_result.get("error") is None
            and description_result.get("error") is None,
        }

        metadata_results["topic_metadata"].append(topic_metadata)

        if topic_metadata["generation_success"]:
            metadata_results["successful_generations"] += 1
        else:
            metadata_results["failed_generations"] += 1

    logging.info(
        f"YouTube metadata generation complete: {metadata_results['successful_generations']}/{metadata_results['total_topics']} topics processed successfully"
    )
    return metadata_results


def evaluate_video_interest_with_ai(enriched_video_groups):
    """
    Evaluates video interest for YouTube upload using OpenAI.

    ONLY evaluates main topics (not interventions).
    For main topics: Considers the speakers participating in the topic

    Args:
        enriched_video_groups: Enriched video groups from enrich_with_metadata function

    Returns:
        Dict with evaluation results for each main topic containing:
        - total_videos_evaluated: Total number of videos evaluated
        - successful_evaluations: Number of successful evaluations
        - failed_evaluations: Number of failed evaluations
        - evaluations: List of evaluation results per video
    """
    evaluation_results = {
        "total_videos_evaluated": 0,
        "successful_evaluations": 0,
        "failed_evaluations": 0,
        "evaluations": [],
    }

    for group in enriched_video_groups:
        if group.get("type") == "topic_group":
            main_topic = group.get("main_topic", {})
            main_topic_content = main_topic.get("content", "")
            main_topic_entry_id = main_topic.get("entry_id")

            # Collect speakers from interventions for main topic evaluation
            speakers_info = []
            for intervention in group.get("interventions", []):
                speakers_info.append(
                    {
                        "speaker_name": intervention.get("speaker_name", "Desconocido"),
                        "role": intervention.get("role", "Sin rol especificado"),
                    }
                )

            # Evaluate ONLY main topic (not interventions)
            main_topic_score = _evaluate_main_topic_interest(
                main_topic_content, speakers_info, main_topic.get("metadata_url", {})
            )

            evaluation_results["evaluations"].append(
                {
                    "entry_id": main_topic_entry_id,
                    "video_type": "main_topic",
                    "interest_score": main_topic_score.get("score", 5),
                    "reasoning": main_topic_score.get("reasoning", ""),
                    "evaluation_success": main_topic_score.get("error") is None,
                    "error": main_topic_score.get("error"),
                }
            )

            evaluation_results["total_videos_evaluated"] += 1
            if main_topic_score.get("error") is None:
                evaluation_results["successful_evaluations"] += 1
            else:
                evaluation_results["failed_evaluations"] += 1

    logging.info(
        f"Main topic evaluation complete: {evaluation_results['successful_evaluations']}/{evaluation_results['total_videos_evaluated']} main topics evaluated successfully"
    )
    return evaluation_results


def _evaluate_main_topic_interest(topic_content, speakers_info, video_metadata):
    """
    Evaluates the interest level of a main topic video for YouTube upload.

    Args:
        topic_content: The main topic title/description
        speakers_info: List of speakers participating in this topic
        video_metadata: Video metadata (duration, etc.)

    Returns:
        Dict with score (1-10) and reasoning containing:
        - score: Interest score from 1-10
        - reasoning: Explanation for the score
        - error: Error message if evaluation failed
    """
    try:
        # Format speakers list
        speakers_text = format_speaker_list(speakers_info, max_speakers=10)

        duration_minutes = video_metadata.get("duration_seconds", 0) // 60

        # Format user prompt
        user_prompt = VIDEO_INTEREST_MAIN_TOPIC_USER_PROMPT_TEMPLATE.format(
            topic_content=topic_content,
            speakers_text=speakers_text,
            duration_minutes=duration_minutes,
        )

        # Generate JSON evaluation
        result = generate_json_completion(
            system_prompt=VIDEO_INTEREST_MAIN_TOPIC_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=200,
        )

        if result["error"]:
            logging.warning(f"Failed to parse AI response: {result['error']}")
            return {
                "score": 5,
                "reasoning": "Error al procesar la evaluación de IA",
                "error": result["error"],
            }

        data = result["data"]
        score = int(data.get("score", 5))
        score = clamp_value(score, 1, 10)

        return {
            "score": score,
            "reasoning": data.get("reasoning", "Sin justificación"),
            "error": None,
        }

    except Exception as e:
        logging.error(f"Error evaluating main topic interest: {str(e)}")
        return {
            "score": 5,
            "reasoning": f"Error en evaluación: {str(e)}",
            "error": str(e),
        }


def _evaluate_intervention_interest(
    speaker_name, speaker_role, main_topic_context, video_metadata
):
    """
    Evaluates the interest level of an intervention video for YouTube upload.

    Args:
        speaker_name: Name of the speaker
        speaker_role: Role/position of the speaker
        main_topic_context: Description of the main topic this intervention belongs to
        video_metadata: Video metadata (duration, etc.)

    Returns:
        Dict with score (1-10) and reasoning
    """
    try:
        duration_minutes = video_metadata.get("duration_seconds", 0) // 60

        # Format user prompt
        user_prompt = VIDEO_INTEREST_INTERVENTION_USER_PROMPT_TEMPLATE.format(
            main_topic_context=main_topic_context,
            speaker_name=speaker_name,
            speaker_role=speaker_role,
            duration_minutes=duration_minutes,
        )

        # Generate JSON evaluation
        result = generate_json_completion(
            system_prompt=VIDEO_INTEREST_INTERVENTION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=200,
        )

        if result["error"]:
            logging.warning(f"Failed to parse AI response: {result['error']}")
            return {
                "score": 5,
                "reasoning": "Error al procesar la evaluación de IA",
                "error": result["error"],
            }

        data = result["data"]
        score = int(data.get("score", 5))
        score = clamp_value(score, 1, 10)

        return {
            "score": score,
            "reasoning": data.get("reasoning", "Sin justificación"),
            "error": None,
        }

    except Exception as e:
        logging.error(f"Error evaluating intervention interest: {str(e)}")
        return {
            "score": 5,
            "reasoning": f"Error en evaluación: {str(e)}",
            "error": str(e),
        }


def generate_youtube_metadata_for_selected_videos(top_videos):
    """
    Generates YouTube metadata (titles and descriptions) for a list of selected videos.

    Args:
        top_videos: List of video records from database (from get_top_videos_for_upload)

    Returns:
        Dict with metadata for each video
    """
    metadata_results = {
        "total_videos": len(top_videos) if top_videos else 0,
        "successful_generations": 0,
        "failed_generations": 0,
        "topic_metadata": [],
    }

    if not top_videos:
        logging.warning("No videos provided for metadata generation")
        return metadata_results

    for video in top_videos:
        topic_entry_id = video.get("entry_id")
        topic_title = video.get("topic_title", "")
        session_number = video.get("session_number")

        # For now, use empty speakers info - we'll need to query interventions if needed
        speakers_info = []
        video_metadata = {"duration_seconds": video.get("duration_seconds", 0)}

        # Generate title and description
        logging.info(f"Generating YouTube metadata for video {topic_entry_id}")
        title_result = generate_youtube_title(topic_title, speakers_info)
        description_result = generate_youtube_description(
            topic_title, speakers_info, video_metadata, session_number, None
        )

        topic_metadata = {
            "topic_entry_id": topic_entry_id,
            "video_file_path": video.get("video_file_path"),
            "title": title_result,
            "description": description_result,
            "main_topic_content": topic_title,
            "video_url": video.get("video_url"),
            "session_number": session_number,
            "ai_interest_score": video.get("ai_interest_score"),
            "generation_success": title_result.get("error") is None
            and description_result.get("error") is None,
        }

        metadata_results["topic_metadata"].append(topic_metadata)

        if topic_metadata["generation_success"]:
            metadata_results["successful_generations"] += 1
        else:
            metadata_results["failed_generations"] += 1

    logging.info(
        f"YouTube metadata generation complete: {metadata_results['successful_generations']}/{metadata_results['total_videos']} videos processed successfully"
    )
    return metadata_results


def score_chapters_relevance(merged_chapters):
    """
    Score the relevance of merged interesting chapters using AI (0-5 scale).

    Evaluates each chapter based on three criteria. The final score is calculated
    automatically by summing the points from each criterion:
    - Speaker relevance (0-2 points): Are key political figures involved?
    - Topic relevance (0-2 points): Is it a current/hot topic in Spain?
    - Public interest potential (0-1 point): Could it generate media interest?

    Final score = speaker_pts + topic_pts + interest_pts (range: 0-5)

    Args:
        merged_chapters: Results from merge_interesting_chapters function
                        Expected structure:
                        {
                            'total_videos': int,
                            'videos': [
                                {
                                    'video_id': str,
                                    'video_title': str,
                                    'total_chapters': int,
                                    'final_chapters': [
                                        {
                                            'title': str,
                                            'description': str,
                                            'duration_minutes': float,
                                            'speakers': [str],
                                            'topics': [str],
                                            'start_time': str,
                                            'end_time': str
                                        }
                                    ]
                                }
                            ]
                        }

    Returns:
        Dict with scored chapters:
        {
            'total_videos': int,
            'total_chapters_scored': int,
            'successful_scores': int,
            'failed_scores': int,
            'videos': [
                {
                    'video_id': str,
                    'video_title': str,
                    'total_chapters': int,
                    'scored_chapters': [
                        {
                            'title': str,
                            'description': str,
                            'duration_minutes': float,
                            'speakers': [str],
                            'topics': [str],
                            'start_time': str,
                            'end_time': str,
                            'relevance_score': int (0-5, sum of the 3 criteria below),
                            'speaker_relevance_points': int (0-2),
                            'topic_relevance_points': int (0-2),
                            'public_interest_points': int (0-1),
                            'scoring_reasoning': str,
                            'key_speakers': [str],
                            'is_current_topic': bool,
                            'scoring_error': str or None
                        }
                    ]
                }
            ]
        }
    """
    if not merged_chapters or not merged_chapters.get('videos'):
        logging.warning("No merged chapters to score")
        return {
            'total_videos': 0,
            'total_chapters_scored': 0,
            'successful_scores': 0,
            'failed_scores': 0,
            'videos': []
        }

    scored_results = {
        'total_videos': 0,
        'total_chapters_scored': 0,
        'successful_scores': 0,
        'failed_scores': 0,
        'videos': []
    }

    for video_data in merged_chapters['videos']:
        video_id = video_data.get('video_id')
        final_chapters = video_data.get('final_chapters', [])

        if video_data.get('error') or not final_chapters:
            logging.warning(f"Skipping video {video_id}: {video_data.get('error', 'no chapters')}")
            scored_results['videos'].append({
                'video_id': video_id,
                'error': video_data.get('error', 'No chapters to score')
            })
            continue

        scored_chapters = []

        for chapter in final_chapters:
            scored_results['total_chapters_scored'] += 1

            # Extract chapter information
            chapter_title = chapter.get('title', 'Sin título')
            chapter_description = chapter.get('description', 'Sin descripción')
            duration_minutes = chapter.get('duration_minutes', 0)
            speakers = chapter.get('speakers', [])
            topics = chapter.get('topics', [])

            # Format speakers list
            speakers_list = "\n".join([f"- {speaker}" for speaker in speakers]) if speakers else "- (No especificado)"

            # Format topics list
            topics_list = "\n".join([f"- {topic}" for topic in topics]) if topics else "- (No especificado)"

            # Build user prompt
            user_prompt = CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE.format(
                chapter_title=chapter_title,
                chapter_description=chapter_description,
                duration_minutes=duration_minutes,
                speakers_list=speakers_list,
                topics_list=topics_list
            )

            try:
                # Generate scoring using AI
                result = generate_json_completion(
                    system_prompt=CHAPTER_RELEVANCE_SCORING_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    model="gpt-4o-mini",
                    temperature=0.3,
                    max_tokens=500
                )

                if result['error']:
                    logging.warning(f"Failed to score chapter '{chapter_title}': {result['error']}")
                    # Default middle values on error
                    speaker_pts = 1
                    topic_pts = 1
                    interest_pts = 0
                    scored_chapter = {
                        **chapter,
                        'relevance_score': speaker_pts + topic_pts + interest_pts,  # Sum = 2
                        'speaker_relevance_points': speaker_pts,
                        'topic_relevance_points': topic_pts,
                        'public_interest_points': interest_pts,
                        'scoring_reasoning': f"Error en scoring: {result['error']}",
                        'key_speakers': speakers,
                        'is_current_topic': False,
                        'scoring_error': result['error']
                    }
                    scored_results['failed_scores'] += 1
                else:
                    data = result['data']

                    # Extract individual criterion points
                    speaker_pts = int(data.get('speaker_relevance_points', 0))
                    topic_pts = int(data.get('topic_relevance_points', 0))
                    interest_pts = int(data.get('public_interest_points', 0))

                    # Clamp values to valid ranges
                    speaker_pts = clamp_value(speaker_pts, 0, 2)
                    topic_pts = clamp_value(topic_pts, 0, 2)
                    interest_pts = clamp_value(interest_pts, 0, 1)

                    # Calculate final score as sum (0-5)
                    final_score = speaker_pts + topic_pts + interest_pts

                    scored_chapter = {
                        **chapter,
                        'relevance_score': final_score,
                        'speaker_relevance_points': speaker_pts,
                        'topic_relevance_points': topic_pts,
                        'public_interest_points': interest_pts,
                        'scoring_reasoning': data.get('reasoning', 'Sin justificación'),
                        'key_speakers': data.get('key_speakers', speakers),
                        'is_current_topic': data.get('is_current_topic', False),
                        'scoring_error': None
                    }
                    scored_results['successful_scores'] += 1

                    logging.info(
                        f"Chapter '{chapter_title}' scored: {final_score}/5 "
                        f"(speakers:{speaker_pts}, topics:{topic_pts}, interest:{interest_pts})"
                    )

            except Exception as e:
                logging.error(f"Error scoring chapter '{chapter_title}': {str(e)}", exc_info=True)
                # Default middle values on exception
                speaker_pts = 1
                topic_pts = 1
                interest_pts = 0
                scored_chapter = {
                    **chapter,
                    'relevance_score': speaker_pts + topic_pts + interest_pts,  # Sum = 2
                    'speaker_relevance_points': speaker_pts,
                    'topic_relevance_points': topic_pts,
                    'public_interest_points': interest_pts,
                    'scoring_reasoning': f"Excepción en scoring: {str(e)}",
                    'key_speakers': speakers,
                    'is_current_topic': False,
                    'scoring_error': str(e)
                }
                scored_results['failed_scores'] += 1

            scored_chapters.append(scored_chapter)

        # Sort chapters by relevance_score (highest first)
        scored_chapters.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)

        scored_results['videos'].append({
            'video_id': video_id,
            'video_title': video_data.get('video_title'),
            'total_chapters': len(scored_chapters),
            'scored_chapters': scored_chapters
        })
        scored_results['total_videos'] += 1

    logging.info(
        f"Chapter relevance scoring complete: {scored_results['successful_scores']}/{scored_results['total_chapters_scored']} chapters scored successfully"
    )
    logging.info(
        f"Processed {scored_results['total_videos']} videos with a total of {scored_results['total_chapters_scored']} chapters"
    )

    return scored_results

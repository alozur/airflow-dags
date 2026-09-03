"""
Speaker formatting utilities for Congressional YouTube project.

This module provides helper functions for formatting speaker information
specific to the congressional video processing workflow.
"""

from congress_videos.modules.speaker_placeholders import is_placeholder


def format_speaker_list(
    speakers_info: list[dict[str, str]],
    max_speakers: int = 10,
    include_role: bool = True,
) -> str:
    """
    Format a list of speakers into a text representation.

    Entries whose speaker_name is missing or is a known placeholder are skipped.

    Args:
        speakers_info: List of dicts with 'speaker_name' and 'role' keys
        max_speakers: Maximum number of speakers to include
        include_role: Whether to include speaker roles in output

    Returns:
        Formatted string with speaker information
    """
    if not speakers_info:
        return "No hay participantes especificados"

    speaker_lines = []
    for speaker in speakers_info[:max_speakers]:
        name = speaker.get("speaker_name", "")
        if is_placeholder(name):
            continue
        role = speaker.get("role", "")

        if include_role and role:
            speaker_lines.append(f"- {name} ({role})")
        else:
            speaker_lines.append(f"- {name}")

    if not speaker_lines:
        return "No hay participantes especificados"

    result = "\n".join(speaker_lines)

    # Add indication if there are more speakers
    if len(speakers_info) > max_speakers:
        remaining = len(speakers_info) - max_speakers
        result += f"\n... y {remaining} participante{'s' if remaining > 1 else ''} más"

    return result


def format_speaker_context(
    speakers_info: list[dict[str, str]], max_speakers: int = 3, prefix: str = "Participan"
) -> str:
    """
    Format speakers into a compact context string for prompts.

    Entries whose speaker_name is missing or is a known placeholder are skipped.

    Args:
        speakers_info: List of dicts with 'speaker_name' and 'role' keys
        max_speakers: Maximum number of speakers to include
        prefix: Text prefix for the speaker list

    Returns:
        Formatted string like "Participan: Name1 (role), Name2 (role)"
    """
    if not speakers_info:
        return ""

    speaker_names = []
    for speaker in speakers_info[:max_speakers]:
        name = speaker.get("speaker_name", "")
        if is_placeholder(name):
            continue
        role = speaker.get("role", "")
        if role:
            speaker_names.append(f"{name} ({role})")
        else:
            speaker_names.append(name)

    if not speaker_names:
        return ""

    return f"{prefix}: {', '.join(speaker_names)}"

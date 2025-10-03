"""
Video data extraction and organization utilities.

This module handles extraction of video data from HTML, organizing
videos into topic groups, and enriching with metadata.
"""

import logging
import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


def extract_video_data(html_string):
    """
    Extracts video data from the session HTML.

    Args:
        html_string: HTML content from congressional session page

    Returns:
        List of dictionaries with speaker_name, video_url, profile_link,
        entry_id, and is_bold flag indicating main topics
    """
    soup = BeautifulSoup(html_string, "html.parser")
    results = []

    # Log the total number of rows found
    rows = soup.find_all("tr")
    logging.info(f"Found {len(rows)} total <tr> elements in HTML")

    for idx, row in enumerate(rows):
        row_html = str(row)

        # Debug logging for all rows to see what we're dealing with
        logging.debug(f"Row {idx}: checking for video data...")

        # Check if this row contains a bold entry (main topic/question)
        # The <b> tag wraps the main link with the question
        is_bold_entry = False
        bold_tag = row.find("b")
        if bold_tag:
            # Check if the bold tag contains our target link
            bold_link = bold_tag.find("a", {"id": re.compile(r"\d+")})
            if bold_link and "directopartes" in str(bold_link.get("onclick", "")):
                is_bold_entry = True
                logging.debug(f"Row {idx}: Found BOLD entry (main topic)")

        # Look for any <a> tag with an ID and onclick that contains directopartes
        all_links = row.find_all("a")

        speaker_info = None
        entry_id = None

        for link in all_links:
            link_str = str(link)
            # Check if this link has an ID and onclick with directopartes
            if "id=" in link_str and "onclick=" in link_str and "directopartes" in link_str:
                # Extract the ID
                id_match = re.search(r'id="(\d+)"', link_str)
                if id_match:
                    entry_id = id_match.group(1)

                # Extract the text content (speaker name or question)
                span = link.find("span")
                if span:
                    speaker_info = span.get_text(strip=True)
                    logging.info(
                        f"Found {'BOLD ' if is_bold_entry else ''}entry ID {entry_id}: {speaker_info[:50]}..."
                    )
                    break

        if not speaker_info:
            logging.debug(f"Row {idx}: No speaker info found, skipping")
            continue

        # Process the speaker info to extract the actual name
        if "PREGUNTA" in speaker_info or "Pregunta" in speaker_info:
            # This is a question, keep the full text for bold entries
            if is_bold_entry:
                speaker_name = speaker_info  # Keep full question text
            else:
                # For non-bold, extract the person asking
                name_match = re.search(
                    r"(?:Diputad[oa]|Sr\.|Sra\.|D\.|Dña\.)\s+([^,]+)", speaker_info
                )
                if name_match:
                    speaker_name = name_match.group(1).strip()
                else:
                    speaker_name = speaker_info
        else:
            # This is a direct speaker name (e.g., "Núñez Feijóo, Alberto (GP)")
            speaker_name = speaker_info

        # Look for MP4 video download link in the same row
        video_url = None
        for link in all_links:
            href = link.get("href", "")
            if "static.congreso.es" in href and ".mp4" in href:
                video_url = href
                logging.info(f"Found video URL: {video_url[:80]}...")
                break

        if not video_url:
            logging.debug(f"No MP4 link found for: {speaker_name[:50]}")
            continue

        # Look for profile/ficha link
        profile_link = None
        for link in all_links:
            href = link.get("href", "")
            if "busqueda-de-diputados" in href:
                profile_link = href.replace("&amp;", "&")
                logging.debug(f"Found profile link: {profile_link[:80]}...")
                break

        # Check if we also have a role/position (e.g., "Presidente del Gobierno")
        role = None
        if "(Presidente" in row_html or "(Vicepresidente" in row_html:
            role_match = re.search(
                r"\((Presidente[^)]*|Vicepresidente[^)]*)\)", row_html
            )
            if role_match:
                role = role_match.group(1)
                logging.debug(f"Found role: {role}")

        # Create the result entry
        result_entry = {
            "speaker_name": speaker_name,
            "video_url": video_url,
            "profile_link": profile_link,
            "entry_id": entry_id,
            "is_bold": is_bold_entry,  # Flag to indicate if this is a main topic
        }

        if role:
            result_entry["role"] = role

        results.append(result_entry)
        logging.info(
            f"Added {'BOLD ' if is_bold_entry else ''}entry: {speaker_name[:50]} - ID: {entry_id}"
        )

    logging.info(f"Successfully extracted {len(results)} video items from HTML")
    return results


def organize_video_groups(video_items):
    """
    Organizes video items into logical groups based on bold entries (main topics).

    Bold entries (marked with <b> tag in HTML) are main topics/questions.
    Following non-bold entries are interventions related to that topic.

    Args:
        video_items: List of dictionaries from extract_video_data() with 'is_bold' flag

    Returns:
        List of organized groups where bold entries are parents and following items are children
    """
    if not video_items:
        return []

    organized_groups = []
    current_group = None

    for idx, item in enumerate(video_items):
        is_bold = item.get("is_bold", False)
        speaker_name = item.get("speaker_name", "")

        if is_bold:
            # This is a main topic/question (bold entry) - start a new group
            if current_group:
                # Save the previous group if it exists
                organized_groups.append(current_group)

            # Create new group with this bold entry as the parent
            current_group = {
                "type": "topic_group",
                "main_topic": {
                    "content": speaker_name,  # Full question/topic text
                    "entry_id": item.get("entry_id"),
                    "video_url": item.get("video_url"),
                    "profile_link": item.get("profile_link"),
                    "is_bold": True,  # Preserve the main topic flag
                },
                "interventions": [],  # Children - speakers discussing this topic
            }
            logging.debug(f"Created new topic group: {speaker_name[:50]}...")

        else:
            # This is an intervention (non-bold entry)
            if current_group:
                # Add this as a child to the current group
                intervention = {
                    "speaker_name": speaker_name,
                    "entry_id": item.get("entry_id"),
                    "video_url": item.get("video_url"),
                    "profile_link": item.get("profile_link"),
                    "role": item.get("role"),
                }
                current_group["interventions"].append(intervention)
                logging.debug(
                    f"Added intervention to current group: {speaker_name[:50]}..."
                )
            else:
                # No current group (shouldn't happen with well-formed data)
                # Create a standalone entry
                organized_groups.append(
                    {
                        "type": "standalone",
                        "content": speaker_name,
                        "entry_id": item.get("entry_id"),
                        "video_url": item.get("video_url"),
                        "profile_link": item.get("profile_link"),
                        "role": item.get("role"),
                    }
                )
                logging.warning(
                    f"Found standalone entry without parent: {speaker_name[:50]}..."
                )

    # Don't forget the last group
    if current_group:
        organized_groups.append(current_group)

    # Log summary
    total_topics = sum(1 for g in organized_groups if g["type"] == "topic_group")
    total_interventions = sum(
        len(g.get("interventions", []))
        for g in organized_groups
        if g["type"] == "topic_group"
    )

    logging.info(f"Organized {len(video_items)} items into {len(organized_groups)} groups")
    logging.info(
        f"Found {total_topics} main topics with {total_interventions} total interventions"
    )

    return organized_groups


def extract_video_metadata(video_url):
    """
    Extracts essential metadata from a video URL by making a HEAD request.

    Args:
        video_url: The MP4 video URL to analyze

    Returns:
        Dictionary containing: url, file_size_bytes, file_size_mb, last_modified,
        filename, duration_estimated, duration_seconds, error
    """
    metadata = {
        "url": video_url,
        "file_size_bytes": None,
        "file_size_mb": None,
        "last_modified": None,
        "filename": None,
        "duration_estimated": None,
        "duration_seconds": None,
        "error": None,
    }

    try:
        # Extract filename from URL
        parsed_url = urlparse(video_url)
        metadata["filename"] = (
            parsed_url.path.split("/")[-1] if parsed_url.path else None
        )

        # Make HEAD request to get basic metadata
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "video/mp4,video/*;q=0.9,*/*;q=0.8",
            "Accept-Language": "es,en;q=0.9",
            "Accept-Encoding": "identity",
        }

        logging.debug(f"Making HEAD request to: {video_url}")
        response = requests.head(video_url, headers=headers, timeout=15, verify=False)

        if response.status_code == 200:
            # Extract only required headers
            metadata["last_modified"] = response.headers.get("last-modified")

            # Extract file size
            if "content-length" in response.headers:
                file_size = int(response.headers["content-length"])
                metadata["file_size_bytes"] = file_size
                metadata["file_size_mb"] = round(file_size / (1024 * 1024), 2)

                # Better duration estimation based on Congressional video analysis
                # From actual test: 74.24MB = ~7 minutes 28 seconds (448 seconds)
                # This gives us approximately 0.166MB per second or 9.96MB per minute
                if metadata["file_size_mb"]:
                    estimated_seconds = round(metadata["file_size_mb"] / 0.166)
                    metadata["duration_seconds"] = estimated_seconds

                    minutes = estimated_seconds // 60
                    seconds = estimated_seconds % 60
                    metadata["duration_estimated"] = f"{minutes}:{seconds:02d}"

                    logging.info(
                        f"Duration estimated: {minutes}:{seconds:02d} for {metadata['file_size_mb']}MB"
                    )

            logging.info(
                f"Video metadata extracted successfully: {metadata['file_size_mb']}MB, Duration: {metadata.get('duration_estimated', 'unknown')}"
            )

        else:
            metadata["error"] = f"HTTP {response.status_code}: {response.reason}"
            logging.warning(f"Failed to access video: {metadata['error']}")

    except requests.exceptions.Timeout:
        metadata["error"] = "Request timeout"
        logging.warning(f"Timeout accessing video: {video_url}")
    except requests.exceptions.RequestException as e:
        metadata["error"] = str(e)
        logging.warning(f"Request error for video {video_url}: {e}")
    except Exception as e:
        metadata["error"] = f"Unexpected error: {str(e)}"
        logging.error(f"Unexpected error extracting metadata for {video_url}: {e}")

    return metadata


def enrich_with_metadata(organized_groups):
    """
    Enriches organized video groups with metadata for each video URL.

    Args:
        organized_groups: List of organized groups from organize_video_groups()

    Returns:
        Enriched groups with metadata_url field added to each video entry
    """
    enriched_groups = []
    total_videos = 0
    successful_extractions = 0

    for group in organized_groups:
        enriched_group = group.copy()

        if group["type"] == "topic_group":
            # Extract metadata for main topic video
            main_topic = enriched_group["main_topic"].copy()
            if main_topic.get("video_url"):
                logging.info(
                    f"Extracting metadata for main topic: {main_topic.get('content', 'Unknown')[:50]}..."
                )
                main_topic["metadata_url"] = extract_video_metadata(
                    main_topic["video_url"]
                )
                total_videos += 1
                if main_topic["metadata_url"]["error"] is None:
                    successful_extractions += 1
            enriched_group["main_topic"] = main_topic

            # Extract metadata for intervention videos
            enriched_interventions = []
            for intervention in group.get("interventions", []):
                enriched_intervention = intervention.copy()
                if enriched_intervention.get("video_url"):
                    logging.info(
                        f"Extracting metadata for speaker: {enriched_intervention.get('speaker_name', 'Unknown')}"
                    )
                    enriched_intervention["metadata_url"] = extract_video_metadata(
                        enriched_intervention["video_url"]
                    )
                    total_videos += 1
                    if enriched_intervention["metadata_url"]["error"] is None:
                        successful_extractions += 1
                enriched_interventions.append(enriched_intervention)
            enriched_group["interventions"] = enriched_interventions

        elif group["type"] == "standalone":
            # Extract metadata for standalone video
            if enriched_group.get("video_url"):
                logging.info(
                    f"Extracting metadata for standalone: {enriched_group.get('content', 'Unknown')[:50]}..."
                )
                enriched_group["metadata_url"] = extract_video_metadata(
                    enriched_group["video_url"]
                )
                total_videos += 1
                if enriched_group["metadata_url"]["error"] is None:
                    successful_extractions += 1

        enriched_groups.append(enriched_group)

    logging.info(
        f"Metadata extraction complete: {successful_extractions}/{total_videos} videos successfully processed"
    )
    return enriched_groups


def limit_enriched_groups_for_testing(enriched_groups, max_topics=2):
    """
    Limits enriched video groups to a specified number of main topics for testing purposes.

    Args:
        enriched_groups: List of enriched groups from enrich_with_metadata()
        max_topics: Maximum number of topic groups to keep (default: 2)

    Returns:
        Limited list of enriched groups containing only the specified number of topic groups
    """
    if not enriched_groups:
        return enriched_groups

    # Filter only topic groups (not standalone entries)
    topic_groups = [
        group for group in enriched_groups if group.get("type") == "topic_group"
    ]
    standalone_groups = [
        group for group in enriched_groups if group.get("type") != "topic_group"
    ]

    # Limit topic groups to max_topics
    limited_topic_groups = topic_groups[:max_topics]

    # Combine limited topic groups with any standalone entries
    limited_groups = limited_topic_groups + standalone_groups

    original_count = len(topic_groups)
    limited_count = len(limited_topic_groups)

    logging.info(
        f"Testing mode: Limited enriched groups from {original_count} to {limited_count} main topics"
    )

    return limited_groups

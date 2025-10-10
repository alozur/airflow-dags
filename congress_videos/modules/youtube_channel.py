"""
YouTube channel monitoring operations.

This module handles fetching and filtering videos from YouTube channels,
specifically for monitoring the Congress YouTube channel for plenary sessions.
"""

import logging
import os
import re
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from PyPDF2 import PdfReader


def fetch_youtube_channel_videos(channel_id: str, max_results: int = 10):
    """
    Fetch recent videos from YouTube channel using YouTube Data API.

    Uses API key authentication (no OAuth needed for public data).

    Args:
        channel_id: YouTube channel ID
        max_results: Maximum number of videos to fetch

    Returns:
        Dict with video information:
        - total_videos: Number of videos found
        - videos: List of video details

    Raises:
        ValueError: If API key is missing
        RuntimeError: If channel not found or API request fails
    """
    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        error_msg = "YOUTUBE_API_KEY environment variable not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    logging.info(f"Fetching STREAM videos from YouTube channel: {channel_id}")

    try:
        # Build YouTube service with API key (no OAuth needed for public data)
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        # Search for completed LIVE STREAMS only (not regular uploads)
        # This specifically queries the channel's "Streams" tab
        # eventType='completed' ensures we only get finished broadcasts
        search_response = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=max_results,
            order='date',
            type='video',
            eventType='completed'  # Only completed broadcasts (finished streams)
        ).execute()

        videos = []
        for item in search_response.get('items', []):
            video_data = {
                'video_id': item['id']['videoId'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'published_at': item['snippet']['publishedAt'],
                'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
                'channel_title': item['snippet']['channelTitle'],
            }
            videos.append(video_data)

        logging.info(f"Found {len(videos)} videos from channel")
        return {
            'total_videos': len(videos),
            'videos': videos
        }

    except (ValueError, RuntimeError):
        # Re-raise known errors
        raise
    except Exception as e:
        error_msg = f"Error fetching YouTube videos: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg) from e


def filter_plenary_session_videos(channel_videos, target_title: str, target_date: str):
    """
    Filter videos for "Sesión Plenaria (original)" based on title and date.

    Args:
        channel_videos: Results from fetch_youtube_channel_videos
        target_title: Title to filter for (e.g., "Sesión Plenaria (original)")
        target_date: Target date in YYYY-MM-DD format

    Returns:
        Dict with filtered videos:
        - total_matches: Number of matching videos
        - videos: List of matching video details
        - target_date: Target date used for filtering
    """
    if not channel_videos or not channel_videos.get('videos'):
        logging.warning("No channel videos to filter")
        return {
            'total_matches': 0,
            'videos': [],
            'target_date': target_date
        }

    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
    logging.info(f"Filtering for videos with title containing '{target_title}' on {target_date}")

    matching_videos = []
    for video in channel_videos['videos']:
        # Check if title contains target string (case-insensitive)
        if target_title.lower() in video['title'].lower():
            # Parse published date
            published_at = datetime.fromisoformat(video['published_at'].replace('Z', '+00:00'))
            published_date = published_at.date()

            # Check if date matches
            if published_date == target_date_obj:
                logging.info(f"Match found: {video['title']} - {video['video_id']}")
                matching_videos.append(video)

    logging.info(f"Found {len(matching_videos)} matching videos for {target_date}")
    return {
        'total_matches': len(matching_videos),
        'videos': matching_videos,
        'target_date': target_date
    }


def get_video_details(plenary_videos):
    """
    Get detailed information for videos (duration, timing, etc.).

    Note: We already filtered for completed streams, so no need to check status again.

    Args:
        plenary_videos: Results from filter_plenary_session_videos

    Returns:
        Dict with enriched video information:
        - total_videos: Number of videos
        - videos: List of video details with duration, timing, etc.
    """
    if not plenary_videos or not plenary_videos.get('videos'):
        logging.warning("No plenary videos to process")
        return {'total_videos': 0, 'videos': []}

    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        error_msg = "YOUTUBE_API_KEY environment variable not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Build YouTube service with API key (no OAuth needed for public data)
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        enriched_videos = []
        for video in plenary_videos['videos']:
            video_id = video['video_id']

            # Get detailed video information
            video_response = youtube.videos().list(
                part='snippet,contentDetails,liveStreamingDetails',
                id=video_id
            ).execute()

            if not video_response.get('items'):
                logging.warning(f"Video not found: {video_id}")
                continue

            video_details = video_response['items'][0]
            live_details = video_details.get('liveStreamingDetails', {})

            # Extract duration
            duration_iso = video_details['contentDetails']['duration']

            # Parse ISO 8601 duration (PT2H30M15S -> 2:30:15)
            duration_match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
            if duration_match:
                hours = int(duration_match.group(1) or 0)
                minutes = int(duration_match.group(2) or 0)
                seconds = int(duration_match.group(3) or 0)
                duration_seconds = hours * 3600 + minutes * 60 + seconds
            else:
                duration_seconds = 0

            enriched_video = {
                **video,
                'duration_seconds': duration_seconds,
                'duration_formatted': f"{hours}:{minutes:02d}:{seconds:02d}",
                'actual_start_time': live_details.get('actualStartTime'),
                'actual_end_time': live_details.get('actualEndTime'),
                'youtube_url': f"https://www.youtube.com/watch?v={video_id}",
            }

            logging.info(f"Video details: {video['title']} - Duration: {enriched_video['duration_formatted']}")
            enriched_videos.append(enriched_video)

        logging.info(f"Total videos enriched: {len(enriched_videos)}")
        return {
            'total_videos': len(enriched_videos),
            'videos': enriched_videos
        }

    except (ValueError, RuntimeError):
        # Re-raise known errors
        raise
    except Exception as e:
        error_msg = f"Error getting video details: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg) from e


def get_video_descriptions(plenary_videos):
    """
    Get full descriptions for videos.

    The search API only returns truncated descriptions, so we need to fetch
    full descriptions separately.

    Args:
        plenary_videos: Results from filter_plenary_session_videos

    Returns:
        Dict with video descriptions:
        - total_videos: Number of videos
        - videos: List with video_id and full description
    """
    if not plenary_videos or not plenary_videos.get('videos'):
        logging.warning("No plenary videos to process")
        return {'total_videos': 0, 'videos': []}

    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        error_msg = "YOUTUBE_API_KEY environment variable not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Build YouTube service with API key
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        video_descriptions = []
        for video in plenary_videos['videos']:
            video_id = video['video_id']

            # Get full video description
            video_response = youtube.videos().list(
                part='snippet',
                id=video_id
            ).execute()

            if not video_response.get('items'):
                logging.warning(f"Video not found: {video_id}")
                continue

            video_details = video_response['items'][0]
            full_description = video_details['snippet'].get('description', '')

            video_desc_data = {
                'video_id': video_id,
                'title': video['title'],
                'description': full_description,
                'description_length': len(full_description)
            }

            logging.info(f"Description fetched: {video['title']} ({len(full_description)} chars)")
            video_descriptions.append(video_desc_data)

        logging.info(f"Total descriptions fetched: {len(video_descriptions)}")
        return {
            'total_videos': len(video_descriptions),
            'videos': video_descriptions
        }

    except (ValueError, RuntimeError):
        # Re-raise known errors
        raise
    except Exception as e:
        error_msg = f"Error getting video descriptions: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg) from e


def parse_description_links(video_descriptions):
    """
    Parse video descriptions to extract Nota de prensa and Orden del día links.

    Args:
        video_descriptions: Results from get_video_descriptions

    Returns:
        Dict with extracted links:
        - total_videos: Number of videos processed
        - videos: List with video_id, press_release_link, agenda_link
    """
    if not video_descriptions or not video_descriptions.get('videos'):
        logging.warning("No video descriptions to parse")
        return {'total_videos': 0, 'videos': []}

    # Patterns to find links
    # Look for "Nota de prensa:" followed by URL
    press_release_pattern = r'Nota de prensa:\s*(https?://[^\s]+)'
    # Look for "Orden del día:" followed by URL (PDF)
    agenda_pattern = r'Orden del día:\s*(https?://[^\s]+)'

    parsed_videos = []
    for video in video_descriptions['videos']:
        description = video.get('description', '')
        video_id = video['video_id']

        # Find press release link
        press_match = re.search(press_release_pattern, description, re.IGNORECASE)
        press_link = press_match.group(1) if press_match else None

        # Find agenda link
        agenda_match = re.search(agenda_pattern, description, re.IGNORECASE)
        agenda_link = agenda_match.group(1) if agenda_match else None

        parsed_data = {
            'video_id': video_id,
            'title': video['title'],
            'press_release_link': press_link,
            'agenda_link': agenda_link,
        }

        logging.info(f"Links parsed for {video['title']}: Press={bool(press_link)}, Agenda={bool(agenda_link)}")
        parsed_videos.append(parsed_data)

    logging.info(f"Total videos parsed: {len(parsed_videos)}")
    return {
        'total_videos': len(parsed_videos),
        'videos': parsed_videos
    }


def scrape_press_release(parsed_links):
    """
    Scrape Nota de prensa (press release) websites.

    Follows shortened URLs (ow.ly) to the actual press release page and extracts content.

    Args:
        parsed_links: Results from parse_description_links

    Returns:
        Dict with scraped press releases:
        - total_scraped: Number of press releases scraped
        - videos: List with video_id, press_release_url, press_release_content
    """
    if not parsed_links or not parsed_links.get('videos'):
        logging.warning("No parsed links to scrape")
        return {'total_scraped': 0, 'videos': []}

    # Headers to make the request look like it's from a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    scraped_releases = []
    for video in parsed_links['videos']:
        video_id = video['video_id']
        press_link = video.get('press_release_link')

        if not press_link:
            logging.info(f"No press release link for {video['title']}")
            continue

        try:
            # Follow redirects to get actual URL
            logging.info(f"Fetching press release from {press_link}")
            response = requests.get(press_link, timeout=10, allow_redirects=True, verify=False, headers=headers)
            response.raise_for_status()

            actual_url = response.url
            logging.info(f"Resolved to: {actual_url}")

            # Parse HTML content
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract main content (adjust selectors based on actual site structure)
            # Try common content containers
            content = None
            for selector in ['article', 'main', '.content', '#content', '.post-content']:
                element = soup.select_one(selector)
                if element:
                    content = element.get_text(strip=True, separator='\n')
                    break

            if not content:
                # Fallback: get all paragraph text
                paragraphs = soup.find_all('p')
                content = '\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])

            # Extract title
            title_tag = soup.find('h1') or soup.find('title')
            page_title = title_tag.get_text(strip=True) if title_tag else "No title"

            scraped_data = {
                'video_id': video_id,
                'video_title': video['title'],
                'press_release_url': actual_url,
                'press_release_title': page_title,
                'press_release_content': content,
                'content_length': len(content) if content else 0
            }

            logging.info(f"Press release scraped: {page_title} ({len(content) if content else 0} chars)")
            scraped_releases.append(scraped_data)

        except Exception as e:
            logging.error(f"Error scraping press release for {video_id}: {e}")
            scraped_releases.append({
                'video_id': video_id,
                'video_title': video['title'],
                'press_release_url': press_link,
                'error': str(e)
            })

    logging.info(f"Total press releases scraped: {len(scraped_releases)}")
    return {
        'total_scraped': len(scraped_releases),
        'videos': scraped_releases
    }


def download_and_read_agenda(parsed_links):
    """
    Download and read Orden del día (agenda) PDFs.

    Args:
        parsed_links: Results from parse_description_links

    Returns:
        Dict with agenda content:
        - total_downloaded: Number of PDFs downloaded
        - videos: List with video_id, agenda_url, agenda_file_path, agenda_text
    """
    if not parsed_links or not parsed_links.get('videos'):
        logging.warning("No parsed links to download")
        return {'total_downloaded': 0, 'videos': []}

    # Get data directory path
    from congress_videos.config.paths import PROJECT_DATA_DIR, ensure_directory_exists

    # Create agenda directory
    agenda_dir = f"{PROJECT_DATA_DIR}/agendas"
    ensure_directory_exists(agenda_dir)

    # Headers to make the request look like it's from a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    downloaded_agendas = []
    for video in parsed_links['videos']:
        video_id = video['video_id']
        agenda_link = video.get('agenda_link')

        if not agenda_link:
            logging.info(f"No agenda link for {video['title']}")
            continue

        try:
            # Follow redirects to get actual URL
            logging.info(f"Downloading agenda from {agenda_link}")
            response = requests.get(agenda_link, timeout=30, allow_redirects=True, verify=False, headers=headers)
            response.raise_for_status()

            actual_url = response.url
            logging.info(f"Resolved to: {actual_url}")

            # Save PDF
            pdf_filename = f"{video_id}_agenda.pdf"
            pdf_path = f"{agenda_dir}/{pdf_filename}"

            with open(pdf_path, 'wb') as f:
                f.write(response.content)

            logging.info(f"PDF saved: {pdf_path} ({len(response.content)} bytes)")

            # Read PDF content
            try:
                reader = PdfReader(pdf_path)
                text_content = ""
                for page_num, page in enumerate(reader.pages):
                    text_content += f"\n--- Page {page_num + 1} ---\n"
                    text_content += page.extract_text()

                agenda_data = {
                    'video_id': video_id,
                    'video_title': video['title'],
                    'agenda_url': actual_url,
                    'agenda_file_path': pdf_path,
                    'agenda_text': text_content,
                    'pdf_pages': len(reader.pages),
                    'text_length': len(text_content)
                }

                logging.info(f"PDF read: {len(reader.pages)} pages, {len(text_content)} chars")
                downloaded_agendas.append(agenda_data)

            except Exception as pdf_error:
                logging.error(f"Error reading PDF {pdf_path}: {pdf_error}")
                downloaded_agendas.append({
                    'video_id': video_id,
                    'video_title': video['title'],
                    'agenda_url': actual_url,
                    'agenda_file_path': pdf_path,
                    'error': f"PDF read error: {str(pdf_error)}"
                })

        except Exception as e:
            logging.error(f"Error downloading agenda for {video_id}: {e}")
            downloaded_agendas.append({
                'video_id': video_id,
                'video_title': video['title'],
                'agenda_url': agenda_link,
                'error': str(e)
            })

    logging.info(f"Total agendas downloaded: {len(downloaded_agendas)}")
    return {
        'total_downloaded': len(downloaded_agendas),
        'videos': downloaded_agendas
    }

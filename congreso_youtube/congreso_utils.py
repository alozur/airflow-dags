# dags/congreso_youtube/congreso_utils.py
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import logging
import re
import urllib3
import json
import os
from urllib.parse import urlparse
import openai

# -------------------------
# Constants
# -------------------------
BASE_ARCHIVE_URL = "https://www.congreso.es/es/archivo-audiovisual"
BASE_SESSION_URL = "https://app.congreso.es/AudiovisualCongreso/audiovisualdetalledisponible"
LEGISLATURE_ID = 15
ORGANO_ID = 400

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging setup
logging.basicConfig(level=logging.INFO)

# OpenAI Configuration - gets API key from Airflow environment
openai.api_key = os.getenv('OPENAI_API_KEY')

# -------------------------
# Functions
# -------------------------

def construct_url(target_date=None):
  """
  Constructs the Congreso audiovisual archive URL for a given date.
  If target_date is None, defaults to yesterday.
  target_date can be a datetime object or 'YYYY-MM-DD' string.
  """
  if target_date:
      if isinstance(target_date, str):
          target_date = datetime.strptime(target_date, "%Y-%m-%d")
  else:
      target_date = datetime.now() - timedelta(days=1)  # default: yesterday

  day, month, year = target_date.day, target_date.month, target_date.year

  url = (
      f"{BASE_ARCHIVE_URL}"
      f"?p_p_id=emisiones&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view"
      f"&_emisiones_idLegislaturaElegida={LEGISLATURE_ID}"
      f"&_emisiones_dia={day:02d}"
      f"&_emisiones_mes={month:02d}"
      f"&_emisiones_anio={year}"
  )
  logging.info(f"Constructed URL: {url}")
  return url

def get_soup(url):
    """Fetches and parses HTML from a given URL."""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page: {response.status_code}")
    return BeautifulSoup(response.content, "html.parser")

def has_plenary_session(soup):
    """Checks if 'Sesión Plenaria' is present in the HTML."""
    return bool(soup.find(string=lambda text: "Sesión Plenaria" in text))

def get_session_number(soup):
    """Extracts the session number from the HTML table."""
    table = soup.find("table", class_="table-agenda")
    if not table:
        return None
    tbody = table.find("tbody")
    if not tbody:
        return None
    rows = tbody.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) > 1:
            session_text = cells[1].get_text(strip=True)
            match = re.search(r"Sesión Plenaria \(Sesión número:\s*(\d+)", session_text)
            if match:
                return match.group(1)
    return None

def construct_session_link(cod_sesion, target_date=None):
  """
  Constructs the direct session link.
  If target_date is None, defaults to yesterday.
  target_date can be a datetime object or 'YYYY-MM-DD' string.
  """
  if target_date:
      if isinstance(target_date, str):
          target_date = datetime.strptime(target_date, "%Y-%m-%d")
  else:
      target_date = datetime.now() - timedelta(days=1)  # default: yesterday

  sesion_date = target_date.strftime("%d/%m/%Y")

  url = (
      f"{BASE_SESSION_URL}?"
      f"codOrgano={ORGANO_ID}"
      f"&codSesion={cod_sesion}"
      f"&idLegislaturaElegida={LEGISLATURE_ID}"
      f"&fechaSesion={sesion_date}"
  )
  logging.info(f"Constructed session link: {url}")
  return url

def extract_video_data(html_string):
    """
    Extracts video data from the session HTML.
    Returns a list of dictionaries with speaker_name, video_url, profile_link, and is_bold flag.
    The is_bold flag indicates if this is a main topic/question (wrapped in <b> tag).
    """
    soup = BeautifulSoup(html_string, 'html.parser')
    results = []
    
    # Log the total number of rows found
    rows = soup.find_all('tr')
    logging.info(f"Found {len(rows)} total <tr> elements in HTML")
    
    for idx, row in enumerate(rows):
        row_html = str(row)
        
        # Debug logging for all rows to see what we're dealing with
        logging.debug(f"Row {idx}: checking for video data...")
        
        # Check if this row contains a bold entry (main topic/question)
        # The <b> tag wraps the main link with the question
        is_bold_entry = False
        bold_tag = row.find('b')
        if bold_tag:
            # Check if the bold tag contains our target link
            bold_link = bold_tag.find('a', {'id': re.compile(r'\d+')})
            if bold_link and 'directopartes' in str(bold_link.get('onclick', '')):
                is_bold_entry = True
                logging.debug(f"Row {idx}: Found BOLD entry (main topic)")
        
        # Look for any <a> tag with an ID and onclick that contains directopartes
        all_links = row.find_all('a')
        
        speaker_info = None
        entry_id = None
        
        for link in all_links:
            link_str = str(link)
            # Check if this link has an ID and onclick with directopartes
            if 'id=' in link_str and 'onclick=' in link_str and 'directopartes' in link_str:
                # Extract the ID
                id_match = re.search(r'id="(\d+)"', link_str)
                if id_match:
                    entry_id = id_match.group(1)
                    
                # Extract the text content (speaker name or question)
                span = link.find('span')
                if span:
                    speaker_info = span.get_text(strip=True)
                    logging.info(f"Found {'BOLD ' if is_bold_entry else ''}entry ID {entry_id}: {speaker_info[:50]}...")
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
                name_match = re.search(r'(?:Diputad[oa]|Sr\.|Sra\.|D\.|Dña\.)\s+([^,]+)', speaker_info)
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
            href = link.get('href', '')
            if 'static.congreso.es' in href and '.mp4' in href:
                video_url = href
                logging.info(f"Found video URL: {video_url[:80]}...")
                break
        
        if not video_url:
            logging.debug(f"No MP4 link found for: {speaker_name[:50]}")
            continue
        
        # Look for profile/ficha link
        profile_link = None
        for link in all_links:
            href = link.get('href', '')
            if 'busqueda-de-diputados' in href:
                profile_link = href.replace('&amp;', '&')
                logging.debug(f"Found profile link: {profile_link[:80]}...")
                break
        
        # Check if we also have a role/position (e.g., "Presidente del Gobierno")
        role = None
        if "(Presidente" in row_html or "(Vicepresidente" in row_html:
            role_match = re.search(r'\((Presidente[^)]*|Vicepresidente[^)]*)\)', row_html)
            if role_match:
                role = role_match.group(1)
                logging.debug(f"Found role: {role}")
        
        # Create the result entry
        result_entry = {
            'speaker_name': speaker_name,
            'video_url': video_url,
            'profile_link': profile_link,
            'entry_id': entry_id,
            'is_bold': is_bold_entry  # Flag to indicate if this is a main topic
        }
        
        if role:
            result_entry['role'] = role
        
        results.append(result_entry)
        logging.info(f"Added {'BOLD ' if is_bold_entry else ''}entry: {speaker_name[:50]} - ID: {entry_id}")
    
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
        is_bold = item.get('is_bold', False)
        speaker_name = item.get('speaker_name', '')
        
        if is_bold:
            # This is a main topic/question (bold entry) - start a new group
            if current_group:
                # Save the previous group if it exists
                organized_groups.append(current_group)
            
            # Create new group with this bold entry as the parent
            current_group = {
                'type': 'topic_group',
                'main_topic': {
                    'content': speaker_name,  # Full question/topic text
                    'entry_id': item.get('entry_id'),
                    'video_url': item.get('video_url'),
                    'profile_link': item.get('profile_link'),
                    'is_bold': True  # Preserve the main topic flag
                },
                'interventions': []  # Children - speakers discussing this topic
            }
            logging.debug(f"Created new topic group: {speaker_name[:50]}...")
            
        else:
            # This is an intervention (non-bold entry)
            if current_group:
                # Add this as a child to the current group
                intervention = {
                    'speaker_name': speaker_name,
                    'entry_id': item.get('entry_id'),
                    'video_url': item.get('video_url'),
                    'profile_link': item.get('profile_link'),
                    'role': item.get('role')
                }
                current_group['interventions'].append(intervention)
                logging.debug(f"Added intervention to current group: {speaker_name[:50]}...")
            else:
                # No current group (shouldn't happen with well-formed data)
                # Create a standalone entry
                organized_groups.append({
                    'type': 'standalone',
                    'content': speaker_name,
                    'entry_id': item.get('entry_id'),
                    'video_url': item.get('video_url'),
                    'profile_link': item.get('profile_link'),
                    'role': item.get('role')
                })
                logging.warning(f"Found standalone entry without parent: {speaker_name[:50]}...")
    
    # Don't forget the last group
    if current_group:
        organized_groups.append(current_group)
    
    # Log summary
    total_topics = sum(1 for g in organized_groups if g['type'] == 'topic_group')
    total_interventions = sum(len(g.get('interventions', [])) for g in organized_groups if g['type'] == 'topic_group')
    
    logging.info(f"Organized {len(video_items)} items into {len(organized_groups)} groups")
    logging.info(f"Found {total_topics} main topics with {total_interventions} total interventions")
    
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
        "error": None
    }
    
    try:
        # Extract filename from URL
        parsed_url = urlparse(video_url)
        metadata["filename"] = parsed_url.path.split('/')[-1] if parsed_url.path else None
        
        # Make HEAD request to get basic metadata
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "video/mp4,video/*;q=0.9,*/*;q=0.8",
            "Accept-Language": "es,en;q=0.9",
            "Accept-Encoding": "identity"
        }
        
        logging.debug(f"Making HEAD request to: {video_url}")
        response = requests.head(video_url, headers=headers, timeout=15, verify=False)
        
        if response.status_code == 200:
            # Extract only required headers
            metadata["last_modified"] = response.headers.get('last-modified')
            
            # Extract file size
            if 'content-length' in response.headers:
                file_size = int(response.headers['content-length'])
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
                    
                    logging.info(f"Duration estimated: {minutes}:{seconds:02d} for {metadata['file_size_mb']}MB")
            
            logging.info(f"Video metadata extracted successfully: {metadata['file_size_mb']}MB, Duration: {metadata.get('duration_estimated', 'unknown')}")
            
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
        
        if group['type'] == 'topic_group':
            # Extract metadata for main topic video
            main_topic = enriched_group['main_topic'].copy()
            if main_topic.get('video_url'):
                logging.info(f"Extracting metadata for main topic: {main_topic.get('content', 'Unknown')[:50]}...")
                main_topic['metadata_url'] = extract_video_metadata(main_topic['video_url'])
                total_videos += 1
                if main_topic['metadata_url']['error'] is None:
                    successful_extractions += 1
            enriched_group['main_topic'] = main_topic
            
            # Extract metadata for intervention videos
            enriched_interventions = []
            for intervention in group.get('interventions', []):
                enriched_intervention = intervention.copy()
                if enriched_intervention.get('video_url'):
                    logging.info(f"Extracting metadata for speaker: {enriched_intervention.get('speaker_name', 'Unknown')}")
                    enriched_intervention['metadata_url'] = extract_video_metadata(enriched_intervention['video_url'])
                    total_videos += 1
                    if enriched_intervention['metadata_url']['error'] is None:
                        successful_extractions += 1
                enriched_interventions.append(enriched_intervention)
            enriched_group['interventions'] = enriched_interventions
            
        elif group['type'] == 'standalone':
            # Extract metadata for standalone video
            if enriched_group.get('video_url'):
                logging.info(f"Extracting metadata for standalone: {enriched_group.get('content', 'Unknown')[:50]}...")
                enriched_group['metadata_url'] = extract_video_metadata(enriched_group['video_url'])
                total_videos += 1
                if enriched_group['metadata_url']['error'] is None:
                    successful_extractions += 1
        
        enriched_groups.append(enriched_group)
    
    logging.info(f"Metadata extraction complete: {successful_extractions}/{total_videos} videos successfully processed")
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
    topic_groups = [group for group in enriched_groups if group.get('type') == 'topic_group']
    standalone_groups = [group for group in enriched_groups if group.get('type') != 'topic_group']

    # Limit topic groups to max_topics
    limited_topic_groups = topic_groups[:max_topics]

    # Combine limited topic groups with any standalone entries
    limited_groups = limited_topic_groups + standalone_groups

    original_count = len(topic_groups)
    limited_count = len(limited_topic_groups)

    logging.info(f"Testing mode: Limited enriched groups from {original_count} to {limited_count} main topics")

    return limited_groups

def create_session_folder(session_number, base_data_path="/opt/airflow/data"):
    """
    Creates a session folder inside the congreso_youtube data directory.

    Args:
        session_number: The session number from get_session_number task
        base_data_path: Base path for data directories (default: '/opt/airflow/data')

    Returns:
        Full path to the created session directory
    """
    session_folder_path = os.path.join(base_data_path, "congreso_youtube", session_number)

    if not os.path.exists(session_folder_path):
        os.makedirs(session_folder_path, exist_ok=True)
        logging.info(f"Created session folder: {session_folder_path}")
    else:
        logging.info(f"Session folder already exists: {session_folder_path}")

    return session_folder_path

def download_video_file(video_url, output_path):
    """
    Downloads a video file from URL to the specified path.

    Args:
        video_url: URL of the video to download
        output_path: Full file path where to save the video

    Returns:
        Dict with success status and details
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "video/mp4,video/*;q=0.9,*/*;q=0.8",
            "Accept-Language": "es,en;q=0.9",
        }

        logging.info(f"Starting download: {video_url}")
        response = requests.get(video_url, headers=headers, stream=True, verify=False, timeout=300)

        if response.status_code == 200:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(output_path)
            logging.info(f"Successfully downloaded {file_size} bytes to: {output_path}")

            return {
                "success": True,
                "file_path": output_path,
                "file_size_bytes": file_size,
                "error": None
            }
        else:
            error_msg = f"HTTP {response.status_code}: {response.reason}"
            logging.error(f"Failed to download video: {error_msg}")
            return {
                "success": False,
                "file_path": None,
                "file_size_bytes": 0,
                "error": error_msg
            }

    except Exception as e:
        error_msg = f"Download error: {str(e)}"
        logging.error(f"Exception during video download: {error_msg}")
        return {
            "success": False,
            "file_path": None,
            "file_size_bytes": 0,
            "error": error_msg
        }

def create_topic_info_file(topic_entry_id, main_topic, interventions, topic_folder_path):
    """
    Creates an informational file for a topic with entry details and speaker information.

    Args:
        topic_entry_id: Entry ID for the main topic
        main_topic: Main topic data from enriched video groups
        interventions: List of interventions for this topic
        topic_folder_path: Path to the topic folder

    Returns:
        Path to the created info file
    """
    info_file_path = os.path.join(topic_folder_path, f"{topic_entry_id}_info.txt")

    try:
        with open(info_file_path, 'w', encoding='utf-8') as f:
            f.write(f"TOPIC INFORMATION\n")
            f.write(f"=================\n\n")
            f.write(f"Entry ID: {topic_entry_id}\n")
            f.write(f"Profile Link: {main_topic.get('profile_link', 'N/A')}\n\n")

            f.write(f"MAIN TOPIC:\n")
            f.write(f"-----------\n")
            f.write(f"{main_topic.get('content', 'N/A')}\n\n")

            f.write(f"SPEAKERS AND INTERVENTIONS:\n")
            f.write(f"---------------------------\n")

            for i, intervention in enumerate(interventions, 1):
                speaker_name = intervention.get('speaker_name', 'Unknown')
                role = intervention.get('role')

                f.write(f"{i}. {speaker_name}")
                if role:
                    f.write(f" - {role}")
                f.write(f"\n")

            if not interventions:
                f.write("No interventions recorded for this topic.\n")

        logging.info(f"Created topic info file: {info_file_path}")
        return info_file_path

    except Exception as e:
        logging.error(f"Failed to create info file {info_file_path}: {str(e)}")
        return None

def download_main_topic_videos(enriched_video_groups, session_folder_path):
    """
    Downloads ONLY main topic videos and creates folder structure for each topic.

    IMPORTANT: This function downloads only the main topic videos (tema principal)
    from each topic group. Individual intervention videos (intervenciones de diputados)
    are NOT downloaded - they are processed for metadata only.

    For each topic group:
    - Downloads: 1 main topic video per group
    - Skips: All intervention videos within that group
    - Creates: Topic folder structure and info files

    Args:
        enriched_video_groups: Enriched video groups from previous task
        session_folder_path: Path to the session folder

    Returns:
        Dict with download results and summary containing:
        - successful_downloads: Number of main topic videos downloaded successfully
        - failed_downloads: Number of main topic videos that failed to download
        - total_topics: Total number of topic groups processed
        - download_details: List of detailed results per topic
    """
    download_results = {
        "successful_downloads": 0,
        "failed_downloads": 0,
        "total_topics": 0,
        "download_details": []
    }

    for group in enriched_video_groups:
        if group.get('type') == 'topic_group':
            download_results["total_topics"] += 1

            main_topic = group.get('main_topic', {})
            topic_entry_id = main_topic.get('entry_id')

            if not topic_entry_id:
                logging.warning("Skipping topic group without entry_id")
                continue

            # Create folder for this topic
            topic_folder_path = os.path.join(session_folder_path, topic_entry_id)
            os.makedirs(topic_folder_path, exist_ok=True)

            # Create info file
            interventions = group.get('interventions', [])
            info_file_path = create_topic_info_file(
                topic_entry_id, main_topic, interventions, topic_folder_path
            )

            # Download main topic video
            video_url = main_topic.get('video_url')
            if video_url:
                # Extract filename from metadata or URL
                metadata = main_topic.get('metadata_url', {})
                filename = metadata.get('filename')
                if not filename:
                    # Fallback to extracting from URL
                    parsed_url = urlparse(video_url)
                    filename = parsed_url.path.split('/')[-1] if parsed_url.path else f"{topic_entry_id}.mp4"

                video_output_path = os.path.join(topic_folder_path, filename)

                download_result = download_video_file(video_url, video_output_path)

                result_detail = {
                    "topic_entry_id": topic_entry_id,
                    "video_url": video_url,
                    "output_path": video_output_path if download_result["success"] else None,
                    "info_file_path": info_file_path,
                    "success": download_result["success"],
                    "error": download_result["error"],
                    "file_size_bytes": download_result["file_size_bytes"]
                }

                download_results["download_details"].append(result_detail)

                if download_result["success"]:
                    download_results["successful_downloads"] += 1
                    logging.info(f"Successfully processed topic {topic_entry_id}")
                else:
                    download_results["failed_downloads"] += 1
                    logging.error(f"Failed to download video for topic {topic_entry_id}: {download_result['error']}")
            else:
                logging.warning(f"No video URL found for topic {topic_entry_id}")
                download_results["failed_downloads"] += 1

    logging.info(f"Download summary: {download_results['successful_downloads']}/{download_results['total_topics']} topics processed successfully")
    return download_results

def generate_youtube_title(main_topic_content, speakers_info, max_length=100):
    """
    Generates a YouTube-optimized title for a congressional video using OpenAI.

    Args:
        main_topic_content: The main topic/question content
        speakers_info: List of speaker information including names and roles
        max_length: Maximum title length (YouTube recommends under 100 characters)

    Returns:
        Dict with generated title and metadata
    """
    try:
        # Prepare speaker context
        speaker_context = ""
        if speakers_info:
            main_speakers = [
                f"{s.get('speaker_name', 'Unknown')}" +
                (f" ({s.get('role')})" if s.get('role') else "")
                for s in speakers_info[:3]  # Limit to first 3 speakers
            ]
            speaker_context = f" Participan: {', '.join(main_speakers)}"

        prompt = f"""
Genera un título optimizado para YouTube de un vídeo del Congreso de España. El título debe ser:
- Atractivo y claro para audiencia general
- Máximo {max_length} caracteres
- Incluir palabras clave relevantes
- Evitar jerga política compleja

Contenido del debate: {main_topic_content}
{speaker_context}

Genera solo el título, sin explicaciones adicionales.
"""

        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Eres un experto en crear títulos atractivos para contenido político español en YouTube."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,
            temperature=0.7
        )

        generated_title = response.choices[0].message.content.strip()

        # Ensure title doesn't exceed max_length
        if len(generated_title) > max_length:
            generated_title = generated_title[:max_length-3] + "..."

        logging.info(f"Generated YouTube title ({len(generated_title)} chars): {generated_title}")

        return {
            "title": generated_title,
            "character_count": len(generated_title),
            "within_limit": len(generated_title) <= max_length,
            "error": None
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
            "error": error_msg
        }

def generate_youtube_description(main_topic_content, speakers_info, video_metadata, session_number, target_date=None):
    """
    Generates a YouTube-optimized description for a congressional video using OpenAI.

    Args:
        main_topic_content: The main topic/question content
        speakers_info: List of speaker information including names and roles
        video_metadata: Video file metadata (duration, size, etc.)
        session_number: Congressional session number
        target_date: Date of the session for generating session link

    Returns:
        Dict with generated description and metadata
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

        # Prepare speaker list for prompt
        speaker_context = ""
        if speakers_info:
            speaker_names = []
            for speaker in speakers_info[:4]:  # Limit to 4 speakers for prompt
                name = speaker.get('speaker_name', 'Unknown')
                role = speaker.get('role', '')
                if role:
                    speaker_names.append(f"{name} ({role})")
                else:
                    speaker_names.append(name)
            speaker_context = f"Participantes principales: {', '.join(speaker_names)}"

        # Prepare video info
        duration = video_metadata.get('duration_estimated', 'N/A')
        duration_info = f"Duración: {duration}" if duration != 'N/A' else ""

        prompt = f"""
Crea una descripción para YouTube de un debate del Congreso español. La descripción debe:

- Ser natural y conversacional (no robótica)
- Usar saltos de línea para separar secciones claramente
- Explicar el contexto político de forma accesible
- Incluir emojis relevantes para hacer más atractivo el contenido
- Terminar con hashtags españoles relevantes

CONTENIDO DEL DEBATE:
{main_topic_content}

INFORMACIÓN ADICIONAL:
- Sesión número: {session_number}
- {speaker_context}
- {duration_info}

ESTRUCTURA REQUERIDA:
1. Párrafo introductorio explicando el tema (con emojis)
2. Salto de línea doble
3. Contexto político o relevancia del debate
4. Salto de línea doble
5. Información sobre los participantes
6. Salto de línea doble
7. Hashtags relevantes (mínimo 5)

Escribe de forma natural, como si fueras un periodista explicando el debate a ciudadanos interesados en política.
"""

        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Eres un periodista político español experto en comunicar de forma clara y atractiva. Usas un lenguaje natural, cercano pero profesional, y estructuras bien el contenido con saltos de línea."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.7
        )

        ai_generated_content = response.choices[0].message.content.strip()

        # Build final structured description
        final_description = ai_generated_content

        # Add technical information section with proper formatting
        final_description += "\n\n" + "─" * 40
        final_description += "\n📺 INFORMACIÓN DE LA SESIÓN\n"

        if duration != 'N/A':
            final_description += f"⏱️ Duración: {duration}\n"

        final_description += f"🏛️ Sesión Plenaria Nº {session_number}\n"

        if session_link and session_link != "https://www.congreso.es":
            final_description += f"🔗 Ver sesión completa: {session_link}\n"

        final_description += f"📜 Fuente oficial: Congreso de los Diputados\n"
        final_description += f"🌐 www.congreso.es"

        logging.info(f"Generated structured YouTube description ({len(final_description)} chars)")

        return {
            "description": final_description,
            "character_count": len(final_description),
            "word_count": len(final_description.split()),
            "error": None
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
            "error": error_msg
        }

def generate_youtube_metadata_from_enriched_groups(enriched_video_groups, session_number, target_date=None):
    """
    Generates YouTube metadata (titles and descriptions) directly from enriched video groups.
    This function works without requiring downloads to be completed first.

    Args:
        enriched_video_groups: Enriched video groups from enrich_with_metadata function
        session_number: Congressional session number
        target_date: Date of the session for generating session links

    Returns:
        Dict with metadata for each topic
    """
    metadata_results = {
        "total_topics": 0,
        "successful_generations": 0,
        "failed_generations": 0,
        "topic_metadata": []
    }

    for group in enriched_video_groups:
        if group.get('type') == 'topic_group':
            metadata_results["total_topics"] += 1

            main_topic = group.get('main_topic', {})
            topic_entry_id = main_topic.get('entry_id')
            main_topic_content = main_topic.get('content', '')

            # Extract speakers info from interventions
            speakers_info = []
            for intervention in group.get('interventions', []):
                speakers_info.append({
                    'speaker_name': intervention.get('speaker_name', ''),
                    'role': intervention.get('role', '')
                })

            # Get video metadata from the enriched data
            video_metadata = main_topic.get('metadata_url', {})

            # Generate title and description
            logging.info(f"Generating YouTube metadata for topic {topic_entry_id}")
            title_result = generate_youtube_title(main_topic_content, speakers_info)
            description_result = generate_youtube_description(
                main_topic_content, speakers_info, video_metadata, session_number, target_date
            )

            topic_metadata = {
                "topic_entry_id": topic_entry_id,
                "video_file_path": None,  # Will be set later after download
                "info_file_path": None,   # Will be set later after download
                "title": title_result,
                "description": description_result,
                "main_topic_content": main_topic_content,
                "video_url": main_topic.get('video_url'),
                "video_metadata": video_metadata,
                "speakers_info": speakers_info,
                "generation_success": title_result.get('error') is None and description_result.get('error') is None
            }

            metadata_results["topic_metadata"].append(topic_metadata)

            if topic_metadata["generation_success"]:
                metadata_results["successful_generations"] += 1
            else:
                metadata_results["failed_generations"] += 1

    logging.info(f"YouTube metadata generation complete: {metadata_results['successful_generations']}/{metadata_results['total_topics']} topics processed successfully")
    return metadata_results

def generate_youtube_metadata_for_topics(download_results, session_number, target_date=None):
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
        "total_topics": len(download_results.get('download_details', [])),
        "successful_generations": 0,
        "failed_generations": 0,
        "topic_metadata": []
    }

    for detail in download_results.get('download_details', []):
        if not detail.get('success'):
            # Skip topics that weren't downloaded successfully
            continue

        topic_entry_id = detail.get('topic_entry_id')

        # Read the info file to get topic and speaker data
        info_file_path = detail.get('info_file_path')
        main_topic_content = ""
        speakers_info = []

        if info_file_path and os.path.exists(info_file_path):
            try:
                with open(info_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Extract main topic (basic parsing)
                    if "MAIN TOPIC:" in content:
                        topic_start = content.find("MAIN TOPIC:") + len("MAIN TOPIC:")
                        topic_end = content.find("SPEAKERS AND INTERVENTIONS:")
                        if topic_end == -1:
                            topic_end = len(content)
                        main_topic_content = content[topic_start:topic_end].strip().replace("-----------", "").strip()

            except Exception as e:
                logging.warning(f"Could not read info file {info_file_path}: {e}")

        # Get video metadata if available
        video_metadata = {}
        if 'metadata' in detail:
            video_metadata = detail['metadata']

        # Generate title
        logging.info(f"Generating YouTube metadata for topic {topic_entry_id}")
        title_result = generate_youtube_title(main_topic_content, speakers_info)
        description_result = generate_youtube_description(
            main_topic_content, speakers_info, video_metadata, session_number, target_date
        )

        topic_metadata = {
            "topic_entry_id": topic_entry_id,
            "video_file_path": detail.get('output_path'),
            "info_file_path": info_file_path,
            "title": title_result,
            "description": description_result,
            "main_topic_content": main_topic_content,
            "generation_success": title_result.get('error') is None and description_result.get('error') is None
        }

        metadata_results["topic_metadata"].append(topic_metadata)

        if topic_metadata["generation_success"]:
            metadata_results["successful_generations"] += 1
        else:
            metadata_results["failed_generations"] += 1

    logging.info(f"YouTube metadata generation complete: {metadata_results['successful_generations']}/{metadata_results['total_topics']} topics processed successfully")
    return metadata_results

def evaluate_video_interest_with_ai(enriched_video_groups):
    """
    Evaluates video interest for YouTube upload using OpenAI.

    ONLY evaluates main topics (not interventions).
    For main topics: Considers the speakers participating in the topic

    Args:
        enriched_video_groups: Enriched video groups from enrich_with_metadata function

    Returns:
        Dict with evaluation results for each main topic
    """
    evaluation_results = {
        "total_videos_evaluated": 0,
        "successful_evaluations": 0,
        "failed_evaluations": 0,
        "evaluations": []
    }

    for group in enriched_video_groups:
        if group.get('type') == 'topic_group':
            main_topic = group.get('main_topic', {})
            main_topic_content = main_topic.get('content', '')
            main_topic_entry_id = main_topic.get('entry_id')

            # Collect speakers from interventions for main topic evaluation
            speakers_info = []
            for intervention in group.get('interventions', []):
                speakers_info.append({
                    'speaker_name': intervention.get('speaker_name', 'Desconocido'),
                    'role': intervention.get('role', 'Sin rol especificado')
                })

            # Evaluate ONLY main topic (not interventions)
            main_topic_score = _evaluate_main_topic_interest(
                main_topic_content,
                speakers_info,
                main_topic.get('metadata_url', {})
            )

            evaluation_results["evaluations"].append({
                "entry_id": main_topic_entry_id,
                "video_type": "main_topic",
                "interest_score": main_topic_score.get('score', 5),
                "reasoning": main_topic_score.get('reasoning', ''),
                "evaluation_success": main_topic_score.get('error') is None,
                "error": main_topic_score.get('error')
            })

            evaluation_results["total_videos_evaluated"] += 1
            if main_topic_score.get('error') is None:
                evaluation_results["successful_evaluations"] += 1
            else:
                evaluation_results["failed_evaluations"] += 1

    logging.info(f"Main topic evaluation complete: {evaluation_results['successful_evaluations']}/{evaluation_results['total_videos_evaluated']} main topics evaluated successfully")
    return evaluation_results

def _evaluate_main_topic_interest(topic_content, speakers_info, video_metadata):
    """
    Evaluates the interest level of a main topic video for YouTube upload.

    Args:
        topic_content: The main topic title/description
        speakers_info: List of speakers participating in this topic
        video_metadata: Video metadata (duration, etc.)

    Returns:
        Dict with score (1-10) and reasoning
    """
    try:
        # Format speakers list
        speakers_text = "\n".join([
            f"- {speaker['speaker_name']} ({speaker['role']})"
            for speaker in speakers_info[:10]  # Limit to first 10 speakers
        ])

        if len(speakers_info) > 10:
            speakers_text += f"\n... y {len(speakers_info) - 10} participantes más"

        duration_minutes = video_metadata.get('duration_seconds', 0) // 60

        prompt = f"""Evalúa el interés de este vídeo del Congreso de los Diputados para subirlo a YouTube.

TEMA: {topic_content}

PARTICIPANTES:
{speakers_text}

DURACIÓN: {duration_minutes} minutos

Criterios de evaluación:
1. Relevancia política y social (0-3 puntos)
2. Notoriedad de los participantes (0-3 puntos)
3. Potencial de debate público (0-2 puntos)
4. Interés mediático (0-2 puntos)

Responde SOLO con un JSON en este formato:
{{"score": <número del 1-10>, "reasoning": "<explicación breve en español>"}}"""

        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un experto en política española y contenido viral para YouTube. Evalúas objetivamente el interés público de debates parlamentarios."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=200
        )

        result_text = response.choices[0].message.content.strip()

        # Try to parse JSON response
        try:
            # Remove markdown code blocks if present
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]

            result = json.loads(result_text)
            score = int(result.get('score', 5))
            score = max(1, min(10, score))  # Clamp between 1-10

            return {
                "score": score,
                "reasoning": result.get('reasoning', 'Sin justificación'),
                "error": None
            }
        except json.JSONDecodeError:
            logging.warning(f"Failed to parse AI response as JSON: {result_text}")
            return {
                "score": 5,
                "reasoning": "Error al procesar la evaluación de IA",
                "error": "JSON parse error"
            }

    except Exception as e:
        logging.error(f"Error evaluating main topic interest: {str(e)}")
        return {
            "score": 5,
            "reasoning": f"Error en evaluación: {str(e)}",
            "error": str(e)
        }

def _evaluate_intervention_interest(speaker_name, speaker_role, main_topic_context, video_metadata):
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
        duration_minutes = video_metadata.get('duration_seconds', 0) // 60

        prompt = f"""Evalúa el interés de esta intervención parlamentaria individual para subirla a YouTube.

CONTEXTO DEL TEMA: {main_topic_context}

INTERVINIENTE: {speaker_name}
CARGO: {speaker_role}
DURACIÓN: {duration_minutes} minutos

Criterios de evaluación:
1. Relevancia del tema (0-3 puntos)
2. Notoriedad del interviniente (0-3 puntos)
3. Duración apropiada para YouTube (0-2 puntos)
4. Potencial de interés público (0-2 puntos)

Responde SOLO con un JSON en este formato:
{{"score": <número del 1-10>, "reasoning": "<explicación breve en español>"}}"""

        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un experto en política española y contenido viral para YouTube. Evalúas objetivamente el interés público de intervenciones parlamentarias."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=200
        )

        result_text = response.choices[0].message.content.strip()

        # Try to parse JSON response
        try:
            # Remove markdown code blocks if present
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]

            result = json.loads(result_text)
            score = int(result.get('score', 5))
            score = max(1, min(10, score))  # Clamp between 1-10

            return {
                "score": score,
                "reasoning": result.get('reasoning', 'Sin justificación'),
                "error": None
            }
        except json.JSONDecodeError:
            logging.warning(f"Failed to parse AI response as JSON: {result_text}")
            return {
                "score": 5,
                "reasoning": "Error al procesar la evaluación de IA",
                "error": "JSON parse error"
            }

    except Exception as e:
        logging.error(f"Error evaluating intervention interest: {str(e)}")
        return {
            "score": 5,
            "reasoning": f"Error en evaluación: {str(e)}",
            "error": str(e)
        }
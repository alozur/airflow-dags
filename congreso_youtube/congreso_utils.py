# dags/congreso_youtube/congreso_utils.py
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import logging
import re
import urllib3

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
                    'profile_link': item.get('profile_link')
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
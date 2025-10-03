"""
Web scraping utilities for congressional audiovisual archive.

This module handles URL construction, HTML fetching, and parsing
of congressional session data.
"""

from datetime import datetime, timedelta
import logging
import re

import requests
from bs4 import BeautifulSoup

from congreso_youtube.constants import (
    BASE_ARCHIVE_URL,
    BASE_SESSION_URL,
    LEGISLATURE_ID,
    ORGANO_ID,
)


def construct_url(target_date=None):
    """
    Constructs the Congreso audiovisual archive URL for a given date.

    Args:
        target_date: datetime object or 'YYYY-MM-DD' string. If None, defaults to yesterday.

    Returns:
        Constructed URL string for the congressional archive
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
    """
    Fetches and parses HTML from a given URL.

    Args:
        url: URL to fetch

    Returns:
        BeautifulSoup object containing parsed HTML

    Raises:
        Exception: If request fails or returns non-200 status code
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page: {response.status_code}")
    return BeautifulSoup(response.content, "html.parser")


def has_plenary_session(soup):
    """
    Checks if 'Sesión Plenaria' is present in the HTML.

    Args:
        soup: BeautifulSoup object containing parsed HTML

    Returns:
        Boolean indicating whether a plenary session was found
    """
    return bool(soup.find(string=lambda text: "Sesión Plenaria" in text))


def get_session_number(soup):
    """
    Extracts the session number from the HTML table.

    Args:
        soup: BeautifulSoup object containing parsed HTML

    Returns:
        Session number as string, or None if not found
    """
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

    Args:
        cod_sesion: Session code/number
        target_date: datetime object or 'YYYY-MM-DD' string. If None, defaults to yesterday.

    Returns:
        Constructed session URL string
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

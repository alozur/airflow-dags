"""
Web scraping utilities for congressional audiovisual archive.

This module handles URL construction for congressional session links.
"""

from datetime import datetime, timedelta
import logging

from congress_videos.config.constants import (
    BASE_SESSION_URL,
    LEGISLATURE_ID,
    ORGANO_ID,
)


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

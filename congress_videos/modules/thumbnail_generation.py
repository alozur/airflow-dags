"""Domain logic for generic thumbnail generation.

This module orchestrates the full thumbnail pipeline:

1. Resolve participant photo (DB lookup → HTTP download) or party-logo fallback.
2. Generate 2 thumbnail options via Pikzels, download each locally immediately.
3. Score each option via Pikzels; select the best by max score (tie → first).
4. Generate a YouTube title for the chosen option via OpenAI (ai_helpers).
5. Persist both options to the ``video_thumbnails`` table.

No direct OpenAI SDK calls — all AI calls go through
``utils.ai_helpers.generate_json_completion``.
"""

from __future__ import annotations

import base64
import logging
import re
from pathlib import Path
from typing import Optional

import requests

from congress_videos.config.ai_prompts import (
    ART_DIRECTION_SYSTEM_PROMPT,
    ART_DIRECTION_USER_PROMPT_TEMPLATE,
    THUMBNAIL_TITLE_SYSTEM_PROMPT,
    THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE,
)
from congress_videos.config.constants import CONGRESO_BROWSER_USER_AGENT
from utils.ai_helpers import generate_json_completion
from utils.postgres_helpers import PostgresConnection

logger = logging.getLogger(__name__)

# Forbidden characters in generated titles (besides emojis).
_FORBIDDEN_CHARS_RE = re.compile(r"[#@|~^]")

# Unicode emoji detector (matches any character in emoji-relevant Unicode ranges).
_EMOJI_RE = re.compile(
    "[\U00010000-\U0010ffff"
    "\U0001f300-\U0001f9ff"
    "☀-➿"
    "⌀-⏿"
    "⬀-⯿"
    "■-◿"
    "✀-➿"
    "]",
    flags=re.UNICODE,
)

TITLE_MAX_CHARS = 90

# Sentinel returned when no photo source is available (tolerant mode — no raise).
EMPTY_RESULT: dict = {"support_image_b64": "", "source": "none"}

# Fallback art-direction brief used when both OpenAI attempts fail.
# Background must never reference hemiciclo or parliamentary chamber.
_DEFAULT_ART_BRIEF: dict = {
    "background": (
        "una calle española con gente caminando, luz de tarde, tono documental"
    ),
    "person": (
        "un ciudadano español de mediana edad, expresión seria y preocupada, ropa casual"
    ),
    "text": "LO QUE NO TE CUENTAN",
    "mood": "tensión y curiosidad",
    "logo": "",
}

_ART_BRIEF_REQUIRED_KEYS = ("text", "background", "person", "mood")


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def art_direct(debate_summary: str, domain_cfg: dict) -> dict:
    """Generate an art-direction brief for a Pikzels thumbnail via OpenAI.

    Re-prompts once when the response is incomplete, then returns a safe
    fallback brief. This callable must never prevent the thumbnail DAG from
    continuing.
    """

    def _call_api(extra_instruction: str = "") -> Optional[dict]:
        user_prompt = ART_DIRECTION_USER_PROMPT_TEMPLATE.format(
            debate_summary=debate_summary
        )
        if extra_instruction:
            user_prompt += f"\n\nINSTRUCCIÓN ADICIONAL: {extra_instruction}"
        try:
            result = generate_json_completion(
                system_prompt=ART_DIRECTION_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                max_tokens=400,
            )
        except Exception as exc:
            logger.warning("art_direct: generate_json_completion raised: %s", exc)
            return None

        if result.get("error"):
            logger.warning("art_direct: API error: %s", result["error"])
            return None

        data = result.get("data") or {}
        if not all(key in data for key in _ART_BRIEF_REQUIRED_KEYS):
            return None
        return data

    brief = _call_api()
    if brief is None:
        brief = _call_api(
            extra_instruction=(
                "Asegúrate de devolver un JSON con EXACTAMENTE los campos: "
                "text, background, person, mood."
            )
        )

    if brief is None:
        logger.warning(
            "art_direct: both OpenAI attempts failed — using _DEFAULT_ART_BRIEF"
        )
        brief = dict(_DEFAULT_ART_BRIEF)

    brief.setdefault("logo", "")
    return {
        key: value.replace("http", "") if isinstance(value, str) else value
        for key, value in brief.items()
    }


def resolve_participant_photo(slug: str, cfg: dict) -> dict:
    """Resolve the support image for a participant using DB lookup then HTTP download.

    Resolution order:
    0. If ``slug`` is absent, empty, or whitespace, return EMPTY_RESULT (WARNING logged).
    1. Look up participant via ``cfg["participants_lookup"]`` by slug.
       If not found, return EMPTY_RESULT (WARNING logged).
    2. If ``photo_url`` is non-null, perform HTTP GET and return raw bytes.
       If the GET returns non-200 or raises, return EMPTY_RESULT (WARNING logged).
    3. If photo unavailable and ``cfg["party_logo_map"]`` is set, read logo file.
    4. If neither source is available, return EMPTY_RESULT (WARNING logged).

    Args:
        slug: Stable participant slug to look up.
        cfg: Per-domain config dict from ``THUMBNAIL_CONFIG``.

    Returns:
        Dict with keys ``support_image_b64`` (base64-encoded image bytes as str,
        empty string when no source found) and ``source``
        (``"photo"``, ``"party_logo"``, or ``"none"``).
    """
    # Guard: absent / blank slug — skip lookup entirely.
    if not slug or not str(slug).strip():
        logger.warning(
            "resolve_participant_photo: slug is absent or blank — returning empty result"
        )
        return EMPTY_RESULT

    lookup_fn = cfg["participants_lookup"]
    participant = lookup_fn(slug)

    if participant is None:
        raise LookupError(f"Participant not found: {slug!r}")

    photo_url = participant.get("photo_url")

    if photo_url:
        try:
            response = requests.get(
                photo_url,
                timeout=30,
                headers={"User-Agent": CONGRESO_BROWSER_USER_AGENT},
            )
            if response.status_code == 200:
                image_bytes = response.content
                return {
                    "support_image_b64": base64.b64encode(image_bytes).decode(),
                    "source": "photo",
                }
            else:
                logger.warning(
                    "resolve_participant_photo: HTTP %d for photo_url %r — falling back to logo",
                    response.status_code,
                    photo_url,
                )
        except requests.RequestException as exc:
            logger.warning(
                "resolve_participant_photo: request error for %r: %s — falling back to logo",
                photo_url,
                exc,
            )

    # Fallback: party logo
    logo_path = cfg.get("party_logo_map")
    if logo_path:
        logo_bytes = Path(logo_path).read_bytes()
        return {
            "support_image_b64": base64.b64encode(logo_bytes).decode(),
            "source": "party_logo",
        }

    raise ValueError(
        f"no photo source available for participant {slug!r}: "
        "photo_url is absent/undownloadable and no party_logo_map configured"
    )
    return EMPTY_RESULT


def choose_best_option(options: list[dict]) -> dict:
    """Select the best thumbnail option by highest Pikzels score.

    Tie-break: when two or more options share the maximum score, the first
    option in the list (lowest index) is chosen.

    Args:
        options: List of option dicts, each containing at least ``main_score``.

    Returns:
        The selected option dict with ``is_chosen=True`` merged in.
    """
    best = max(options, key=lambda o: (o["main_score"], -options.index(o)))
    return {**best, "is_chosen": True}


def generate_title(summary: str, best: dict, cfg: dict) -> str:
    """Generate a YouTube title for the chosen thumbnail option via OpenAI.

    Validates the returned title against constraints (≤90 chars, no emojis,
    no forbidden characters). Re-prompts once if the first attempt is invalid.
    If both attempts are invalid, strips emojis/forbidden chars and truncates to
    90 characters, logs a WARNING, and returns the sanitised result without raising.

    Args:
        summary: Debate summary text used to contextualise the title.
        best: The chosen option dict (must contain ``style`` and ``prompt``).
        cfg: Per-domain config dict (currently unused but kept for future extensibility).

    Returns:
        A YouTube title string (≤90 chars, no emojis, no forbidden chars).
    """

    def _is_valid(title: str) -> bool:
        if len(title) > TITLE_MAX_CHARS:
            return False
        if _EMOJI_RE.search(title):
            return False
        if _FORBIDDEN_CHARS_RE.search(title):
            return False
        return True

    def _sanitise(title: str) -> str:
        """Strip emojis and forbidden chars, then truncate to TITLE_MAX_CHARS."""
        cleaned = _EMOJI_RE.sub("", title)
        cleaned = _FORBIDDEN_CHARS_RE.sub("", cleaned)
        cleaned = cleaned.strip().strip('"').strip("'")
        return cleaned[:TITLE_MAX_CHARS]

    def _call_openai(extra_instruction: str = "") -> Optional[str]:
        style_text = best.get("style", "")
        prompt_text = best.get("prompt", "")

        user_prompt = THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE.format(
            summary=summary,
            style=style_text,
            prompt=prompt_text,
        )
        if extra_instruction:
            user_prompt += f"\n\nINSTRUCCIÓN ADICIONAL: {extra_instruction}"

        result = generate_json_completion(
            system_prompt=THUMBNAIL_TITLE_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            max_tokens=120,
        )
        if result.get("error"):
            logger.warning("generate_title: OpenAI error: %s", result["error"])
            return None
        data = result.get("data") or {}
        raw = data.get("title", "")
        return raw.strip().strip('"').strip("'") if raw else None

    # First attempt
    title = _call_openai()

    if title and _is_valid(title):
        return title

    # Determine re-prompt instruction
    if title and len(title) > TITLE_MAX_CHARS:
        instruction = f"El título es demasiado largo. Máximo {TITLE_MAX_CHARS} caracteres."
    else:
        instruction = (
            "El título contiene caracteres no permitidos (emojis, #, @, |, ~, ^). "
            "Elimínalos y devuelve un título limpio."
        )

    # Second attempt
    second = _call_openai(extra_instruction=instruction)
    if second and _is_valid(second):
        return second

    # Fallback: sanitise whatever we have
    candidate = second or title or ""
    sanitised = _sanitise(candidate)
    logger.warning(
        "generate_title: both OpenAI attempts returned invalid titles — "
        "sanitised fallback applied: %r (original: %r)",
        sanitised,
        candidate,
    )
    return sanitised


def persist_results(
    chapter_id: int,
    youtube_video_id: str,
    title: str,
    options: list[dict],
    best_label: str,
) -> None:
    """Persist both thumbnail options as rows in the ``video_thumbnails`` table.

    Uses INSERT … ON CONFLICT (chapter_id, label) DO UPDATE (upsert) so that
    re-triggering the DAG for the same chapter replaces prior rows cleanly.

    The chosen row has ``openai_title=title`` and ``is_chosen=TRUE``.
    The non-chosen row has ``openai_title=NULL`` and ``is_chosen=FALSE``.

    Args:
        chapter_id: FK to ``video_chapters.chapter_id``.
        youtube_video_id: YouTube video identifier.
        title: OpenAI-generated title for the chosen option.
        options: List of option dicts from the DAG's score task callables.
        best_label: Label of the chosen option.
    """
    pg = PostgresConnection()
    table = pg.get_qualified_table("video_thumbnails")

    sql = f"""
        INSERT INTO {table} (
            chapter_id, youtube_video_id, label, style, prompt,
            main_score, local_path, output_url, openai_title, is_chosen
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (chapter_id, label) DO UPDATE SET
            youtube_video_id = EXCLUDED.youtube_video_id,
            style            = EXCLUDED.style,
            prompt           = EXCLUDED.prompt,
            main_score       = EXCLUDED.main_score,
            local_path       = EXCLUDED.local_path,
            output_url       = EXCLUDED.output_url,
            openai_title     = EXCLUDED.openai_title,
            is_chosen        = EXCLUDED.is_chosen
    """

    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            for opt in options:
                is_chosen = opt["label"] == best_label
                openai_title = title if is_chosen else None

                params = (
                    chapter_id,
                    youtube_video_id,
                    opt["label"],
                    opt.get("style"),
                    opt.get("prompt"),
                    opt.get("main_score"),
                    opt["local_path"],
                    opt.get("output_url"),
                    openai_title,
                    is_chosen,
                )
                cur.execute(sql, params)

    logger.info(
        "persist_results: upserted %d thumbnail rows for chapter_id=%d (chosen=%s)",
        len(options),
        chapter_id,
        best_label,
    )

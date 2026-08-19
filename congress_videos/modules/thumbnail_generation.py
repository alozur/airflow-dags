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
    ART_DIRECTION_RETRY_INSTRUCTION,
    ART_DIRECTION_SIBLING_INSTRUCTION,
    ART_DIRECTION_SYSTEM_PROMPT,
    ART_DIRECTION_USER_PROMPT_TEMPLATE,
    LAPIDARY_RANKING_SYSTEM_PROMPT,
    LAPIDARY_RANKING_USER_TEMPLATE,
    SPEAKER_PLACEHOLDERS,
    THUMBNAIL_TITLE_NAMELESS_INSTRUCTION,
    THUMBNAIL_TITLE_SIBLING_INSTRUCTION,
    THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION,
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
    "archetype": "generico",
}

_ART_BRIEF_REQUIRED_KEYS = ("text", "background", "person", "mood")

# Archetype enum — dramatic form classification for thumbnail composition.
# Must stay in sync with the ARQUETIPO DRAMÁTICO section in ART_DIRECTION_SYSTEM_PROMPT.
_ARCHETYPES = ("careo", "denuncia", "monologo", "anuncio", "generico")
_DEFAULT_ARCHETYPE = "generico"


def _coerce_archetype(value: object) -> str:
    """Map any LLM-returned value to a valid _ARCHETYPES member or 'generico'.

    Pure: lowercases/strips str input; non-str, unknown, empty, or None → 'generico'.
    """
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _ARCHETYPES:
            return token
    return _DEFAULT_ARCHETYPE


# Spanish stop-words that disqualify a candidate when they appear as the first token.
_LAPIDARY_STOP_WORDS = frozenset(
    "de que y o pero si en con por a el la los las un una".split()
)

# Clause boundary splitter for lapidary candidate extraction.
_LAPIDARY_SPLIT_RE = re.compile(r"[.!?;,]\s+")


def extract_lapidary_quote(
    srt_fragment: str,
    max_chars: int = 40,
    min_words: int = 3,
    max_words: int = 8,
    completion_fn=None,
) -> str | None:
    """Extract the most impactful verbatim quote from an SRT fragment.

    Splits the fragment on clause boundaries, filters by word count and length,
    removes stop-word-leading candidates, deduplicates, then asks an LLM to rank
    the survivors.  Returns the verbatim candidate string at the selected index,
    or ``None`` when no candidates survive or the LLM declines.

    Args:
        srt_fragment: Raw SRT text for a chapter time window.
        max_chars: Maximum character length of an accepted candidate (default 40).
        min_words: Minimum word count per candidate (default 3).
        max_words: Maximum word count per candidate (default 8).
        completion_fn: Injectable callable with the same signature as
            ``generate_chat_completion`` (for unit-test isolation).  Defaults to
            the real ``generate_chat_completion`` when ``None``.

    Returns:
        Verbatim candidate string, or ``None``.
    """
    if completion_fn is None:
        from utils.ai_helpers import generate_chat_completion as _real_fn

        completion_fn = _real_fn

    if not srt_fragment:
        return None

    # 1. Split on clause boundaries.
    clauses = _LAPIDARY_SPLIT_RE.split(srt_fragment)

    # 2. Filter by word count and character length.
    candidates: list[str] = []
    seen_lower: set[str] = set()
    for clause in clauses:
        clause = clause.strip()
        if not clause:
            continue
        words = clause.split()
        if not (min_words <= len(words) <= max_words):
            continue
        if len(clause) > max_chars:
            continue
        # 3. Discard when first token is a Spanish stop-word (case-insensitive).
        if words[0].lower() in _LAPIDARY_STOP_WORDS:
            continue
        # 4. Deduplicate case-insensitively.
        lower = clause.lower()
        if lower in seen_lower:
            continue
        seen_lower.add(lower)
        candidates.append(clause)

    # 5. No candidates → return None without calling the LLM.
    if not candidates:
        return None

    # 6. Build numbered list and call the LLM ranker.
    numbered = "\n".join(f"{i + 1}. {c}" for i, c in enumerate(candidates))
    user_prompt = LAPIDARY_RANKING_USER_TEMPLATE.format(candidates=numbered)

    response = completion_fn(
        system_prompt=LAPIDARY_RANKING_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        model="gpt-4o-mini",
        temperature=0.2,
        max_tokens=5,
    )

    content: str = (response or {}).get("content") or ""
    content = content.strip()

    if content.upper() == "NONE":
        return None

    # 7. Parse 1-based index.
    match = re.search(r"\d+", content)
    if not match:
        return None

    try:
        idx = int(match.group()) - 1
    except (ValueError, AttributeError):
        return None

    if idx < 0 or idx >= len(candidates):
        return None

    return candidates[idx]


def _real_speakers(key_speakers: list | None) -> list[str]:
    """Return the subset of key_speakers that are real (non-placeholder) names.

    Normalizes each entry (dict → name string), strips whitespace, drops empty
    strings, and filters out any entry whose lowercased form appears in
    SPEAKER_PLACEHOLDERS (case-insensitive comparison).

    Args:
        key_speakers: List of speaker entries (strings or dicts with a ``name`` key),
            or None.

    Returns:
        A list of real speaker name strings; empty list when none survive.
    """
    if not key_speakers:
        return []
    result: list[str] = []
    for entry in key_speakers:
        name = entry.get("name", "") if isinstance(entry, dict) else str(entry)
        name = name.strip()
        if not name:
            continue
        if name.lower() in SPEAKER_PLACEHOLDERS:
            continue
        result.append(name)
    return result


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def art_direct(
    debate_summary: str,
    domain_cfg: dict,
    previous_brief: dict | None = None,
    sibling_briefs: list[str] | None = None,
    srt_fragment: str | None = None,
) -> dict:
    """Generate an art-direction brief for a Pikzels thumbnail via OpenAI.

    Re-prompts once when the response is incomplete, then returns a safe
    fallback brief. This callable must never prevent the thumbnail DAG from
    continuing.

    Args:
        debate_summary: Text summary of the parliamentary debate.
        domain_cfg: Per-domain configuration dict from THUMBNAIL_CONFIG.
        previous_brief: When set, injects ART_DIRECTION_RETRY_INSTRUCTION
            into the prompt to force a DIFFERENT visual approach. Used by
            the art_direction_retry DAG task. None means first generation.
        sibling_briefs: When non-empty, injects a "NO REPITAS" block listing
            recent chosen briefs to steer the model away from repetition.
            None or empty list → prompt unchanged (backward compatible).
        srt_fragment: When provided, ``extract_lapidary_quote`` is called and,
            if it returns a non-None string, ``brief["text"]`` is overridden
            with the verbatim SRT quote.  ``None`` (default) leaves the
            existing invented-from-summary flow fully unchanged.
    """
    import json

    def _call_api(extra_instruction: str = "") -> Optional[dict]:
        user_prompt = ART_DIRECTION_USER_PROMPT_TEMPLATE.format(
            debate_summary=debate_summary
        )
        if previous_brief is not None:
            retry_instruction = ART_DIRECTION_RETRY_INSTRUCTION.format(
                previous_brief_json=json.dumps(previous_brief, ensure_ascii=False)
            )
            user_prompt += f"\n\n{retry_instruction}"
        if sibling_briefs:
            sibling_list = "\n".join(f"- {b}" for b in sibling_briefs)
            sibling_block = ART_DIRECTION_SIBLING_INSTRUCTION.format(
                sibling_list=sibling_list
            )
            user_prompt += f"\n\n{sibling_block}"
        if extra_instruction:
            user_prompt += f"\n\nINSTRUCCIÓN ADICIONAL: {extra_instruction}"
        try:
            result = generate_json_completion(
                system_prompt=ART_DIRECTION_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                max_tokens=400,
                temperature=0.7,
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
    brief["archetype"] = _coerce_archetype(brief.get("archetype"))
    result = {
        key: value.replace("http", "") if isinstance(value, str) else value
        for key, value in brief.items()
    }

    # SRT lapidary override: replace the invented text with a verbatim quote.
    if srt_fragment is not None:
        quote = extract_lapidary_quote(srt_fragment)
        if quote is not None:
            result["text"] = quote

    return result


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
        logger.warning(
            "resolve_participant_photo: participant not found for slug %r — returning empty result",
            slug,
        )
        return EMPTY_RESULT

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

    logger.warning(
        "resolve_participant_photo: no photo source available for participant %r "
        "(photo_url absent/undownloadable and no party_logo_map configured) — "
        "returning empty result",
        slug,
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


def generate_title(
    summary: str,
    best: dict,
    cfg: dict,
    sibling_titles: list[str] | None = None,
    key_speakers: list | None = None,
) -> str:
    """Generate a YouTube title for the chosen thumbnail option via OpenAI.

    Validates the returned title against constraints (≤90 chars, no emojis,
    no forbidden characters, no question marks). Re-prompts once if the first
    attempt is invalid. If both attempts are invalid, strips forbidden chars
    and truncates to 90 characters, logs a WARNING, and returns the sanitised
    result without raising.

    Args:
        summary: Debate summary text used to contextualise the title.
        best: The chosen option dict (must contain ``style`` and ``prompt``).
        cfg: Per-domain config dict (currently unused but kept for future extensibility).
        sibling_titles: When non-empty, injects a "NO REPITAS" block listing
            recent chosen titles to prevent tonal repetition.
            None or empty list → prompt unchanged (backward compatible).
        key_speakers: Optional list of speaker names (strings or dicts with ``name`` key)
            to soft-hint the LLM toward mentioning them. None or empty list → no injection
            (backward compatible). Best-effort: names are a soft hint only, not validated.

    Returns:
        A YouTube title string (≤90 chars, no emojis, no forbidden chars, no question marks).
    """

    def _is_all_caps(title: str) -> bool:
        """True when the title has letters but none of them are lowercase.

        Party acronyms alone (e.g. "PSOE") do not false-positive a real
        title because a normally-cased title always contains lowercase
        letters, whereas an all-caps title has none.
        """
        return any(c.isalpha() for c in title) and not any(
            c.islower() for c in title
        )

    def _is_valid(title: str) -> bool:
        if len(title) > TITLE_MAX_CHARS:
            return False
        if _EMOJI_RE.search(title):
            return False
        if _FORBIDDEN_CHARS_RE.search(title):
            return False
        if _is_all_caps(title):
            return False
        if title.strip().startswith("¿") or "?" in title:
            return False
        return True

    def _sanitise(title: str) -> str:
        """Strip emojis, forbidden chars, and question marks, then truncate to TITLE_MAX_CHARS."""
        cleaned = _EMOJI_RE.sub("", title)
        cleaned = _FORBIDDEN_CHARS_RE.sub("", cleaned)
        cleaned = cleaned.replace("¿", "").replace("?", "")
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
        if sibling_titles:
            sibling_list = "\n".join(f"- {t}" for t in sibling_titles)
            sibling_block = THUMBNAIL_TITLE_SIBLING_INSTRUCTION.format(
                sibling_list=sibling_list
            )
            user_prompt += f"\n\n{sibling_block}"
        real = _real_speakers(key_speakers)
        if real:
            user_prompt += "\n\n" + THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION.format(
                speaker_list=", ".join(real)
            )
        else:
            # Falsy key_speakers (None / []) and all-placeholder lists both map
            # to the nameless format — this is the hard guarantee that prevents
            # the model from hallucinating politician names when no real speaker
            # is identified.
            user_prompt += f"\n\n{THUMBNAIL_TITLE_NAMELESS_INSTRUCTION}"
        if extra_instruction:
            user_prompt += f"\n\nINSTRUCCIÓN ADICIONAL: {extra_instruction}"

        result = generate_json_completion(
            system_prompt=THUMBNAIL_TITLE_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            max_tokens=120,
            temperature=0.7,
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
    if title and (title.strip().startswith("¿") or "?" in title):
        instruction = (
            "El título es una pregunta. Reescríbelo como titular declarativo de noticias: "
            "[Nombre] + verbo + complemento, sin signos de interrogación."
        )
    elif title and len(title) > TITLE_MAX_CHARS:
        instruction = f"El título es demasiado largo. Máximo {TITLE_MAX_CHARS} caracteres."
    elif title and _is_all_caps(title):
        instruction = (
            "El título está todo en mayúsculas. Usa capitalización normal en "
            "español (solo la primera letra y nombres propios en mayúscula), "
            "respetando las siglas de partidos (PSOE, PP, VOX)."
        )
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


def _summarise_sibling_brief(brief: str) -> str:
    """Extract the key visual axes from a stored Pikzels prompt, cap at 200 chars.

    The stored ``video_thumbnails.prompt`` is the *rendered* Pikzels template,
    whose axis lines look like ``BACKGROUND: ...``, ``SUBJECT (RIGHT HALF): ...``,
    ``TEXT (LEFT HALF, ...): ... '<phrase>'`` and ``Overall mood: ...``. This
    normalises each recognised axis to ``label: value`` and joins them. The
    synthetic ``person:``/``mood:`` prefixes are also accepted for robustness.
    When no axis line is recognised, falls back to raw truncation at 200 chars.

    Args:
        brief: Full Pikzels prompt string stored in ``video_thumbnails.prompt``.

    Returns:
        A short string of at most 200 characters summarising the visual axes.
    """
    # Normalised axis label -> line-prefix(es) as they appear in the rendered
    # Pikzels template (and the synthetic field name, for tolerance).
    _AXIS_PREFIXES: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("background", ("background",)),
        ("person", ("subject", "person")),
        ("mood", ("overall mood", "mood")),
        ("text", ("text",)),
    )
    parts: list[str] = []
    for raw_line in brief.splitlines():
        line = raw_line.strip()
        low = line.lower()
        for label, prefixes in _AXIS_PREFIXES:
            if not low.startswith(prefixes):
                continue
            value = line.split(":", 1)[1].strip() if ":" in line else line
            if label == "text":
                # Prefer the quoted phrase; drop the font/styling boilerplate.
                match = re.search(r"['\"]([^'\"]+)['\"]", value)
                if match:
                    value = match.group(1)
            elif label == "person":
                # Drop the fixed "Face fills…/Looks…" boilerplate tail.
                value = value.split(". Face fills")[0].strip()
            parts.append(f"{label}: {value}")
            break
    if parts:
        return " | ".join(parts)[:200]
    return brief[:200]


def fetch_recent_thumbnail_history(
    limit: int = 5,
) -> tuple[list[str], list[str]]:
    """Return the most recent chosen thumbnail briefs and titles, GLOBAL scope.

    Queries ``video_thumbnails`` for the last ``limit`` rows where
    ``is_chosen = TRUE``, ordered by ``chapter_id DESC``. Returns two parallel
    lists: summarised brief strings and non-null title strings.

    MUST NEVER RAISE: on any exception or empty result, returns ``([], [])``
    and logs a WARNING.

    Args:
        limit: Maximum number of history rows to fetch (default 5).

    Returns:
        Tuple of (briefs, titles). briefs uses ``_summarise_sibling_brief``
        on each stored prompt. titles excludes NULL/empty openai_title values.
    """
    try:
        pg = PostgresConnection()
        table = pg.get_qualified_table("video_thumbnails")
        sql = (
            f"SELECT prompt, openai_title FROM {table} "
            f"WHERE is_chosen = TRUE ORDER BY chapter_id DESC LIMIT %s"
        )
        with pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()

        if not rows:
            return [], []

        briefs = [_summarise_sibling_brief(row[0] or "") for row in rows]
        titles = [row[1] for row in rows if row[1]]
        return briefs, titles

    except Exception as exc:
        logger.warning(
            "fetch_recent_thumbnail_history: failed to load history — %s", exc
        )
        return [], []


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

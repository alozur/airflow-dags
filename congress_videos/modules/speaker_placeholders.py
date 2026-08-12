"""Shared placeholder-speaker detection for congressional chapter attribution.

Exports
-------
PLACEHOLDER_SPEAKERS : frozenset[str]
    Casefolded canonical set of known placeholder speaker strings.
is_placeholder(s: str) -> bool
    Pure, stateless predicate. Returns True when ``s`` is a known placeholder
    or matches a role-only heuristic. Fail-open: unrecognised shapes → False.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Canonical placeholder set (casefolded, exact)
# ---------------------------------------------------------------------------

PLACEHOLDER_SPEAKERS: frozenset[str] = frozenset({
    "desconocido",
    "unknown",
    "interviniente no especificado",
    "(no especificado)",
    "no especificado",
    "",
})

# ---------------------------------------------------------------------------
# Role-keyword heuristic
# ---------------------------------------------------------------------------
# These are role/title tokens that appear in the congressional transcription
# output when the LLM cannot identify a real name. The regex matches the
# token as a complete word at the start of the string (case-insensitive).
# Confirmed against real dirty-speaker samples from ai_prompts.py and the
# speaker_normalization pipeline.

_ROLE_KEYWORDS = (
    r"portavoz",
    r"diputad[oa]",
    r"senadore?[oa]?",
    r"president[ea]",
    r"ministr[oa]",
    r"secretari[oa]",
    r"vicesecretari[oa]",
    r"vicepresidente?",
    r"viceministr[oa]",
    r"vocal",
    r"interventor[a]?",
    r"interveniente",
    r"interviniente",
    r"ponente",
    r"consejere?[oa]?",
    r"delegad[oa]",
    r"coordinadore?[oa]?",
)

# Compiled pattern: role-keyword covers the ENTIRE string with only
# lowercase/article suffixes (role-only, no proper name following).
_ROLE_ONLY_RE = re.compile(
    r"^(?:" + r"|".join(_ROLE_KEYWORDS) + r")(?:\s+(?:del?\s+[a-záéíóúàèìòùüñ]\w*|los?\s+[a-záéíóúàèìòùüñ]\w*|las?\s+[a-záéíóúàèìòùüñ]\w*|[a-záéíóúàèìòùüñ]\w*))*$",
    re.IGNORECASE,
)

# Proper-name suffix: at least one word starting with a Unicode uppercase letter
# (genuine Titlecase, NOT re.IGNORECASE for the suffix), following a role keyword.
# If present the string is treated as a real name (fail-open).
# Note: role keyword itself is matched case-insensitively via a separate prefix group.
_ROLE_PREFIX_RE = re.compile(
    r"^(?:" + r"|".join(_ROLE_KEYWORDS) + r")\s+",
    re.IGNORECASE,
)

# Uppercase letter set for Titlecase detection (Spanish + Latin Extended).
_UPPERCASE_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚÀÈÌÒÙÜÑ")


def is_placeholder(s: str) -> bool:
    """Return True when ``s`` is a known placeholder or a role-only string.

    Decision rules (in order):
    1. Strip and casefold; if in PLACEHOLDER_SPEAKERS → True.
    2. Single-token (no whitespace after strip) → True.
    3. Matches _ROLE_WITH_NAME_RE (role + proper-name Titlecase suffix) → False
       (fail-open: real name).
    4. Matches _ROLE_ONLY_RE → True.
    5. Everything else → False (fail-open: treat as real name).

    The function is pure and stateless — no I/O, no LLM, no DB.
    """
    stripped = s.strip()
    casefolded = stripped.casefold()

    # Rule 1: exact match in canonical set
    if casefolded in PLACEHOLDER_SPEAKERS:
        return True

    # Rule 2: single-token (no whitespace) — e.g. "Portavoz", "PP"
    if " " not in stripped:
        return True

    # Rule 3: role keyword + proper-name suffix → fail-open (real name).
    # Check whether the suffix after the role prefix starts with a Titlecase word
    # (i.e. first char is uppercase). We test this without re.IGNORECASE on the
    # suffix to avoid "del" (lowercase d) being mistaken for a proper name.
    prefix_m = _ROLE_PREFIX_RE.match(stripped)
    if prefix_m:
        suffix = stripped[prefix_m.end():]
        if suffix and suffix[0] in _UPPERCASE_CHARS:
            return False

    # Rule 4: role-keyword pattern covers the whole string (role-only phrase)
    if _ROLE_ONLY_RE.match(stripped):
        return True

    # Rule 5: unrecognised shape → fail-open
    return False

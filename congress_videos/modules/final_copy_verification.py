"""Pure final-copy verifier core (issue #512, design.md slice 2a).

Implements the verdict schema, defensive response parsing, evidence
containment and the bounded-correction contract (design.md D1, D2, D3).
Never raises (module convention: ``mentioned_people_resolution.py``).

Deferred to slice 2b: the two prompt constants in ``ai_prompts.py`` and the
public ``verify_final_copy`` wrapper that renders them and defaults
``completion_fn`` to ``utils.llm_cache.cached_json_completion``. Until then
:func:`run_correction_round` is the tested seam: its ``call_round`` callback
takes the place of a completion_fn+prompt pair, so this module needs no
prompt content to be fully exercised. This module is NOT wired into any DAG.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass

MAX_CORRECTION_ROUNDS: int = 1
"""Rounds beyond the initial call. No loop construct exists in this module:
runaway correction is structurally impossible, not counter-bounded."""

TITLE_MAX_CHARS: int = 100
"""YouTube title limit, mirrors truncate_text(..., max_length=100) at
reap_shorts_uploader_dag.py:300."""

DESCRIPTION_MAX_CHARS: int = 5_000

_VALID_VERDICTS = frozenset({"pass", "correctable", "reject"})
_VALID_FIELDS = frozenset({"title", "description", "thumbnail_text"})
_VALID_CATEGORIES = frozenset({"person_name", "party_name", "spelling", "grammar", "language", "unsupported_claim"})
_VALID_CORRECTED_KEYS = frozenset({"title", "description"})
_TOKEN_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


@dataclass(frozen=True)
class CopyFinding:
    """A single verifier finding for one field."""

    field: str  # title | description | thumbnail_text
    category: str  # person_name | party_name | spelling | grammar | language | unsupported_claim | other
    severity: str  # low | medium | high
    detail: str = ""
    suggestion: str = ""


@dataclass(frozen=True)
class CopyVerdict:
    """Return value of :func:`run_correction_round` (and, in slice 2b,
    ``verify_final_copy``)."""

    ok: bool = False
    """False means inconclusive — NEVER treat as pass."""

    verdict: str = ""  # pass | correctable | reject ("" when not ok)
    findings: tuple[CopyFinding, ...] = ()
    title: str = ""  # value to publish (original or accepted correction)
    description: str = ""
    correction_applied: bool = False
    rationale: str = ""
    content_version: str = ""  # sha256; "" when not ok
    rounds: int = 0  # completion calls actually made (0, 1 or 2)


@dataclass(frozen=True)
class _ParsedRound:
    """One raw response parsed. Not the public return type: these
    ``corrected_*`` fields are the model's own proposal, not yet the
    resolved value to publish."""

    ok: bool = False
    verdict: str = ""
    findings: tuple[CopyFinding, ...] = ()
    corrected_title: str | None = None
    corrected_description: str | None = None
    rationale: str = ""


def _parse_findings(raw_findings: object) -> tuple[CopyFinding, ...] | None:
    """Parse the ``findings`` list. Returns ``None`` on any malformed
    element — the caller treats that as inconclusive."""
    if raw_findings is None:
        raw_findings = []
    if not isinstance(raw_findings, list):
        return None

    findings: list[CopyFinding] = []
    for raw in raw_findings:
        if not isinstance(raw, dict):
            return None
        field = raw.get("field")
        if field not in _VALID_FIELDS:
            continue  # dropped, verdict kept
        category = raw.get("category") if raw.get("category") in _VALID_CATEGORIES else "other"
        findings.append(
            CopyFinding(
                field=field,
                category=category,
                severity=raw.get("severity", "low"),
                detail=raw.get("detail", ""),
                suggestion=raw.get("suggestion", ""),
            )
        )
    return tuple(findings)


def _parse_round_response(response: object) -> _ParsedRound:
    """Defensively parse one raw completion response (design.md D1).

    Every deviation — not a dict, an error set, an unknown verdict token, a
    non-list ``findings``, or a ``corrected`` value with a key outside
    {title, description} — is inconclusive. An unknown finding ``field``
    drops just that finding; an unknown ``category`` is kept and classified
    ``other``. Gating never reads ``category``.
    """
    if not isinstance(response, dict) or response.get("error"):
        return _ParsedRound()

    data = response.get("data")
    if not isinstance(data, dict):
        return _ParsedRound()

    verdict = data.get("verdict")
    if verdict not in _VALID_VERDICTS:
        return _ParsedRound()

    findings = _parse_findings(data.get("findings"))
    if findings is None:
        return _ParsedRound()

    raw_corrected = data.get("corrected")
    corrected_title: str | None = None
    corrected_description: str | None = None
    if raw_corrected is not None:
        if not isinstance(raw_corrected, dict) or (set(raw_corrected) - _VALID_CORRECTED_KEYS):
            return _ParsedRound()
        corrected_title = raw_corrected.get("title")
        corrected_description = raw_corrected.get("description")

    if verdict == "correctable" and raw_corrected is None:
        return _ParsedRound()

    rationale = data.get("rationale", "")
    if not isinstance(rationale, str):
        rationale = ""

    return _ParsedRound(
        ok=True,
        verdict=verdict,
        findings=findings,
        corrected_title=corrected_title,
        corrected_description=corrected_description,
        rationale=rationale,
    )


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text or "")


def _normalize_token(token: str) -> str:
    return unicodedata.normalize("NFC", token).casefold()


def _needs_evidence_backing(token: str) -> bool:
    """Lowercase, accent-free tokens are unconstrained (spelling/grammar
    fixes). Capitalised or accent-bearing tokens must trace back to the
    original text or the evidence — that is what stops a correction from
    inventing a name, party or claim."""
    return token[:1].isupper() or any(not char.isascii() for char in token)


def _flatten_evidence_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in _flatten_evidence_strings(v)]
    if isinstance(value, (list, tuple)):
        return [s for v in value for s in _flatten_evidence_strings(v)]
    return []


def is_contained(
    *,
    corrected_title: str,
    corrected_description: str,
    original_title: str,
    original_description: str,
    evidence: dict,
) -> bool:
    """True only when the correction is provably derivable from the
    evidence bundle plus the original text — never from the model's own
    unbacked claim. Enforces the length bounds too."""
    if len(corrected_title) > TITLE_MAX_CHARS or len(corrected_description) > DESCRIPTION_MAX_CHARS:
        return False

    allowed: set[str] = set()
    for text in (original_title, original_description, *_flatten_evidence_strings(evidence)):
        allowed.update(_normalize_token(tok) for tok in _tokenize(text))

    for candidate in (corrected_title, corrected_description):
        for token in _tokenize(candidate):
            if _needs_evidence_backing(token) and _normalize_token(token) not in allowed:
                return False
    return True


def compute_content_version(*, title: str, description: str, thumbnail_text: str | None, evidence: dict) -> str:
    """sha256 of the same canonical payload the eventual (slice 2b) user
    prompt renders from, so the content version and the LLM cache key move
    in lockstep (design.md D3)."""
    payload = {"title": title, "description": description, "thumbnail_text": thumbnail_text, "evidence": evidence}
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def run_correction_round(
    *,
    original_title: str,
    original_description: str,
    evidence: dict,
    call_round: Callable[[str, str], dict],
    content_version: str = "",
) -> CopyVerdict:
    """Run the bounded-correction contract (design.md D2). NEVER raises.

    ``call_round(title, description)`` stands in for "render the prompt for
    this candidate copy and call completion_fn once", returning the raw
    ``{"data": ..., "error": ...}`` response. This function calls it AT MOST
    TWICE: once for the original copy (round 0) and, only when round 0 is
    ``correctable`` with a contained correction, once more to recheck the
    corrected copy (round 1). Straight-line code, no loop: runaway
    correction is structurally impossible.

    The blocking decision is taken only on round 0's verdict for the value
    actually published — a recheck of corrected text can never turn an
    original the verifier never rejected into a reject.
    """
    try:
        return _run_inner(original_title, original_description, evidence, call_round, content_version)
    except Exception:  # noqa: BLE001
        return CopyVerdict(title=original_title, description=original_description)


def _run_inner(
    original_title: str,
    original_description: str,
    evidence: dict,
    call_round: Callable[[str, str], dict],
    content_version: str,
) -> CopyVerdict:
    round0 = _parse_round_response(call_round(original_title, original_description))
    if not round0.ok:
        return CopyVerdict(title=original_title, description=original_description, rounds=1)

    if round0.verdict in ("pass", "reject"):
        return CopyVerdict(
            ok=True,
            verdict=round0.verdict,
            findings=round0.findings,
            title=original_title,
            description=original_description,
            rationale=round0.rationale,
            content_version=content_version,
            rounds=1,
        )

    # correctable
    corrected_title = round0.corrected_title if round0.corrected_title is not None else original_title
    corrected_description = (
        round0.corrected_description if round0.corrected_description is not None else original_description
    )

    if not is_contained(
        corrected_title=corrected_title,
        corrected_description=corrected_description,
        original_title=original_title,
        original_description=original_description,
        evidence=evidence,
    ):
        unsupported_finding = CopyFinding(
            field="description" if corrected_description != original_description else "title",
            category="unsupported_claim",
            severity="high",
            detail="Correction discarded: not derivable from the supplied evidence.",
        )
        return CopyVerdict(
            ok=True,
            verdict="correctable",
            findings=(*round0.findings, unsupported_finding),
            title=original_title,
            description=original_description,
            rationale=round0.rationale,
            content_version=content_version,
            rounds=1,
        )

    # Round 1 — mandatory recheck of the corrected value. Terminal: never
    # corrects again, regardless of what it returns.
    round1 = _parse_round_response(call_round(corrected_title, corrected_description))

    if round1.ok and round1.verdict == "pass":
        return CopyVerdict(
            ok=True,
            verdict="pass",
            findings=round0.findings,
            title=corrected_title,
            description=corrected_description,
            correction_applied=True,
            rationale=round0.rationale,
            content_version=content_version,
            rounds=2,
        )

    recheck_failed_finding = CopyFinding(
        field="title" if corrected_title != original_title else "description",
        category="other",
        severity="medium",
        detail="Recheck of the correction failed or was inconclusive; publishing the original.",
    )
    return CopyVerdict(
        ok=True,
        verdict=round0.verdict,
        findings=(*round0.findings, recheck_failed_finding),
        title=original_title,
        description=original_description,
        rationale=round0.rationale,
        content_version=content_version,
        rounds=2,
    )

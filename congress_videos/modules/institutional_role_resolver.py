"""Local validation for the bundled institutional-role catalog."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlparse


class CatalogValidationError(ValueError):
    """Raised when the V1 document shape cannot be safely loaded."""


@dataclass(frozen=True)
class Role:
    key: str
    scope: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class Assignment:
    identifier: str
    role: str | None
    participant_slug: str | None
    validity_from: date | None
    validity_to: date | None
    is_open_ended: bool
    diagnostic: str | None

    @property
    def is_valid(self) -> bool:
        return self.diagnostic is None


@dataclass(frozen=True)
class Catalog:
    version: int
    roles: tuple[Role, ...]
    assignments: tuple[Assignment, ...]

    @property
    def valid_assignments(self) -> tuple[Assignment, ...]:
        return tuple(assignment for assignment in self.assignments if assignment.is_valid)


def normalize_role_label(label: str) -> str:
    """Return the catalog's mechanical, accent-insensitive role key."""
    decomposed = unicodedata.normalize("NFKD", label.strip())
    without_accents = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", without_accents.casefold())).strip()


class CatalogLoader:
    """Load and structurally validate one local UTF-8 V1 JSON catalog."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def load(self) -> Catalog:
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CatalogValidationError("catalog_load_failed") from error

        if not isinstance(document, dict):
            raise CatalogValidationError("top_level")
        if document.get("catalog_version") != 1:
            raise CatalogValidationError("catalog_version")
        roles = self._load_roles(document.get("roles"))
        assignments = document.get("assignments")
        if not isinstance(assignments, list):
            raise CatalogValidationError("assignments")

        parsed_assignments = tuple(
            self._parse_assignment(item, {role.key for role in roles})
            for item in assignments
        )
        return Catalog(1, roles, self._mark_duplicate_ids(parsed_assignments))

    def _load_roles(self, raw_roles: object) -> tuple[Role, ...]:
        if not isinstance(raw_roles, list):
            raise CatalogValidationError("roles")

        roles: list[Role] = []
        claimed_labels: set[str] = set()
        for raw_role in raw_roles:
            if not isinstance(raw_role, dict):
                raise CatalogValidationError("roles")
            key = raw_role.get("key")
            scope = raw_role.get("scope")
            aliases = raw_role.get("aliases", [])
            if not isinstance(key, str) or not key or key != normalize_role_label(key):
                raise CatalogValidationError("normalized")
            if scope not in {"ministerial", "presidency_mesa", "parliamentary_group"}:
                raise CatalogValidationError("scope")
            if not isinstance(aliases, list) or any(
                not isinstance(alias, str) or not alias or alias != normalize_role_label(alias)
                for alias in aliases
            ):
                raise CatalogValidationError("aliases")
            labels = [key, *aliases]
            if len(set(labels)) != len(labels) or any(label in claimed_labels for label in labels):
                raise CatalogValidationError("collides")
            claimed_labels.update(labels)
            roles.append(Role(key, scope, tuple(aliases)))
        return tuple(roles)

    def _parse_assignment(self, raw: object, role_keys: set[str]) -> Assignment:
        if not isinstance(raw, dict):
            return Assignment("", None, None, None, None, False, "invalid_assignment")
        identifier = raw.get("id") if isinstance(raw.get("id"), str) else ""
        role = raw.get("role") if isinstance(raw.get("role"), str) else None
        slug = raw.get("participant_slug") if isinstance(raw.get("participant_slug"), str) else None
        validity = raw.get("validity")
        starts_on, ends_on, open_ended, interval_error = self._parse_interval(validity)

        diagnostic = (
            "missing_assignment_id" if not identifier else
            "undeclared_role" if role not in role_keys else
            "missing_participant_slug" if not slug else
            interval_error or
            self._provenance_error(raw.get("provenance"))
        )
        return Assignment(identifier, role, slug, starts_on, ends_on, open_ended, diagnostic)

    @staticmethod
    def _parse_interval(raw: object) -> tuple[date | None, date | None, bool, str | None]:
        if not isinstance(raw, dict):
            return None, None, False, "invalid_from"
        starts_on = _parse_iso_date(raw.get("from"))
        if starts_on is None:
            return None, None, False, "invalid_from"
        raw_end = raw.get("to")
        if raw_end == "open":
            return starts_on, None, True, None
        ends_on = _parse_iso_date(raw_end)
        if ends_on is None:
            return starts_on, None, False, "invalid_to"
        if ends_on < starts_on:
            return starts_on, ends_on, False, "invalid_interval"
        return starts_on, ends_on, False, None

    @staticmethod
    def _provenance_error(raw: object) -> str | None:
        if not isinstance(raw, dict):
            return "missing_provenance"
        required = ("publisher", "reference_url", "evidence_note", "reviewed_on")
        if any(not isinstance(raw.get(field), str) or not raw[field] for field in required):
            return "invalid_provenance"
        parsed_url = urlparse(raw["reference_url"])
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            return "invalid_provenance"
        return None if _parse_iso_date(raw["reviewed_on"]) else "invalid_provenance"

    @staticmethod
    def _mark_duplicate_ids(assignments: tuple[Assignment, ...]) -> tuple[Assignment, ...]:
        identifier_counts = Counter(
            assignment.identifier for assignment in assignments if assignment.identifier
        )
        duplicates = {identifier for identifier, count in identifier_counts.items() if count > 1}
        return tuple(
            Assignment(
                assignment.identifier,
                assignment.role,
                assignment.participant_slug,
                assignment.validity_from,
                assignment.validity_to,
                assignment.is_open_ended,
                assignment.diagnostic or (
                    "duplicate_assignment_id" if assignment.identifier in duplicates else None
                ),
            )
            for assignment in assignments
        )


def _parse_iso_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None

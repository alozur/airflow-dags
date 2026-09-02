#!/usr/bin/env python3
"""
Precondition-guarded, per-turn backfill for issue #339: correct
`resolved_participant_slug` on `speaker_turn_videos` rows misattributed
to the wrong participant.

WARNING — DB-correct is NOT YouTube-correct: this only updates the
database; a published turn's public metadata needs a separate,
manually-approved YouTube fix (see the issue #339 runbook).

Dry run is the DEFAULT (no `--execute`): zero write statements, no
write transaction, never commits. `--execute` raises
`NotImplementedError` in this revision; the write path lands later.

Input: a JSON array at `--input PATH`, one entry per turn —
    [{"turn_id": 8124, "expected_current_slug": "wrong", "new_slug": "right"}]
`expected_current_slug` may be `null`.

Usage:
    uv run python scripts/backfill_turn_resolution_slug.py \\
        --input remediation/339-plan.json --confidence 1.0
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.postgres_helpers import PostgresConnection  # noqa: E402

STATUS_WOULD_UPDATE = "WOULD-UPDATE"
STATUS_DRIFT = "DRIFT"
STATUS_MISSING = "MISSING"
STATUS_NO_CHANGE = "NO-CHANGE"
_DRY_RUN_STATUS_ORDER = (STATUS_WOULD_UPDATE, STATUS_DRIFT, STATUS_MISSING, STATUS_NO_CHANGE)

CURRENT_STATE_QUERY_TEMPLATE = """
    SELECT stv.turn_id AS turn_id,
           stv.output_path AS output_path,
           stv.resolved_participant_slug AS resolved_participant_slug,
           st.speaker_label AS speaker_label
    FROM {stv_table} stv
    JOIN {st_table} st ON stv.turn_id = st.turn_id
    WHERE stv.turn_id = ANY(%s)
"""

METHOD_CONSTRAINT_QUERY = """
    SELECT pg_get_constraintdef(oid) AS definition
    FROM pg_constraint
    WHERE conrelid = %s::regclass AND contype = 'c' AND conname LIKE %s
"""


class BackfillInputError(Exception):
    """Invalid CLI input: malformed plan file or bad confidence."""


class BackfillConstraintError(Exception):
    """An existing CHECK constraint refuses the --method literal."""


@dataclass(frozen=True)
class BackfillEntry:
    """One turn's correction: current slug precondition and target slug."""

    turn_id: int
    expected_current_slug: str | None
    new_slug: str


def load_plan(path) -> list[BackfillEntry]:
    """Parse+validate the JSON plan file into `BackfillEntry` objects."""
    try:
        raw_text = Path(path).read_text()
    except OSError as exc:
        raise BackfillInputError(f"cannot read plan file {path!r}: {exc}") from exc
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise BackfillInputError(
            f"malformed JSON in {path!r} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    if not isinstance(raw, list):
        raise BackfillInputError(f"plan file {path!r} must contain a JSON array")
    if not raw:
        raise BackfillInputError(f"plan file {path!r} is empty — at least one entry is required")

    entries: list[BackfillEntry] = []
    seen_turn_ids: set[int] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise BackfillInputError(f"entry {index}: must be a JSON object")
        missing = [f for f in ("turn_id", "new_slug") if f not in item]
        if missing:
            raise BackfillInputError(f"entry {index}: missing required field(s) {missing}")

        turn_id = item["turn_id"]
        if not isinstance(turn_id, int) or isinstance(turn_id, bool):
            raise BackfillInputError(f"entry {index}: turn_id must be an integer, got {turn_id!r}")
        if turn_id in seen_turn_ids:
            raise BackfillInputError(f"entry {index}: duplicate turn_id {turn_id}")
        seen_turn_ids.add(turn_id)

        expected = item.get("expected_current_slug")
        if expected is not None and not isinstance(expected, str):
            raise BackfillInputError(
                f"entry {index}: expected_current_slug must be a string or null, got {expected!r}"
            )
        new_slug = item["new_slug"]
        if not isinstance(new_slug, str) or not new_slug:
            raise BackfillInputError(f"entry {index}: new_slug must be a non-empty string")

        entries.append(
            BackfillEntry(turn_id=turn_id, expected_current_slug=expected, new_slug=new_slug)
        )
    return entries


def validate_confidence(confidence: float) -> None:
    """Reject a --confidence outside [0.0, 1.0]."""
    if not (0.0 <= confidence <= 1.0):
        raise BackfillInputError(f"--confidence must be within [0.0, 1.0], got {confidence}")


def build_current_state_query(stv_table: str, st_table: str) -> str:
    """Build the read-only pre-read SELECT for the given tables."""
    return CURRENT_STATE_QUERY_TEMPLATE.format(stv_table=stv_table, st_table=st_table)


def fetch_current_state(conn, stv_table: str, st_table: str, turn_ids: list[int]) -> dict:
    """Single parameterized SELECT (`turn_id = ANY(%s)`, no interpolation); returns rows keyed by turn_id."""
    query = build_current_state_query(stv_table, st_table)
    with conn.cursor() as cur:
        cur.execute(query, (list(turn_ids),))
        rows = cur.fetchall()
    return {row["turn_id"]: row for row in rows}


def check_method_constraint(conn, stv_table: str, method: str) -> None:
    """Admit `method` when no CHECK constraint exists or an existing one permits it;
    otherwise raise `BackfillConstraintError`."""
    with conn.cursor() as cur:
        cur.execute(METHOD_CONSTRAINT_QUERY, (stv_table, "%method%"))
        rows = list(cur.fetchall())
    if not rows:
        return
    for row in rows:
        definition = row["definition"] if isinstance(row, dict) else row[0]
        if method in definition:
            return
    raise BackfillConstraintError(
        f"--method {method!r} is not permitted by an existing CHECK constraint on {stv_table}"
    )


def _derive_dry_run_status(entry: BackfillEntry, current_row) -> str:
    """Classify one entry against its current DB row for dry-run reporting."""
    if current_row is None:
        return STATUS_MISSING
    current_slug = current_row.get("resolved_participant_slug")
    if current_slug != entry.expected_current_slug:
        return STATUS_DRIFT
    if current_slug == entry.new_slug:
        return STATUS_NO_CHANGE
    return STATUS_WOULD_UPDATE


def render_summary(
    entries: list[BackfillEntry],
    current_state: dict,
    *,
    mode_label: str,
    qualified_table: str,
    method: str,
    confidence: float,
) -> str:
    """Render turn_id | speaker_label | old_slug | new_slug | status, output_path
    last/untruncated, footer with per-status counts."""
    lines = [
        f"Mode: {mode_label}",
        f"Table: {qualified_table}",
        f"Method: {method}  Confidence: {confidence}",
        f"Entries: {len(entries)}",
        "",
        f"{'turn_id':>10} | {'speaker_label':<16} | {'old_slug':<28} | "
        f"{'new_slug':<28} | {'status':<12} | output_path",
    ]

    status_counts: dict[str, int] = {}
    for entry in entries:
        current_row = current_state.get(entry.turn_id)
        status = _derive_dry_run_status(entry, current_row)
        status_counts[status] = status_counts.get(status, 0) + 1
        old_slug = current_row.get("resolved_participant_slug") if current_row else None
        speaker_label = current_row.get("speaker_label") if current_row else None
        output_path = current_row.get("output_path") if current_row else None
        lines.append(
            f"{entry.turn_id:>10} | {str(speaker_label):<16} | {str(old_slug):<28} | "
            f"{entry.new_slug:<28} | {status:<12} | {output_path}"
        )

    lines.append("")
    for status in _DRY_RUN_STATUS_ORDER:
        if status in status_counts:
            lines.append(f"{status}: {status_counts[status]}")
    return "\n".join(lines)


def apply_backfill(*args, **kwargs):
    """Write path stub — implemented in a follow-up change; single call site to replace."""
    raise NotImplementedError("write path lands in a follow-up PR; use dry-run (omit --execute)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments. Dry run is the absence of `--execute`."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to the JSON plan file.")
    parser.add_argument(
        "--confidence", required=True, type=float, help="Confidence in [0.0, 1.0] for every entry."
    )
    parser.add_argument(
        "--method", default="manual", help="speaker_resolution_method marker (default: manual)."
    )
    parser.add_argument(
        "--execute", action="store_true", help="Perform writes. Absent (default) = dry run."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Dry-run entrypoint: load/validate plan, fetch current state, render summary, exit 0.
    `--execute` raises `NotImplementedError`. Never opens a write transaction or commits."""
    args = parse_args(argv)

    try:
        validate_confidence(args.confidence)
        entries = load_plan(args.input)
    except BackfillInputError as exc:
        print(f"Input error: {exc}", file=sys.stderr)
        return 2

    if args.execute:
        apply_backfill()

    try:
        pg_conn = PostgresConnection()
        stv_table = pg_conn.get_qualified_table("speaker_turn_videos")
        st_table = pg_conn.get_qualified_table("speaker_turns")
        with pg_conn.get_connection() as conn:
            current_state = fetch_current_state(
                conn, stv_table, st_table, [entry.turn_id for entry in entries]
            )
            conn.rollback()
            summary = render_summary(
                entries,
                current_state,
                mode_label="DRY RUN",
                qualified_table=stv_table,
                method=args.method,
                confidence=args.confidence,
            )
    except Exception as exc:
        print(f"Operational error: {exc}", file=sys.stderr)
        return 1

    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())

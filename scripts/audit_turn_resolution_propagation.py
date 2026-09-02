#!/usr/bin/env python3
"""
Read-only audit for issue #321 (AC3): quantify already-published turn groups
whose resolved_participant_slug was propagated onto a sibling
speaker_turn_videos row with a mismatched diarization speaker_label.

Gates A (congress_videos/modules/database.py::mark_turn_resolved) and B
(congress_videos/modules/speaker_roster_crosscheck.py) prevent this defect
on writes going forward; this script only reports on data already published
before the fix, or by any other write path.

A group (single output_path, at least one uploaded row) is flagged when:
  - it joins to MORE THAN ONE distinct speaker_turns.speaker_label, AND
  - every speaker_turn_videos row in the group shares one identical,
    non-null resolved_participant_slug.

Read-only: a single SELECT is executed, no writes.

Usage (same env as the DAGs — POSTGRES_* vars):
    uv run python scripts/audit_turn_resolution_propagation.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.postgres_helpers import PostgresConnection  # noqa: E402

AUDIT_QUERY_TEMPLATE = """
    SELECT stv.output_path AS output_path,
           array_agg(DISTINCT st.speaker_label ORDER BY st.speaker_label) AS speaker_labels,
           MAX(stv.resolved_participant_slug) AS resolved_participant_slug
    FROM {stv_table} stv
    JOIN {st_table} st ON stv.turn_id = st.turn_id
    WHERE stv.output_path IN (
        SELECT output_path FROM {stv_table} WHERE is_uploaded_to_youtube = TRUE
    )
    GROUP BY stv.output_path
    HAVING COUNT(DISTINCT st.speaker_label) > 1
       AND COUNT(DISTINCT stv.resolved_participant_slug) = 1
       AND COUNT(stv.resolved_participant_slug) = COUNT(*)
    ORDER BY stv.output_path
"""


def build_audit_query(stv_table: str, st_table: str) -> str:
    """Build the read-only propagation-audit SELECT for the given tables."""
    return AUDIT_QUERY_TEMPLATE.format(stv_table=stv_table, st_table=st_table)


def fetch_propagated_groups(conn, stv_table: str, st_table: str) -> list[dict]:
    """Run the audit query and return one dict per affected group.

    Read-only: executes a single SELECT via the provided connection's
    cursor and never commits or issues a write statement.
    """
    query = build_audit_query(stv_table, st_table)
    with conn.cursor() as cur:
        cur.execute(query)
        return list(cur.fetchall())


def format_report(rows: list[dict]) -> str:
    """Render the affected-group count and one detail line per group."""
    lines = [f"Affected groups: {len(rows)}"]
    for row in rows:
        lines.append(
            f"  output_path={row['output_path']} "
            f"speaker_labels={row['speaker_labels']} "
            f"resolved_participant_slug={row['resolved_participant_slug']}"
        )
    return "\n".join(lines)


def main() -> None:
    pg_conn = PostgresConnection()
    stv_table = pg_conn.get_qualified_table('speaker_turn_videos')
    st_table = pg_conn.get_qualified_table('speaker_turns')

    with pg_conn.get_connection() as conn:
        rows = fetch_propagated_groups(conn, stv_table, st_table)

    print(format_report(rows))


if __name__ == "__main__":
    main()

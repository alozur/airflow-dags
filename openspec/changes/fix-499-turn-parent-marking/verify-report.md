# Verify Report: issue #499

## Result: PASS — code and guarded repair command

- Future turn uploads: a non-null `turn_id` skips parent chapter marking.
- Legacy chapter uploads: existing no-`turn_id` test remains green.
- Repair: default dry run; execute path is allowlisted to 263–266/519 and rechecks the sibling YouTube-id and pending-prepared predicates in the `UPDATE`.
- Production read-only preflight: qualified chapters are **263, 265, 266, 519**. Chapter 264 is excluded because none of its pending turns is prepared.

## Evidence

| Check | Result |
| --- | --- |
| Focused marking + repair tests | `35 passed` |
| Full suite | `4811 passed, 32 skipped`; coverage `90.78%` |
| Ruff changed files | pass |
| Airflow Docker e2e | unavailable (documented exit 4; Docker daemon inaccessible) |

## Remaining operational gate

No production row has been changed. After the deployed code is active, obtain explicit approval for the preflight set `[263, 265, 266, 519]`, execute the command on the NAS, and verify those prepared sibling turns appear in `production.uploadable_turns`.

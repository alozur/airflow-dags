# Proposal: exclude manual recovery uploads from the scheduled quota

Issue #500: manual/forced recovery uploads must not consume the daily slot reserved for scheduled runs. Persist an explicit `counts_toward_daily_quota` flag on chapter and turn upload records, set it only for `DagRunType.SCHEDULED`, and filter both quota counters by it.

Manual/forced runs still upload normally; the intentional outcome is two videos on one day when a recovery and scheduled run both succeed. Existing rows default to `TRUE`, preserving historic quota semantics.

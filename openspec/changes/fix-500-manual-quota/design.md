# Design

Migration 048 adds `counts_toward_daily_quota BOOLEAN NOT NULL DEFAULT TRUE` to both publication tables. The uploader derives the flag from `dag_run.run_type`: only the literal scheduled value is true; manual/forced runs are false. Both marking paths persist it and both quota counters filter it. Default true preserves existing records and scheduled-call compatibility.

Rollout: apply migration 048 before the new DAG code runs. Rollback is revert code; the additive columns may remain.

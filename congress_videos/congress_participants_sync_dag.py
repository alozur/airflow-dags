"""
Congress Participants Sync DAG.

Weekly pipeline that ingests the active Spanish Congress of Deputies roster
from congreso.es, upserts rows into the ``congress_participants`` table, and
back-fills photo URLs — first from Wikidata, then from the congreso.es official
deterministic URL as a fallback.

Schedule: every Monday at 03:00 UTC  (``0 3 * * 1``)
Tasks:
  1. fetch_and_parse              — resolve download URL, fetch JSON, parse deputies
  2. upsert_participants          — bulk-upsert via CongressParticipantsDB.upsert_batch
  3. enrich_missing_photos        — fuzzy-join Wikidata photos to null-photo rows
  4. fill_congreso_photo_fallback — write deterministic congreso.es URLs for still-null rows

The pipeline is idempotent: running it twice on the same source data produces
the same DB state as running it once (ON CONFLICT DO UPDATE + COALESCE guard).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from congress_videos.modules.participants_db import CongressParticipantsDB
from congress_videos.modules.participants_enrichment import enrich_missing_photos, fill_congreso_photo_fallback
from congress_videos.modules.participants_ingestion import fetch_active_deputies, parse_deputies
from utils.airflow_helpers import xcom_task
from utils.env_loader import load_env_if_local

load_env_if_local()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "congress_participants_sync",
    default_args=default_args,
    description="Weekly sync of Spanish Congress of Deputies members with Wikidata photo enrichment",
    schedule_interval="0 3 * * 1",
    start_date=datetime(2025, 10, 6),  # First Monday after project start
    catchup=False,
    max_active_runs=1,
    tags=["congress", "participants", "sync"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_and_parse",
        python_callable=lambda ti, **ctx: xcom_task(
            ti,
            lambda: parse_deputies(fetch_active_deputies()),
            "participants",
        ),
    )

    t2 = PythonOperator(
        task_id="upsert_participants",
        python_callable=lambda ti, **ctx: xcom_task(
            ti,
            lambda: CongressParticipantsDB().upsert_batch(ti.xcom_pull(key="participants")),
            "upsert_result",
        ),
    )

    t3 = PythonOperator(
        task_id="enrich_missing_photos",
        python_callable=lambda ti, **ctx: xcom_task(
            ti,
            lambda: enrich_missing_photos(),
            "enrich_result",
        ),
    )

    t4 = PythonOperator(
        task_id="fill_congreso_photo_fallback",
        python_callable=lambda ti, **ctx: xcom_task(
            ti,
            lambda: fill_congreso_photo_fallback(),
            "fill_congreso_result",
        ),
    )

    t1 >> t2 >> t3 >> t4

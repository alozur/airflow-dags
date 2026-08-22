"""Regression tests for turn-train chain-trigger imports (issue #159).

Importing a sibling DAG module from a DAG file executes that module, and
Airflow >=2.4 auto-registers every DAG constructed while a file is parsed —
including transitively imported ones. The scheduler then raises
``AirflowDagDuplicatedIdException`` for both files. Chain triggers must import
dag_ids from ``congress_videos.config.constants`` (which defines no DAGs).

These tests parse each turn-train DAG file in isolation, exactly like the
scheduler's DagFileProcessor does, and assert it registers only its own dag_id.
"""

from pathlib import Path

import pytest
from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parents[2]

TURN_TRAIN_FILES = [
    ("congress_videos/speaker_turns_dag.py", "speaker_turns"),
    ("congress_videos/speaker_turn_videos_dag.py", "speaker_turn_videos"),
    ("congress_videos/speaker_turn_prepare_dag.py", "speaker_turn_prepare"),
]


@pytest.mark.parametrize(("rel_path", "expected_dag_id"), TURN_TRAIN_FILES)
def test_file_registers_only_its_own_dag(rel_path, expected_dag_id):
    bag = DagBag(dag_folder=str(REPO_ROOT / rel_path), include_examples=False)

    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {expected_dag_id}


def test_dag_id_constants_match_dag_modules():
    from congress_videos import (
        speaker_turn_prepare_dag,
        speaker_turn_videos_dag,
        speaker_turns_dag,
    )
    from congress_videos.config import constants

    assert speaker_turns_dag.DAG_ID == constants.SPEAKER_TURNS_DAG_ID
    assert speaker_turn_videos_dag.DAG_ID == constants.SPEAKER_TURN_VIDEOS_DAG_ID
    assert speaker_turn_prepare_dag.DAG_ID == constants.SPEAKER_TURN_PREPARE_DAG_ID

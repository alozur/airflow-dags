"""Shared fixtures for the airflow-dags test suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_json_fixture(name: str) -> Any:
    path = FIXTURES_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


def _load_text_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


@pytest.fixture
def mock_openai_client(mocker):
    """Mock openai.OpenAI() and its .chat.completions.create() method."""
    fake_response = MagicMock()
    fake_response.choices = [MagicMock()]
    fake_response.choices[0].message.content = '{"chapters": []}'
    fake_response.usage = MagicMock(prompt_tokens=10, completion_tokens=10, total_tokens=20)

    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = fake_response

    mocker.patch("openai.OpenAI", return_value=fake_client)
    return fake_client


@pytest.fixture
def mock_psycopg2_connection(mocker):
    """Mock psycopg2.connect → connection → cursor."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.execute.return_value = None
    mock_cursor.rowcount = 0
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.commit.return_value = None
    mock_conn.rollback.return_value = None
    mock_conn.close.return_value = None

    mock_connect = mocker.patch("psycopg2.connect", return_value=mock_conn)
    return mock_connect, mock_conn, mock_cursor


@pytest.fixture
def mock_task_instance() -> MagicMock:
    """Airflow TaskInstance double with an in-memory XCom store."""
    xcom_store: dict[str, Any] = {}

    ti = MagicMock(name="TaskInstance")
    ti.xcom_store = xcom_store

    def _push(key: str, value: Any, **_kw: Any) -> None:
        xcom_store[key] = value

    def _pull(key: str | None = None, **_kw: Any) -> Any:
        if key is None:
            return None
        return xcom_store.get(key)

    ti.xcom_push.side_effect = _push
    ti.xcom_pull.side_effect = _pull
    ti.task_id = "test_task"
    ti.dag_id = "test_dag"
    ti.run_id = "test_run"
    return ti


@pytest.fixture
def mock_subprocess_run(mocker):
    """Mock subprocess.run with a successful default (returncode=0)."""
    completed = MagicMock()
    completed.returncode = 0
    completed.stdout = ""
    completed.stderr = ""
    completed.args = []
    return mocker.patch("subprocess.run", return_value=completed)


@pytest.fixture
def mock_youtube_service(mocker):
    """Mock googleapiclient.discovery.build → YouTube service resource."""
    fake_service = MagicMock()

    fake_service.videos.return_value.insert.return_value.execute.return_value = {
        "id": "test-video-id",
        "snippet": {"title": "Test Title"},
        "status": {"uploadStatus": "uploaded"},
    }
    fake_service.search.return_value.list.return_value.execute.return_value = {
        "items": [],
        "nextPageToken": None,
    }
    fake_service.channels.return_value.list.return_value.execute.return_value = {
        "items": [{"id": "UC_test_channel"}],
    }

    mocker.patch("googleapiclient.discovery.build", return_value=fake_service)
    return fake_service


@pytest.fixture
def mock_requests(mocker):
    """Mock requests.get / requests.post / requests.put."""
    import types as _types

    def _make_response(status_code: int = 200, json_data: dict | None = None, text: str = "") -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.ok = 200 <= status_code < 300
        resp.text = text
        resp.content = text.encode("utf-8")
        resp.json.return_value = json_data or {}
        resp.raise_for_status.return_value = None
        return resp

    mock_get = mocker.patch("requests.get", return_value=_make_response())
    mock_post = mocker.patch("requests.post", return_value=_make_response())
    mock_put = mocker.patch("requests.put", return_value=_make_response())

    return _types.SimpleNamespace(
        get=mock_get,
        post=mock_post,
        put=mock_put,
        make_response=_make_response,
    )


@pytest.fixture
def sample_segments() -> list[dict[str, Any]]:
    """Whisper transcription segments."""
    return _load_json_fixture("sample_segments.json")


@pytest.fixture
def sample_srt_content() -> str:
    """SRT subtitle file content."""
    return _load_text_fixture("sample_srt.txt")


@pytest.fixture
def sample_chapters() -> dict[str, Any]:
    """AI chapter-analysis output."""
    return _load_json_fixture("sample_chapters.json")


@pytest.fixture
def sample_youtube_response() -> dict[str, Any]:
    """YouTube Data API v3 search.list response."""
    return _load_json_fixture("sample_youtube_response.json")


@pytest.fixture
def temp_audio_file(tmp_path: Path) -> Path:
    """Create a dummy audio file."""
    audio_dir = tmp_path / "downloads" / "2025-05-23" / "video123" / "audio_chunks"
    audio_dir.mkdir(parents=True, exist_ok=True)
    audio_path = audio_dir / "chunk_001.webm"
    audio_path.write_bytes(b"\x00" * 16)
    return audio_path


@pytest.fixture
def temp_srt_file(tmp_path: Path, sample_srt_content: str) -> Path:
    """Write sample_srt_content to a temp .srt file."""
    srt_path = tmp_path / "test.srt"
    srt_path.write_text(sample_srt_content, encoding="utf-8")
    return srt_path

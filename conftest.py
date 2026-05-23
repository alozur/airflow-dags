"""
Root-level pytest configuration for airflow-dags.

CRITICAL: This file runs before any test imports. We use it to:
1. Set environment variables that production modules read at import-time
2. Inject stub modules into sys.modules for libraries we never want to actually
   import during tests (yt_dlp, whisper, pytubefix)
3. Fix sys.path so `import utils.*` and `import congress_videos.*` work from
   the repo root without installing the package.
4. Configure logging to be silent unless a test fails.
"""

from __future__ import annotations

import logging
import os
import sys
import types
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. sys.path: make repo root importable as a package root.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ---------------------------------------------------------------------------
# 2. Environment variables — set BEFORE any production module is imported.
# ---------------------------------------------------------------------------
_TEST_ENV: dict[str, str] = {
    "OPENAI_API_KEY": "test-openai-key-not-real",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "test_db",
    "POSTGRES_USER": "test_user",
    "POSTGRES_PASSWORD": "test_password",
    "POSTGRES_SCHEMA": "test_schema",
    "WHISPER_API_HOST": "whisper-api-test",
    "WHISPER_API_PORT": "9000",
    "AIRFLOW__CORE__DAGS_FOLDER": "/opt/airflow/dags",
    "AIRFLOW_HOME": "/tmp/airflow_test_home",
    "YOUTUBE_API_KEY": "test-youtube-api-key",
    "ANTHROPIC_API_KEY": "test-anthropic-key",
    "ENV": "testing",
}
for _key, _value in _TEST_ENV.items():
    os.environ.setdefault(_key, _value)

# ---------------------------------------------------------------------------
# 3. Stub heavy/optional third-party modules BEFORE production code imports them.
# ---------------------------------------------------------------------------
def _install_stub_module(name: str, attrs: dict[str, object] | None = None) -> types.ModuleType:
    """Insert a stub module into sys.modules if it is not already present."""
    if name in sys.modules:
        return sys.modules[name]
    module = types.ModuleType(name)
    for attr_name, attr_value in (attrs or {}).items():
        setattr(module, attr_name, attr_value)
    sys.modules[name] = module
    return module


class _StubYoutubeDL:
    """Minimal yt_dlp.YoutubeDL stand-in usable as a context manager."""

    def __init__(self, *args, **kwargs) -> None:
        self.params = kwargs

    def __enter__(self) -> "_StubYoutubeDL":
        return self

    def __exit__(self, *_exc_info) -> None:
        return None

    def extract_info(self, *_args, **_kwargs) -> dict[str, object]:
        return {"id": "stub", "title": "stub", "duration": 0, "ext": "mp4"}

    def download(self, *_args, **_kwargs) -> int:
        return 0


_install_stub_module(
    "yt_dlp",
    attrs={
        "YoutubeDL": _StubYoutubeDL,
        "DownloadError": type("DownloadError", (Exception,), {}),
        "utils": types.SimpleNamespace(DownloadError=Exception),
    },
)

_install_stub_module("pytubefix", attrs={"YouTube": object, "cli": types.SimpleNamespace(on_progress=lambda *a, **k: None)})
_install_stub_module("pytubefix.cli", attrs={"on_progress": lambda *a, **k: None})
_install_stub_module("whisper", attrs={"load_model": lambda *_a, **_kw: None})

# ---------------------------------------------------------------------------
# 4. Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("openai").setLevel(logging.ERROR)

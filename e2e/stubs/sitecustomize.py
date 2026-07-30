"""e2e/stubs/sitecustomize.py — no-op stubs so DAG *code* parses without the real
heavy libs (N7 resolution).

Container analog of `conftest.py`: this file is mounted at
`/opt/airflow/stubs/sitecustomize.py` and `/opt/airflow/stubs` is placed on
`PYTHONPATH` in `docker-compose.test.yml`. Python's `site` module auto-imports any
`sitecustomize` module found on `sys.path` at interpreter startup, so this runs before
Airflow's DagBag parses the DAG folder — and, crucially, in every `DagFileProcessor`
subprocess too (they inherit `PYTHONPATH`).

Only registers modules that Airflow's own bundled/base image does not already provide.
See design.md "Stub modules for e2e (N7 resolution)" for the grep-verified package list
and rationale (each package below is imported at *module top level* somewhere in
`utils/**` or `congress_videos/**`, so it must resolve at DAG-parse time even though the
lightweight e2e image never installs the real heavy packages).

Do NOT stub: `requests`, `urllib3`, `psycopg2` — Airflow's own base image already
provides these.

CRITICAL: never register a fake top-level `google` module. `google` is a real
namespace package (used by `google.protobuf` etc., which Airflow/grpc may load); a
fabricated top-level `google` module would shadow it and break the runtime. Only the
specific `google.auth*` / `google.oauth2*` submodules are registered.
"""

from __future__ import annotations

import sys
import types


def _stub(name: str, **attrs: object) -> types.ModuleType:
    """Register a no-op stub module in sys.modules, never clobbering a real module.

    Mirrors conftest.py's `_install_stub_module` shape/intent (sys.modules.setdefault
    -style injection): if `name` is already present in sys.modules (a real installed
    package, or a package another stub call already registered), it is returned
    untouched — this stub call becomes a no-op.
    """
    if name in sys.modules:
        return sys.modules[name]
    module = types.ModuleType(name)
    for attr_name, attr_value in attrs.items():
        setattr(module, attr_name, attr_value)
    sys.modules[name] = module
    return module


# --- Confirmed heavy top-level imports (design.md grep-verified list) -------------
_stub("yt_dlp", YoutubeDL=object, DownloadError=type("DownloadError", (Exception,), {}))
_stub("openai", OpenAI=object)
_stub("PIL", Image=object, ImageDraw=object, ImageFont=object, ImageEnhance=object)
_stub("bs4", BeautifulSoup=object)
_stub("googleapiclient")
_stub("googleapiclient.discovery", build=lambda *a, **k: None)
_stub("googleapiclient.http", MediaFileUpload=object)
_stub("PyPDF2", PdfReader=object)
_stub("numpy")
_stub("rapidfuzz")
_stub("rapidfuzz.fuzz", token_sort_ratio=lambda *a, **k: 0.0)

# google-auth: register ONLY the submodules, never a fake top-level `google`, so a real
# `google` namespace package (e.g. google.protobuf pulled in by Airflow/grpc) is NOT
# shadowed.
_stub("google.auth")
_stub("google.auth.transport")
_stub("google.auth.transport.requests", Request=object)
_stub("google.oauth2")
_stub("google.oauth2.credentials", Credentials=object)

# --- Parity-only stubs (function-level imports in production; stubbed here only for
# 1:1 parity with conftest.py, per design's maintenance note) ---------------------
_stub("pytubefix", YouTube=object)
_stub("pytubefix.cli", on_progress=lambda *a, **k: None)
_stub("whisper", load_model=lambda *a, **k: None)

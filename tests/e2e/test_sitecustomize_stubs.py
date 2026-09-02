"""
RED/GREEN/TRIANGULATE test for e2e/stubs/sitecustomize.py (N7 heavy-import stub mechanism).

This does NOT rely on Python's automatic `site`-driven sitecustomize import (that only
fires for interpreter startup with the directory on sys.path before the interpreter
begins). Instead it imports the module directly the same way the design's test-first
guidance describes: add `e2e/stubs` to `sys.path`, import `sitecustomize` directly, then
assert the confirmed heavy packages become importable and the real `google` namespace is
not shadowed.

Test isolation: heavy stub module names are removed from `sys.modules` before each test
and any names this test itself injects are cleaned up after, so this test file does not
leak stub modules into the rest of the pytest session (verified separately by running
`uv run pytest` without `e2e/stubs` on PYTHONPATH per T4's manual sanity check).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

E2E_STUBS_DIR = Path(__file__).resolve().parent.parent.parent / "e2e" / "stubs"

# The 8 confirmed heavy top-level packages from design.md's grep-verified list
# (google.auth / google.oauth2 counted as one "google-auth" family entry here for the
# import-name list; the design table lists these as separate confirmed sites but they
# share the same distribution).
CONFIRMED_HEAVY_IMPORT_NAMES = [
    "yt_dlp",
    "openai",
    "PIL",
    "bs4",
    "googleapiclient",
    "pypdf",
    "numpy",
    "google.auth",
]

# Additional submodules that must also be importable after the stub loads (not
# top-level distribution names, but confirmed parse-time import sites).
CONFIRMED_HEAVY_SUBMODULE_IMPORTS = [
    "googleapiclient.discovery",
    "googleapiclient.http",
    "google.auth.transport.requests",
    "google.auth.jwt",
    "google.oauth2",
    "google.oauth2.credentials",
]

# Parity-only stubs (function-level in production, stubbed here only for 1:1 parity
# with conftest.py per design).
PARITY_STUB_IMPORT_NAMES = ["pytubefix", "whisper"]

_STUB_MODULE_NAMES = (
    CONFIRMED_HEAVY_IMPORT_NAMES
    + CONFIRMED_HEAVY_SUBMODULE_IMPORTS
    + PARITY_STUB_IMPORT_NAMES
    + ["pytubefix.cli", "sitecustomize"]
)


def _affected_module_names() -> list[str]:
    return [
        name
        for name in list(sys.modules)
        if name == "google"
        or any(name == stub or name.startswith(stub + ".") for stub in _STUB_MODULE_NAMES)
    ]


@pytest.fixture(autouse=True)
def _isolate_stub_modules():
    """Snapshot and restore sys.modules entries this test's stub loading affects.

    CRITICAL: some of these names (e.g. `yt_dlp`, `numpy`, `PIL`) are REAL installed
    runtime dependencies of this project (see pyproject.toml), possibly already
    imported by other test modules before this fixture ever runs. Blanket-deleting
    them from sys.modules after the test would force a stale re-import elsewhere in
    the process, producing a *second*, distinct module/class object for the same
    name — which breaks identity-sensitive code (e.g. `except yt_dlp.DownloadError`
    checks in production code that already hold a reference to the original module
    object). We must restore the exact original objects, never just delete-and-let-
    Python-reimport.
    """
    snapshot = {name: sys.modules.get(name) for name in _affected_module_names()}
    added_names_before = set(sys.modules)
    if str(E2E_STUBS_DIR) not in sys.path:
        sys.path.insert(0, str(E2E_STUBS_DIR))
    yield
    if str(E2E_STUBS_DIR) in sys.path:
        sys.path.remove(str(E2E_STUBS_DIR))
    # Remove any newly-added affected-name modules this test's stub loading created,
    # then restore exactly what was present before (real modules keep their original
    # object identity; names that never existed before stay absent).
    for name in _affected_module_names():
        if name not in snapshot:
            sys.modules.pop(name, None)
    for name, original_module in snapshot.items():
        if original_module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original_module
    # Clean up any brand-new top-level entries (e.g. a freshly stub-created
    # `sitecustomize`) that did not exist at all beforehand.
    for name in set(sys.modules) - added_names_before:
        if name in _STUB_MODULE_NAMES or name == "sitecustomize":
            sys.modules.pop(name, None)


def _load_sitecustomize():
    """Import e2e/stubs/sitecustomize.py directly (not via Python's automatic site hook)."""
    import importlib

    if "sitecustomize" in sys.modules:
        del sys.modules["sitecustomize"]
    return importlib.import_module("sitecustomize")


def test_all_confirmed_heavy_packages_importable_after_stub_loads():
    _load_sitecustomize()

    for name in CONFIRMED_HEAVY_IMPORT_NAMES:
        __import__(name)

    for name in CONFIRMED_HEAVY_SUBMODULE_IMPORTS:
        __import__(name)


def test_google_namespace_is_not_shadowed_by_stub():
    _load_sitecustomize()

    import google  # noqa: F401 -- must resolve without error

    # The stub must never register a fake top-level `google` module. Only real
    # namespace-package behavior (a __path__, potentially multiple search locations)
    # is acceptable — a stub-created plain module would have no __path__ at all, or
    # would be a single fabricated types.ModuleType standing in for the whole package.
    assert hasattr(google, "__path__"), (
        "google must remain a real namespace package (has __path__); "
        "the stub must only register google.auth / google.oauth2 submodules, "
        "never a fake top-level `google` module"
    )


def test_stub_helper_never_clobbers_a_real_installed_package():
    """TRIANGULATE-adjacent: importing a stubbed name that was already real before
    sitecustomize ran must not be replaced by the stub."""
    import types

    sentinel = types.ModuleType("numpy")
    sentinel.__is_real_sentinel__ = True  # type: ignore[attr-defined]
    sys.modules["numpy"] = sentinel

    _load_sitecustomize()

    import numpy

    assert getattr(numpy, "__is_real_sentinel__", False) is True, (
        "sitecustomize's _stub() helper must use sys.modules.setdefault-style "
        "injection and never clobber an already-present module"
    )


def test_non_stubbed_package_still_imports_normally_triangulate():
    """TRIANGULATE: a package NOT in the stub list (requests) must still import as the
    real package — proves the stub mechanism does not over-shadow unrelated packages."""
    _load_sitecustomize()

    import requests

    # The real `requests` package ships `requests.exceptions.RequestException`; a stub
    # module created by this mechanism would not have this real attribute structure.
    assert hasattr(requests, "exceptions")
    assert hasattr(requests.exceptions, "RequestException")

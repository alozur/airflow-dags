"""Tests for congress_videos.generic_thumbnail_generator_dag (Slice 3 + W-01 remediation).

Strict TDD: all tests below are written BEFORE the production DAG file exists.
First run must produce ImportError failures.

Test groups:
    T-01  DAG imports cleanly (no import errors)
    T-02  schedule is None
    T-03  task-id set (exact, no more, no fewer)
    T-04  task dependency graph shape
    T-05  no Congreso-specific string literals in source
    T-06  validate_input callable behaviour
    T-07  TRIANGULATE — PIKZELS_API_KEY absent, ConfigError on unknown domain
    T-08  _task_generate_thumbnail callable (W-01 backfill)
    T-09  _task_download_option callable (W-01 backfill)
    T-10  _task_score_option callable (W-01 backfill)
"""

from __future__ import annotations

import importlib
import os
import re
import sys
from datetime import UTC
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# ThumbnailConfigWiring tests (GROUP 2 — REQ-A2)
# ---------------------------------------------------------------------------


class TestThumbnailConfigWiring:
    """Verify congreso config wires lookup_participant_by_slug, not fuzzy."""

    def test_congreso_participants_lookup_is_lookup_participant_by_slug(self) -> None:
        """congreso config 'participants_lookup' must be lookup_participant_by_slug."""
        from congress_videos.config.thumbnail_config import get_domain_config
        from congress_videos.modules.participants_db import lookup_participant_by_slug

        cfg = get_domain_config("congreso")
        assert cfg["participants_lookup"] is lookup_participant_by_slug or \
               cfg["participants_lookup"].__name__ == "lookup_participant_by_slug"

    def test_lookup_participant_fuzzy_string_absent_from_thumbnail_config_source(self) -> None:
        """'lookup_participant_fuzzy' must NOT appear in thumbnail_config.py source."""
        import inspect

        import congress_videos.config.thumbnail_config as _tcfg
        source = inspect.getsource(_tcfg)
        assert "lookup_participant_fuzzy" not in source, (
            "thumbnail_config.py must not reference lookup_participant_fuzzy"
        )


# ---------------------------------------------------------------------------
# T-01  DAG imports cleanly
# ---------------------------------------------------------------------------


class TestDagImport:
    """T-01: The DAG module must import without raising any exception."""

    def test_dag_imports_cleanly(self) -> None:
        """Importing the DAG module must not raise any exception."""
        # Force a fresh import so this test works regardless of import order.
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        # This must not raise.
        mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        assert mod is not None


# ---------------------------------------------------------------------------
# T-02  schedule is None
# ---------------------------------------------------------------------------


class TestDagSchedule:
    """T-02: The loaded DAG must have schedule=None (on-demand only)."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

    def test_schedule_is_none(self) -> None:
        dag = self.mod.dag
        # Airflow 2.x exposes the schedule via schedule_interval; schedule=None → schedule_interval is None.
        assert dag.schedule_interval is None

    def test_start_date_is_a_fixed_constant(self) -> None:
        """start_date must be a fixed past constant, not a parse-time-relative
        value — a `datetime.now() - timedelta(...)` default silently shifts on
        every DAG-file parse and can trigger a spurious catchup/backfill run
        on a schedule=None DAG (issue #206)."""
        from datetime import datetime, timezone
        dag = self.mod.dag
        assert dag.start_date == datetime(2025, 1, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# T-03  Exact task-id set
# ---------------------------------------------------------------------------


EXPECTED_TASK_IDS = {
    "validate_input",
    "resolve_participant_photo",
    "fetch_recent_history",
    "art_direction",
    "generate_thumbnail_option_a",
    "download_option_a",
    "score_option_a",
    "check_score_threshold",
    "art_direction_retry",
    "generate_thumbnail_option_b",
    "download_option_b",
    "score_option_b",
    "choose_best_option",
    "generate_title",
    "persist_results",
    "thumbnail_result",
}


def test_art_direction_task_delegates_to_art_direct(mocker) -> None:
    """The DAG task wrapper supplies its run context to art_direct (no srt_fragment)."""
    dag_mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
    ti = _make_fake_ti({"validate_input": _FAKE_CONF, "fetch_recent_history": None})
    domain_cfg = {"styles": []}

    mocker.patch.object(dag_mod, "get_domain_config", return_value=domain_cfg)
    art_direct = mocker.patch.object(dag_mod, "art_direct", return_value={"text": "BRIEF"})

    assert dag_mod._task_art_direction(ti) == {"text": "BRIEF"}
    art_direct.assert_called_once_with(
        _FAKE_CONF["debate_summary"],
        domain_cfg,
        previous_brief=None,
        sibling_briefs=None,
        srt_fragment=None,
        resolved_speaker_name=None,
        forbidden_archetype=None,
    )


def test_art_direction_task_forwards_srt_fragment_when_present(mocker) -> None:
    """When conf contains srt_fragment, it is forwarded non-None to art_direct."""
    dag_mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
    conf_with_srt = {**_FAKE_CONF, "srt_fragment": "Hay que tener cara señoría"}
    ti = _make_fake_ti({"validate_input": conf_with_srt, "fetch_recent_history": None})
    domain_cfg = {"styles": []}

    mocker.patch.object(dag_mod, "get_domain_config", return_value=domain_cfg)
    art_direct = mocker.patch.object(dag_mod, "art_direct", return_value={"text": "BRIEF"})

    dag_mod._task_art_direction(ti)
    art_direct.assert_called_once_with(
        conf_with_srt["debate_summary"],
        domain_cfg,
        previous_brief=None,
        sibling_briefs=None,
        srt_fragment="Hay que tener cara señoría",
        resolved_speaker_name=None,
        forbidden_archetype=None,
    )


# ---------------------------------------------------------------------------
# Issue #279: resolved-photo gating wired into both art_direction DAG tasks
# ---------------------------------------------------------------------------


class TestArtDirectionResolvedPhotoWiring:
    """_task_art_direction pulls resolve_participant_photo + conf.key_speakers
    and forwards the derived name (or None) to art_direct as resolved_speaker_name.
    """

    def test_photo_source_and_real_speaker_forwards_name(self, mocker) -> None:
        """source='photo' + real key_speakers name → resolved_speaker_name is that name."""
        dag_mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF_WITH_SPEAKERS,
                "fetch_recent_history": None,
                "resolve_participant_photo": {"source": "photo"},
            }
        )
        domain_cfg = {"styles": []}
        mocker.patch.object(dag_mod, "get_domain_config", return_value=domain_cfg)
        art_direct = mocker.patch.object(
            dag_mod, "art_direct", return_value={"text": "BRIEF"}
        )

        dag_mod._task_art_direction(ti)

        art_direct.assert_called_once_with(
            _FAKE_CONF_WITH_SPEAKERS["debate_summary"],
            domain_cfg,
            previous_brief=None,
            sibling_briefs=None,
            srt_fragment=None,
            resolved_speaker_name="Cervera Pinar",
            forbidden_archetype=None,
        )

    def test_non_activating_gate_inputs_forward_none(self, mocker) -> None:
        """party_logo source and placeholder-only key_speakers both forward None,
        even though each is individually paired with an otherwise-activating input.
        """
        dag_mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        domain_cfg = {"styles": []}
        mocker.patch.object(dag_mod, "get_domain_config", return_value=domain_cfg)

        for conf, photo_data in (
            (_FAKE_CONF_WITH_SPEAKERS, {"source": "party_logo"}),
            (_FAKE_CONF_WITH_PLACEHOLDER_SPEAKERS, {"source": "photo"}),
        ):
            ti = _make_fake_ti(
                {
                    "validate_input": conf,
                    "fetch_recent_history": None,
                    "resolve_participant_photo": photo_data,
                }
            )
            art_direct = mocker.patch.object(
                dag_mod, "art_direct", return_value={"text": "BRIEF"}
            )
            dag_mod._task_art_direction(ti)
            _, kwargs = art_direct.call_args
            assert kwargs["resolved_speaker_name"] is None


class TestDagTaskIds:
    """T-03: DAG must contain exactly the expected task IDs — no more, no fewer."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

    def test_exact_task_id_set(self) -> None:
        dag = self.mod.dag
        actual_ids = {task.task_id for task in dag.tasks}
        assert actual_ids == EXPECTED_TASK_IDS, (
            f"Task ID mismatch.\n"
            f"  Extra: {actual_ids - EXPECTED_TASK_IDS}\n"
            f"  Missing: {EXPECTED_TASK_IDS - actual_ids}"
        )


# ---------------------------------------------------------------------------
# T-04  Task dependency graph shape
# ---------------------------------------------------------------------------


class TestDagDependencies:
    """T-04: Verify the exact upstream sets for key DAG tasks."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        self.dag = self.mod.dag

    def _upstream_ids(self, task_id: str) -> set[str]:
        task = self.dag.get_task(task_id)
        return {t.task_id for t in task.upstream_list}

    def test_resolve_participant_photo_upstream_is_validate_input(self) -> None:
        assert self._upstream_ids("resolve_participant_photo") == {"validate_input"}

    def test_art_direction_upstream_is_resolve_participant_photo_and_fetch_history(self) -> None:
        assert self._upstream_ids("art_direction") == {
            "resolve_participant_photo",
            "fetch_recent_history",
        }

    def test_generate_thumbnail_option_a_upstream_is_art_direction(self) -> None:
        assert self._upstream_ids("generate_thumbnail_option_a") == {"art_direction"}

    def test_generate_thumbnail_option_b_upstream_is_art_direction_retry(self) -> None:
        assert self._upstream_ids("generate_thumbnail_option_b") == {"art_direction_retry"}

    def test_download_option_a_upstream_is_generate_thumbnail_option_a(self) -> None:
        assert self._upstream_ids("download_option_a") == {"generate_thumbnail_option_a"}

    def test_download_option_b_upstream_is_generate_thumbnail_option_b(self) -> None:
        assert self._upstream_ids("download_option_b") == {"generate_thumbnail_option_b"}

    def test_score_option_a_upstream_is_download_option_a(self) -> None:
        assert self._upstream_ids("score_option_a") == {"download_option_a"}

    def test_score_option_b_upstream_is_download_option_b(self) -> None:
        assert self._upstream_ids("score_option_b") == {"download_option_b"}

    def test_check_score_threshold_upstream_is_score_option_a(self) -> None:
        assert self._upstream_ids("check_score_threshold") == {"score_option_a"}

    def test_art_direction_retry_upstream_is_check_score_threshold_and_fetch_history(self) -> None:
        assert self._upstream_ids("art_direction_retry") == {
            "check_score_threshold",
            "fetch_recent_history",
        }

    def test_choose_best_option_upstream_is_check_score_threshold_and_score_option_b(self) -> None:
        assert self._upstream_ids("choose_best_option") == {"check_score_threshold", "score_option_b"}

    def test_generate_title_upstream_is_choose_best_option_and_fetch_history(self) -> None:
        assert self._upstream_ids("generate_title") == {
            "choose_best_option",
            "fetch_recent_history",
        }

    def test_persist_results_upstream_is_generate_title(self) -> None:
        assert self._upstream_ids("persist_results") == {"generate_title"}

    def test_thumbnail_result_upstream_is_persist_results(self) -> None:
        assert self._upstream_ids("thumbnail_result") == {"persist_results"}


# ---------------------------------------------------------------------------
# T-05  No Congreso-specific string literals in DAG source
# ---------------------------------------------------------------------------


class TestNoCongresoBranding:
    """T-05: DAG source must not contain Congreso-specific literals outside imports."""

    def test_no_congreso_literals_in_dag_source(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod
        source_path = dag_mod.__file__
        assert source_path is not None, "Module has no __file__"
        with open(source_path, encoding="utf-8") as fh:
            source = fh.read()

        # Strip import lines to allow 'congress_videos' package paths in import statements.
        non_import_lines = [
            line for line in source.splitlines()
            if not re.match(r"\s*(import|from)\s+", line)
        ]
        body = "\n".join(non_import_lines)

        forbidden = re.compile(r'\b(congreso|diputado|CONGRESO)\b', re.IGNORECASE)
        matches = forbidden.findall(body)
        assert not matches, (
            f"Found Congreso-specific literals in DAG task logic: {matches}"
        )


# ---------------------------------------------------------------------------
# T-06  validate_input callable behaviour
# ---------------------------------------------------------------------------


_REQUIRED_KEYS = [
    "youtube_video_id",
    "chapter_id",
    "debate_summary",
    "session",
    "domain",
]

VALID_CONF = {
    "youtube_video_id": "abc123",
    "chapter_id": 7,
    "debate_summary": "El debate sobre pensiones fue intenso.",
    "session": "Pleno 2026-06-10",
    "domain": "congreso",
    "slug": "garcia-lopez-maria",
}


class TestValidateInput:
    """T-06: validate_input must accept valid conf and reject missing/empty keys."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

    def test_valid_conf_does_not_raise(self) -> None:
        """All required keys and an optional participant slug are accepted."""
        # Call the underlying function directly (not via Airflow trigger).
        self.mod.validate_input(VALID_CONF)

    @pytest.mark.parametrize("missing_key", _REQUIRED_KEYS)
    def test_missing_key_raises_value_error(self, missing_key: str) -> None:
        """Omitting any required key must raise ValueError."""
        conf = {k: v for k, v in VALID_CONF.items() if k != missing_key}
        with pytest.raises(ValueError, match=missing_key):
            self.mod.validate_input(conf)

    @pytest.mark.parametrize("empty_key", _REQUIRED_KEYS)
    def test_empty_string_raises_value_error(self, empty_key: str) -> None:
        """Empty string for any required key must raise ValueError."""
        conf = {**VALID_CONF, empty_key: ""}
        with pytest.raises(ValueError, match=empty_key):
            self.mod.validate_input(conf)

    def test_conf_without_slug_is_accepted(self) -> None:
        """A generic thumbnail can be generated without a participant photo."""
        conf = {k: v for k, v in VALID_CONF.items() if k != "slug"}
        self.mod.validate_input(conf)

    def test_conf_with_slug_is_accepted(self) -> None:
        """conf that includes slug= is accepted normally."""
        conf = {**VALID_CONF, "slug": "garcia-lopez-maria"}
        self.mod.validate_input(conf)  # must not raise

    def test_conf_with_none_slug_is_accepted(self) -> None:
        """An unresolved participant slug does not block generic generation."""
        conf = {**VALID_CONF, "slug": None}
        self.mod.validate_input(conf)

    def test_no_normalized_name_key_in_required_keys(self) -> None:
        """'normalized_name' must NOT appear in _REQUIRED_CONF_KEYS."""
        assert "normalized_name" not in self.mod._REQUIRED_CONF_KEYS


# ---------------------------------------------------------------------------
# T-07  TRIANGULATE — PIKZELS_API_KEY absent, ConfigError on unknown domain
# ---------------------------------------------------------------------------


class TestTriangulate:
    """T-07: Edge cases — missing API key and unknown domain."""

    def test_pikzels_api_key_absent_raises_environment_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When PIKZELS_API_KEY is absent, PikzelsClient construction raises EnvironmentError."""
        # Remove key from environment; force fresh module import.
        monkeypatch.delenv("PIKZELS_API_KEY", raising=False)

        # Clear cached pikzels_client so it re-reads env on import.
        for mod_name in list(sys.modules.keys()):
            if "pikzels_client" in mod_name:
                del sys.modules[mod_name]

        with pytest.raises(EnvironmentError, match="PIKZELS_API_KEY"):
            from congress_videos.modules import pikzels_client as _pc  # noqa: F401
            _pc.PikzelsClient()

    def test_config_error_on_unknown_domain(self) -> None:
        """validate_input with an unknown domain raises ConfigError via get_domain_config."""
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

        from congress_videos.config.thumbnail_config import ConfigError

        conf = {**VALID_CONF, "domain": "unknown_domain_xyz"}
        # The DAG's validate_input must call get_domain_config and propagate ConfigError.
        with pytest.raises(ConfigError):
            mod.validate_input(conf)


# ---------------------------------------------------------------------------
# Helpers shared by T-08 / T-09 / T-10
# ---------------------------------------------------------------------------

_FAKE_CONF = {
    "youtube_video_id": "vid_test_001",
    "chapter_id": 42,
    "debate_summary": "El Congreso debate el presupuesto general.",
    "session": "Pleno 2026-07-31",
    "domain": "congreso",
    "slug": "garcia-lopez-maria",
}

# Issue #279: fixtures for the resolved-photo art-direction gating tests.
_FAKE_CONF_WITH_SPEAKERS = {
    **_FAKE_CONF,
    "key_speakers": ["Cervera Pinar"],
}

_FAKE_CONF_WITH_PLACEHOLDER_SPEAKERS = {
    **_FAKE_CONF,
    "key_speakers": ["Interviniente no identificado"],
}

_FAKE_STYLES = [
    {
        "label": "option_a",
        "layout": "A",
    },
    {
        "label": "option_b",
        "layout": "B",
    },
]

_FAKE_DOMAIN_CFG = {
    "styles": _FAKE_STYLES,
    "participants_lookup": lambda slug: None,
    "party_logo_map": None,
}

# Art-direction brief returned by the art_direction task xcom.
_FAKE_ART_BRIEF = {
    "text": "LO QUE NO TE CUENTAN",
    "background": "una calle española con gente, luz de tarde",
    "person": "ciudadano de mediana edad, expresión preocupada, ropa casual",
    "mood": "tensión y curiosidad",
    "logo": "",
}


def _make_fake_ti(xcom_map: dict) -> MagicMock:
    """Return a TaskInstance double whose xcom_pull dispatches by task_ids kwarg."""
    ti = MagicMock(name="TaskInstance")

    def _pull(task_ids=None, key="return_value", **_kw):  # noqa: ANN001
        return xcom_map.get(task_ids)

    ti.xcom_pull.side_effect = _pull
    return ti


# ---------------------------------------------------------------------------
# Cross-DAG result contract
# ---------------------------------------------------------------------------


class TestTaskThumbnailResult:
    def test_returns_uploadable_result_after_persistence(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "choose_best_option": {"local_path": "/thumbnails/42/option_a.png"},
                "generate_title": "A title for upload",
            }
        )

        assert dag_mod._task_thumbnail_result(ti) == {
            "chapter_id": 42,
            "success": True,
            "output_path": "/thumbnails/42/option_a.png",
            "title": "A title for upload",
        }


# ---------------------------------------------------------------------------
# T-08: _task_generate_thumbnail callable (W-01 backfill)
# ---------------------------------------------------------------------------


class TestTaskGenerateThumbnail:
    """T-08: _task_generate_thumbnail must call _pkz.thumbnail_from_text with
    support_image_base64= (data URI), pull from art_direction xcom, return a dict
    with label, style (layout letter A/B/C), prompt (rendered Pikzels template
    containing #C9A84C), and NO persona key.
    """

    def test_calls_thumbnail_from_text_with_support_image_base64(self, mocker) -> None:
        """thumbnail_from_text must receive support_image_base64= as a data URI."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        photo_b64 = "abc123base64=="
        fake_photo_data = {"support_image_b64": photo_b64, "source": "photo"}

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "resolve_participant_photo": fake_photo_data,
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_pkz.thumbnail_from_text.return_value = {"output": "https://pikzels.com/out.png"}

        mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_domain_config",
            return_value=_FAKE_DOMAIN_CFG,
        )

        dag_mod._task_generate_thumbnail("option_a", ti=ti)

        mock_pkz.thumbnail_from_text.assert_called_once()
        call_kwargs = mock_pkz.thumbnail_from_text.call_args[1]

        # Must use support_image_base64=, not support_image=
        assert "support_image_base64" in call_kwargs, (
            "thumbnail_from_text must be called with support_image_base64= kwarg"
        )
        assert "support_image" not in call_kwargs or "support_image_base64" in call_kwargs

        data_url: str = call_kwargs["support_image_base64"]
        assert data_url.startswith("data:image/jpeg;base64,"), (
            f"support_image_base64 must be a data URI, got: {data_url[:60]!r}"
        )
        assert photo_b64 in data_url

    def test_returns_dict_with_label_and_style_as_layout_letter(self, mocker) -> None:
        """Return dict must carry label and style (layout letter A/B/C), no 'persona' key."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "resolve_participant_photo": {"support_image_b64": "b64data", "source": "photo"},
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_pkz.thumbnail_from_text.return_value = {"output": "https://pikzels.com/out.png"}

        mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_domain_config",
            return_value=_FAKE_DOMAIN_CFG,
        )

        result = dag_mod._task_generate_thumbnail("option_a", ti=ti)

        assert result["label"] == "option_a"
        # style must be the layout letter, not the old style prose
        assert result["style"] in {"A", "B", "C"}, (
            f"style must be a layout letter (A/B/C), got: {result['style']!r}"
        )
        # prompt must be a rendered Pikzels template containing the gold border spec
        assert "#C9A84C" in result["prompt"], (
            "prompt must be the rendered Pikzels template containing '#C9A84C'"
        )
        # No persona key expected
        assert "persona" not in result, (
            "result must not contain 'persona' key — removed in this change"
        )

    def test_no_photo_sends_none_support_image(self, mocker) -> None:
        """When support_image_b64 is empty, support_image_base64 is None."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "resolve_participant_photo": {"support_image_b64": "", "source": "party_logo"},
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_pkz.thumbnail_from_text.return_value = {"output": "https://pikzels.com/out.png"}

        mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_domain_config",
            return_value=_FAKE_DOMAIN_CFG,
        )

        dag_mod._task_generate_thumbnail("option_a", ti=ti)

        call_kwargs = mock_pkz.thumbnail_from_text.call_args[1]
        assert call_kwargs.get("support_image_base64") is None


# ---------------------------------------------------------------------------
# T-09: _task_download_option callable (W-01 backfill)
# ---------------------------------------------------------------------------


class TestTaskDownloadOption:
    """T-09: _task_download_option must call _pkz.download with the correct
    local path ({youtube_video_id}/{label}.png) and return a dict with label,
    local_path, and output_url.
    """

    def test_download_called_with_correct_path(self, mocker) -> None:
        """_pkz.download must be called with output_url and the label.png local path."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        gen_result = {
            "output": "https://pikzels.com/generated.png",
            "label": "option_a",
            "style": "A",
            "prompt": "A thick gold (#C9A84C) border...",
        }

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "generate_thumbnail_option_a": gen_result,
            }
        )

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")

        mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_thumbnail_dir",
            side_effect=lambda vid_id: __import__("pathlib").Path(f"/thumbnails/{vid_id}"),
        )

        result = dag_mod._task_download_option("option_a", ti=ti)

        mock_pkz.download.assert_called_once()
        call_args = mock_pkz.download.call_args[0]
        downloaded_url, local_path_str = call_args[0], call_args[1]

        assert downloaded_url == "https://pikzels.com/generated.png"
        assert "option_a.png" in local_path_str
        assert _FAKE_CONF["youtube_video_id"] in local_path_str

    def test_returns_dict_with_required_keys(self, mocker) -> None:
        """Return dict must contain label, local_path, and output_url."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        gen_result = {
            "output": "https://pikzels.com/generated.png",
            "style": "A",
            "prompt": "A thick gold (#C9A84C) border...",
        }

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "generate_thumbnail_option_a": gen_result,
            }
        )

        mocker.patch.object(dag_mod, "_pkz")
        mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_thumbnail_dir",
            side_effect=lambda vid_id: __import__("pathlib").Path(f"/thumbnails/{vid_id}"),
        )

        result = dag_mod._task_download_option("option_a", ti=ti)

        assert "label" in result
        assert result["label"] == "option_a"
        assert "local_path" in result
        assert "output_url" in result
        assert result["output_url"] == "https://pikzels.com/generated.png"


# ---------------------------------------------------------------------------
# T-10: _task_score_option callable (W-01 backfill)
# ---------------------------------------------------------------------------


class TestTaskScoreOption:
    """T-10: _task_score_option must read the local file, call
    _pkz.to_base64_data_url and then _pkz.score_thumbnail(image_base64=...),
    and return a dict with main_score equal to the value from score_thumbnail.
    """

    def test_score_thumbnail_called_with_image_base64_kwarg(self, mocker, tmp_path) -> None:
        """score_thumbnail must be called with image_base64= kwarg, not a file path."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        # Create a real temp file so Path.read_bytes() succeeds.
        fake_image = tmp_path / "option_a.png"
        fake_image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)

        download_info = {
            "label": "option_a",
            "local_path": str(fake_image),
            "output_url": "https://pikzels.com/generated.png",
            "style": "A",
            "prompt": "A thick gold (#C9A84C) border...",
        }

        ti = _make_fake_ti({"download_option_a": download_info})

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_pkz.to_base64_data_url.return_value = "data:image/png;base64,FAKEENCODED=="
        mock_pkz.score_thumbnail.return_value = {"main_score": 77.0}

        dag_mod._task_score_option("option_a", ti=ti)

        mock_pkz.score_thumbnail.assert_called_once()
        score_kwargs = mock_pkz.score_thumbnail.call_args[1]
        assert "image_base64" in score_kwargs, (
            "score_thumbnail must be called with image_base64= kwarg"
        )
        assert score_kwargs["image_base64"] == "data:image/png;base64,FAKEENCODED=="

    def test_main_score_extracted_from_score_result_dict(self, mocker, tmp_path) -> None:
        """main_score in return dict must come from score_thumbnail dict, not raw numeric."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        fake_image = tmp_path / "option_b.png"
        fake_image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)

        download_info = {
            "label": "option_b",
            "local_path": str(fake_image),
            "output_url": "https://pikzels.com/b.png",
            "style": "B",
            "prompt": "A thick gold (#C9A84C) border...",
        }

        ti = _make_fake_ti({"download_option_b": download_info})

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_pkz.to_base64_data_url.return_value = "data:image/png;base64,ENCODED=="
        mock_pkz.score_thumbnail.return_value = {"main_score": 77.0}

        result = dag_mod._task_score_option("option_b", ti=ti)

        assert result["main_score"] == 77.0

    def test_to_base64_data_url_called_with_file_bytes(self, mocker, tmp_path) -> None:
        """to_base64_data_url must be called with the raw bytes read from local_path."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        expected_bytes = b"\x89PNG\r\n\x1a\n" + b"\xab" * 20
        fake_image = tmp_path / "option_a.png"
        fake_image.write_bytes(expected_bytes)

        download_info = {
            "label": "option_a",
            "local_path": str(fake_image),
            "output_url": "https://pikzels.com/a.png",
        }

        ti = _make_fake_ti({"download_option_a": download_info})

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_pkz.to_base64_data_url.return_value = "data:image/png;base64,XYZ="
        mock_pkz.score_thumbnail.return_value = {"main_score": 55.0}

        dag_mod._task_score_option("option_a", ti=ti)

        mock_pkz.to_base64_data_url.assert_called_once_with(expected_bytes, "image/png")


# ---------------------------------------------------------------------------
# D-1 additions: BranchPythonOperator type + trigger_rule assertions
# ---------------------------------------------------------------------------


class TestDagBranchAndTriggerRule:
    """D-1: check_score_threshold must be a BranchPythonOperator;
    choose_best_option must have trigger_rule='none_failed_min_one_success'."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        self.dag = self.mod.dag

    def test_check_score_threshold_is_branch_python_operator(self) -> None:
        """check_score_threshold must be a BranchPythonOperator."""
        from airflow.operators.python import BranchPythonOperator

        task = self.dag.get_task("check_score_threshold")
        assert isinstance(task, BranchPythonOperator), (
            f"check_score_threshold must be BranchPythonOperator, got {type(task).__name__}"
        )

    def test_choose_best_option_trigger_rule_is_none_failed_min_one_success(self) -> None:
        """choose_best_option must use trigger_rule='none_failed_min_one_success'."""
        task = self.dag.get_task("choose_best_option")
        assert task.trigger_rule == "none_failed_min_one_success", (
            f"choose_best_option trigger_rule must be 'none_failed_min_one_success', "
            f"got {task.trigger_rule!r}"
        )


# ---------------------------------------------------------------------------
# D-2: TestTaskCheckScoreThreshold — unit tests for branch callable
# ---------------------------------------------------------------------------


class TestTaskCheckScoreThreshold:
    """D-2: _task_check_score_threshold returns correct branch task_id per score."""

    def _make_ti(self, score: float) -> MagicMock:
        score_result = {"main_score": score, "label": "option_a", "style": "A"}
        return _make_fake_ti({"score_option_a": score_result, "validate_input": _FAKE_CONF})

    def test_score_below_threshold_returns_retry(self, mocker) -> None:
        """Score 40 < 60 → returns 'art_direction_retry'."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(
            dag_mod, "get_domain_config", return_value={**_FAKE_DOMAIN_CFG, "score_retry_threshold": 60}
        )
        ti = self._make_ti(40.0)
        result = dag_mod._task_check_score_threshold(ti)
        assert result == "art_direction_retry"

    def test_score_equal_threshold_returns_fast_path(self, mocker) -> None:
        """Score 60 == 60 → returns 'choose_best_option' (fast path)."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(
            dag_mod, "get_domain_config", return_value={**_FAKE_DOMAIN_CFG, "score_retry_threshold": 60}
        )
        ti = self._make_ti(60.0)
        result = dag_mod._task_check_score_threshold(ti)
        assert result == "choose_best_option"

    def test_score_above_threshold_returns_fast_path(self, mocker) -> None:
        """Score 75 > 60 → returns 'choose_best_option' (fast path)."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(
            dag_mod, "get_domain_config", return_value={**_FAKE_DOMAIN_CFG, "score_retry_threshold": 60}
        )
        ti = self._make_ti(75.0)
        result = dag_mod._task_check_score_threshold(ti)
        assert result == "choose_best_option"

    def test_domain_override_threshold_75_score_70_triggers_retry(self, mocker) -> None:
        """Domain threshold 75, score 70 < 75 → retry."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(
            dag_mod, "get_domain_config", return_value={**_FAKE_DOMAIN_CFG, "score_retry_threshold": 75}
        )
        ti = self._make_ti(70.0)
        result = dag_mod._task_check_score_threshold(ti)
        assert result == "art_direction_retry"

    def test_domain_override_threshold_75_score_75_fast_path(self, mocker) -> None:
        """Domain threshold 75, score 75 == 75 → fast path."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(
            dag_mod, "get_domain_config", return_value={**_FAKE_DOMAIN_CFG, "score_retry_threshold": 75}
        )
        ti = self._make_ti(75.0)
        result = dag_mod._task_check_score_threshold(ti)
        assert result == "choose_best_option"

    def test_missing_main_score_defaults_to_zero_triggers_retry(self, mocker) -> None:
        """When score_option_a has no main_score, defaults to 0.0 → retry."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(
            dag_mod, "get_domain_config", return_value={**_FAKE_DOMAIN_CFG, "score_retry_threshold": 60}
        )
        ti = _make_fake_ti({"score_option_a": {}, "validate_input": _FAKE_CONF})
        result = dag_mod._task_check_score_threshold(ti)
        assert result == "art_direction_retry"


# ---------------------------------------------------------------------------
# D-3: TestTaskArtDirectionRetry — unit tests for art_direction_retry callable
# ---------------------------------------------------------------------------


class TestTaskArtDirectionRetry:
    """D-3: _task_art_direction_retry pulls from task_ids='art_direction' (not self)
    and calls art_direct with previous_brief set.
    """

    def test_pulls_from_art_direction_not_self(self, mocker) -> None:
        """_task_art_direction_retry must pull previous_brief from task_ids='art_direction'."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(dag_mod, "art_direct", return_value={"text": "NUEVO"})

        dag_mod._task_art_direction_retry(ti)

        # Verify pull was from 'art_direction', not 'art_direction_retry'
        pull_calls = [str(c) for c in ti.xcom_pull.call_args_list]
        assert any("art_direction" in c and "art_direction_retry" not in c for c in pull_calls), (
            "_task_art_direction_retry must pull from task_ids='art_direction', not 'art_direction_retry'"
        )

    def test_calls_art_direct_with_previous_brief(self, mocker) -> None:
        """art_direct must be called with previous_brief=<the original brief>."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(dag_mod, "art_direct", return_value={"text": "NUEVO"})

        dag_mod._task_art_direction_retry(ti)

        mock_art_direct.assert_called_once()
        _, kwargs = mock_art_direct.call_args
        assert "previous_brief" in kwargs, "art_direct must be called with previous_brief= kwarg"
        assert kwargs["previous_brief"] == _FAKE_ART_BRIEF

    def test_returns_art_direct_result(self, mocker) -> None:
        """_task_art_direction_retry must return the result of art_direct."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        expected = {"text": "REINTENTO", "background": "plaza", "person": "joven", "mood": "urgencia"}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mocker.patch.object(dag_mod, "art_direct", return_value=expected)

        result = dag_mod._task_art_direction_retry(ti)
        assert result == expected

    def test_photo_source_and_real_speaker_forwards_name(self, mocker) -> None:
        """Issue #279: retry task applies the same resolved-photo gating as the primary path."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF_WITH_SPEAKERS,
                "art_direction": _FAKE_ART_BRIEF,
                "fetch_recent_history": None,
                "resolve_participant_photo": {"source": "photo"},
            }
        )
        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(
            dag_mod, "art_direct", return_value={"text": "NUEVO"}
        )

        dag_mod._task_art_direction_retry(ti)

        _, kwargs = mock_art_direct.call_args
        assert kwargs["resolved_speaker_name"] == "Cervera Pinar"

    def test_non_activating_gate_inputs_forward_none(self, mocker) -> None:
        """Issue #279: party_logo source and placeholder-only key_speakers both
        forward None on the retry path, matching the primary-path gating."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)

        for conf, photo_data in (
            (_FAKE_CONF_WITH_SPEAKERS, {"source": "party_logo"}),
            (_FAKE_CONF_WITH_PLACEHOLDER_SPEAKERS, {"source": "photo"}),
        ):
            ti = _make_fake_ti(
                {
                    "validate_input": conf,
                    "art_direction": _FAKE_ART_BRIEF,
                    "fetch_recent_history": None,
                    "resolve_participant_photo": photo_data,
                }
            )
            mock_art_direct = mocker.patch.object(
                dag_mod, "art_direct", return_value={"text": "NUEVO"}
            )
            dag_mod._task_art_direction_retry(ti)
            _, kwargs = mock_art_direct.call_args
            assert kwargs["resolved_speaker_name"] is None


# ---------------------------------------------------------------------------
# D-4: TestTaskChooseBestEmptyFilter + TestTaskPersistResultsEmptyFilter
# ---------------------------------------------------------------------------


class TestTaskChooseBestEmptyFilter:
    """D-4: _task_choose_best filters out empty option_b when it is {} (fast path)."""

    def test_empty_option_b_is_filtered_single_item_list(self, mocker) -> None:
        """When option_b is {}, choose_best_option is called with [option_a] only."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        option_a = {"label": "option_a", "main_score": 72.0, "output_url": "u", "local_path": "/a.png", "style": "A"}
        ti = _make_fake_ti(
            {
                "score_option_a": option_a,
                "score_option_b": {},
            }
        )
        mock_choose = mocker.patch.object(dag_mod, "choose_best_option", return_value={**option_a, "is_chosen": True})

        dag_mod._task_choose_best(ti)

        mock_choose.assert_called_once()
        options_arg = mock_choose.call_args[0][0]
        assert len(options_arg) == 1, "choose_best_option must be called with 1 item when option_b is {}"
        assert options_arg[0]["label"] == "option_a"

    def test_both_options_present_passes_two_items(self, mocker) -> None:
        """When both options are non-empty, choose_best_option gets 2 items."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        option_a = {"label": "option_a", "main_score": 72.0, "output_url": "u", "local_path": "/a.png", "style": "A"}
        option_b = {"label": "option_b", "main_score": 85.0, "output_url": "u", "local_path": "/b.png", "style": "B"}
        ti = _make_fake_ti({"score_option_a": option_a, "score_option_b": option_b})

        mock_choose = mocker.patch.object(dag_mod, "choose_best_option", return_value={**option_b, "is_chosen": True})
        dag_mod._task_choose_best(ti)

        options_arg = mock_choose.call_args[0][0]
        assert len(options_arg) == 2


class TestTaskPersistResultsEmptyFilter:
    """D-4: _task_persist_results filters out empty option_b when it is {} (fast path)."""

    def test_empty_option_b_persists_one_row(self, mocker) -> None:
        """When option_b is {}, persist_results is called with options=[option_a] only."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        option_a = {"label": "option_a", "main_score": 72.0, "output_url": "u", "local_path": "/a.png", "style": "A"}
        best = {**option_a, "is_chosen": True}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "score_option_a": option_a,
                "score_option_b": {},
                "choose_best_option": best,
                "generate_title": "El debate",
            }
        )

        mock_persist = mocker.patch.object(dag_mod, "persist_results")
        dag_mod._task_persist_results(ti)

        mock_persist.assert_called_once()
        call_kwargs = mock_persist.call_args[1]
        options_passed = call_kwargs.get("options", mock_persist.call_args[0][3] if mock_persist.call_args[0] else None)
        # options could be positional — inspect via call_args
        all_args = mock_persist.call_args
        # Find the options list from either positional or keyword args
        options_list = None
        if all_args.kwargs.get("options") is not None:
            options_list = all_args.kwargs["options"]
        elif len(all_args.args) >= 4:
            options_list = all_args.args[3]
        assert options_list is not None, "persist_results must be called with options argument"
        assert len(options_list) == 1, f"persist_results options must have 1 item, got {len(options_list)}"
        assert options_list[0]["label"] == "option_a"


class TestTitleXComNoneNotEmptyString:
    """Issue #317 (G3, E-1/E-2): a missing generate_title XCom must stay
    ``None`` through persist_results and _task_thumbnail_result, never
    coerced to ``""`` — the coercion is what made a raised/blank title
    indistinguishable from "not run yet"."""

    def test_persist_results_called_with_title_none(self, mocker) -> None:
        """E-1: generate_title XCom None -> persist_results(title=None)."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        option_a = {"label": "option_a", "main_score": 72.0, "output_url": "u", "local_path": "/a.png", "style": "A"}
        best = {**option_a, "is_chosen": True}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "score_option_a": option_a,
                "score_option_b": {},
                "choose_best_option": best,
                "generate_title": None,
            }
        )

        mock_persist = mocker.patch.object(dag_mod, "persist_results")
        dag_mod._task_persist_results(ti)

        call_kwargs = mock_persist.call_args.kwargs
        assert call_kwargs.get("title") is None

    def test_task_thumbnail_result_title_none(self) -> None:
        """E-2: _task_thumbnail_result returns {"title": None}, not "" ."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "choose_best_option": {"local_path": "/thumbnails/42/option_a.png"},
                "generate_title": None,
            }
        )

        result = dag_mod._task_thumbnail_result(ti)

        assert result["title"] is None


# ---------------------------------------------------------------------------
# Phase 6: fetch_recent_history DAG task wiring
# ---------------------------------------------------------------------------


class TestFetchRecentHistoryDagTask:
    """Phase 6: DAG must include fetch_recent_history as a root pre-task wired to
    art_direction, art_direction_retry, and generate_title.
    """

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        self.dag = self.mod.dag

    def _upstream_ids(self, task_id: str) -> set[str]:
        task = self.dag.get_task(task_id)
        return {t.task_id for t in task.upstream_list}

    def test_dag_has_fetch_recent_history_task(self) -> None:
        """DAG must contain a task with task_id='fetch_recent_history'."""
        task_ids = {task.task_id for task in self.dag.tasks}
        assert "fetch_recent_history" in task_ids, (
            f"DAG must have 'fetch_recent_history' task, got: {task_ids}"
        )

    def test_fetch_recent_history_is_upstream_of_art_direction(self) -> None:
        """fetch_recent_history must be in the upstream set of art_direction."""
        upstream = self._upstream_ids("art_direction")
        assert "fetch_recent_history" in upstream, (
            f"art_direction upstream must include 'fetch_recent_history', got: {upstream}"
        )

    def test_fetch_recent_history_is_upstream_of_art_direction_retry(self) -> None:
        """fetch_recent_history must be in the upstream set of art_direction_retry."""
        upstream = self._upstream_ids("art_direction_retry")
        assert "fetch_recent_history" in upstream, (
            f"art_direction_retry upstream must include 'fetch_recent_history', got: {upstream}"
        )

    def test_fetch_recent_history_is_upstream_of_generate_title(self) -> None:
        """fetch_recent_history must be in the upstream set of generate_title."""
        upstream = self._upstream_ids("generate_title")
        assert "fetch_recent_history" in upstream, (
            f"generate_title upstream must include 'fetch_recent_history', got: {upstream}"
        )

    def test_fetch_recent_history_callable_returns_briefs_and_titles(self, mocker) -> None:
        """_task_fetch_recent_history must call fetch_recent_thumbnail_history and
        push {briefs: [...], titles: [...]} via XCom return value."""
        dag_mod = self.mod

        mocker.patch.object(
            dag_mod,
            "fetch_recent_thumbnail_history",
            return_value=(["brief A", "brief B"], ["Title A"]),
        )

        ti = MagicMock(name="TaskInstance")
        result = dag_mod._task_fetch_recent_history(ti)

        assert result == {"briefs": ["brief A", "brief B"], "titles": ["Title A"]}, (
            f"_task_fetch_recent_history must return {{briefs: [...], titles: [...]}}, got: {result}"
        )

    def test_task_art_direction_passes_sibling_briefs(self, mocker) -> None:
        """_task_art_direction must pull fetch_recent_history XCom and pass sibling_briefs."""
        dag_mod = self.mod

        history_data = {"briefs": ["brief A sobre mercado"], "titles": ["Title A"]}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "fetch_recent_history": history_data,
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(dag_mod, "art_direct", return_value=_FAKE_ART_BRIEF)

        dag_mod._task_art_direction(ti)

        mock_art_direct.assert_called_once()
        _, kwargs = mock_art_direct.call_args
        assert "sibling_briefs" in kwargs, (
            "_task_art_direction must pass sibling_briefs= to art_direct"
        )
        assert kwargs["sibling_briefs"] == ["brief A sobre mercado"]

    def test_task_generate_title_passes_sibling_titles(self, mocker) -> None:
        """_task_generate_title must pull fetch_recent_history XCom and pass sibling_titles."""
        dag_mod = self.mod

        history_data = {"briefs": ["brief A"], "titles": ["Title X", "Title Y"]}
        best = {"style": "A", "prompt": "mercado", "label": "option_a", "main_score": 77.0}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "choose_best_option": best,
                "fetch_recent_history": history_data,
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_generate_title = mocker.patch.object(dag_mod, "generate_title", return_value="Un título")

        dag_mod._task_generate_title(ti)

        mock_generate_title.assert_called_once()
        _, kwargs = mock_generate_title.call_args
        assert "sibling_titles" in kwargs, (
            "_task_generate_title must pass sibling_titles= to generate_title"
        )
        assert kwargs["sibling_titles"] == ["Title X", "Title Y"]


# ---------------------------------------------------------------------------
# title-news-format Phase 5: key_speakers threading in _task_generate_title
# ---------------------------------------------------------------------------


class TestTaskGenerateTitleKeySpeakers:
    """Phase 5: _task_generate_title must forward key_speakers from XCom conf."""

    def test_key_speakers_forwarded_when_present(self, mocker) -> None:
        """When validate_input conf has key_speakers, _task_generate_title passes them to generate_title."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        conf_with_speakers = {**_FAKE_CONF, "key_speakers": ["Ana Pastor"]}
        best = {"style": "A", "prompt": "debate", "label": "option_a", "main_score": 77.0}
        ti = _make_fake_ti(
            {
                "validate_input": conf_with_speakers,
                "choose_best_option": best,
                "fetch_recent_history": {"briefs": [], "titles": []},
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_generate_title = mocker.patch.object(
            dag_mod, "generate_title", return_value="Un título"
        )

        dag_mod._task_generate_title(ti)

        mock_generate_title.assert_called_once()
        _, kwargs = mock_generate_title.call_args
        assert "key_speakers" in kwargs, (
            "_task_generate_title must pass key_speakers= to generate_title"
        )
        assert kwargs["key_speakers"] == ["Ana Pastor"]

    def test_missing_key_speakers_does_not_raise(self, mocker) -> None:
        """When validate_input conf has no key_speakers key, _task_generate_title succeeds."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        conf_without = {k: v for k, v in _FAKE_CONF.items() if k != "key_speakers"}
        best = {"style": "A", "prompt": "debate", "label": "option_a", "main_score": 77.0}
        ti = _make_fake_ti(
            {
                "validate_input": conf_without,
                "choose_best_option": best,
                "fetch_recent_history": {"briefs": [], "titles": []},
            }
        )

        mocker.patch.object(dag_mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_generate_title = mocker.patch.object(
            dag_mod, "generate_title", return_value="Un título"
        )

        # Must not raise KeyError
        dag_mod._task_generate_title(ti)
        mock_generate_title.assert_called_once()


# ---------------------------------------------------------------------------
# thumbnail-canonical-path (Slice 4a): canonical download path + reconciliation
# ---------------------------------------------------------------------------


class TestTaskDownloadOptionCanonicalPath:
    """_task_download_option must write to dirname(output_path) for turn-type items,
    fall back to get_thumbnail_dir for chapter/standalone items.

    Reconciliation: _task_thumbnail_result must return output_path ending in
    'thumbnail.png' when the canonical write was used.
    """

    def test_turn_conf_writes_to_dirname_of_output_path(self, mocker) -> None:
        """output_path in conf → _pkz.download called with path in dirname(output_path)."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        conf_with_output_path = {
            **_FAKE_CONF,
            "output_path": "/data/oradores/42/video.mp4",
        }
        gen_result = {
            "output": "https://pikzels.com/generated.png",
            "label": "option_a",
            "style": "A",
            "prompt": "A thick gold border...",
        }
        ti = _make_fake_ti(
            {
                "validate_input": conf_with_output_path,
                "generate_thumbnail_option_a": gen_result,
            }
        )

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_get_thumbnail_dir = mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_thumbnail_dir"
        )

        result = dag_mod._task_download_option("option_a", ti=ti)

        # Must use canonical dir, not get_thumbnail_dir
        mock_get_thumbnail_dir.assert_not_called()
        # Must write to dirname(output_path)/option_a.png
        mock_pkz.download.assert_called_once()
        _, local_path_str = mock_pkz.download.call_args[0]
        assert local_path_str.startswith("/data/oradores/42/"), (
            f"Expected path under /data/oradores/42/, got {local_path_str!r}"
        )
        assert local_path_str.endswith("option_a.png"), (
            f"Expected filename option_a.png, got {local_path_str!r}"
        )

    def test_chapter_conf_falls_back_to_get_thumbnail_dir(self, mocker) -> None:
        """No output_path in conf → legacy get_thumbnail_dir is used."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        gen_result = {
            "output": "https://pikzels.com/generated.png",
            "label": "option_a",
            "style": "A",
            "prompt": "A thick gold border...",
        }
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,  # no output_path
                "generate_thumbnail_option_a": gen_result,
            }
        )

        mock_pkz = mocker.patch.object(dag_mod, "_pkz")
        mock_get_thumbnail_dir = mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_thumbnail_dir",
            side_effect=lambda vid_id: __import__("pathlib").Path(f"/thumbnails/{vid_id}"),
        )

        result = dag_mod._task_download_option("option_a", ti=ti)

        mock_get_thumbnail_dir.assert_called_once_with(_FAKE_CONF["youtube_video_id"])
        _, local_path_str = mock_pkz.download.call_args[0]
        assert local_path_str.startswith("/thumbnails/"), (
            f"Expected legacy path under /thumbnails/, got {local_path_str!r}"
        )

    def test_standalone_trigger_without_output_path_completes(self, mocker) -> None:
        """Conf with only required keys and no output_path → calls get_thumbnail_dir."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        standalone_conf = {
            "youtube_video_id": "abc123",
            "chapter_id": 7,
            "debate_summary": "El debate ...",
            "session": "Pleno 2026-06-10",
            "domain": "congreso",
            # No output_path
        }
        gen_result = {
            "output": "https://pikzels.com/generated.png",
            "label": "option_a",
            "style": "A",
            "prompt": "A gold border...",
        }
        ti = _make_fake_ti(
            {
                "validate_input": standalone_conf,
                "generate_thumbnail_option_a": gen_result,
            }
        )

        mocker.patch.object(dag_mod, "_pkz")
        mock_get_thumbnail_dir = mocker.patch(
            "congress_videos.generic_thumbnail_generator_dag.get_thumbnail_dir",
            side_effect=lambda vid_id: __import__("pathlib").Path(f"/thumbnails/{vid_id}"),
        )

        dag_mod._task_download_option("option_a", ti=ti)

        mock_get_thumbnail_dir.assert_called_once_with("abc123")

    def test_grouped_turn_both_write_to_same_anchor_dir(self, mocker) -> None:
        """Two turns with same output_path → both write to the same anchor directory."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        shared_output_path = "/data/oradores/7/video.mp4"
        conf_turn1 = {**_FAKE_CONF, "output_path": shared_output_path}
        conf_turn2 = {**_FAKE_CONF, "output_path": shared_output_path}

        gen_result = {
            "output": "https://pikzels.com/generated.png",
            "label": "option_a",
            "style": "A",
            "prompt": "A gold border...",
        }

        paths_written = []

        def _fake_download(url, path_str):
            paths_written.append(path_str)

        for conf in [conf_turn1, conf_turn2]:
            ti = _make_fake_ti(
                {
                    "validate_input": conf,
                    "generate_thumbnail_option_a": gen_result,
                }
            )
            mock_pkz = mocker.patch.object(dag_mod, "_pkz")
            mock_pkz.download.side_effect = _fake_download
            dag_mod._task_download_option("option_a", ti=ti)

        assert len(paths_written) == 2
        for p in paths_written:
            assert p.startswith("/data/oradores/7/"), (
                f"Both turns must write to /data/oradores/7/, got {p!r}"
            )

    def test_turn_conf_thumbnail_result_basename_is_thumbnail_png(self, mocker) -> None:
        """Reconciliation: when canonical path used, _task_thumbnail_result.output_path ends with thumbnail.png."""
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        conf_with_output_path = {
            **_FAKE_CONF,
            "output_path": "/data/oradores/42/video.mp4",
        }
        # Simulate what _task_download_option returns for the canonical path:
        # local_path = /data/oradores/42/option_a.png
        ti = _make_fake_ti(
            {
                "validate_input": conf_with_output_path,
                "choose_best_option": {"local_path": "/data/oradores/42/option_a.png"},
                "generate_title": "Un título canónico",
            }
        )

        # Patch the file-copy helper so we don't need a real filesystem
        mocker.patch.object(
            dag_mod,
            "_persist_canonical_thumbnail",
            side_effect=lambda best_path, canonical_dir: str(canonical_dir / "thumbnail.png"),
        )

        result = dag_mod._task_thumbnail_result(ti)

        assert result["output_path"].endswith("thumbnail.png"), (
            f"output_path must end with thumbnail.png for turn-type items, got {result['output_path']!r}"
        )
        assert result["output_path"] == "/data/oradores/42/thumbnail.png", (
            f"output_path must be /data/oradores/42/thumbnail.png, got {result['output_path']!r}"
        )


# ---------------------------------------------------------------------------
# Conf passthrough: previous_brief/previous_archetype/previous_title (#102)
#
# The action DAG (video_analytics_actions) triggers this DAG for a
# regeneration with negative-steering conf on the FIRST attempt, not only
# via the internal art_direction_retry path.
# ---------------------------------------------------------------------------


class TestArtDirectionConfPassthrough:
    """Spec: Initial art_direction accepts negative-steering conf."""

    def setup_method(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        self.mod = dag_mod

    def test_initial_art_direction_passes_previous_brief_from_conf(self, mocker) -> None:
        conf_with_previous = {
            **_FAKE_CONF,
            "previous_brief": {"text": "prior brief text", "archetype": "denuncia"},
        }
        ti = _make_fake_ti({"validate_input": conf_with_previous})

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(
            self.mod, "art_direct", return_value=_FAKE_ART_BRIEF
        )

        self.mod._task_art_direction(ti)

        mock_art_direct.assert_called_once()
        _, kwargs = mock_art_direct.call_args
        assert kwargs.get("previous_brief") == {
            "text": "prior brief text",
            "archetype": "denuncia",
        }

    def test_initial_art_direction_passes_previous_archetype_as_forbidden(self, mocker) -> None:
        conf_with_previous = {
            **_FAKE_CONF,
            "previous_archetype": "denuncia",
        }
        ti = _make_fake_ti({"validate_input": conf_with_previous})

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(
            self.mod, "art_direct", return_value=_FAKE_ART_BRIEF
        )

        self.mod._task_art_direction(ti)

        _, kwargs = mock_art_direct.call_args
        assert kwargs.get("forbidden_archetype") == "denuncia"

    def test_no_previous_conf_keys_forbidden_archetype_stays_none(self, mocker) -> None:
        """Backward compatible: conf without previous_* keys never passes a
        forbidden_archetype/previous_brief."""
        ti = _make_fake_ti({"validate_input": _FAKE_CONF})

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_art_direct = mocker.patch.object(
            self.mod, "art_direct", return_value=_FAKE_ART_BRIEF
        )

        self.mod._task_art_direction(ti)

        _, kwargs = mock_art_direct.call_args
        assert kwargs.get("previous_brief") is None
        assert kwargs.get("forbidden_archetype") is None


class TestGenerateTitleConfPassthrough:
    """Spec: 24h title regeneration passes the prior title as negative input."""

    def setup_method(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        self.mod = dag_mod

    def test_previous_title_from_conf_passed_as_forbidden_title(self, mocker) -> None:
        conf_with_previous = {
            **_FAKE_CONF,
            "previous_title": "El Congreso vota el futuro de las pensiones",
        }
        best = {"style": "A", "prompt": "mercado", "label": "option_a", "main_score": 77.0}
        ti = _make_fake_ti(
            {"validate_input": conf_with_previous, "choose_best_option": best}
        )

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_generate_title = mocker.patch.object(
            self.mod, "generate_title", return_value="Un título nuevo"
        )

        self.mod._task_generate_title(ti)

        _, kwargs = mock_generate_title.call_args
        assert kwargs.get("forbidden_title") == "El Congreso vota el futuro de las pensiones"

    def test_no_previous_title_forbidden_title_stays_none(self, mocker) -> None:
        best = {"style": "A", "prompt": "mercado", "label": "option_a", "main_score": 77.0}
        ti = _make_fake_ti({"validate_input": _FAKE_CONF, "choose_best_option": best})

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mock_generate_title = mocker.patch.object(
            self.mod, "generate_title", return_value="Un título"
        )

        self.mod._task_generate_title(ti)

        _, kwargs = mock_generate_title.call_args
        assert kwargs.get("forbidden_title") is None


class TestArchetypeCarriedOnOptionDict:
    """Spec: Archetype persisted on chosen thumbnail — carried end to end
    through _task_generate_thumbnail -> download -> score -> persist_results."""

    def setup_method(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        self.mod = dag_mod

    def test_generate_thumbnail_attaches_archetype_from_brief(self, mocker) -> None:
        brief_with_archetype = {**_FAKE_ART_BRIEF, "archetype": "monologo"}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "resolve_participant_photo": {},
                "art_direction": brief_with_archetype,
            }
        )

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mocker.patch.object(self.mod, "build_pikzels_prompt", return_value="prompt text")
        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.thumbnail_from_text.return_value = {"output": "https://pikzels/a.png"}

        result = self.mod._task_generate_thumbnail("option_a", ti)

        assert result["archetype"] == "monologo"

    def test_download_option_carries_archetype_from_generate_result(self, mocker, tmp_path) -> None:
        gen_result = {
            "label": "option_a",
            "style": "A",
            "prompt": "p",
            "output": "https://pikzels/a.png",
            "archetype": "careo",
        }
        conf = {**_FAKE_CONF, "output_path": str(tmp_path / "video.mp4")}
        ti = _make_fake_ti(
            {"validate_input": conf, "generate_thumbnail_option_a": gen_result}
        )

        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.download.return_value = None

        result = self.mod._task_download_option("option_a", ti)

        assert result["archetype"] == "careo"

    def test_score_option_preserves_archetype_via_spread(self, mocker, tmp_path) -> None:
        thumb_file = tmp_path / "option_a.png"
        thumb_file.write_bytes(b"\x89PNG" * 10)
        download_info = {
            "label": "option_a",
            "local_path": str(thumb_file),
            "output_url": "u",
            "style": "A",
            "prompt": "p",
            "archetype": "anuncio",
        }
        ti = _make_fake_ti({"download_option_a": download_info})

        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.to_base64_data_url.return_value = "data:image/png;base64,xx"
        mock_pkz.score_thumbnail.return_value = {"main_score": 90.0}

        result = self.mod._task_score_option("option_a", ti)

        assert result["archetype"] == "anuncio"


class TestArtDirectionBriefCarriedOnOptionDict:
    """Spec: Art direction brief persisted per option (migration 043, #292) —
    carried end to end through _task_generate_thumbnail -> download -> score,
    using each option's own brief source (art_direction / art_direction_retry)."""

    def setup_method(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod

        self.mod = dag_mod

    def test_option_a_result_carries_art_direction_brief_verbatim(self, mocker) -> None:
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "resolve_participant_photo": {},
                "art_direction": _FAKE_ART_BRIEF,
            }
        )

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mocker.patch.object(self.mod, "build_pikzels_prompt", return_value="prompt text")
        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.thumbnail_from_text.return_value = {"output": "https://pikzels/a.png"}

        result = self.mod._task_generate_thumbnail("option_a", ti)

        assert result["art_direction_brief"] == _FAKE_ART_BRIEF

    def test_option_b_result_carries_art_direction_retry_brief_verbatim(
        self, mocker
    ) -> None:
        retry_brief = {**_FAKE_ART_BRIEF, "text": "otro enfoque"}
        ti = _make_fake_ti(
            {
                "validate_input": _FAKE_CONF,
                "resolve_participant_photo": {},
                "art_direction_retry": retry_brief,
            }
        )

        mocker.patch.object(self.mod, "get_domain_config", return_value=_FAKE_DOMAIN_CFG)
        mocker.patch.object(self.mod, "build_pikzels_prompt", return_value="prompt text")
        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.thumbnail_from_text.return_value = {"output": "https://pikzels/b.png"}

        result = self.mod._task_generate_thumbnail("option_b", ti)

        assert result["art_direction_brief"] == retry_brief
        assert result["art_direction_brief"] != _FAKE_ART_BRIEF

    def test_download_option_carries_art_direction_brief_from_generate_result(
        self, mocker, tmp_path
    ) -> None:
        gen_result = {
            "label": "option_a",
            "style": "A",
            "prompt": "p",
            "output": "https://pikzels/a.png",
            "art_direction_brief": _FAKE_ART_BRIEF,
        }
        conf = {**_FAKE_CONF, "output_path": str(tmp_path / "video.mp4")}
        ti = _make_fake_ti(
            {"validate_input": conf, "generate_thumbnail_option_a": gen_result}
        )

        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.download.return_value = None

        result = self.mod._task_download_option("option_a", ti)

        assert result["art_direction_brief"] == _FAKE_ART_BRIEF

    def test_score_option_preserves_art_direction_brief_via_spread(
        self, mocker, tmp_path
    ) -> None:
        thumb_file = tmp_path / "option_a.png"
        thumb_file.write_bytes(b"\x89PNG" * 10)
        download_info = {
            "label": "option_a",
            "local_path": str(thumb_file),
            "output_url": "u",
            "style": "A",
            "prompt": "p",
            "art_direction_brief": _FAKE_ART_BRIEF,
        }
        ti = _make_fake_ti({"download_option_a": download_info})

        mock_pkz = mocker.patch.object(self.mod, "_pkz")
        mock_pkz.to_base64_data_url.return_value = "data:image/png;base64,xx"
        mock_pkz.score_thumbnail.return_value = {"main_score": 90.0}

        result = self.mod._task_score_option("option_a", ti)

        assert result["art_direction_brief"] == _FAKE_ART_BRIEF

"""Unit tests for congress_videos.modules.final_copy_verification (issue #512).

TDD RED cycle: all scenarios written before implementation. Covers design.md
D1 (verdict schema/defensive parsing), D2 (bounded correction + evidence
containment) and D3 (content versioning).

Slice 2a scope: the tested seam is ``run_correction_round``, whose injected
``call_round(title, description)`` stands in for "render the prompt and call
completion_fn once" — the two prompt constants and the public
``verify_final_copy`` wrapper were deferred to slice 2b.

Slice 2b adds the two prompt constants (design.md D6) and the public
``verify_final_copy`` wrapper. Per design.md's Testing Strategy ("never
module internals"), the ``TestVerifyFinalCopyPublicSeam`` class below tests
through that public seam with an injected ``completion_fn`` shaped like
``cached_json_completion`` (``(system_prompt, user_prompt, model=..., **kw)
-> {"data": ..., "error": ...}``) — most branch coverage already lives in
the slice 2a tests above, exercised through ``run_correction_round``.
"""

from __future__ import annotations

import pytest


def _evidence():
    return {"speaker": {"slug": "pedro-sanchez", "display_name": "Pedro Sánchez", "party": "PSOE"}}


def _stub(response, calls=None):
    call_log = calls if calls is not None else []

    def _fn(title, description):
        call_log.append((title, description))
        return response

    return _fn


def _sequenced(responses, calls=None):
    call_log = calls if calls is not None else []
    remaining = list(responses)

    def _fn(title, description):
        call_log.append((title, description))
        return remaining.pop(0) if remaining else {"data": None, "error": "exhausted"}

    return _fn


def _pass_response(rationale=""):
    return {"data": {"verdict": "pass", "findings": [], "corrected": None, "rationale": rationale}, "error": None}


def _correctable_response(corrected_title=None, corrected_description=None, findings=None):
    return {
        "data": {
            "verdict": "correctable",
            "findings": findings or [],
            "corrected": {"title": corrected_title, "description": corrected_description},
            "rationale": "",
        },
        "error": None,
    }


def _reject_response(findings=None):
    return {"data": {"verdict": "reject", "findings": findings or [], "corrected": None}, "error": None}


def _completion_stub(response, calls=None):
    """Stub matching cached_json_completion's shape: (system_prompt,
    user_prompt, model=..., **kw) -> {"data": ..., "error": ...}."""
    call_log = calls if calls is not None else []

    def _fn(system_prompt, user_prompt, model=None, **kw):
        call_log.append((system_prompt, user_prompt, model))
        return response

    return _fn


def _completion_sequenced(responses, calls=None):
    call_log = calls if calls is not None else []
    remaining = list(responses)

    def _fn(system_prompt, user_prompt, model=None, **kw):
        call_log.append((system_prompt, user_prompt, model))
        return remaining.pop(0) if remaining else {"data": None, "error": "exhausted"}

    return _fn


class TestConsistentCopyPasses:
    def test_pass_verdict_with_no_findings(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        result = run_correction_round(
            original_title="Título correcto",
            original_description="Descripción correcta",
            evidence=_evidence(),
            call_round=_stub(_pass_response()),
        )

        assert result.ok is True
        assert result.verdict == "pass"
        assert result.findings == ()
        assert (result.title, result.description) == ("Título correcto", "Descripción correcta")


class TestDefensiveParsingReturnsInconclusive:
    @pytest.mark.parametrize(
        "response",
        [
            {"data": None, "error": "rate_limited"},
            {"data": None, "error": None},
            {"error": "missing data key entirely"},
            {"data": {"verdict": "maybe", "findings": [], "corrected": None}, "error": None},
            {"data": {"verdict": "pass", "findings": "not-a-list"}, "error": None},
            {
                "data": {
                    "verdict": "pass",
                    "findings": [{"field": "title", "category": "spelling"}],
                    "corrected": {"thumbnail_text": "x"},
                },
                "error": None,
            },
        ],
        ids=[
            "error-set",
            "data-none",
            "missing-data-key",
            "unknown-verdict",
            "non-list-findings",
            "corrected-stray-key",
        ],
    )
    def test_malformed_response_is_inconclusive_and_publishes_original(self, response):
        from congress_videos.modules.final_copy_verification import run_correction_round

        result = run_correction_round(
            original_title="Título original",
            original_description="Descripción original",
            evidence=_evidence(),
            call_round=_stub(response),
        )

        assert result.ok is False
        assert result.verdict == ""
        assert (result.title, result.description) == ("Título original", "Descripción original")
        assert result.content_version == ""

    def test_unknown_finding_field_is_dropped_but_verdict_kept(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        response = _pass_response()
        response["data"]["findings"] = [{"field": "not_a_real_field", "category": "spelling", "severity": "low"}]

        result = run_correction_round(
            original_title="T", original_description="D", evidence=_evidence(), call_round=_stub(response)
        )

        assert result.ok is True
        assert result.findings == ()

    def test_unknown_finding_category_is_kept_and_classified_other(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        response = _pass_response()
        response["data"]["findings"] = [{"field": "title", "category": "something_new", "severity": "low"}]

        result = run_correction_round(
            original_title="T", original_description="D", evidence=_evidence(), call_round=_stub(response)
        )

        assert result.ok is True
        assert result.findings[0].category == "other"


class TestFindingCategoriesPassThrough:
    @pytest.mark.parametrize(
        "category", ["person_name", "party_name", "spelling", "grammar", "language", "unsupported_claim"]
    )
    def test_valid_category_preserved_verbatim(self, category):
        from congress_videos.modules.final_copy_verification import run_correction_round

        response = _pass_response()
        response["data"]["findings"] = [{"field": "description", "category": category, "severity": "medium"}]

        result = run_correction_round(
            original_title="T", original_description="D", evidence=_evidence(), call_round=_stub(response)
        )

        assert result.findings[0].category == category

    def test_party_true_positive_is_a_finding(self):
        """Evidence: PSOE. Copy states VOX. The stub represents a
        correctly-behaving verifier flagging the contradiction — this module
        only passes such findings through, it does not decide the party
        rule itself (that lives in the slice-2b prompt)."""
        from congress_videos.modules.final_copy_verification import run_correction_round

        response = _pass_response()
        response["data"]["findings"] = [
            {"field": "description", "category": "party_name", "severity": "medium", "detail": "VOX != PSOE"}
        ]

        result = run_correction_round(
            original_title="T",
            original_description="Sánchez, del grupo VOX, interviene",
            evidence=_evidence(),
            call_round=_stub(response),
        )

        assert result.ok is True
        assert len(result.findings) == 1
        assert result.findings[0].category == "party_name"

    def test_party_same_party_variant_is_not_a_finding(self):
        """Evidence: PSE-EE (PSOE). Copy states PSOE. No finding — same party."""
        from congress_videos.modules.final_copy_verification import run_correction_round

        evidence = {"speaker": {"party": "PSE-EE (PSOE)"}}

        result = run_correction_round(
            original_title="T",
            original_description="X, del PSOE, interviene",
            evidence=evidence,
            call_round=_stub(_pass_response()),
        )

        assert result.ok is True
        assert result.verdict == "pass"
        assert result.findings == ()


class TestThumbnailTextNeverCorrected:
    def test_thumbnail_finding_recorded_corrected_cannot_carry_it(self):
        """A response whose "corrected" ever carries "thumbnail_text" is
        structurally rejected by the parser (stray-key rule), which is what
        makes thumbnail correction impossible."""
        from congress_videos.modules.final_copy_verification import run_correction_round

        response = _pass_response()
        response["data"]["findings"] = [
            {"field": "thumbnail_text", "category": "person_name", "severity": "high", "detail": "unlisted name"}
        ]

        result = run_correction_round(
            original_title="T", original_description="D", evidence=_evidence(), call_round=_stub(response)
        )

        assert result.ok is True
        assert any(f.field == "thumbnail_text" for f in result.findings)


class TestBoundedCorrection:
    def test_evidence_backed_correction_applied_and_rechecked(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        round0 = _correctable_response(
            corrected_title="Sánchez responde",
            corrected_description="Descripción original",
            findings=[{"field": "title", "category": "person_name", "severity": "high"}],
        )
        calls = []
        result = run_correction_round(
            original_title="Sanchez responde",
            original_description="Descripción original",
            evidence=_evidence(),
            call_round=_sequenced([round0, _pass_response()], calls=calls),
        )

        assert result.ok is True
        assert result.verdict == "pass"
        assert result.title == "Sánchez responde"
        assert result.correction_applied is True
        assert result.rounds == 2
        assert len(calls) == 2

    def test_unsupported_claim_correction_is_discarded_original_published(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        round0 = _correctable_response(
            corrected_title="Título",
            corrected_description="Descripción original con un Ministerio Inventado",
            findings=[{"field": "description", "category": "unsupported_claim", "severity": "high"}],
        )
        calls = []
        result = run_correction_round(
            original_title="Título",
            original_description="Descripción original",
            evidence=_evidence(),
            call_round=_sequenced([round0], calls=calls),
        )

        assert result.ok is True
        assert (result.title, result.description) == ("Título", "Descripción original")
        assert result.correction_applied is False
        assert any(f.category == "unsupported_claim" for f in result.findings)
        assert len(calls) == 1  # containment rejected before any recheck call

    @pytest.mark.parametrize(
        "responses,expected_rounds",
        [
            ([_pass_response()], 1),
            ([_reject_response(findings=[{"field": "title", "category": "person_name", "severity": "high"}])], 1),
            ([{"data": None, "error": "boom"}], 1),
            ([_correctable_response("Sánchez responde", "Descripción original"), _pass_response()], 2),
            ([_correctable_response("Sánchez responde", "Descripción original"), _reject_response()], 2),
        ],
        ids=["pass", "reject", "inconclusive", "correctable-recheck-pass", "correctable-recheck-fails"],
    )
    def test_call_round_invoked_at_most_twice_on_every_branch(self, responses, expected_rounds):
        from congress_videos.modules.final_copy_verification import run_correction_round

        calls = []
        result = run_correction_round(
            original_title="Sanchez responde",
            original_description="Descripción original",
            evidence=_evidence(),
            call_round=_sequenced(responses, calls=calls),
        )

        assert len(calls) <= 2
        assert len(calls) == expected_rounds
        assert result.rounds == expected_rounds

    def test_recheck_failure_publishes_original_not_corrected(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        round0 = _correctable_response("Sánchez responde", "Descripción original")
        calls = []
        result = run_correction_round(
            original_title="Sanchez responde",
            original_description="Descripción original",
            evidence=_evidence(),
            call_round=_sequenced([round0, _reject_response()], calls=calls),
        )

        assert result.ok is True
        assert result.title == "Sanchez responde"
        assert result.correction_applied is False
        assert len(calls) == 2

    def test_original_never_blocked_by_a_failed_recheck_of_the_correction(self):
        """The blocking decision reads round 0's verdict for the value
        actually published only: a recheck of CORRECTED text failing must
        never retroactively turn an original the verifier never rejected
        into a reject."""
        from congress_videos.modules.final_copy_verification import run_correction_round

        round0 = _correctable_response("Sánchez responde", "Descripción original")
        result = run_correction_round(
            original_title="Sanchez responde",
            original_description="Descripción original",
            evidence=_evidence(),
            call_round=_sequenced([round0, _reject_response()]),
        )

        assert result.verdict != "reject"
        assert result.title == "Sanchez responde"


class TestContainmentLengthBounds:
    def test_correction_exceeding_title_length_bound_is_discarded(self):
        from congress_videos.modules.final_copy_verification import TITLE_MAX_CHARS, run_correction_round

        overlong_title = "Sánchez " * (TITLE_MAX_CHARS // 5)
        round0 = _correctable_response(overlong_title, "Descripción original")
        calls = []

        result = run_correction_round(
            original_title="Título",
            original_description="Descripción original",
            evidence={"speaker": {"display_name": "Sánchez"}},
            call_round=_sequenced([round0], calls=calls),
        )

        assert result.title == "Título"
        assert len(calls) == 1


class TestContentVersion:
    def test_stable_across_identical_inputs_different_across_changed_evidence(self):
        from congress_videos.modules.final_copy_verification import compute_content_version

        base = compute_content_version(
            title="T", description="D", thumbnail_text=None, evidence={"speaker": {"party": "PSOE"}}
        )
        same = compute_content_version(
            title="T", description="D", thumbnail_text=None, evidence={"speaker": {"party": "PSOE"}}
        )
        changed = compute_content_version(
            title="T", description="D", thumbnail_text=None, evidence={"speaker": {"party": "VOX"}}
        )

        assert base != ""
        assert base == same
        assert base != changed

    def test_threaded_through_on_success_and_empty_on_inconclusive(self):
        from congress_videos.modules.final_copy_verification import compute_content_version, run_correction_round

        version = compute_content_version(title="T", description="D", thumbnail_text=None, evidence=_evidence())

        ok_result = run_correction_round(
            original_title="T",
            original_description="D",
            evidence=_evidence(),
            call_round=_stub(_pass_response()),
            content_version=version,
        )
        inconclusive_result = run_correction_round(
            original_title="T",
            original_description="D",
            evidence=_evidence(),
            call_round=_stub({"data": None, "error": "boom"}),
            content_version=version,
        )

        assert ok_result.content_version == version
        assert inconclusive_result.content_version == ""


class TestNeverRaises:
    def test_never_raises_when_call_round_raises(self):
        from congress_videos.modules.final_copy_verification import run_correction_round

        def _raising(title, description):
            raise TimeoutError("upstream timed out")

        result = run_correction_round(
            original_title="T", original_description="D", evidence=_evidence(), call_round=_raising
        )

        assert result.ok is False
        assert (result.title, result.description) == ("T", "D")


class TestPinnedPromptContent:
    """Guards design.md D6's exact prompt text against silent drift."""

    def test_system_prompt_states_the_no_invention_rule(self):
        from congress_videos.config.ai_prompts import FINAL_COPY_VERIFICATION_SYSTEM_PROMPT

        assert "No inventes identidades" in FINAL_COPY_VERIFICATION_SYSTEM_PROMPT

    def test_system_prompt_states_the_party_variant_rule(self):
        from congress_videos.config.ai_prompts import FINAL_COPY_VERIFICATION_SYSTEM_PROMPT

        assert "PSC-PSOE" in FINAL_COPY_VERIFICATION_SYSTEM_PROMPT
        assert "PSE-EE (PSOE)" in FINAL_COPY_VERIFICATION_SYSTEM_PROMPT

    def test_system_prompt_never_corrects_thumbnail_text(self):
        from congress_videos.config.ai_prompts import FINAL_COPY_VERIFICATION_SYSTEM_PROMPT

        assert "NUNCA se corrige" in FINAL_COPY_VERIFICATION_SYSTEM_PROMPT

    def test_user_template_names_title_description_and_thumbnail_as_separate_fields(self):
        from congress_videos.modules.final_copy_verification import verify_final_copy

        calls = []
        verify_final_copy(
            title="T",
            description="D",
            thumbnail_text="Miniatura",
            evidence=_evidence(),
            completion_fn=_completion_stub(_pass_response(), calls=calls),
        )

        _, user_prompt, _ = calls[0]
        assert "Título" in user_prompt
        assert "Descripción" in user_prompt
        assert "miniatura" in user_prompt.lower()


class TestVerifyFinalCopyPublicSeam:
    """Integration tests through the public seam (design.md's confirmed seam,
    "never module internals"). Branch coverage for the bounded-correction
    contract already lives in the slice 2a classes above via
    ``run_correction_round``; these tests confirm the wiring."""

    def test_pass_verdict(self):
        from congress_videos.modules.final_copy_verification import verify_final_copy

        result = verify_final_copy(
            title="Título correcto",
            description="Descripción correcta",
            evidence=_evidence(),
            completion_fn=_completion_stub(_pass_response()),
        )

        assert result.ok is True
        assert result.verdict == "pass"
        assert (result.title, result.description) == ("Título correcto", "Descripción correcta")
        assert result.content_version != ""

    def test_correctable_verdict_gets_corrected(self):
        from congress_videos.modules.final_copy_verification import verify_final_copy

        round0 = _correctable_response(corrected_title="Sánchez responde", corrected_description="Descripción original")
        result = verify_final_copy(
            title="Sanchez responde",
            description="Descripción original",
            evidence=_evidence(),
            completion_fn=_completion_sequenced([round0, _pass_response()]),
        )

        assert result.ok is True
        assert result.title == "Sánchez responde"
        assert result.correction_applied is True

    def test_inconclusive_verdict_on_malformed_output(self):
        from congress_videos.modules.final_copy_verification import verify_final_copy

        response = {"data": {"verdict": "maybe-not-real"}, "error": None}
        result = verify_final_copy(
            title="T", description="D", evidence=_evidence(), completion_fn=_completion_stub(response)
        )

        assert result.ok is False
        assert (result.title, result.description) == ("T", "D")

    def test_verifier_failure_never_raises_and_publishes_original(self):
        from congress_videos.modules.final_copy_verification import verify_final_copy

        def _raising(system_prompt, user_prompt, model=None, **kw):
            raise TimeoutError("upstream timed out")

        result = verify_final_copy(title="T", description="D", evidence=_evidence(), completion_fn=_raising)

        assert result.ok is False
        assert (result.title, result.description) == ("T", "D")

    def test_default_completion_fn_resolves_to_cached_json_completion_with_llm_default(self, monkeypatch):
        from congress_videos.modules.final_copy_verification import verify_final_copy
        from utils.llm_config import LLM_DEFAULT

        calls = []

        def _fake(system_prompt, user_prompt, model=None, **kw):
            calls.append((system_prompt, user_prompt, model, kw))
            return _pass_response()

        monkeypatch.setattr("utils.llm_cache.cached_json_completion", _fake)

        result = verify_final_copy(title="T", description="D", evidence=_evidence())

        assert result.ok is True
        assert len(calls) == 1
        _, _, model, kw = calls[0]
        assert model == LLM_DEFAULT
        assert "temperature" not in kw
        assert "max_tokens" not in kw
        assert "max_completion_tokens" not in kw

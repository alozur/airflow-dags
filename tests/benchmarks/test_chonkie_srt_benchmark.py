"""Behavior tests for the isolated Chonkie SRT benchmark parser."""

from __future__ import annotations

import json
import sys
import types

import pytest

from benchmarks.chonkie_srt.benchmark_srt import (
    TraceabilityError,
    parse_srt,
    trace_section_offsets,
)


def test_parse_srt_normalizes_cues_into_continuous_text(tmp_path):
    source = tmp_path / "sample.srt"
    source.write_text(
        "1\n00:00:00,000 --> 00:00:02,000\nHello   world\n\n2\n00:00:02,000 --> 00:00:04,000\nsecond\nline\n",
        encoding="utf-8",
    )

    parsed = parse_srt(source)

    assert parsed.text == "Hello world second line"
    assert [(cue.start_offset, cue.end_offset) for cue in parsed.cues] == [
        (0, 11),
        (12, 23),
    ]


def test_parse_srt_stops_at_the_first_thirty_minutes(tmp_path):
    source = tmp_path / "window.srt"
    source.write_text(
        "1\n00:29:59,000 --> 00:30:00,000\nIncluded cue\n\n2\n00:30:00,000 --> 00:30:01,000\nExcluded cue\n",
        encoding="utf-8",
    )

    parsed = parse_srt(source)

    assert parsed.text == "Included cue"
    assert len(parsed.cues) == 1
    assert parsed.cues[0].end_seconds == 1800


def test_traceable_sections_resolve_to_chronological_cue_timestamps(tmp_path):
    source = tmp_path / "traceable.srt"
    source.write_text(
        "1\n00:00:00,000 --> 00:00:02,000\nFirst cue\n\n"
        "2\n00:00:02,000 --> 00:00:04,000\nSecond cue\n\n"
        "3\n00:00:04,000 --> 00:00:06,000\nThird cue\n",
        encoding="utf-8",
    )
    parsed = parse_srt(source)

    sections = trace_section_offsets(parsed, [(0, 20), (21, 30)])

    assert [(section.start_seconds, section.end_seconds) for section in sections] == [
        (0, 4),
        (4, 6),
    ]


@pytest.mark.parametrize(
    "offsets",
    [
        [(0, 9), (0, 9)],
        [(0, 9), (30, 31)],
    ],
)
def test_traceability_fails_closed_for_out_of_order_or_invalid_offsets(tmp_path, offsets):
    source = tmp_path / "invalid.srt"
    source.write_text(
        "1\n00:00:00,000 --> 00:00:02,000\nFirst cue\n\n2\n00:00:02,000 --> 00:00:04,000\nSecond cue\n",
        encoding="utf-8",
    )

    with pytest.raises(TraceabilityError):
        trace_section_offsets(parse_srt(source), offsets)


def test_parse_srt_rejects_out_of_order_source_cues(tmp_path):
    source = tmp_path / "out-of-order.srt"
    source.write_text(
        "1\n00:00:03,000 --> 00:00:04,000\nLater cue\n\n2\n00:00:01,000 --> 00:00:02,000\nEarlier cue\n",
        encoding="utf-8",
    )

    with pytest.raises(TraceabilityError, match="chronological"):
        parse_srt(source)


def test_cli_rejects_an_incompatible_chonkie_api_before_reading_source(monkeypatch, tmp_path, capsys):
    chonkie = types.ModuleType("chonkie")
    embeddings = types.ModuleType("chonkie.embeddings")
    monkeypatch.setitem(sys.modules, "chonkie", chonkie)
    monkeypatch.setitem(sys.modules, "chonkie.embeddings", embeddings)

    from benchmarks.chonkie_srt.benchmark_srt import main

    output = tmp_path / "result.json"
    exit_code = main(["--source", str(tmp_path / "missing.srt"), "--output", str(output)])

    assert exit_code == 2
    assert "Chonkie 1.7.0 API is incompatible" in capsys.readouterr().err
    assert not output.exists()


def test_cli_rejects_a_chonkie_version_other_than_the_direct_pin(monkeypatch, tmp_path):
    class FakeEmbeddings:
        def __init__(self, *_args, **_kwargs):
            pass

    class FakeChunker:
        def __init__(self, **_kwargs):
            pass

        def __call__(self, text):
            return [types.SimpleNamespace(text=text)]

    chonkie = types.ModuleType("chonkie")
    chonkie.__version__ = "1.7.1"
    chonkie.SemanticChunker = FakeChunker
    embeddings = types.ModuleType("chonkie.embeddings")
    embeddings.SentenceTransformerEmbeddings = FakeEmbeddings
    monkeypatch.setitem(sys.modules, "chonkie", chonkie)
    monkeypatch.setitem(sys.modules, "chonkie.embeddings", embeddings)

    source = tmp_path / "sample.srt"
    source.write_text("1\n00:00:00,000 --> 00:00:02,000\nCue\n", encoding="utf-8")
    output = tmp_path / "result.json"

    from benchmarks.chonkie_srt.benchmark_srt import main

    assert main(["--source", str(source), "--output", str(output)]) == 2
    assert not output.exists()


def test_cli_writes_traceable_sections_using_the_documented_configuration(monkeypatch, tmp_path):
    calls = {}

    class FakeEmbeddings:
        def __init__(self, model_name, *, device):
            calls["embeddings"] = (model_name, device)

    class FakeChunker:
        def __init__(self, **kwargs):
            calls["chunker"] = kwargs

        def __call__(self, text):
            assert text == "First cue Second cue"
            return [
                types.SimpleNamespace(text="First cue"),
                types.SimpleNamespace(text="Second cue"),
            ]

    chonkie = types.ModuleType("chonkie")
    chonkie.__version__ = "1.7.0"
    chonkie.SemanticChunker = FakeChunker
    embeddings = types.ModuleType("chonkie.embeddings")
    embeddings.SentenceTransformerEmbeddings = FakeEmbeddings
    monkeypatch.setitem(sys.modules, "chonkie", chonkie)
    monkeypatch.setitem(sys.modules, "chonkie.embeddings", embeddings)

    source = tmp_path / "sample.srt"
    source.write_text(
        "1\n00:00:00,000 --> 00:00:02,000\nFirst cue\n\n2\n00:00:02,000 --> 00:00:04,000\nSecond cue\n",
        encoding="utf-8",
    )
    output = tmp_path / "result.json"

    from benchmarks.chonkie_srt.benchmark_srt import main

    assert main(["--source", str(source), "--output", str(output)]) == 0
    assert calls["embeddings"] == ("intfloat/multilingual-e5-small", "cpu")
    assert set(calls["chunker"]) == {
        "embedding_model",
        "threshold",
        "chunk_size",
        "similarity_window",
        "min_sentences_per_chunk",
        "min_characters_per_sentence",
        "delim",
        "skip_window",
    }
    assert {key: value for key, value in calls["chunker"].items() if key != "embedding_model"} == {
        "threshold": 0.75,
        "chunk_size": 512,
        "similarity_window": 2,
        "min_sentences_per_chunk": 4,
        "min_characters_per_sentence": 1,
        "delim": [". ", "! ", "? "],
        "skip_window": 0,
    }
    assert json.loads(output.read_text(encoding="utf-8"))["sections"] == [
        {
            "end_cue_index": 0,
            "end_seconds": 2.0,
            "start_cue_index": 0,
            "start_seconds": 0.0,
        },
        {
            "end_cue_index": 1,
            "end_seconds": 4.0,
            "start_cue_index": 1,
            "start_seconds": 2.0,
        },
    ]

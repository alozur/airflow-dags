#!/usr/bin/env python3
"""Isolated, traceable NAS benchmark for Chonkie semantic SRT chunking."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any

MAX_INPUT_SECONDS = 1_800
_TIMESTAMP = re.compile(
    r"^(?P<start>\d{2}:\d{2}:\d{2}[,.]\d{3})\s+-->\s+"
    r"(?P<end>\d{2}:\d{2}:\d{2}[,.]\d{3})"
)


@dataclass(frozen=True)
class Cue:
    """One normalized SRT cue and its offsets in the continuous input text."""

    start_seconds: float
    end_seconds: float
    start_offset: int
    end_offset: int


@dataclass(frozen=True)
class ParsedSrt:
    """Continuous text and the cue map needed to trace section offsets."""

    text: str
    cues: tuple[Cue, ...]


@dataclass(frozen=True)
class SectionRange:
    """A section resolved to source cue timestamps without retaining its text."""

    start_seconds: float
    end_seconds: float
    start_cue_index: int
    end_cue_index: int


class TraceabilityError(ValueError):
    """Raised when a semantic section cannot be traced to chronological cues."""


class ChonkieApiError(RuntimeError):
    """Raised when the pinned Chonkie API cannot run the benchmark contract."""


def _timestamp_seconds(value: str) -> float:
    hours, minutes, seconds = value.replace(",", ".").split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def parse_srt(source: Path, max_seconds: int = MAX_INPUT_SECONDS) -> ParsedSrt:
    """Parse cues beginning within ``max_seconds`` into whitespace-normalized text."""
    raw_blocks = re.split(r"\r?\n\s*\r?\n", source.read_text(encoding="utf-8-sig").strip())
    parts: list[str] = []
    cues: list[Cue] = []
    offset = 0
    previous_source_start: float | None = None

    for raw_block in raw_blocks:
        lines = [line.strip() for line in raw_block.splitlines() if line.strip()]
        timestamp_index = next((index for index, line in enumerate(lines) if _TIMESTAMP.match(line)), None)
        if timestamp_index is None:
            continue
        match = _TIMESTAMP.match(lines[timestamp_index])
        assert match is not None
        start_seconds = _timestamp_seconds(match.group("start"))
        end_seconds = _timestamp_seconds(match.group("end"))
        if end_seconds < start_seconds:
            raise TraceabilityError("source cue ends before it starts")
        if previous_source_start is not None and start_seconds < previous_source_start:
            raise TraceabilityError("source cues are not chronological")
        previous_source_start = start_seconds
        if start_seconds >= max_seconds:
            break
        text = " ".join(" ".join(lines[timestamp_index + 1 :]).split())
        if not text:
            continue
        if parts:
            offset += 1
        start_offset = offset
        end_offset = start_offset + len(text)
        parts.append(text)
        cues.append(Cue(start_seconds, end_seconds, start_offset, end_offset))
        offset = end_offset

    return ParsedSrt(text=" ".join(parts), cues=tuple(cues))


def trace_section_offsets(parsed: ParsedSrt, offsets: list[tuple[int, int]]) -> list[SectionRange]:
    """Resolve ordered character offsets to chronological source cue timestamps."""
    sections: list[SectionRange] = []
    previous_end = 0
    for start_offset, end_offset in offsets:
        if not 0 <= start_offset < end_offset <= len(parsed.text):
            raise TraceabilityError("section offsets are outside the normalized input")
        if start_offset < previous_end:
            raise TraceabilityError("section offsets are not chronological")
        matching = [
            (index, cue)
            for index, cue in enumerate(parsed.cues)
            if cue.end_offset > start_offset and cue.start_offset < end_offset
        ]
        if not matching:
            raise TraceabilityError("section cannot be mapped to source cue timestamps")
        first_index, first_cue = matching[0]
        last_index, last_cue = matching[-1]
        if sections and first_cue.start_seconds < sections[-1].end_seconds:
            raise TraceabilityError("section timestamps are not chronological")
        sections.append(
            SectionRange(
                start_seconds=first_cue.start_seconds,
                end_seconds=last_cue.end_seconds,
                start_cue_index=first_index,
                end_cue_index=last_index,
            )
        )
        previous_end = end_offset
    return sections


MODEL_NAME = "intfloat/multilingual-e5-small"
CHONKIE_VERSION = "1.7.0"
CHUNKER_CONFIGURATION = {
    "threshold": 0.75,
    "chunk_size": 512,
    "similarity_window": 2,
    "min_sentences_per_chunk": 4,
    "min_characters_per_sentence": 1,
    "delim": [". ", "! ", "? "],
    "skip_window": 0,
}


def build_chunker() -> Any:
    """Construct only the Chonkie 1.7.0 API documented for this benchmark."""
    try:
        from chonkie import SemanticChunker, __version__
        from chonkie.embeddings import SentenceTransformerEmbeddings
    except (ImportError, AttributeError) as error:
        raise ChonkieApiError(
            "Chonkie 1.7.0 API is incompatible: expected SemanticChunker and SentenceTransformerEmbeddings"
        ) from error
    if __version__ != CHONKIE_VERSION:
        raise ChonkieApiError(f"Chonkie 1.7.0 API is incompatible: installed version is {__version__!r}")

    try:
        embeddings = SentenceTransformerEmbeddings(MODEL_NAME, device="cpu")
        return SemanticChunker(embedding_model=embeddings, **CHUNKER_CONFIGURATION)
    except TypeError as error:
        raise ChonkieApiError(
            "Chonkie 1.7.0 API is incompatible with the documented benchmark configuration"
        ) from error


def _chunk_offsets(chunker: Any, text: str) -> list[tuple[int, int]]:
    """Return exact, ordered offsets for Chonkie chunks or fail closed."""
    try:
        chunks = list(chunker(text))
    except TypeError as error:
        raise ChonkieApiError("Chonkie 1.7.0 API is incompatible: SemanticChunker must accept text") from error

    offsets: list[tuple[int, int]] = []
    cursor = 0
    for chunk in chunks:
        chunk_text = getattr(chunk, "text", None)
        if not isinstance(chunk_text, str) or not chunk_text:
            raise TraceabilityError("Chonkie chunk does not expose non-empty text")
        start_offset = text.find(chunk_text, cursor)
        if start_offset < 0:
            raise TraceabilityError("Chonkie chunk text cannot be exactly aligned to the normalized input")
        end_offset = start_offset + len(chunk_text)
        offsets.append((start_offset, end_offset))
        cursor = end_offset
    return offsets


def benchmark(source: Path, max_seconds: int = MAX_INPUT_SECONDS) -> dict[str, Any]:
    """Run the pinned semantic chunker and return traceable benchmark results."""
    initialization_started = perf_counter()
    chunker = build_chunker()
    initialization_seconds = perf_counter() - initialization_started

    if not source.is_file():
        raise FileNotFoundError(f"source SRT does not exist or is not a file: {source}")
    parsed = parse_srt(source, max_seconds=max_seconds)
    if not parsed.cues:
        raise TraceabilityError("source SRT contains no traceable cues in the benchmark window")

    chunking_started = perf_counter()
    sections = trace_section_offsets(parsed, _chunk_offsets(chunker, parsed.text))
    chunking_seconds = perf_counter() - chunking_started
    return {
        "chonkie_version": CHONKIE_VERSION,
        "model": MODEL_NAME,
        "configuration": CHUNKER_CONFIGURATION,
        "source": str(source),
        "max_input_seconds": max_seconds,
        "input": {"cue_count": len(parsed.cues), "character_count": len(parsed.text)},
        "timing_seconds": {
            "model_initialization": initialization_seconds,
            "semantic_chunking": chunking_seconds,
        },
        "sections": [
            {
                "start_seconds": section.start_seconds,
                "end_seconds": section.end_seconds,
                "start_cue_index": section.start_cue_index,
                "end_cue_index": section.end_cue_index,
            }
            for section in sections
        ],
    }


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Chonkie 1.7.0 semantic SRT chunking with traceability.")
    parser.add_argument("--source", type=Path, required=True, help="source SRT file")
    parser.add_argument("--output", type=Path, required=True, help="result JSON file")
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=MAX_INPUT_SECONDS,
        help=f"maximum source duration in seconds (default: {MAX_INPUT_SECONDS})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the benchmark CLI and write a result only after complete success."""
    args = _arguments(argv)
    if args.max_seconds <= 0:
        print("--max-seconds must be positive", file=sys.stderr)
        return 2

    try:
        result = benchmark(args.source, args.max_seconds)
        args.output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (ChonkieApiError, FileNotFoundError, TraceabilityError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

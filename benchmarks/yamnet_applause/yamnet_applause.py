#!/usr/bin/env python3
"""Offline YAMNet (TFLite) applause detector for the NAS benchmark harness.

Two modes:
  --prepare  Download the YAMNet TFLite model and the AudioSet class map into
             MODEL_CACHE_DIR. This is the only step that needs network access.
  (default)  Read a pre-generated 16 kHz mono WAV, run the TFLite YAMNet frame by
             frame, and emit a transcript-free JSON of applause candidates.
             Candidates are contiguous frame runs whose applause score exceeds the
             threshold, short gaps merged, kept only when the run lasts at least
             the minimum duration. They are proposals for review, never automatic
             cuts, and a candidate overlapping sustained voice must defer to voice.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import urllib.request

import numpy as np
import soundfile as sf
from tflite_runtime.interpreter import Interpreter

# TFLite YAMNet classifier with a resizable waveform input that returns per-frame
# AudioSet scores, plus the canonical 521-class map.
MODEL_URL = "https://tfhub.dev/google/lite-model/yamnet/classification/tflite/1?lite-format=tflite"
CLASS_MAP_URL = (
    "https://raw.githubusercontent.com/tensorflow/models/master/research/audioset/yamnet/yamnet_class_map.csv"
)
MODEL_FILENAME = "yamnet_classification.tflite"
CLASS_MAP_FILENAME = "yamnet_class_map.csv"

TARGET_SAMPLE_RATE = 16000
# The classification TFLite model takes a FIXED 15600-sample (0.975 s) clip and
# returns one [1,521] score vector — it does NOT frame internally. We slide the
# window ourselves with a 0.48 s hop to recover per-frame scores.
WINDOW_SAMPLES = 15600
FRAME_HOP_SECONDS = 0.48
FRAME_WINDOW_SECONDS = WINDOW_SAMPLES / TARGET_SAMPLE_RATE  # 0.975 s
HOP_SAMPLES = int(round(FRAME_HOP_SECONDS * TARGET_SAMPLE_RATE))  # 7680
APPLAUSE_DISPLAY_NAME = "Applause"


def _cache_dir() -> str:
    return os.environ.get("MODEL_CACHE_DIR", "/model-cache")


def _prepare() -> None:
    cache = _cache_dir()
    os.makedirs(cache, exist_ok=True)
    for url, name in ((MODEL_URL, MODEL_FILENAME), (CLASS_MAP_URL, CLASS_MAP_FILENAME)):
        destination = os.path.join(cache, name)
        urllib.request.urlretrieve(url, destination)
        print(f"cached {name} ({os.path.getsize(destination)} bytes)")
    print(f"YAMNet TFLite cached at: {cache}")


def _applause_class_index(cache: str) -> int:
    with open(os.path.join(cache, CLASS_MAP_FILENAME), newline="") as handle:
        for row in csv.DictReader(handle):
            if row["display_name"] == APPLAUSE_DISPLAY_NAME:
                return int(row["index"])
    raise RuntimeError(f"{APPLAUSE_DISPLAY_NAME!r} not found in YAMNet class map")


def _read_waveform(audio_path: str) -> np.ndarray:
    data, sample_rate = sf.read(audio_path, dtype="float32", always_2d=True)
    if sample_rate != TARGET_SAMPLE_RATE:
        raise SystemExit(f"audio must be {TARGET_SAMPLE_RATE} Hz mono PCM, got {sample_rate} Hz")
    return data.mean(axis=1).astype(np.float32)


def _frame_scores(cache: str, waveform: np.ndarray) -> np.ndarray:
    interpreter = Interpreter(model_path=os.path.join(cache, MODEL_FILENAME))
    input_index = interpreter.get_input_details()[0]["index"]
    output_index = interpreter.get_output_details()[0]["index"]
    interpreter.allocate_tensors()

    total = len(waveform)
    scores: list[np.ndarray] = []
    start = 0
    # One inference per sliding 0.975 s window; zero-pad the final short window.
    while start < total:
        window = waveform[start : start + WINDOW_SAMPLES]
        if len(window) < WINDOW_SAMPLES:
            window = np.pad(window, (0, WINDOW_SAMPLES - len(window)))
        interpreter.set_tensor(input_index, window.astype(np.float32))
        interpreter.invoke()
        scores.append(interpreter.get_tensor(output_index)[0])
        start += HOP_SAMPLES
    return np.asarray(scores, dtype=np.float32)


def _runs_above_threshold(
    applause: np.ndarray,
    threshold: float,
    merge_gap_seconds: float,
    min_duration_seconds: float,
    audio_duration_seconds: float,
) -> list[dict]:
    active = applause >= threshold
    merge_gap_frames = int(round(merge_gap_seconds / FRAME_HOP_SECONDS))

    runs: list[list[int]] = []
    for index, is_active in enumerate(active):
        if not is_active:
            continue
        if runs and index - runs[-1][1] - 1 <= merge_gap_frames:
            runs[-1][1] = index
        else:
            runs.append([index, index])

    candidates: list[dict] = []
    for first_frame, last_frame in runs:
        start = first_frame * FRAME_HOP_SECONDS
        end = min(
            last_frame * FRAME_HOP_SECONDS + FRAME_WINDOW_SECONDS,
            audio_duration_seconds,
        )
        duration = end - start
        if duration < min_duration_seconds:
            continue
        window = applause[first_frame : last_frame + 1]
        candidates.append(
            {
                "start_seconds": round(float(start), 3),
                "end_seconds": round(float(end), 3),
                "duration_seconds": round(float(duration), 3),
                "frame_count": int(last_frame - first_frame + 1),
                "max_score": round(float(window.max()), 4),
                "mean_score": round(float(window.mean()), 4),
            }
        )
    return candidates


def _run_inference(args: argparse.Namespace) -> None:
    cache = _cache_dir()
    waveform = _read_waveform(args.audio)
    audio_duration_seconds = len(waveform) / TARGET_SAMPLE_RATE

    applause_index = _applause_class_index(cache)
    scores = _frame_scores(cache, waveform)
    applause = scores[:, applause_index]

    candidates = _runs_above_threshold(
        applause=applause,
        threshold=args.threshold,
        merge_gap_seconds=args.merge_gap,
        min_duration_seconds=args.min_duration,
        audio_duration_seconds=audio_duration_seconds,
    )

    result = {
        "audio_path": os.path.basename(args.audio),
        "model_file": MODEL_FILENAME,
        "applause_class_index": applause_index,
        "audio_duration_seconds": round(float(audio_duration_seconds), 3),
        "total_frames": int(scores.shape[0]),
        "frame_hop_seconds": FRAME_HOP_SECONDS,
        "frame_window_seconds": FRAME_WINDOW_SECONDS,
        "threshold": args.threshold,
        "merge_gap_seconds": args.merge_gap,
        "min_duration_seconds": args.min_duration,
        "applause_frame_count": int((applause >= args.threshold).sum()),
        "peak_applause_score": round(float(applause.max()), 4),
        "candidate_count": len(candidates),
        "candidates": candidates,
    }

    with open(args.output, "w") as handle:
        json.dump(result, handle, indent=2)
    print(f"YAMNet applause candidates: {len(candidates)} (peak score {applause.max():.3f}) -> {args.output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true", help="download the model only")
    parser.add_argument("--audio", help="absolute path to a 16 kHz mono WAV")
    parser.add_argument("--output", help="absolute path for the candidate JSON")
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--min-duration", type=float, default=3.0, dest="min_duration")
    parser.add_argument("--merge-gap", type=float, default=0.5, dest="merge_gap")
    args = parser.parse_args()

    if args.prepare:
        _prepare()
        return

    if not args.audio or not args.output:
        parser.error("--audio and --output are required unless --prepare is given")
    _run_inference(args)


if __name__ == "__main__":
    main()

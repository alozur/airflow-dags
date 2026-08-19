#!/usr/bin/env bash
# Run one fixed 30-minute whisper.cpp TinyDiarize sample inside the image.
set -euo pipefail

SOURCE_VIDEO="${SOURCE_VIDEO:-/source/input}"
OUTPUT_DIRECTORY="${OUTPUT_DIRECTORY:-/output}"
MODEL_PATH="${MODEL_PATH:-/models/ggml-small.en-tdrz.bin}"
MODEL_IDENTITY="${MODEL_IDENTITY:-ggml-org/whisper.cpp small.en-tdrz}"
IMAGE_IDENTITY="${IMAGE_IDENTITY:-unknown}"
IMAGE_BUILD_SECONDS="${IMAGE_BUILD_SECONDS:-0}"
SAMPLE_DURATION_SECONDS=1800
TURN_MARKER='[SPEAKER_TURN]'

elapsed_seconds() {
    awk -v started_at="$1" -v finished_at="$EPOCHREALTIME" \
        'BEGIN { printf "%.6f", finished_at - started_at }'
}

container_started_at="$EPOCHREALTIME"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTPUT_DIRECTORY"

audio_path="$OUTPUT_DIRECTORY/${timestamp}-sample.wav"
raw_cli_output_path="$OUTPUT_DIRECTORY/${timestamp}-whisper-cli.log"
report_path="$OUTPUT_DIRECTORY/${timestamp}-report.json"

full_video_duration_seconds="$(
    ffprobe -v error -show_entries format=duration \
        -of default=noprint_wrappers=1:nokey=1 "$SOURCE_VIDEO"
)"
python3 -c 'from benchmark_report import validate_full_duration; import sys; validate_full_duration(sys.argv[1])' \
    "$full_video_duration_seconds"

audio_extraction_started_at="$EPOCHREALTIME"
ffmpeg -hide_banner -loglevel error -y -i "$SOURCE_VIDEO" \
    -t "$SAMPLE_DURATION_SECONDS" -ac 1 -ar 16000 -c:a pcm_s16le "$audio_path"
audio_extraction_seconds="$(elapsed_seconds "$audio_extraction_started_at")"

diarization_started_at="$EPOCHREALTIME"
set +e
whisper-cli -m "$MODEL_PATH" -f "$audio_path" -tdrz -ng 0 >"$raw_cli_output_path" 2>&1
diarization_exit_status=$?
set -e
diarization_seconds="$(elapsed_seconds "$diarization_started_at")"
speaker_turn_marker_count="$(grep -cF "$TURN_MARKER" "$raw_cli_output_path" || true)"
container_end_to_end_seconds="$(elapsed_seconds "$container_started_at")"

python3 /app/benchmark_report.py \
    --full-video-duration-seconds "$full_video_duration_seconds" \
    --audio-extraction-seconds "$audio_extraction_seconds" \
    --diarization-seconds "$diarization_seconds" \
    --container-end-to-end-seconds "$container_end_to_end_seconds" \
    --speaker-turn-marker-count "$speaker_turn_marker_count" \
    --diarization-exit-status "$diarization_exit_status" \
    --image-identity "$IMAGE_IDENTITY" \
    --model-identity "$MODEL_IDENTITY" \
    --timestamp "$timestamp" \
    --audio-path "$audio_path" \
    --raw-cli-output-path "$raw_cli_output_path" \
    --report-path "$report_path" \
    --image-build-seconds "$IMAGE_BUILD_SECONDS" \
    --output "$report_path"

exit "$diarization_exit_status"

#!/usr/bin/env bash
# Stage a YouTube audio calibration, then run CPU-only offline Pyannote inference.
set -euo pipefail
umask 077

SCRIPT_DIRECTORY="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
IMAGE_TAG="${IMAGE_TAG:-pyannote-diarization-benchmark:local}"
DOCKER_BIN="${DOCKER_BIN:-}"
if [[ -z "$DOCKER_BIN" ]]; then
    for candidate in /usr/local/bin/docker /usr/bin/docker; do
        if [[ -x "$candidate" ]]; then
            DOCKER_BIN="$candidate"
            break
        fi
    done
    DOCKER_BIN="${DOCKER_BIN:-docker}"
fi
SAMPLE_DURATION_SECONDS="${SAMPLE_DURATION_SECONDS:-600}"

usage() {
    printf 'usage: BENCHMARK_CPU=<cpu> HF_TOKEN=<token> %s <youtube-url> <absolute-output-directory>\n' \
        "${0##*/}" >&2
}

if [[ ! "${BENCHMARK_CPU:-}" =~ ^[0-9]+$ ]]; then
    printf 'BENCHMARK_CPU must be a single non-negative integer\n' >&2
    exit 2
fi
if [[ -z "${HF_TOKEN:-}" ]]; then
    printf 'HF_TOKEN must be set in the environment\n' >&2
    exit 2
fi
if [[ ! "$SAMPLE_DURATION_SECONDS" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]] || \
    [[ "$SAMPLE_DURATION_SECONDS" =~ ^0*([.]0*)?$ ]]; then
    printf 'SAMPLE_DURATION_SECONDS must be a positive finite number\n' >&2
    exit 2
fi
if [[ $# -ne 2 ]]; then
    usage
    exit 2
fi

SOURCE_URL="$1"
OUTPUT_BASE_DIRECTORY="$2"
if [[ "$SOURCE_URL" == *$'\n'* || "$SOURCE_URL" == *$'\r'* ]]; then
    printf 'source YouTube URL must not contain line breaks\n' >&2
    exit 2
fi
case "$OUTPUT_BASE_DIRECTORY" in
    /*) ;;
    *)
        printf 'output directory must be an absolute path: %s\n' "$OUTPUT_BASE_DIRECTORY" >&2
        exit 2
        ;;
esac

if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
    printf 'Docker executable is unavailable: %s\n' "$DOCKER_BIN" >&2
    exit 2
fi

# When invoked through sudo, retain the calling unprivileged user's identity
# inside the containers and on their mounted benchmark artifacts.
HOST_UID="${SUDO_UID:-$(id -u)}"
HOST_GID="${SUDO_GID:-$(id -g)}"
if [[ ! "$HOST_UID" =~ ^[0-9]+$ || ! "$HOST_GID" =~ ^[0-9]+$ ]]; then
    printf 'host uid and gid must be numeric\n' >&2
    exit 2
fi

mkdir -p -- "$OUTPUT_BASE_DIRECTORY"
OUTPUT_BASE_DIRECTORY="$(cd -- "$OUTPUT_BASE_DIRECTORY" && pwd -P)"
RUN_DIRECTORY="$(mktemp -d "$OUTPUT_BASE_DIRECTORY/run.XXXXXXXX")"
MODEL_CACHE_DIRECTORY="${MODEL_CACHE_DIRECTORY:-$OUTPUT_BASE_DIRECTORY/model-cache}"
case "$MODEL_CACHE_DIRECTORY" in
    /*) ;;
    *)
        printf 'MODEL_CACHE_DIRECTORY must be an absolute path: %s\n' \
            "$MODEL_CACHE_DIRECTORY" >&2
        exit 2
        ;;
esac
mkdir -p -- "$MODEL_CACHE_DIRECTORY"
MODEL_CACHE_DIRECTORY="$(cd -- "$MODEL_CACHE_DIRECTORY" && pwd -P)"
STAGING_DIRECTORY="$(mktemp -d "$RUN_DIRECTORY/.staging.XXXXXXXX")"
if (( EUID == 0 )); then
    chown --recursive "$HOST_UID:$HOST_GID" \
        "$OUTPUT_BASE_DIRECTORY" "$RUN_DIRECTORY" "$MODEL_CACHE_DIRECTORY" "$STAGING_DIRECTORY"
fi

cleanup() {
    rm -rf -- "$STAGING_DIRECTORY"
}
trap cleanup EXIT

build_started_at="$SECONDS"
"$DOCKER_BIN" build \
    --iidfile "$RUN_DIRECTORY/image-id.txt" \
    --tag "$IMAGE_TAG" \
    "$SCRIPT_DIRECTORY"
image_build_seconds=$((SECONDS - build_started_at))
printf '%s\n' "$image_build_seconds" > "$RUN_DIRECTORY/image-build-seconds.txt"

staging_script="$(cat <<'BASH'
set -euo pipefail

full_duration="$(yt-dlp --no-playlist --skip-download --print '%(duration)s' -- "$SOURCE_URL")"
printf '%s\n' "$full_duration" > /output/full-video-duration-seconds.txt

yt-dlp --no-playlist --no-progress --format 'bestaudio/best' \
    --download-sections "*0-${SAMPLE_DURATION_SECONDS}" \
    --output '/staging/source.%(ext)s' \
    -- "$SOURCE_URL"
shopt -s nullglob
sources=(/staging/source.*)
if (( ${#sources[@]} != 1 )); then
    printf 'expected exactly one staged source audio file\n' >&2
    exit 2
fi
ffmpeg -hide_banner -loglevel error -y -i "${sources[0]}" \
    -t "$SAMPLE_DURATION_SECONDS" -ac 1 -ar 16000 -c:a pcm_s16le \
    "/output/calibration-${SAMPLE_DURATION_SECONDS}s.wav"
sample_duration="$(ffprobe -v error -show_entries format=duration \
    -of default=noprint_wrappers=1:nokey=1 \
    "/output/calibration-${SAMPLE_DURATION_SECONDS}s.wav")"
python3 - "$full_duration" "$sample_duration" "$SAMPLE_DURATION_SECONDS" <<'PY'
import math
import sys

try:
    full_duration, sample_duration, expected_sample_duration = (
        float(value) for value in sys.argv[1:]
    )
except ValueError as error:
    raise SystemExit("YouTube did not provide a numeric duration") from error
if not (
    math.isfinite(full_duration)
    and math.isfinite(expected_sample_duration)
    and full_duration >= expected_sample_duration
):
    raise SystemExit(f"source media must be at least {expected_sample_duration} seconds")
if not (
    math.isfinite(sample_duration)
    and expected_sample_duration - 0.05 <= sample_duration <= expected_sample_duration + 0.05
):
    raise SystemExit(
        f"failed to create a {expected_sample_duration}-second calibration WAV"
    )
PY
exec /app/entrypoint.py --prepare-model
BASH
)"

# This is the only networked runtime phase. It writes only the staged WAV, duration,
# and model cache mounted from the explicit host output area.
"$DOCKER_BIN" run --rm \
    --network bridge \
    --read-only \
    --tmpfs /tmp:rw,mode=1777 \
    --cap-drop ALL \
    --user "$HOST_UID:$HOST_GID" \
    --mount "type=bind,source=$STAGING_DIRECTORY,target=/staging" \
    --mount "type=bind,source=$RUN_DIRECTORY,target=/output" \
    --mount "type=bind,source=$MODEL_CACHE_DIRECTORY,target=/model-cache" \
    --env HF_TOKEN \
    --env MODEL_CACHE_DIR=/model-cache \
    --env HOME=/tmp \
    --env "SOURCE_URL=$SOURCE_URL" \
    --env "SAMPLE_DURATION_SECONDS=$SAMPLE_DURATION_SECONDS" \
    --entrypoint /bin/bash \
    "$IMAGE_TAG" -c "$staging_script"

CALIBRATION_AUDIO="$RUN_DIRECTORY/calibration-${SAMPLE_DURATION_SECONDS}s.wav"
FULL_DURATION_FILE="$RUN_DIRECTORY/full-video-duration-seconds.txt"
if [[ ! -f "$CALIBRATION_AUDIO" || ! -s "$CALIBRATION_AUDIO" || ! -s "$FULL_DURATION_FILE" ]]; then
    printf 'staging did not produce the required calibration artifacts\n' >&2
    exit 1
fi
FULL_VIDEO_DURATION_SECONDS="$(<"$FULL_DURATION_FILE")"

# Inference has no network and can read, but never write, its audio source or model cache.
"$DOCKER_BIN" run --rm \
    --network none \
    --cpuset-cpus "$BENCHMARK_CPU" \
    --memory 4g \
    --read-only \
    --tmpfs /tmp:rw,mode=1777 \
    --cap-drop ALL \
    --user "$HOST_UID:$HOST_GID" \
    --mount "type=bind,source=$CALIBRATION_AUDIO,target=/source/input.wav,readonly" \
    --mount "type=bind,source=$MODEL_CACHE_DIRECTORY,target=/model-cache,readonly" \
    --mount "type=bind,source=$RUN_DIRECTORY,target=/output" \
    --env HF_TOKEN \
    --env MODEL_CACHE_DIR=/model-cache \
    --env HOME=/tmp \
    "$IMAGE_TAG" \
    --audio /source/input.wav \
    --output /output/summary.json \
    --sample-duration-seconds "$SAMPLE_DURATION_SECONDS" \
    --full-video-duration-seconds "$FULL_VIDEO_DURATION_SECONDS"

if [[ ! -s "$RUN_DIRECTORY/summary.json" ]]; then
    printf 'inference did not produce a JSON summary\n' >&2
    exit 1
fi

estimate="$(python3 - "$RUN_DIRECTORY/summary.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as summary_file:
    summary = json.load(summary_file)
print(summary["extrapolated_full_video_diarization_seconds"])
PY
)"
printf 'Calibration sample duration: %s seconds\n' "$SAMPLE_DURATION_SECONDS"
printf 'Extrapolated full-video diarization estimate: %s seconds\n' "$estimate"
printf 'Benchmark artifacts: %s\n' "$RUN_DIRECTORY"

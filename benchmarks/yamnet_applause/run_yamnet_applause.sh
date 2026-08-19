#!/usr/bin/env bash
# Run offline YAMNet applause detection against an existing WAV and a populated
# YAMNet model cache. No network, read-only inputs, single pinned CPU.
set -euo pipefail
umask 077

IMAGE_TAG="${IMAGE_TAG:-yamnet-applause-benchmark:local}"
THRESHOLD="${APPLAUSE_THRESHOLD:-0.5}"
MIN_DURATION="${APPLAUSE_MIN_DURATION:-3.0}"
MERGE_GAP="${APPLAUSE_MERGE_GAP:-0.5}"

usage() {
    printf 'usage: BENCHMARK_CPU=<cpu> %s <absolute-wav-path> <absolute-model-cache-directory> <absolute-output-path>\n' \
        "${0##*/}" >&2
}

if [[ ! "${BENCHMARK_CPU:-}" =~ ^[0-9]+$ ]]; then
    printf 'BENCHMARK_CPU must be a single non-negative integer\n' >&2
    exit 2
fi
if [[ $# -ne 3 ]]; then usage; exit 2; fi

WAV_PATH="$1"
MODEL_CACHE_DIRECTORY="$2"
OUTPUT_PATH="$3"
for path in "$WAV_PATH" "$MODEL_CACHE_DIRECTORY" "$OUTPUT_PATH"; do
    case "$path" in
        /*) ;;
        *) printf 'all input and output paths must be absolute: %s\n' "$path" >&2; exit 2 ;;
    esac
done
[[ -f "$WAV_PATH" ]] || { printf 'WAV must be a regular file: %s\n' "$WAV_PATH" >&2; exit 2; }
[[ -d "$MODEL_CACHE_DIRECTORY" ]] || { printf 'model cache must be a directory: %s\n' "$MODEL_CACHE_DIRECTORY" >&2; exit 2; }
OUTPUT_DIRECTORY="$(dirname -- "$OUTPUT_PATH")"
[[ -d "$OUTPUT_DIRECTORY" ]] || { printf 'output parent directory must already exist: %s\n' "$OUTPUT_DIRECTORY" >&2; exit 2; }
[[ -e "$OUTPUT_PATH" ]] && { printf 'output artifact must not already exist: %s\n' "$OUTPUT_PATH" >&2; exit 2; }

DOCKER_BIN=""
for docker_candidate in "/usr/local/bin/docker" "/usr/bin/docker"; do
    if [[ -x "$docker_candidate" ]]; then DOCKER_BIN="$docker_candidate"; break; fi
done
[[ -z "$DOCKER_BIN" ]] && DOCKER_BIN="$(command -v docker || true)"
if [[ -z "$DOCKER_BIN" ]]; then printf 'Docker executable is unavailable\n' >&2; exit 2; fi

CONTAINER_UID="${SUDO_UID:-$(id -u)}"
CONTAINER_GID="${SUDO_GID:-$(id -g)}"

WAV_PATH="$(cd -- "$(dirname -- "$WAV_PATH")" && pwd -P)/$(basename -- "$WAV_PATH")"
MODEL_CACHE_DIRECTORY="$(cd -- "$MODEL_CACHE_DIRECTORY" && pwd -P)"
OUTPUT_DIRECTORY="$(cd -- "$OUTPUT_DIRECTORY" && pwd -P)"
OUTPUT_FILENAME="$(basename -- "$OUTPUT_PATH")"

"$DOCKER_BIN" run --rm \
    --user "$CONTAINER_UID:$CONTAINER_GID" \
    --network none \
    --cpuset-cpus "$BENCHMARK_CPU" \
    --memory 4g \
    --read-only \
    --tmpfs /tmp:rw,mode=1777 \
    --cap-drop ALL \
    --mount "type=bind,source=$WAV_PATH,target=/source/input.wav,readonly" \
    --mount "type=bind,source=$MODEL_CACHE_DIRECTORY,target=/model-cache,readonly" \
    --mount "type=bind,source=$OUTPUT_DIRECTORY,target=/output" \
    --env MODEL_CACHE_DIR=/model-cache \
    --env HOME=/tmp \
    "$IMAGE_TAG" \
    --audio /source/input.wav \
    --output "/output/$OUTPUT_FILENAME" \
    --threshold "$THRESHOLD" \
    --min-duration "$MIN_DURATION" \
    --merge-gap "$MERGE_GAP"

if [[ ! -s "$OUTPUT_DIRECTORY/$OUTPUT_FILENAME" ]]; then
    printf 'offline YAMNet did not produce a candidate JSON artifact\n' >&2
    exit 1
fi
printf 'YAMNet applause candidates: %s\n' "$OUTPUT_DIRECTORY/$OUTPUT_FILENAME"

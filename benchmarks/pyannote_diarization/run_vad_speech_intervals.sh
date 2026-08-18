#!/usr/bin/env bash
# Run offline VAD against an existing WAV and fully populated Pyannote model cache.
set -euo pipefail
umask 077

SCRIPT_DIRECTORY="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
IMAGE_TAG="${IMAGE_TAG:-pyannote-diarization-benchmark:local}"

usage() {
    printf 'usage: BENCHMARK_CPU=<cpu> VAD_SEGMENTATION_MODEL_ID=<cached-model-id> VAD_PIPELINE_PARAMETERS=<absolute-cache-path> %s <absolute-wav-path> <absolute-model-cache-directory> <absolute-output-path>\n' \
        "${0##*/}" >&2
}

if [[ ! "${BENCHMARK_CPU:-}" =~ ^[0-9]+$ ]]; then
    printf 'BENCHMARK_CPU must be a single non-negative integer\n' >&2
    exit 2
fi
if [[ -z "${VAD_SEGMENTATION_MODEL_ID:-}" ]]; then
    printf 'VAD_SEGMENTATION_MODEL_ID must name an already cached segmentation model\n' >&2
    exit 2
fi
if [[ -z "${VAD_PIPELINE_PARAMETERS:-}" ]]; then
    printf 'VAD_PIPELINE_PARAMETERS must be an absolute JSON file in the model cache\n' >&2
    exit 2
fi
if [[ $# -ne 3 ]]; then
    usage
    exit 2
fi

WAV_PATH="$1"
MODEL_CACHE_DIRECTORY="$2"
OUTPUT_PATH="$3"
for path in "$WAV_PATH" "$MODEL_CACHE_DIRECTORY" "$OUTPUT_PATH" "$VAD_PIPELINE_PARAMETERS"; do
    case "$path" in
        /*) ;;
        *)
            printf 'all input and output paths must be absolute: %s\n' "$path" >&2
            exit 2
            ;;
    esac
done
if [[ ! -f "$WAV_PATH" ]]; then
    printf 'WAV must be a regular file: %s\n' "$WAV_PATH" >&2
    exit 2
fi
if [[ ! -d "$MODEL_CACHE_DIRECTORY" ]]; then
    printf 'model cache must be a directory: %s\n' "$MODEL_CACHE_DIRECTORY" >&2
    exit 2
fi
if [[ ! -f "$VAD_PIPELINE_PARAMETERS" ]]; then
    printf 'VAD_PIPELINE_PARAMETERS must be a regular file: %s\n' "$VAD_PIPELINE_PARAMETERS" >&2
    exit 2
fi
OUTPUT_DIRECTORY="$(dirname -- "$OUTPUT_PATH")"
if [[ ! -d "$OUTPUT_DIRECTORY" ]]; then
    printf 'output parent directory must already exist: %s\n' "$OUTPUT_DIRECTORY" >&2
    exit 2
fi
if [[ -e "$OUTPUT_PATH" ]]; then
    printf 'output artifact must not already exist: %s\n' "$OUTPUT_PATH" >&2
    exit 2
fi

if [[ -n "${DOCKER_BIN:-}" ]]; then
    if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
        printf 'Docker executable is unavailable: %s\n' "$DOCKER_BIN" >&2
        exit 2
    fi
else
    DOCKER_BIN=""
    for docker_candidate in "/usr/local/bin/docker" "/usr/bin/docker"; do
        if [[ -x "$docker_candidate" ]]; then
            DOCKER_BIN="$docker_candidate"
            break
        fi
    done
    if [[ -z "$DOCKER_BIN" ]]; then
        DOCKER_BIN="$(command -v docker || true)"
    fi
    if [[ -z "$DOCKER_BIN" ]]; then
        printf 'Docker executable is unavailable: docker\n' >&2
        exit 2
    fi
fi

if [[ -n "${SUDO_UID:-}" && ! "${SUDO_UID}" =~ ^[0-9]+$ ]]; then
    printf 'SUDO_UID must be a non-negative integer\n' >&2
    exit 2
fi
if [[ -n "${SUDO_GID:-}" && ! "${SUDO_GID}" =~ ^[0-9]+$ ]]; then
    printf 'SUDO_GID must be a non-negative integer\n' >&2
    exit 2
fi
if [[ -n "${SUDO_UID:-}" && -n "${SUDO_GID:-}" ]]; then
    CONTAINER_UID="$SUDO_UID"
    CONTAINER_GID="$SUDO_GID"
else
    CONTAINER_UID="$(id -u)"
    CONTAINER_GID="$(id -g)"
    if [[ ! "$CONTAINER_UID" =~ ^[0-9]+$ || ! "$CONTAINER_GID" =~ ^[0-9]+$ ]]; then
        printf 'host UID and GID must be non-negative integers\n' >&2
        exit 2
    fi
fi
DOCKER_USER_ARGS=(--user "$CONTAINER_UID:$CONTAINER_GID")

WAV_PATH="$(cd -- "$(dirname -- "$WAV_PATH")" && pwd -P)/$(basename -- "$WAV_PATH")"
MODEL_CACHE_DIRECTORY="$(cd -- "$MODEL_CACHE_DIRECTORY" && pwd -P)"
VAD_PIPELINE_PARAMETERS="$(cd -- "$(dirname -- "$VAD_PIPELINE_PARAMETERS")" && pwd -P)/$(basename -- "$VAD_PIPELINE_PARAMETERS")"
OUTPUT_DIRECTORY="$(cd -- "$OUTPUT_DIRECTORY" && pwd -P)"
OUTPUT_FILENAME="$(basename -- "$OUTPUT_PATH")"

case "$VAD_PIPELINE_PARAMETERS" in
    "$MODEL_CACHE_DIRECTORY"/*) ;;
    *)
        printf 'VAD_PIPELINE_PARAMETERS must be inside MODEL_CACHE_DIRECTORY\n' >&2
        exit 2
        ;;
esac

"$DOCKER_BIN" run --rm \
    "${DOCKER_USER_ARGS[@]}" \
    --network none \
    --cpuset-cpus "$BENCHMARK_CPU" \
    --memory 4g \
    --read-only \
    --tmpfs /tmp:rw,mode=1777 \
    --cap-drop ALL \
    --mount "type=bind,source=$WAV_PATH,target=/source/input.wav,readonly" \
    --mount "type=bind,source=$MODEL_CACHE_DIRECTORY,target=/model-cache,readonly" \
    --mount "type=bind,source=$VAD_PIPELINE_PARAMETERS,target=/vad-parameters.json,readonly" \
    --mount "type=bind,source=$OUTPUT_DIRECTORY,target=/output" \
    --env HF_HUB_OFFLINE=1 \
    --env TRANSFORMERS_OFFLINE=1 \
    --env MODEL_CACHE_DIR=/model-cache \
    --env HOME=/tmp \
    --entrypoint python3 \
    "$IMAGE_TAG" \
    /app/vad_speech_intervals.py \
    --audio /source/input.wav \
    --model-id "$VAD_SEGMENTATION_MODEL_ID" \
    --pipeline-parameters /vad-parameters.json \
    --output "/output/$OUTPUT_FILENAME"

if [[ ! -s "$OUTPUT_DIRECTORY/$OUTPUT_FILENAME" ]]; then
    printf 'offline VAD did not produce a speech-interval JSON artifact\n' >&2
    exit 1
fi
printf 'Offline VAD speech intervals: %s\n' "$OUTPUT_DIRECTORY/$OUTPUT_FILENAME"

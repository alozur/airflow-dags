#!/usr/bin/env bash
# Build and run the standalone whisper.cpp TinyDiarize benchmark on the NAS host.
set -euo pipefail

SCRIPT_DIRECTORY="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
OUTPUT_BASE_DIRECTORY="${OUTPUT_BASE_DIRECTORY:-$SCRIPT_DIRECTORY/output}"
IMAGE_TAG="${IMAGE_TAG:-whisper-tdrz-benchmark:local}"

if [[ ! "${BENCHMARK_CPU:-}" =~ ^[0-9]+$ ]]; then
    printf 'BENCHMARK_CPU must be a single non-negative integer\n' >&2
    exit 2
fi

if [[ $# -ne 1 ]]; then
    printf 'usage: %s /absolute/path/to/source-video\n' "${0##*/}" >&2
    exit 2
fi
if [[ ! -f "$1" ]]; then
    printf 'source video must be a regular file: %s\n' "$1" >&2
    exit 2
fi

source_path="$(cd -- "$(dirname -- "$1")" && pwd -P)/$(basename -- "$1")"
mkdir -p "$OUTPUT_BASE_DIRECTORY"
output_directory="$(mktemp -d "$OUTPUT_BASE_DIRECTORY/run.XXXXXX")"

DOCKER_BIN="${DOCKER_BIN:-docker}"
if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
    DOCKER_BIN=/usr/local/bin/docker
fi
if [[ ! -x "$DOCKER_BIN" ]] && ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
    printf 'Docker executable is unavailable (tried %s and /usr/local/bin/docker)\n' \
        "${DOCKER_BIN}" >&2
    exit 2
fi

build_started_at="$SECONDS"
"$DOCKER_BIN" build \
    --iidfile "$output_directory/image-id.txt" \
    --tag "$IMAGE_TAG" \
    "$SCRIPT_DIRECTORY"
image_build_seconds=$((SECONDS - build_started_at))
image_identity="$IMAGE_TAG@$(<"$output_directory/image-id.txt")"

"$DOCKER_BIN" run --rm \
    --network none \
    --cpuset-cpus "$BENCHMARK_CPU" \
    --memory 4g \
    --read-only \
    --user "$(id -u):$(id -g)" \
    --mount "type=bind,source=$source_path,target=/source/input,readonly" \
    --mount "type=bind,source=$output_directory,target=/output" \
    --env "IMAGE_IDENTITY=$image_identity" \
    --env "IMAGE_BUILD_SECONDS=$image_build_seconds" \
    "$IMAGE_TAG"

printf 'Benchmark artifacts: %s\n' "$output_directory"

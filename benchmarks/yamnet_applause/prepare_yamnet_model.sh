#!/usr/bin/env bash
# Download the YAMNet module once into an absolute model cache. This is the only
# step that touches the network; inference then runs fully offline.
set -euo pipefail
umask 077

IMAGE_TAG="${IMAGE_TAG:-yamnet-applause-benchmark:local}"

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <absolute-model-cache-directory>\n' "${0##*/}" >&2
    exit 2
fi

MODEL_CACHE_DIRECTORY="$1"
case "$MODEL_CACHE_DIRECTORY" in
    /*) ;;
    *) printf 'model cache path must be absolute: %s\n' "$MODEL_CACHE_DIRECTORY" >&2; exit 2 ;;
esac
mkdir --parents "$MODEL_CACHE_DIRECTORY"
MODEL_CACHE_DIRECTORY="$(cd -- "$MODEL_CACHE_DIRECTORY" && pwd -P)"

DOCKER_BIN=""
for docker_candidate in "/usr/local/bin/docker" "/usr/bin/docker"; do
    if [[ -x "$docker_candidate" ]]; then DOCKER_BIN="$docker_candidate"; break; fi
done
[[ -z "$DOCKER_BIN" ]] && DOCKER_BIN="$(command -v docker || true)"
if [[ -z "$DOCKER_BIN" ]]; then printf 'Docker executable is unavailable\n' >&2; exit 2; fi

CONTAINER_UID="${SUDO_UID:-$(id -u)}"
CONTAINER_GID="${SUDO_GID:-$(id -g)}"

"$DOCKER_BIN" run --rm \
    --user "$CONTAINER_UID:$CONTAINER_GID" \
    --memory 4g \
    --read-only \
    --tmpfs /tmp:rw,mode=1777 \
    --cap-drop ALL \
    --mount "type=bind,source=$MODEL_CACHE_DIRECTORY,target=/model-cache" \
    --env MODEL_CACHE_DIR=/model-cache \
    --env HOME=/tmp \
    "$IMAGE_TAG" \
    --prepare

printf 'YAMNet model cached at: %s\n' "$MODEL_CACHE_DIRECTORY"

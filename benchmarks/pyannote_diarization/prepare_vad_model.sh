#!/usr/bin/env bash
# Download one reviewed Pyannote VAD segmentation model into an explicit cache.
set -euo pipefail
umask 077

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
VAD_SEGMENTATION_MODEL_ID="${VAD_SEGMENTATION_MODEL_ID:-pyannote/segmentation-3.0}"
export VAD_SEGMENTATION_MODEL_ID

usage() {
    printf 'usage: HF_TOKEN=<token> [VAD_SEGMENTATION_MODEL_ID=<model-id>] %s <absolute-model-cache-directory>\n' \
        "${0##*/}" >&2
}

if [[ -z "${HF_TOKEN:-}" ]]; then
    printf 'HF_TOKEN must be set in the environment\n' >&2
    exit 2
fi
if [[ -z "$VAD_SEGMENTATION_MODEL_ID" ]] || [[ "$VAD_SEGMENTATION_MODEL_ID" == *$'\n'* ]] || \
    [[ "$VAD_SEGMENTATION_MODEL_ID" == *$'\r'* ]]; then
    printf 'VAD_SEGMENTATION_MODEL_ID must be a non-empty model identifier without line breaks\n' >&2
    exit 2
fi
if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

MODEL_CACHE_DIRECTORY="$1"
case "$MODEL_CACHE_DIRECTORY" in
    /*) ;;
    *)
        printf 'model cache directory must be an absolute path: %s\n' "$MODEL_CACHE_DIRECTORY" >&2
        exit 2
        ;;
esac
if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
    printf 'Docker executable is unavailable: %s\n' "$DOCKER_BIN" >&2
    exit 2
fi

# When invoked through sudo, retain the calling unprivileged user's identity
# inside the staging container and on all cache files it creates.
HOST_UID="${SUDO_UID:-$(id -u)}"
HOST_GID="${SUDO_GID:-$(id -g)}"
if [[ ! "$HOST_UID" =~ ^[0-9]+$ || ! "$HOST_GID" =~ ^[0-9]+$ ]]; then
    printf 'host uid and gid must be numeric\n' >&2
    exit 2
fi

mkdir -p -- "$MODEL_CACHE_DIRECTORY"
MODEL_CACHE_DIRECTORY="$(cd -- "$MODEL_CACHE_DIRECTORY" && pwd -P)"
if (( EUID == 0 )); then
    chown --recursive "$HOST_UID:$HOST_GID" -- "$MODEL_CACHE_DIRECTORY"
fi

# This is the sole networked operation. The existing benchmark image loads only
# VAD_SEGMENTATION_MODEL_ID and can write only to the caller-provided cache.
"$DOCKER_BIN" run --rm \
    --pull never \
    --network bridge \
    --read-only \
    --tmpfs /tmp:rw,mode=1777 \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --user "$HOST_UID:$HOST_GID" \
    --mount "type=bind,source=$MODEL_CACHE_DIRECTORY,target=/model-cache" \
    --env HF_TOKEN \
    --env VAD_SEGMENTATION_MODEL_ID \
    --env HF_HOME=/model-cache \
    --env MODEL_CACHE_DIR=/model-cache \
    --env HOME=/tmp \
    --entrypoint python3 \
    "$IMAGE_TAG" \
    -c 'import os
import sys

try:
    import re

    from pyannote.audio import Model
    from pyannote.audio.pipelines import VoiceActivityDetection

    parameters = {
        "min_duration_on": 0.0,
        "min_duration_off": 0.0,
    }
    model = Model.from_pretrained(
        os.environ["VAD_SEGMENTATION_MODEL_ID"], token=os.environ["HF_TOKEN"]
    )
    if model is None:
        raise RuntimeError("model loading returned no model")
    VoiceActivityDetection(segmentation=model).instantiate(parameters)
except Exception as error:
    message = str(error).replace(os.environ["HF_TOKEN"], "[REDACTED]")
    message = re.sub(r"hf_[A-Za-z0-9_-]+", "[REDACTED]", message)
    message = " ".join(message.splitlines())[:500]
    print(
        f"VAD model preparation failed: {type(error).__name__}: {message}",
        file=sys.stderr,
    )
    raise SystemExit(1)
'

PARAMETERS_PATH="$MODEL_CACHE_DIRECTORY/vad-parameters.json"
printf '%s\n' \
    '{"min_duration_off":0.0,"min_duration_on":0.0}' \
    > "$PARAMETERS_PATH"
if (( EUID == 0 )); then
    chown "$HOST_UID:$HOST_GID" -- "$PARAMETERS_PATH"
fi
if [[ ! -s "$PARAMETERS_PATH" ]]; then
    printf 'VAD model preparation did not produce the reviewed parameter JSON\n' >&2
    exit 1
fi

printf 'Prepared VAD model %s in %s\n' "$VAD_SEGMENTATION_MODEL_ID" "$MODEL_CACHE_DIRECTORY"

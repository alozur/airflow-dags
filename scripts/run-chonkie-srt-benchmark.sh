#!/usr/bin/env bash
# Run only the isolated Chonkie SRT benchmark image against the approved NAS file.
set -euo pipefail

readonly APPROVED_SOURCE="/volume1/docker/airflow/congress_videos/downloads/2026-07-30/QQIRmbU7UJ0/srt_files/QQIRmbU7UJ0_merged.srt"
readonly IMAGE_TAG="chonkie-srt-benchmark:1.7.0"

usage() {
  cat <<'EOF'
Usage:
  scripts/run-chonkie-srt-benchmark.sh --source <approved-nas-srt> --output-dir <local-output-dir>

The source must be the approved NAS SRT path. The output directory is mounted read-write
and receives chonkie-srt-benchmark.json.
EOF
}

source_file=""
output_dir=""
while (($#)); do
  case "$1" in
    --source)
      source_file="${2:-}"
      shift 2
      ;;
    --output-dir)
      output_dir="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$source_file" != "$APPROVED_SOURCE" ]]; then
  printf 'Refusing unapproved source. Expected exactly: %s\n' "$APPROVED_SOURCE" >&2
  exit 2
fi
if [[ ! -f "$source_file" || ! -r "$source_file" ]]; then
  printf 'Approved NAS source is unavailable or unreadable: %s\n' "$source_file" >&2
  exit 2
fi
if [[ -z "$output_dir" ]]; then
  printf '%s\n' '--output-dir is required' >&2
  usage >&2
  exit 2
fi
docker_bin="${DOCKER_BIN:-/usr/local/bin/docker}"
if [[ ! -x "$docker_bin" ]]; then
  docker_bin="$(command -v docker || true)"
fi
if [[ -z "$docker_bin" ]]; then
  printf '%s\n' 'docker is required to run the isolated benchmark' >&2
  exit 127
fi

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd -P)"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

"$docker_bin" build \
  --tag "$IMAGE_TAG" \
  --file "$repo_root/benchmarks/chonkie_srt/Dockerfile" \
  "$repo_root/benchmarks/chonkie_srt"

"$docker_bin" run --rm \
  --network none \
  --cpus=1 \
  --memory=4g \
  --pids-limit=256 \
  --read-only \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --user "$(id -u):$(id -g)" \
  --env HF_HUB_OFFLINE=1 \
  --env TRANSFORMERS_OFFLINE=1 \
  --mount "type=bind,src=$source_file,dst=/input/source.srt,readonly" \
  --mount "type=bind,src=$output_dir,dst=/output" \
  "$IMAGE_TAG" \
  --source /input/source.srt \
  --output /output/chonkie-srt-benchmark.json

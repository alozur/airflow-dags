# whisper.cpp TinyDiarize NAS benchmark

Standalone CPU benchmark for the first **1,800 seconds** of one source video. It does not use Airflow, Compose, or Chonkie.

## Contract

- Input: one regular video file at least 1,800 seconds long. The host runner mounts it read-only at `/source/input`.
- Image build: `Dockerfile` clones the official [`ggml-org/whisper.cpp`](https://github.com/ggml-org/whisper.cpp) CLI at `v1.7.4` and downloads the official `small.en-tdrz` GGML model during `docker build`.
- Runtime: ffprobe records full media duration, then ffmpeg writes exactly the first 1,800 seconds as mono 16 kHz PCM WAV. `whisper-cli -m <model> -f <wav> -tdrz -ng 0` runs once, CPU-only, with no network.
- Output: an isolated host directory contains a timestamped WAV, raw `whisper-cli` output, image ID, and a timestamped JSON report. The report deliberately excludes transcript text; inspect the raw log if needed.
- The report separates host image-build time, audio extraction time, diarization time, and end-to-end container time. It also separates full source duration from the fixed processed sample duration.

TinyDiarize (`small.en-tdrz`) detects **speaker turns** (`[SPEAKER_TURN]` markers). It does **not** identify or persist stable speaker identities. The selected model is English-oriented; Spanish source audio is intentional for this benchmark and may reduce transcription quality. The JSON marker count is therefore a turn-marker count, not a speaker count.

## NAS invocation

Before running on the NAS, choose one CPU explicitly:

1. Read the online CPU list:
   ```bash
   cat /sys/devices/system/cpu/online
   ```
2. Sample per-core load and choose the least-loaded CPU from that online list:
   ```bash
   mpstat -P ALL 1 3
   ```
3. Pass that CPU number to the runner:
   ```bash
   BENCHMARK_CPU=<n> ./benchmarks/whisper_tdrz/run_nas_benchmark.sh \
     /nas/path/to/spanish-source-video.mp4
   ```

`BENCHMARK_CPU` is required and must be one non-negative integer. The runner passes it only with Docker `--cpuset-cpus`; it never uses `--cpus`. This is NAS-specific: the verified Docker cgroup-v1 CPU-quota failure came from the old `--cpus` setting. That failure is a container-runtime configuration result, not an inference result.

The runner uses `docker` when configured and falls back to `/usr/local/bin/docker`. It creates a private, isolated output directory under `benchmarks/whisper_tdrz/output/` (or `OUTPUT_BASE_DIRECTORY`) and runs the container with `--network none`, a 4 GiB memory cap, a read-only root filesystem, a read-only source mount, and only the output mount writable.

No Docker image, model download, or NAS workload has been run by this harness setup.

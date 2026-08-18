# Pyannote CPU NAS diarization benchmark

Standalone calibration harness for `pyannote/speaker-diarization-community-1`. It downloads the first **600 seconds** of a runtime-supplied YouTube URL, creates mono 16 kHz PCM audio, measures CPU diarization, and linearly extrapolates diarization time for the full video. It does not use Airflow, Docker Compose, or existing containers.

## Usage

Accept the model's Hugging Face access conditions first, then run from an unprivileged NAS account with Docker access:

```bash
BENCHMARK_CPU=7 \
HF_TOKEN='hf_...' \
./benchmarks/pyannote_diarization/run_nas_benchmark.sh \
  'https://www.youtube.com/watch?v=VIDEO_ID' \
  '/absolute/nas/path/pyannote-benchmark-output'
```

`BENCHMARK_CPU`, `HF_TOKEN`, the YouTube URL, and an absolute output directory are required. `BENCHMARK_CPU` must be one non-negative CPU number. Set `MODEL_CACHE_DIRECTORY=/absolute/path` to persist or reuse the model cache somewhere other than `<output-directory>/model-cache`.

The runner creates a private `run.*` directory containing the 600-second WAV, image ID/build timing, full-duration metadata, and `summary.json`. It prints the linear full-video diarization estimate. The estimate covers diarization time only; download, model staging, and container/image startup are not extrapolated.

## Security and isolation

- The Docker image is Python 3.11 with CPU-only PyTorch, `pyannote.audio`, `yt-dlp`, and `ffmpeg`; it has a non-root default user and contains no Hugging Face token.
- `HF_TOKEN` is read only from the environment. Neither the token nor transcript text is written to the JSON summary or runner artifacts. The summary contains timings, anonymous speaker clusters, timestamped turns, and anonymous-label changes only.
- The staging container is the only networked container. It downloads the calibration audio and model into writable output/cache mounts.
- Inference runs with `--network none`, a single `--cpuset-cpus` CPU, `--memory 4g`, a read-only root filesystem, and read-only audio/cache mounts. Only the selected run directory and a temporary in-memory filesystem are writable.
- The runner removes its transient download staging directory on exit. Run artifacts and the model cache remain for inspection and cache reuse.

No image build, model download, YouTube request, or Docker execution has been performed while adding this harness.

## Candidate cuts from an existing summary

After a run has produced `summary.json`, derive a separate JSON artifact without rerunning Pyannote, Docker, or any network operation:

```bash
RUN_DIRECTORY=/absolute/nas/path/pyannote-benchmark-output/run.123456789
python3 benchmarks/pyannote_diarization/candidate_intervals.py \
  --summary "$RUN_DIRECTORY/summary.json" \
  --output "$RUN_DIRECTORY/candidate-intervals.json"
```

The CLI validates `raw_turns`, clamps them to `full_video_duration_seconds`, unions overlapping active diarization intervals, and emits gaps of at least `--min-gap-seconds` (default `3.0`) as `NO_DIARIZED_SPEECH` candidates.

These are raw diarization gaps only. They are candidate cuts, not classified non-speech, and do not classify applause. Audio classification and validation are required before using them as cuts.

## Offline VAD validation of candidate cuts

VAD can rule out a `NO_DIARIZED_SPEECH` candidate when it finds voice activity in that interval, without rerunning speaker diarization. It does **not** classify applause: `CONFIRMED_NO_VOICE` means only that this VAD pass found no voice activity, while applause classification remains a separate step.

Prepare the VAD model in a separate explicit networked step. It uses the existing benchmark image, defaults to `pyannote/segmentation-3.0`, and writes the reviewed `vad-parameters.json` (`min_duration_on=0.0`, `min_duration_off=0.0`) into the same cache. This powerset model fixes both onset and offset thresholds at `0.5`; they are not configurable and must not appear in the parameter file. The cache path must be absolute; set `VAD_SEGMENTATION_MODEL_ID` only to use a different reviewed model. Build or otherwise make the configured `IMAGE_TAG` available first; this script neither builds nor pulls an image.

```bash
HF_TOKEN='hf_...' \
./benchmarks/pyannote_diarization/prepare_vad_model.sh \
  '/absolute/nas/path/pyannote-model-cache'
```

The preparation container has network access only for this download, uses a read-only root filesystem, drops capabilities, and can write only to the supplied cache. It never runs Airflow or Docker Compose. When invoked through `sudo`, the cache remains owned by the original caller's UID/GID. `HF_TOKEN` is passed only as a container environment variable and is never printed.

The offline adapter requires `pyannote.audio` v4, imports `pyannote.audio.pipelines.VoiceActivityDetection`, loads the specified segmentation model while `HF_HUB_OFFLINE=1`, and instantiates the supplied reviewed parameters. If the installed API, cached model, or parameter contract is incompatible, it fails closed without downloading a model.

Given an existing full-run WAV, an existing read-only model cache, and a new output path that does not yet exist, produce a separate transcript-free VAD artifact with this explicit NAS command:

```bash
BENCHMARK_CPU=7 \
VAD_SEGMENTATION_MODEL_ID='pyannote/segmentation-3.0' \
VAD_PIPELINE_PARAMETERS='/absolute/nas/path/pyannote-model-cache/vad-parameters.json' \
bash ./benchmarks/pyannote_diarization/run_vad_speech_intervals.sh \
  '/absolute/nas/path/existing-full-run.wav' \
  '/absolute/nas/path/pyannote-model-cache' \
  '/absolute/nas/path/vad-speech-intervals.json'
```

The command uses one pinned CPU, `--memory 4g`, `--network none`, a read-only WAV, and a read-only model cache. Its only writable mount is the output artifact's parent directory; use a dedicated output directory containing no other files. It never stages media, calls YouTube, downloads models, or reruns speaker diarization.

Then compare the existing candidates and VAD evidence in a separate deterministic artifact:

```bash
python3 benchmarks/pyannote_diarization/vad_validation.py \
  --candidates '/absolute/nas/path/candidate-intervals.json' \
  --vad-speech-intervals '/absolute/nas/path/vad-speech-intervals.json' \
  --output '/absolute/nas/path/vad-validated-candidates.json'
```

Each result preserves the candidate and VAD source references plus the indexes of overlapping VAD intervals. A positive-duration overlap is `REJECTED_VOICE_PRESENT`; no overlap is `CONFIRMED_NO_VOICE`. Intervals that only touch at an endpoint do not overlap.

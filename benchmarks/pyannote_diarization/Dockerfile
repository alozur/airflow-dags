FROM python:3.11-slim

LABEL org.opencontainers.image.description="CPU-only Pyannote speaker-diarization NAS benchmark"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bash ca-certificates ffmpeg findutils \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch only from the official CPU wheel index before pyannote resolves its
# dependencies, so the image does not pull CUDA-enabled PyTorch packages.
RUN pip install --no-cache-dir \
        --index-url https://download.pytorch.org/whl/cpu \
        torch==2.5.1+cpu torchaudio==2.5.1+cpu \
    && pip install --no-cache-dir \
        "pyannote.audio>=4.0,<5.0" \
        "yt-dlp>=2025.1.0"

RUN groupadd --gid 10001 benchmark \
    && useradd --uid 10001 --gid benchmark --create-home benchmark \
    && mkdir --parents /app /model-cache /output /source /staging \
    && chown --recursive benchmark:benchmark /app /model-cache /output /source /staging

COPY entrypoint.py /app/entrypoint.py
COPY vad_speech_intervals.py /app/vad_speech_intervals.py
RUN chmod 0555 /app/entrypoint.py /app/vad_speech_intervals.py

ENV PYTHONDONTWRITEBYTECODE=1 \
    HF_HOME=/model-cache \
    MODEL_CACHE_DIR=/model-cache \
    HOME=/tmp

WORKDIR /app
USER benchmark
ENTRYPOINT ["/app/entrypoint.py"]

FROM python:3.11-slim

LABEL org.opencontainers.image.description="CPU-only YAMNet (TFLite) applause-detection sidecar API"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bash ca-certificates findutils curl \
    && rm -rf /var/lib/apt/lists/*

# LiteRT/TFLite runtime is a few MB versus the ~2 GB TensorFlow build. soundfile
# reads the pre-generated 16 kHz mono WAV; numpy handles the waveform framing.
RUN pip install --no-cache-dir \
        "tflite-runtime>=2.14,<2.15" \
        "soundfile>=0.12,<0.13" \
        "numpy>=1.26,<2.0" \
        "fastapi>=0.110,<1.0" \
        "uvicorn[standard]>=0.29,<1.0" \
        "python-multipart>=0.0.9"

RUN groupadd --gid 10001 benchmark \
    && useradd --uid 10001 --gid benchmark --create-home benchmark \
    && mkdir --parents /app /model-cache /output /source \
    && chown --recursive benchmark:benchmark /app /model-cache /output /source

COPY yamnet_applause.py /app/yamnet_applause.py
COPY server.py /app/server.py
RUN chmod 0555 /app/yamnet_applause.py /app/server.py

ENV PYTHONDONTWRITEBYTECODE=1 \
    MODEL_CACHE_DIR=/model-cache \
    HOME=/tmp \
    YAMNET_API_PORT=8081

WORKDIR /app
USER benchmark

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${YAMNET_API_PORT:-8081}/health || exit 1

ENTRYPOINT ["sh", "-c", "exec uvicorn server:app --host 0.0.0.0 --port ${YAMNET_API_PORT:-8081}"]

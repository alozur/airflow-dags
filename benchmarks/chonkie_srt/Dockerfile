FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/opt/huggingface \
    SENTENCE_TRANSFORMERS_HOME=/opt/huggingface/sentence-transformers

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt

# Download the only model before the runtime container is network-isolated.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('intfloat/multilingual-e5-small')"

RUN groupadd --gid 10001 benchmark \
    && useradd --uid 10001 --gid benchmark --create-home benchmark \
    && chown -R benchmark:benchmark /app /opt/huggingface

COPY benchmark_srt.py ./
RUN chown benchmark:benchmark benchmark_srt.py

USER benchmark

ENTRYPOINT ["python", "/app/benchmark_srt.py"]

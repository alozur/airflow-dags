# Build the official ggml-org/whisper.cpp CLI and its required TDRZ model.
FROM debian:bookworm-slim AS builder

ARG WHISPER_CPP_REF=v1.7.4

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential ca-certificates cmake curl git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone https://github.com/ggml-org/whisper.cpp.git \
    && cd whisper.cpp \
    && git checkout "$WHISPER_CPP_REF" \
    && cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --config Release --target whisper-cli -j"$(nproc)" \
    && ./models/download-ggml-model.sh small.en-tdrz

FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/ggml-org/whisper.cpp" \
      org.opencontainers.image.description="Standalone CPU TinyDiarize benchmark"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bash ca-certificates ffmpeg libgomp1 libstdc++6 python3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 benchmark \
    && useradd --uid 10001 --gid benchmark --create-home benchmark \
    && mkdir /app /models /output /source \
    && touch /source/input \
    && chown benchmark:benchmark /app /output

COPY --from=builder /src/whisper.cpp/build/bin/whisper-cli /usr/local/bin/whisper-cli
# whisper-cli is dynamically linked; ship whisper.cpp and ggml shared libraries with it.
COPY --from=builder /src/whisper.cpp/build/src/ /opt/whisper-lib/
COPY --from=builder /src/whisper.cpp/build/ggml/src/ /opt/whisper-lib/
COPY --from=builder /src/whisper.cpp/models/ggml-small.en-tdrz.bin /models/ggml-small.en-tdrz.bin
COPY benchmark_report.py entrypoint.sh /app/
RUN chmod 0555 /app/entrypoint.sh /app/benchmark_report.py

ENV PYTHONDONTWRITEBYTECODE=1 \
    LD_LIBRARY_PATH=/opt/whisper-lib \
    MODEL_PATH=/models/ggml-small.en-tdrz.bin \
    MODEL_IDENTITY="ggml-org/whisper.cpp small.en-tdrz" \
    OUTPUT_DIRECTORY=/output

WORKDIR /app
USER benchmark
ENTRYPOINT ["/app/entrypoint.sh"]

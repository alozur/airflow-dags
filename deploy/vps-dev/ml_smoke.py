"""Prove the ML sidecars answer offline with pre-seeded models; print counts, never payloads."""

import io
import math
import random
import struct
import sys
import wave

import requests

DIARIZE = "http://diarize-api:8080"
YAMNET = "http://yamnet-api:8081"
WHISPER = "http://whisper-api:9000"
SAMPLE_RATE = 16000
SECONDS = 3


def synthetic_wav():
    """Three seconds of a 440 Hz tone with deterministic noise, 16 kHz mono 16-bit PCM."""
    rng = random.Random(7)
    frames = bytearray()
    for index in range(SECONDS * SAMPLE_RATE):
        sample = 0.3 * math.sin(2 * math.pi * 440 * index / SAMPLE_RATE) + 0.02 * (rng.random() - 0.5)
        frames += struct.pack("<h", int(max(-1.0, min(1.0, sample)) * 32767))
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(SAMPLE_RATE)
        output.writeframes(bytes(frames))
    return buffer.getvalue()


def probe(name, url):
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"{name} health returned HTTP {response.status_code}")
    print(f"{name}=healthy")


def infer(name, url, wav, key, timeout, data=None, params=None):
    files = {"audio_file": ("smoke.wav", wav, "audio/wav")}
    response = requests.post(url, files=files, data=data, params=params, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"{name} inference returned HTTP {response.status_code}")
    if key is None:
        print(f"{name}=ok")
        return
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get(key), list):
        raise RuntimeError(f"{name} response lacks list field {key}")
    print(f"{name}=ok {key}={len(payload[key])}")


def main():
    wav = synthetic_wav()
    probe("diarize", f"{DIARIZE}/health")
    probe("yamnet", f"{YAMNET}/health")
    probe("whisper", f"{WHISPER}/docs")
    infer("yamnet", f"{YAMNET}/detect", wav, "applause_intervals", 300, data={"offset": "0"})
    infer("diarize", f"{DIARIZE}/diarize", wav, "speaker_changes", 900, data={"chapter_offset": "0"})
    whisper_params = {"encode": "true", "task": "transcribe", "language": "es", "output": "json"}
    infer("whisper", f"{WHISPER}/asr", wav, None, 600, params=whisper_params)
    print("ML sidecars: offline inference proven on synthetic audio")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - report a short reason only
        print(f"ml_smoke=failed reason={type(error).__name__}: {error}")
        sys.exit(1)

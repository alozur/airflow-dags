# Sidecar Model APIs — Operations Guide

Two persistent FastAPI sidecars replace the former ephemeral `docker run`
wrappers (issue #109). Both containers live on `ml_api_network` (a Docker
bridge) and are reached by the Airflow scheduler via container-name DNS.

## Services

| Service | Port | Memory (burst) | Purpose |
|---------|------|----------------|---------|
| `diarize-api` | 8080 | 6 g | Pyannote speaker diarization (CPU) |
| `yamnet-api` | 8081 | 1 g | YAMNet TFLite applause detection (CPU) |

## RAM Prerequisites

The 6 g (`diarize-api`) and 1 g (`yamnet-api`) figures are **burst ceilings**
during active inference. When idle, each sidecar is a lightweight FastAPI
process holding no model in memory (see Sleep/Wake below). Plan for the
burst ceiling if both are likely to run inference concurrently:

- Airflow scheduler + webserver: ~2–3 g
- `diarize-api` burst (pyannote CPU): 6 g
- `yamnet-api` burst (TFLite): 1 g
- **Total burst: at least 10 g free RAM**

Under the default idle-exit policy (15 minutes), steady-state RAM while idle
drops to ~200–300 MB per sidecar.

## Sleep / Wake Lifecycle (idle-exit)

Both sidecars use a lazy-load + idle-exit model (issue #113):

1. **Start**: process starts without loading the model; `/health` responds immediately.
2. **First inference**: model loads on demand (cold-start latency; see below).
3. **Subsequent inferences**: model is cached in memory; no reload.
4. **Idle timeout**: after `IDLE_TIMEOUT_SECONDS` of inactivity with no requests
   in-flight, the watchdog exits the process with code 0.
5. **Restart**: compose `restart: unless-stopped` relaunches the process light,
   returning to step 1.

The watchdog never exits while an inference request is in-flight. Existing
`retries: 3` absorbs the brief restart window without marking the container unhealthy.

### Cold-Start Latency

After an idle-exit restart, the first inference call incurs a model-load delay:

| Service | Cold-start estimate |
|---------|---------------------|
| `diarize-api` | 30–120 s (pyannote CPU, depends on NAS I/O) |
| `yamnet-api` | 2–5 s (TFLite, small model) |

Callers should use a read timeout ≥ 120 s for diarize-api, or retry with
backoff if the NAS is under I/O load.

### IDLE_TIMEOUT_SECONDS Tuning

Set via environment variable (default: 900 seconds / 15 minutes):

```bash
# In .env or docker-compose override:
IDLE_TIMEOUT_SECONDS=1800   # 30 minutes — reduce restart frequency
IDLE_TIMEOUT_SECONDS=300    # 5 minutes — free RAM faster
IDLE_TIMEOUT_SECONDS=0      # Disable watchdog; model stays resident once loaded (v1 behavior)
```

Pass it through compose (already wired in `docker-compose-ml-apis.yml`):

```bash
IDLE_TIMEOUT_SECONDS=1800 docker compose -f docker-compose-ml-apis.yml up -d
```

Setting `IDLE_TIMEOUT_SECONDS=0` or any negative value disables the watchdog
entirely; the model stays in memory once loaded. Use this if cold-start latency
is unacceptable or the NAS always has enough free RAM.

## HF_TOKEN Setup

`diarize-api` requires a Hugging Face access token to download the
`pyannote/speaker-diarization-community-1` model weights.

1. Generate a token at <https://huggingface.co/settings/tokens> with `read` scope.
2. Accept the model gate at <https://huggingface.co/pyannote/speaker-diarization-community-1>.
3. Add to `.env` (NAS `/volume1/docker/airflow/.env`):

   ```
   HF_TOKEN=hf_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   ```

## Pre-loading Model Weights (HF_HOME volume)

The pyannote model is ~1 GB. Pre-load it once so the container starts
immediately on restart without re-downloading:

```bash
# On the NAS, or any machine with Docker and HF_TOKEN set:
docker run --rm \
  -e HF_TOKEN="${HF_TOKEN}" \
  -e HF_HOME=/model-cache \
  -v hf_home:/model-cache \
  diarize-api:latest \
  sh -c "python3 -c \"from pyannote.audio import Pipeline; Pipeline.from_pretrained('pyannote/speaker-diarization-community-1', use_auth_token='${HF_TOKEN}')\""
```

For `yamnet-api`, run the prepare script bundled in the image:

```bash
docker run --rm \
  -v yamnet_model_cache:/model-cache \
  yamnet-api:latest \
  sh -c "python3 /app/yamnet_applause.py --prepare"
```

## Starting the Sidecars

```bash
# From the repo root on the NAS:
docker compose -f docker-compose-ml-apis.yml up -d --build
```

Check health:

```bash
docker compose -f docker-compose-ml-apis.yml ps
curl http://localhost:8080/health   # {"status": "ok"}
curl http://localhost:8081/health   # {"status": "ok"}
```

## Network Configuration

The `ml_api_network` bridge is defined in `docker-compose-ml-apis.yml` with
`driver: bridge`. Airflow (dev and prod) joins it as `external: true` so the
scheduler reaches each sidecar by container-name DNS:

- `http://diarize-api:8080` — resolved by `DIARIZE_API_HOST=diarize-api` (default)
- `http://yamnet-api:8081` — resolved by `YAMNET_API_HOST=yamnet-api` (default)

If the Airflow containers are on a different host, override the env vars:

```
DIARIZE_API_HOST=<NAS-IP>
YAMNET_API_HOST=<NAS-IP>
```

## Rollback

Each PR is independently revertible:

- PR1 (diarize-api): revert `feat(diarize-api)` commit; restore
  `speaker_turns_docker.py`; drop `ml_api_network` from compose files.
- PR2 (yamnet-api): revert `feat(yamnet-api)` commit; restore
  `trim_proposals_docker.py`; remove yamnet-api from `docker-compose-ml-apis.yml`.
- PR3 (idle-exit): revert this commit; servers return to eager-resident behavior
  (#109 state), independently revertible without affecting PR1 or PR2.

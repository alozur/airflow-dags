# Sidecar Model APIs — Operations Guide

Two persistent FastAPI sidecars replace the former ephemeral `docker run`
wrappers (issue #109). Both containers live on `ml_api_network` (a Docker
bridge) and are reached by the Airflow scheduler via container-name DNS.

## Services

| Service | Port | Memory | Purpose |
|---------|------|--------|---------|
| `diarize-api` | 8080 | 6 g | Pyannote speaker diarization (CPU) |
| `yamnet-api` | 8081 | 1 g | YAMNet TFLite applause detection (CPU) |

## RAM Prerequisites

Check total free memory on the NAS before starting both sidecars alongside
Airflow. Minimum reserved:

- Airflow scheduler + webserver: ~2–3 g
- `diarize-api` (pyannote CPU): 6 g
- `yamnet-api` (TFLite): 1 g
- **Total: at least 10 g free RAM**

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

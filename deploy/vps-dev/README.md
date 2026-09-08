# Standalone VPS DEV image

This overlay is intentionally independent of the NAS Compose stack. It embeds
the reviewed DEV source into a read-only DAG tree, uses fresh project volumes,
and gives every runtime container only an internal network. The runtime
publishes no Docker ports: the webserver has a fixed address inside the
internal network, and the host relay owned by `homeserver-config/ansible/vps-dev`
exposes it on the VPS loopback only. All business DAGs start paused and
examples are disabled.

Deployment is owned by `homeserver-config/ansible/vps-dev/run.sh`; use that
entrypoint rather than a bare `docker compose up`. It creates persistent keys,
starts PostgreSQL, runs `init.py`, then starts consumers and runs `verify.py`.
The complete launch, acceptance, backup/restore, and rollback runbook is in
`homeserver-config/ansible/vps-dev/README.md`.

`test_contract.py` is a standalone local YAML/static test, not a DAG import or
full application test. Run it with a Python environment containing PyYAML.
`verify.py` imports real DAGs **only inside the isolated deployed runtime**.
Do not execute that verifier directly on a workstation with production access.

The Dockerfile uses the source revision's frozen `uv.lock`, but OS packages
are not snapshotted. Preserve the built image and recorded image ID for exact
reuse.

The three ML sidecars (`whisper-api`, `yamnet-api`, `diarize-api`) run on the
same internal network with model volumes pre-seeded by Ansible: no runtime
download and no Hugging Face token exist on the VPS. `diarize-api` and
`yamnet-api` are built from `benchmarks/` at the release revision; `whisper-api`
is the third-party image pinned by digest. Everything is CPU-only.
`ml_smoke.py` runs inside the scheduler and proves each sidecar answers on
synthetic audio. Airflow's local Whisper path is intentionally absent from the
frozen image, so DEV transcription goes through `whisper-api` text-only (no
SRT), a known functional gap versus production's start-time pip install.
Production OAuth and the business database are absent. This foundation does
not prove the full video pipeline works; no runtime claim follows from the
passing static contracts.

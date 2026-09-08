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
reuse. Local Whisper, YAMNet, diarization services, models, production OAuth,
and the business database are absent. This foundation does not prove video
processing works. Build and deployment are pending; no runtime claim follows
from the passing static contracts.

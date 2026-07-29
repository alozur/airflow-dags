#!/usr/bin/env bash
# scripts/dag-paths-changed.sh — conditional-trigger gate for the e2e smoke test.
#
# Exit 0 = DAG-relevant paths changed -> run e2e.
# Exit 1 = no DAG-relevant paths changed -> skip e2e.
#
# Design reference: openspec/changes/test-runner-uv-docker-e2e/design.md, Q4.
#
# BASE_REF resolution chain: origin/dev (this repo's integration branch) ->
# origin/main -> HEAD~1 (detached/shallow-checkout/no-remote fallback).
#
# Locked glob set (do NOT expand; see design/spec):
#   congress_videos/**, examples/**, utils/**, docker-compose*.yml, Dockerfile
set -euo pipefail

BASE_REF="${E2E_DIFF_BASE:-origin/dev}"
if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
  if git rev-parse --verify --quiet origin/main >/dev/null; then
    BASE_REF="origin/main"
  else
    BASE_REF="HEAD~1"
  fi
fi

MERGE_BASE="$(git merge-base HEAD "$BASE_REF" 2>/dev/null || echo "$BASE_REF")"

# Locked glob set -> extended-regex (do NOT expand).
PATTERN='^(congress_videos/|examples/|utils/|docker-compose[^/]*\.yml$|Dockerfile$)'

if git diff --name-only "$MERGE_BASE"...HEAD | grep -Eq "$PATTERN"; then
  exit 0   # DAG-relevant paths changed -> run e2e
else
  exit 1   # no DAG-relevant paths -> skip e2e
fi

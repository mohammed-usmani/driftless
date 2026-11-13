#!/usr/bin/env bash
# Fetches the Ask Ubuntu demo dataset (CC BY-SA) into ops/demo-data/askubuntu.jsonl.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec uv run --no-project --python 3.12 ops/demo-data/fetch.py "$@"

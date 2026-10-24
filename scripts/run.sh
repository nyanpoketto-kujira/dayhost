#!/usr/bin/env bash
# DayHost dev runner.
#   scripts/run.sh            -> 127.0.0.1:8000, reload on
#   HOST=0.0.0.0 scripts/run.sh
#
# Production does NOT use this script; use deploy/dayhost.service (single worker!).
set -euo pipefail

cd "$(dirname "$0")/.."

PY_BIN=".venv/bin/python"
if [[ ! -x "$PY_BIN" ]]; then
  echo "error: $PY_BIN not found. Create the venv first (see README):" >&2
  echo "  uv venv --python 3.12 .venv && uv pip install --python $PY_BIN -r requirements.txt" >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "warn: .env not found — copy .env.example and fill it in." >&2
fi

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
RELOAD="${RELOAD:-1}"

ARGS=(app.main:app --host "$HOST" --port "$PORT")
if [[ "$RELOAD" == "1" ]]; then
  ARGS+=(--reload)
fi

echo "DayHost dev server -> http://$HOST:$PORT  (reload=$RELOAD, worker=1)"
exec .venv/bin/uvicorn "${ARGS[@]}"

#!/bin/bash
set -e

echo 'Starting transformer API...'

HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8000}

exec python -m uvicorn src.main:app --host $HOST --port $PORT

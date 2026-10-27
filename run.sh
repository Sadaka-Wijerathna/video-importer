#!/bin/bash
cd /opt/buddystore/video-importer
set -a
[ -f .env ] && . ./.env
set +a
exec ./venv/bin/uvicorn main:app --host 0.0.0.0 --port 10000

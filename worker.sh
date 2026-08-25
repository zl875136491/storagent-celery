#!/usr/bin/env bash
set -euo pipefail
worker_name="${CELERY_WORKER_NAME:-storagent-${REGION:-unknown}@%h}"
exec celery -A celery_app:app worker --beat --events --hostname="${worker_name}" --loglevel="${CELERY_LOG_LEVEL:-INFO}" --concurrency="${CELERY_WORKER_CONCURRENCY:-2}"

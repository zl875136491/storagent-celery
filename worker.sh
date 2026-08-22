#!/usr/bin/env bash
set -euo pipefail
exec celery -A celery_app:app worker --beat --loglevel="${CELERY_LOG_LEVEL:-INFO}" --concurrency="${CELERY_WORKER_CONCURRENCY:-2}"

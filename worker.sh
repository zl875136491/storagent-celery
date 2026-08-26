#!/usr/bin/env bash
set -euo pipefail
: "${REGION:?REGION is required}"
worker_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The embedded Beat process is spawned separately by Celery.  It does not
# consistently retain the CLI's current-directory import entry, so make the
# Worker and backend modules importable through the inherited environment.
backend_root="${STORAGENT_BACKEND_ROOT:-${worker_dir}/backend/storagent}"
export PYTHONPATH="${worker_dir}:${backend_root}${PYTHONPATH:+:${PYTHONPATH}}"
protocol="${CELERY_TASK_PROTOCOL_VERSION:-2}"
prefix="${CELERY_TASK_QUEUE_PREFIX:-storagent}"
region="$(printf '%s' "${REGION}" | tr '[:upper:]' '[:lower:]')"
if ! printf '%s' "${region}" | grep -Eq '^[a-z0-9][a-z0-9._-]{0,63}$'; then
  echo "invalid REGION for Celery queue: ${REGION}" >&2
  exit 2
fi
if ! printf '%s' "${prefix}" | grep -Eq '^[a-z0-9][a-z0-9._-]{0,63}$'; then
  echo "invalid CELERY_TASK_QUEUE_PREFIX: ${prefix}" >&2
  exit 2
fi
if ! printf '%s' "${protocol}" | grep -Eq '^[1-9][0-9]{0,7}$'; then
  echo "invalid CELERY_TASK_PROTOCOL_VERSION: ${protocol}" >&2
  exit 2
fi
worker_queue="${prefix}.${region}.v${protocol}"
export CELERY_TASK_QUEUE="${worker_queue}"
worker_name="${CELERY_WORKER_NAME:-storagent-${region}@%h}"
args=(worker --events --queues="${worker_queue}" --hostname="${worker_name}" --loglevel="${CELERY_LOG_LEVEL:-INFO}" --concurrency="${CELERY_WORKER_CONCURRENCY:-2}")
case "${CELERY_BEAT_ENABLED:-true}" in
  1|true|TRUE|yes|YES|on|ON)
    args+=(--beat --scheduler="scheduler:RegionMongoBeatScheduler")
    ;;
esac
exec celery -A celery_app:app "${args[@]}"

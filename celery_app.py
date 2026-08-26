"""Celery application for Storagent background work."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from celery import Celery
from kombu import Queue

_worker_dir = Path(__file__).resolve().parent
PROJECT_ROOT = _worker_dir.parent.parent
BACKEND_ROOT = Path(os.getenv("STORAGENT_BACKEND_ROOT", str(PROJECT_ROOT / "backend" / "storagent")))
if str(BACKEND_ROOT) not in sys.path:
  sys.path.insert(0, str(BACKEND_ROOT))

from src.core.celery_routing import (  # noqa: E402
  normalize_protocol_version,
  normalize_queue_prefix,
  normalize_region,
  task_headers,
  task_queue_name,
)


def _required(name: str) -> str:
  value = os.getenv(name, "").strip()
  if not value:
    raise RuntimeError(f"missing required environment variable: {name}")
  return value


def _as_bool(name: str, default: bool) -> bool:
  value = os.getenv(name, "").strip().lower()
  if not value:
    return default
  return value in {"1", "true", "yes", "on"}


def _schedule_entry(task: str, interval_env: str, default_interval: float) -> dict:
  return {
    "task": task,
    "schedule": float(os.getenv(interval_env, str(default_interval))),
    "options": {
      "queue": worker_queue,
      "routing_key": worker_queue,
      "headers": task_headers(region, protocol_version=protocol_version),
    },
  }


region = normalize_region(_required("REGION"))
protocol_version = normalize_protocol_version(os.getenv("CELERY_TASK_PROTOCOL_VERSION", "2"))
queue_prefix = normalize_queue_prefix(os.getenv("CELERY_TASK_QUEUE_PREFIX", "storagent"))
worker_queue = task_queue_name(
  region,
  queue_prefix=queue_prefix,
  protocol_version=protocol_version,
)
authority_region = normalize_region(os.getenv("SYNC_AUTHORITY_REGION", "beijing"))


broker_url = _required("CELERY_BROKER_URL")
result_backend = os.getenv("CELERY_RESULT_BACKEND", broker_url).strip() or broker_url
app = Celery("storagent", broker=broker_url, backend=result_backend, include=["tasks"])
beat_schedule = {
  "storagent.cleanup-expired-tokens": _schedule_entry(
    "storagent.auth.cleanup_expired_tokens", "AUTH_CLEANUP_INTERVAL_SECONDS", 3600,
  ),
  "storagent.etcd-reconcile": _schedule_entry(
    "storagent.etcd.reconcile", "SYNC_RECONCILE_INTERVAL_SECONDS", 30,
  ),
  "storagent.recover-queued-maintenance": _schedule_entry(
    "storagent.maintenance.recover_queued_tasks", "CELERY_OPERATION_WATCHDOG_INTERVAL_SECONDS", 60,
  ),
}
if _as_bool("OBJECT_ARCHIVE_ENABLED", False):
  beat_schedule["storagent.archive-expired-objects"] = _schedule_entry(
    "storagent.files.archive_expired_objects", "OBJECT_ARCHIVE_INTERVAL_SECONDS", 300,
  )
if region == authority_region:
  beat_schedule.update({
    "storagent.replication-reconcile": _schedule_entry(
      "storagent.replication.reconcile_policies", "REPLICATION_RECONCILE_INTERVAL_SECONDS", 300,
    ),
    "storagent.quota-aggregate-refresh": _schedule_entry(
      "storagent.public.refresh_quota_aggregates", "APPLICATION_QUOTA_AGGREGATE_INTERVAL_SECONDS", 3600,
    ),
    "storagent.capacity-snapshot": _schedule_entry(
      "storagent.capacity.snapshot", "CAPACITY_SNAPSHOT_INTERVAL_SECONDS", 3600,
    ),
  })
  if _as_bool("AUTO_HEAL_ENABLED", True):
    beat_schedule["storagent.cluster-health"] = _schedule_entry(
      "storagent.storage.monitor_cluster_health", "CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS", 120,
    )
app.conf.update(
  task_serializer="json",
  result_serializer="json",
  accept_content=["json"],
  timezone=os.getenv("TIMEZONE", "Asia/Shanghai"),
  enable_utc=True,
  task_track_started=True,
  task_send_sent_event=True,
  worker_send_task_events=True,
  task_default_queue=worker_queue,
  task_default_exchange=worker_queue,
  task_default_routing_key=worker_queue,
  task_queues=(Queue(worker_queue, routing_key=worker_queue),),
  task_create_missing_queues=True,
  task_acks_late=True,
  task_reject_on_worker_lost=True,
  worker_prefetch_multiplier=1,
  broker_transport_options={
    "ttl": True,
    "messages_collection": os.getenv("CELERY_MONGODB_MESSAGES_COLLECTION", "celery.messages"),
    "routing_collection": os.getenv("CELERY_MONGODB_ROUTING_COLLECTION", "celery.routing"),
    "queues_collection": os.getenv("CELERY_MONGODB_QUEUES_COLLECTION", "celery.queues"),
  },
  mongodb_backend_settings={
    "database": os.getenv("CELERY_MONGODB_RESULT_DATABASE", "storagent_celery"),
    "taskmeta_collection": os.getenv("CELERY_MONGODB_RESULT_COLLECTION", "celery_taskmeta"),
  },
  beat_schedule=beat_schedule,
)

# Register process-local signal handlers before the worker builds its event
# dispatcher. The handlers write only operational metadata, never task args.
import observability  # noqa: E402,F401

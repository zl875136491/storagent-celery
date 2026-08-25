"""Celery application for Storagent background work."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from celery import Celery

_worker_dir = Path(__file__).resolve().parent
PROJECT_ROOT = _worker_dir.parent.parent
BACKEND_ROOT = Path(os.getenv("STORAGENT_BACKEND_ROOT", str(PROJECT_ROOT / "backend" / "storagent")))
if str(BACKEND_ROOT) not in sys.path:
  sys.path.insert(0, str(BACKEND_ROOT))


def _required(name: str) -> str:
  value = os.getenv(name, "").strip()
  if not value:
    raise RuntimeError(f"missing required environment variable: {name}")
  return value


broker_url = _required("CELERY_BROKER_URL")
result_backend = os.getenv("CELERY_RESULT_BACKEND", broker_url).strip() or broker_url
app = Celery("storagent", broker=broker_url, backend=result_backend, include=["tasks"])
app.conf.update(
  task_serializer="json",
  result_serializer="json",
  accept_content=["json"],
  timezone=os.getenv("TIMEZONE", "Asia/Shanghai"),
  enable_utc=True,
  task_track_started=True,
  task_send_sent_event=True,
  worker_send_task_events=True,
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
  beat_schedule={
    "storagent.cleanup-expired-tokens": {"task": "storagent.auth.cleanup_expired_tokens", "schedule": float(os.getenv("AUTH_CLEANUP_INTERVAL_SECONDS", "3600"))},
    "storagent.archive-expired-objects": {"task": "storagent.files.archive_expired_objects", "schedule": float(os.getenv("OBJECT_ARCHIVE_INTERVAL_SECONDS", "300"))},
    "storagent.etcd-reconcile": {"task": "storagent.etcd.reconcile", "schedule": float(os.getenv("SYNC_RECONCILE_INTERVAL_SECONDS", "30"))},
    "storagent.replication-reconcile": {"task": "storagent.replication.reconcile_policies", "schedule": float(os.getenv("REPLICATION_RECONCILE_INTERVAL_SECONDS", "300"))},
    "storagent.quota-aggregate-refresh": {"task": "storagent.public.refresh_quota_aggregates", "schedule": float(os.getenv("APPLICATION_QUOTA_AGGREGATE_INTERVAL_SECONDS", "3600"))},
    "storagent.capacity-snapshot": {"task": "storagent.capacity.snapshot", "schedule": float(os.getenv("CAPACITY_SNAPSHOT_INTERVAL_SECONDS", "3600"))},
    "storagent.cluster-health": {"task": "storagent.storage.monitor_cluster_health", "schedule": float(os.getenv("CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS", "120"))},
  },
)

# Register process-local signal handlers before the worker builds its event
# dispatcher. The handlers write only operational metadata, never task args.
import observability  # noqa: E402,F401

"""Celery task entry points for Storagent."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable

from celery_app import app
from task_runtime import TaskEnvelopeError, require_task_origin


_PERIODIC_TASK_OPTIONS = {
  "autoretry_for": (Exception,),
  "dont_autoretry_for": (TaskEnvelopeError,),
  "retry_backoff": True,
  "retry_backoff_max": 300,
  "retry_jitter": True,
  "retry_kwargs": {"max_retries": 5},
}

_MANUAL_TASK_OPTIONS = {
  # A persisted operation has an external side effect. On an ambiguous worker
  # loss it is deliberately surfaced to the watchdog for review instead of
  # blindly replaying a MinIO/Etcd command.
  "autoretry_for": (),
  "throws": (TaskEnvelopeError,),
}

_IDEMPOTENT_TASK_OPTIONS = {
  "autoretry_for": (Exception,),
  "dont_autoretry_for": (TaskEnvelopeError,),
  "retry_backoff": True,
  "retry_backoff_max": 300,
  "retry_jitter": True,
  "retry_kwargs": {"max_retries": 5},
}


def _run(awaitable: Awaitable[Any]) -> Any:
  async def execute():
    from src.core.database import close_db, init_db
    await init_db()
    try:
      return await awaitable
    finally:
      await close_db()
  return asyncio.run(execute())


@app.task(bind=True, name="storagent.auth.cleanup_expired_tokens", **_PERIODIC_TASK_OPTIONS)
def cleanup_expired_tokens(self):
  require_task_origin(self)
  from src.modules.auth.crud import cleanup_expired_tokens_once
  return _run(cleanup_expired_tokens_once())


@app.task(bind=True, name="storagent.files.archive_expired_objects", **_PERIODIC_TASK_OPTIONS)
def archive_expired_objects(self):
  require_task_origin(self)
  from src.modules.files.archive import archive_expired_objects_once
  return _run(archive_expired_objects_once())


@app.task(bind=True, name="storagent.etcd.reconcile", **_PERIODIC_TASK_OPTIONS)
def reconcile_etcd(self):
  require_task_origin(self)
  from src.core.etcd_op import reconcile_etcd_once
  return _run(reconcile_etcd_once())


@app.task(bind=True, name="storagent.replication.reconcile_policies", **_PERIODIC_TASK_OPTIONS)
def reconcile_replication_policies(self):
  require_task_origin(self)
  from src.core.sync import reconcile_replication_policies_task
  return _run(reconcile_replication_policies_task(single_pass=True))


@app.task(bind=True, name="storagent.public.refresh_quota_aggregates", **_PERIODIC_TASK_OPTIONS)
def refresh_quota_aggregates(self):
  require_task_origin(self)
  from src.modules.public.service import refresh_application_quota_aggregates_once
  return _run(refresh_application_quota_aggregates_once())


@app.task(bind=True, name="storagent.capacity.snapshot", **_PERIODIC_TASK_OPTIONS)
def capacity_snapshot(self):
  require_task_origin(self)
  from src.modules.capacity.service import collect_snapshot
  return _run(collect_snapshot())


@app.task(bind=True, name="storagent.storage.monitor_cluster_health", **_PERIODIC_TASK_OPTIONS)
def monitor_cluster_health(self):
  require_task_origin(self)
  from src.modules.storage.operations import monitor_cluster_health_task
  return _run(monitor_cluster_health_task(single_pass=True))


@app.task(bind=True, name="storagent.etcd.execute", **_MANUAL_TASK_OPTIONS)
def execute_etcd_task(self, task_id: str, actor: str, revision: int | None = None):
  origin_region = require_task_origin(self)
  from src.modules.etcd.service import _execute_task
  return _run(_execute_task(task_id, actor, revision, origin_region=origin_region))


@app.task(bind=True, name="storagent.storage.execute_operation", **_MANUAL_TASK_OPTIONS)
def execute_storage_operation(self, operation_id: str):
  origin_region = require_task_origin(self)
  from src.modules.storage.operations import execute_storage_operation
  return _run(execute_storage_operation(operation_id, origin_region=origin_region))


@app.task(bind=True, name="storagent.audit.persist", **_IDEMPOTENT_TASK_OPTIONS)
def persist_audit(
  self,
  action: str,
  actor: str,
  resource: str,
  success: bool,
  detail: str,
  event_id: str = "",
):
  require_task_origin(self)
  from src.core.audit import persist_audit_event
  return _run(persist_audit_event(action, actor, resource, success, detail, event_id=event_id))


@app.task(bind=True, name="storagent.maintenance.recover_queued_tasks", **_PERIODIC_TASK_OPTIONS)
def recover_queued_tasks(self):
  require_task_origin(self)

  async def recover():
    from src.modules.etcd.service import recover_stale_tasks_once
    from src.modules.storage.operations import recover_stale_storage_operations_once

    storage, etcd = await asyncio.gather(
      recover_stale_storage_operations_once(),
      recover_stale_tasks_once(),
    )
    return {"storage": storage, "etcd": etcd}

  return _run(recover())

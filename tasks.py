"""Celery task entry points for Storagent."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable

from celery_app import app


def _run(awaitable: Awaitable[Any]) -> Any:
  async def execute():
    from src.core.database import close_db, init_db
    await init_db()
    try:
      return await awaitable
    finally:
      await close_db()
  return asyncio.run(execute())


@app.task(bind=True, name="storagent.auth.cleanup_expired_tokens", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def cleanup_expired_tokens(self):
  from src.modules.auth.crud import cleanup_expired_tokens_once
  return _run(cleanup_expired_tokens_once())


@app.task(bind=True, name="storagent.files.archive_expired_objects", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def archive_expired_objects(self):
  from src.modules.files.archive import archive_expired_objects_once
  return _run(archive_expired_objects_once())


@app.task(bind=True, name="storagent.etcd.reconcile", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def reconcile_etcd(self):
  from src.core.etcd_op import reconcile_etcd_once
  return _run(reconcile_etcd_once())


@app.task(bind=True, name="storagent.replication.reconcile_policies", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def reconcile_replication_policies(self):
  from src.core.sync import reconcile_replication_policies_task
  return _run(reconcile_replication_policies_task(single_pass=True))


@app.task(bind=True, name="storagent.public.refresh_quota_aggregates", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def refresh_quota_aggregates(self):
  from src.modules.public.service import refresh_application_quota_aggregates_once
  return _run(refresh_application_quota_aggregates_once())


@app.task(bind=True, name="storagent.capacity.snapshot", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def capacity_snapshot(self):
  from src.modules.capacity.service import collect_snapshot
  return _run(collect_snapshot())


@app.task(bind=True, name="storagent.storage.monitor_cluster_health", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def monitor_cluster_health(self):
  from src.modules.storage.operations import monitor_cluster_health_task
  return _run(monitor_cluster_health_task(single_pass=True))


@app.task(bind=True, name="storagent.etcd.execute", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def execute_etcd_task(self, task_id: str, actor: str, revision: int | None = None):
  from src.modules.etcd.service import _execute_task
  return _run(_execute_task(task_id, actor, revision))


@app.task(bind=True, name="storagent.storage.execute_operation", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def execute_storage_operation(self, operation_id: str):
  from src.modules.storage.operations import execute_storage_operation
  return _run(execute_storage_operation(operation_id))


@app.task(bind=True, name="storagent.audit.persist", autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def persist_audit(self, action: str, actor: str, resource: str, success: bool, detail: str):
  from src.core.audit import persist_audit_event
  return _run(persist_audit_event(action, actor, resource, success, detail))

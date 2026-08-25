"""Persist minimal, non-sensitive Celery worker and task lifecycle metadata."""
from __future__ import annotations

import os
import socket
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlparse

from celery import signals
from pymongo import ASCENDING, DESCENDING, MongoClient


_last_heartbeat_monotonic = 0.0


def _now() -> datetime:
  return datetime.now(timezone.utc)


def _database_name(url: str) -> str:
  try:
    path = unquote(urlparse(url).path or "").strip("/")
  except Exception:
    path = ""
  return os.getenv("CELERY_MONGODB_RESULT_DATABASE", "").strip() or path.split("/", 1)[0] or "storagent_celery"


def _result_backend_url() -> str:
  return os.getenv("CELERY_RESULT_BACKEND", "").strip() or os.environ["CELERY_BROKER_URL"]


def _worker_name() -> str:
  hostname = socket.gethostname()
  configured = os.getenv("CELERY_WORKER_NAME", "").strip()
  if configured:
    return configured.replace("%h", hostname).replace("%n", hostname.split(".", 1)[0])
  region = os.getenv("REGION", "unknown").strip() or "unknown"
  return f"storagent-{region}@{hostname}"


def _concurrency() -> int:
  try:
    return max(int(os.getenv("CELERY_WORKER_CONCURRENCY", "2")), 1)
  except ValueError:
    return 2


def _collections():
  timeout_ms = max(int(float(os.getenv("CELERY_OBSERVABILITY_TIMEOUT_SECONDS", "1.5")) * 1000), 500)
  client = MongoClient(
    _result_backend_url(),
    serverSelectionTimeoutMS=timeout_ms,
    connectTimeoutMS=timeout_ms,
  )
  database = client[_database_name(_result_backend_url())]
  return client, database[
    os.getenv("CELERY_TASK_HISTORY_COLLECTION", "celery_task_history")
  ], database[
    os.getenv("CELERY_WORKER_HEARTBEAT_COLLECTION", "celery_worker_heartbeats")
  ]


def _safe_summary(value: Any, limit: int = 500) -> str:
  if value is None:
    return ""
  text = str(value)
  text = " ".join(text.split())
  return text[:limit] + ("..." if len(text) > limit else "")


def _task_request(task: Any) -> dict[str, Any]:
  request = getattr(task, "request", None)
  delivery = getattr(request, "delivery_info", None)
  delivery = delivery if isinstance(delivery, dict) else {}
  headers = getattr(request, "headers", None)
  headers = headers if isinstance(headers, dict) else {}
  return {
    "task_id": str(getattr(request, "id", "") or ""),
    "task_name": str(getattr(task, "name", "") or ""),
    "worker": _worker_name(),
    "region": os.getenv("REGION", "").strip(),
    "queue": str(delivery.get("routing_key") or "celery"),
    "retries": int(getattr(request, "retries", 0) or 0),
    "periodic_task": str(headers.get("periodic_task_name") or ""),
  }


def _touch_worker(*, status: str = "online") -> None:
  now = _now()
  client = None
  try:
    client, _history, heartbeats = _collections()
    heartbeats.update_one(
      {"worker": _worker_name()},
      {
        "$set": {
          "worker": _worker_name(),
          "hostname": socket.gethostname(),
          "region": os.getenv("REGION", "").strip(),
          "status": status,
          "last_seen": now,
          "concurrency": _concurrency(),
          "beat_enabled": True,
        },
        "$setOnInsert": {"started_at": now},
      },
      upsert=True,
    )
  except Exception:
    # Observability must never delay or fail a storage/background task.
    return
  finally:
    if client is not None:
      client.close()


def _touch_heartbeat() -> None:
  global _last_heartbeat_monotonic
  minimum = max(float(os.getenv("CELERY_WORKER_HEARTBEAT_RECORD_SECONDS", "15")), 2.0)
  current = time.monotonic()
  if current - _last_heartbeat_monotonic < minimum:
    return
  _last_heartbeat_monotonic = current
  _touch_worker()


def _ensure_indexes() -> None:
  client = None
  try:
    client, history, heartbeats = _collections()
    history.create_index([("task_id", ASCENDING)], unique=True)
    history.create_index([("updated_at", DESCENDING)])
    heartbeats.create_index([("worker", ASCENDING)], unique=True)
  except Exception:
    return
  finally:
    if client is not None:
      client.close()


@signals.worker_ready.connect
def worker_ready(**_kwargs) -> None:
  _ensure_indexes()
  _touch_worker()


@signals.heartbeat_sent.connect
def heartbeat_sent(**_kwargs) -> None:
  _touch_heartbeat()


@signals.worker_shutdown.connect
def worker_shutdown(**_kwargs) -> None:
  _touch_worker(status="offline")


@signals.task_prerun.connect
def task_prerun(task_id: str, task: Any, **_kwargs) -> None:
  client = None
  try:
    now = _now()
    client, history, _heartbeats = _collections()
    payload = _task_request(task)
    payload["task_id"] = str(task_id or payload["task_id"])
    history.update_one(
      {"task_id": payload["task_id"]},
      {
        "$set": {
          **payload,
          "status": "STARTED",
          "started_at": now,
          "updated_at": now,
          "error": "",
        },
        "$setOnInsert": {"received_at": now},
      },
      upsert=True,
    )
  except Exception:
    return
  finally:
    if client is not None:
      client.close()


@signals.task_retry.connect
def task_retry(request: Any, reason: Any, **_kwargs) -> None:
  task_id = str(getattr(request, "id", "") or "")
  if not task_id:
    return
  client = None
  try:
    now = _now()
    client, history, _heartbeats = _collections()
    history.update_one(
      {"task_id": task_id},
      {
        "$set": {
          "status": "RETRY",
          "retries": int(getattr(request, "retries", 0) or 0) + 1,
          "error": _safe_summary(reason),
          "updated_at": now,
        },
      },
      upsert=False,
    )
  except Exception:
    return
  finally:
    if client is not None:
      client.close()


@signals.task_postrun.connect
def task_postrun(task_id: str, task: Any, state: str, retval: Any, **_kwargs) -> None:
  client = None
  try:
    now = _now()
    client, history, _heartbeats = _collections()
    payload = _task_request(task)
    payload["task_id"] = str(task_id or payload["task_id"])
    previous = history.find_one({"task_id": payload["task_id"]}, {"started_at": 1}) or {}
    started_at = previous.get("started_at")
    duration_ms = None
    if isinstance(started_at, datetime):
      if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
      duration_ms = max(int((now - started_at).total_seconds() * 1000), 0)
    update = {
      **payload,
      "status": str(state or "UNKNOWN"),
      "finished_at": now,
      "updated_at": now,
      "duration_ms": duration_ms,
    }
    if str(state) == "SUCCESS":
      update["result_summary"] = _safe_summary(retval)
      update["error"] = ""
    history.update_one(
      {"task_id": payload["task_id"]},
      {"$set": update, "$setOnInsert": {"received_at": now, "started_at": now}},
      upsert=True,
    )
  except Exception:
    return
  finally:
    if client is not None:
      client.close()


@signals.task_failure.connect
def task_failure(task_id: str, exception: Exception, **_kwargs) -> None:
  client = None
  try:
    now = _now()
    client, history, _heartbeats = _collections()
    history.update_one(
      {"task_id": str(task_id)},
      {
        "$set": {
          "status": "FAILURE",
          "error": _safe_summary(f"{type(exception).__name__}: {exception}"),
          "updated_at": now,
          "finished_at": now,
        },
      },
      upsert=False,
    )
  except Exception:
    return
  finally:
    if client is not None:
      client.close()

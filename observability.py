"""Persist minimal, non-sensitive Celery worker and task lifecycle metadata."""
from __future__ import annotations

import os
import json
import re
import socket
import time
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any
from urllib.parse import unquote, urlparse

from celery import signals
from pymongo import ASCENDING, MongoClient


_last_heartbeat_monotonic = 0.0
_client_lock = Lock()
_client: MongoClient | None = None
_client_pid = 0
_history = None
_heartbeats = None

_SAFE_RESULT_KEYS = frozenset({
  "status", "operation_id", "task_id", "processed", "succeeded", "failed",
  "skipped", "candidates", "archived", "queued_timeout", "running_timeout",
  "already_running", "recovery_required", "deferred",
})
_SENSITIVE_VALUE_RE = re.compile(
  r"(?i)(api[_-]?key|access[_-]?key|secret|token|password|authorization)"
  r"([=:]\s*)([^\s,;&]+)",
)
_CREDENTIAL_URL_RE = re.compile(r"(?i)((?:mongodb(?:\+srv)?|https?)://)[^/@\s]+@")
_QUERY_SECRET_RE = re.compile(
  r"(?i)([?&](?:api[_-]?key|access[_-]?key|secret|token|password)=)[^&#\s]+",
)


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
  global _client, _client_pid, _history, _heartbeats
  pid = os.getpid()
  timeout_ms = max(int(float(os.getenv("CELERY_OBSERVABILITY_TIMEOUT_SECONDS", "1.5")) * 1000), 500)
  with _client_lock:
    if _client is not None and _client_pid == pid:
      return _client, _history, _heartbeats
    if _client is not None:
      try:
        _client.close()
      except Exception:
        pass
    client = MongoClient(
      _result_backend_url(),
      serverSelectionTimeoutMS=timeout_ms,
      connectTimeoutMS=timeout_ms,
    )
    database = client[_database_name(_result_backend_url())]
    _client = client
    _client_pid = pid
    _history = database[os.getenv("CELERY_TASK_HISTORY_COLLECTION", "celery_task_history")]
    _heartbeats = database[
      os.getenv("CELERY_WORKER_HEARTBEAT_COLLECTION", "celery_worker_heartbeats")
    ]
    return _client, _history, _heartbeats


def _close_collections() -> None:
  global _client, _client_pid, _history, _heartbeats
  with _client_lock:
    if _client is not None:
      try:
        _client.close()
      except Exception:
        pass
    _client = None
    _client_pid = 0
    _history = None
    _heartbeats = None


def _redact(value: Any, limit: int = 500) -> str:
  if value is None:
    return ""
  text = str(value)
  text = " ".join(text.split())
  text = _CREDENTIAL_URL_RE.sub(r"\1***@", text)
  text = _QUERY_SECRET_RE.sub(r"\1***", text)
  text = _SENSITIVE_VALUE_RE.sub(r"\1\2***", text)
  return text[:limit] + ("..." if len(text) > limit else "")


def _safe_result_summary(value: Any) -> str:
  """Expose only stable operational counters, never task return payloads."""
  if not isinstance(value, dict):
    return "" if value is None else "已记录任务结果"
  safe: dict[str, bool | int | float | str] = {}
  for key in _SAFE_RESULT_KEYS:
    item = value.get(key)
    if isinstance(item, (bool, int, float)):
      safe[key] = item
    elif isinstance(item, str) and len(item) <= 128:
      safe[key] = _redact(item, limit=128)
  return json.dumps(safe, ensure_ascii=False, separators=(",", ":")) if safe else "已记录任务结果"


def _safe_error_summary(value: Any) -> str:
  if isinstance(value, BaseException):
    return f"{type(value).__name__}: {_redact(value, limit=500)}"
  return _redact(value, limit=500)


def _expires_at(now: datetime, name: str, default_days: int) -> datetime:
  try:
    days = max(int(os.getenv(name, str(default_days))), 1)
  except ValueError:
    days = default_days
  return now + timedelta(days=days)


def _beat_enabled() -> bool:
  return os.getenv("CELERY_BEAT_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


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
    "origin_region": str(headers.get("storagent-origin-region") or ""),
    "task_protocol": str(headers.get("storagent-task-protocol") or ""),
    "retries": int(getattr(request, "retries", 0) or 0),
    "periodic_task": str(headers.get("periodic_task_name") or ""),
  }


def _touch_worker(*, status: str = "online") -> None:
  now = _now()
  try:
    _client, _history, heartbeats = _collections()
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
          "beat_enabled": _beat_enabled(),
          "queue": os.getenv("CELERY_TASK_QUEUE", "").strip(),
          "task_protocol": os.getenv("CELERY_TASK_PROTOCOL_VERSION", "2").strip(),
          "expires_at": _expires_at(now, "CELERY_WORKER_HEARTBEAT_RETENTION_DAYS", 7),
        },
        "$setOnInsert": {"started_at": now},
      },
      upsert=True,
    )
  except Exception:
    # Observability must never delay or fail a storage/background task.
    return


def _touch_heartbeat() -> None:
  global _last_heartbeat_monotonic
  minimum = max(float(os.getenv("CELERY_WORKER_HEARTBEAT_RECORD_SECONDS", "15")), 2.0)
  current = time.monotonic()
  if current - _last_heartbeat_monotonic < minimum:
    return
  _last_heartbeat_monotonic = current
  _touch_worker()


def _ensure_indexes() -> None:
  try:
    _client, history, heartbeats = _collections()
    history.create_index([("task_id", ASCENDING)], unique=True)
    history.create_index(
      [("expires_at", ASCENDING)],
      expireAfterSeconds=0,
      name="celery_task_history_expiry_ttl",
    )
    heartbeats.create_index([("worker", ASCENDING)], unique=True)
    heartbeats.create_index(
      [("expires_at", ASCENDING)],
      expireAfterSeconds=0,
      name="celery_worker_heartbeat_expiry_ttl",
    )
  except Exception:
    return


@signals.worker_ready.connect
def worker_ready(**_kwargs) -> None:
  _ensure_indexes()
  _touch_worker()


@signals.heartbeat_sent.connect
def heartbeat_sent(**_kwargs) -> None:
  _touch_heartbeat()


@signals.worker_shutdown.connect
def worker_shutdown(**_kwargs) -> None:
  try:
    _touch_worker(status="offline")
  finally:
    _close_collections()


@signals.task_prerun.connect
def task_prerun(task_id: str, task: Any, **_kwargs) -> None:
  try:
    now = _now()
    _client, history, _heartbeats = _collections()
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
          "expires_at": _expires_at(now, "CELERY_TASK_HISTORY_RETENTION_DAYS", 30),
          "error": "",
          "error_summary_version": 2,
        },
        "$setOnInsert": {"received_at": now},
      },
      upsert=True,
    )
  except Exception:
    return


@signals.task_retry.connect
def task_retry(request: Any, reason: Any, **_kwargs) -> None:
  task_id = str(getattr(request, "id", "") or "")
  if not task_id:
    return
  try:
    now = _now()
    _client, history, _heartbeats = _collections()
    history.update_one(
      {"task_id": task_id},
      {
        "$set": {
          "status": "RETRY",
          "retries": int(getattr(request, "retries", 0) or 0) + 1,
          "error": _safe_error_summary(reason),
          "error_summary_version": 2,
          "updated_at": now,
          "expires_at": _expires_at(now, "CELERY_TASK_HISTORY_RETENTION_DAYS", 30),
        },
      },
      upsert=False,
    )
  except Exception:
    return


@signals.task_postrun.connect
def task_postrun(task_id: str, task: Any, state: str, retval: Any, **_kwargs) -> None:
  try:
    now = _now()
    _client, history, _heartbeats = _collections()
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
      "expires_at": _expires_at(now, "CELERY_TASK_HISTORY_RETENTION_DAYS", 30),
      "duration_ms": duration_ms,
    }
    if str(state) == "SUCCESS":
      update["result_summary"] = _safe_result_summary(retval)
      update["result_summary_version"] = 2
      update["error"] = ""
      update["error_summary_version"] = 2
    history.update_one(
      {"task_id": payload["task_id"]},
      {"$set": update, "$setOnInsert": {"received_at": now, "started_at": now}},
      upsert=True,
    )
  except Exception:
    return


@signals.task_failure.connect
def task_failure(task_id: str, exception: Exception, **_kwargs) -> None:
  try:
    now = _now()
    _client, history, _heartbeats = _collections()
    history.update_one(
      {"task_id": str(task_id)},
      {
        "$set": {
          "status": "FAILURE",
          "error": _safe_error_summary(exception),
          "error_summary_version": 2,
          "updated_at": now,
          "finished_at": now,
          "expires_at": _expires_at(now, "CELERY_TASK_HISTORY_RETENTION_DAYS", 30),
        },
      },
      upsert=False,
    )
  except Exception:
    return

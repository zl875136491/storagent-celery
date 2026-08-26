"""A MongoDB-backed per-Region leader lock for Celery Beat.

Celery's built-in PersistentScheduler has no cross-process election.  Multiple
workers in the same Region would otherwise each emit the same periodic jobs.
The broker is MongoDB in Storagent deployments, so a short lease in that same
database is sufficient to make exactly one Beat instance active per Region.
"""
from __future__ import annotations

import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlparse

from celery.beat import PersistentScheduler
from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

from src.core.celery_routing import normalize_protocol_version, normalize_region


def _database_name(url: str) -> str:
  try:
    path = unquote(urlparse(url).path or "").strip("/")
  except Exception:
    path = ""
  return path.split("/", 1)[0] or os.getenv("CELERY_MONGODB_DATABASE", "storagent_celery")


def _seconds(name: str, default: float, minimum: float) -> float:
  try:
    return max(float(os.getenv(name, str(default))), minimum)
  except ValueError:
    return max(default, minimum)


class RegionMongoBeatScheduler(PersistentScheduler):
  """Run Beat ticks only while this process owns the local Region lease."""

  def __init__(self, *args, **kwargs):
    self._lock_client: MongoClient | None = None
    self._lock_collection = None
    self._last_lock_check = 0.0
    self._leader = False
    self._owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    self._lock_ttl = _seconds("CELERY_BEAT_LOCK_TTL_SECONDS", 45, 15)
    self._follower_poll = _seconds("CELERY_BEAT_FOLLOWER_POLL_SECONDS", 5, 1)
    region = normalize_region(os.getenv("REGION", ""))
    protocol = normalize_protocol_version(os.getenv("CELERY_TASK_PROTOCOL_VERSION", "2"))
    self._lock_key = f"storagent-beat:{region}:v{protocol}"
    super().__init__(*args, **kwargs)

  def _collection(self):
    if self._lock_collection is not None:
      return self._lock_collection
    broker = os.environ["CELERY_BROKER_URL"].strip()
    if not broker.startswith("mongodb"):
      raise RuntimeError("Storagent Celery Beat leader lock requires a MongoDB broker")
    timeout_ms = max(int(_seconds("CELERY_RUNTIME_TIMEOUT_SECONDS", 1.5, 0.5) * 1000), 500)
    self._lock_client = MongoClient(
      broker,
      serverSelectionTimeoutMS=timeout_ms,
      connectTimeoutMS=timeout_ms,
    )
    collection = self._lock_client[_database_name(broker)][
      os.getenv("CELERY_BEAT_LOCK_COLLECTION", "celery_beat_locks")
    ]
    collection.create_index([("key", ASCENDING)], unique=True, name="beat_lock_key_unique")
    collection.create_index(
      [("expires_at", ASCENDING)],
      expireAfterSeconds=0,
      name="beat_lock_expiry_ttl",
    )
    self._lock_collection = collection
    return collection

  def _owns_lease(self) -> bool:
    now_monotonic = time.monotonic()
    # Reuse a still-valid result for a short time. Tick can be called much
    # more frequently than task intervals when a schedule entry is overdue.
    if now_monotonic - self._last_lock_check < min(self._lock_ttl / 3, 5):
      return self._leader
    self._last_lock_check = now_monotonic
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=self._lock_ttl)
    try:
      row = self._collection().find_one_and_update(
        {
          "key": self._lock_key,
          "$or": [
            {"owner": self._owner},
            {"expires_at": {"$lte": now}},
          ],
        },
        {
          "$set": {
            "key": self._lock_key,
            "owner": self._owner,
            "expires_at": expires_at,
            "updated_at": now,
          },
          "$setOnInsert": {"created_at": now},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
      )
      self._leader = bool(row and row.get("owner") == self._owner)
    except DuplicateKeyError:
      self._leader = False
    except PyMongoError:
      # Failing closed is intentional. A lost broker connection must not turn
      # every Beat process into an independent scheduler.
      self._leader = False
    return self._leader

  def tick(self, *args, **kwargs):
    if not self._owns_lease():
      return self._follower_poll
    return super().tick(*args, **kwargs)

  def close(self):
    try:
      if self._lock_collection is not None:
        self._lock_collection.delete_one({"key": self._lock_key, "owner": self._owner})
    except PyMongoError:
      pass
    finally:
      if self._lock_client is not None:
        self._lock_client.close()
        self._lock_client = None
        self._lock_collection = None
    return super().close()

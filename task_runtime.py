"""Worker-side task envelope validation shared by every task entry point."""
from __future__ import annotations

import os
from typing import Any

from src.core.celery_routing import TaskEnvelopeError, validate_task_headers


def require_task_origin(task: Any) -> str:
  """Reject a task unless it was routed for this Worker Region and protocol."""
  request = getattr(task, "request", None)
  headers = getattr(request, "headers", None)
  return validate_task_headers(
    headers if isinstance(headers, dict) else {},
    worker_region=os.getenv("REGION", ""),
    protocol_version=os.getenv("CELERY_TASK_PROTOCOL_VERSION", "2"),
  )


__all__ = ["TaskEnvelopeError", "require_task_origin"]

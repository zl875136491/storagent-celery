# Storagent Celery Worker

The worker executes Storagent background tasks. It is deployed with the Backend source tree and must use the same Region, Celery broker, queue prefix, and task protocol as the API instances in that Region.

## Repository Relationship

Storagent is delivered through three independently versioned repositories. Its request and task lifecycle is: `Frontend -> Backend -> Celery Worker -> Backend -> Frontend`.

| Repository | Responsibility | Relationship to the other repositories |
| --- | --- | --- |
| Backend | Provides APIs, authentication, business orchestration, and asynchronous operation intake. | Receives management and query requests from Frontend; publishes regional tasks to Celery Worker; exposes task state and results to Frontend. |
| Frontend | Provides the browser-based management console. | Calls Backend versioned APIs to perform operations and queries, then presents asynchronous task progress. |
| Celery Worker (this repository) | Executes archival, quota aggregation, capacity snapshots, and storage operations in the background. | Consumes Backend tasks for its Region and persists execution state and results for Backend and Frontend to query. |

Related repositories:

- [Storagent Backend](https://github.com/zl875136491/storagent)
- [Storagent Frontend](https://github.com/zl875136491/storagent-frontend)

## Required Configuration

```dotenv
REGION=beijing
SYNC_AUTHORITY_REGION=beijing
CELERY_BROKER_URL=mongodb://...
CELERY_RESULT_BACKEND=mongodb://...
CELERY_TASK_QUEUE_PREFIX=storagent
CELERY_TASK_PROTOCOL_VERSION=2
```

The worker subscribes only to:

```text
<CELERY_TASK_QUEUE_PREFIX>.<REGION lowercase>.v<CELERY_TASK_PROTOCOL_VERSION>
```

Every accepted task carries the source Region and protocol in Celery headers. Missing or mismatched headers are rejected. This prevents a Worker in one Region from consuming a task created in another Region when broker MongoDB is shared.

## Beat

Set `CELERY_BEAT_ENABLED=true` to allow the process to participate in periodic task scheduling. Multiple Workers may enable Beat, but only the holder of the MongoDB lease `storagent-beat:<region>:v<protocol>` emits tasks. A Worker that cannot renew the lease stops scheduling.

The following controls should match the Backend configuration:

```dotenv
CELERY_BEAT_LOCK_COLLECTION=celery_beat_locks
CELERY_BEAT_LOCK_TTL_SECONDS=45
CELERY_BEAT_FOLLOWER_POLL_SECONDS=5
CELERY_OPERATION_START_TIMEOUT_SECONDS=180
CELERY_OPERATION_RUNNING_TIMEOUT_SECONDS=7200
CELERY_OPERATION_WATCHDOG_INTERVAL_SECONDS=60
CELERY_TASK_HISTORY_RETENTION_DAYS=30
CELERY_WORKER_HEARTBEAT_RETENTION_DAYS=7
OBJECT_ARCHIVE_ENABLED=false
OBJECT_ARCHIVE_AUTOCONFIGURE=false
APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE=50
CAPACITY_SNAPSHOT_MAX_CONCURRENCY=3
```

See `backend/docs/CELERY_OPERATIONS.md` in the coordinated source release for task ownership, retry behavior, and rollout/rollback steps. Do not replace only the Worker while an older Backend is still producing the legacy shared `celery` queue.

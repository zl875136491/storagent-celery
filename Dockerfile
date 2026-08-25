FROM python:3.12.10-bookworm

ARG VCS_REF=unknown
ARG IMAGE_VERSION=dev
ARG SOURCE_URL=https://github.com/zl875136491/storagent
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

LABEL org.opencontainers.image.title="Storagent Celery Worker" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.component="celery-worker"

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 STORAGENT_BACKEND_ROOT=/app/backend/storagent
WORKDIR /app

RUN groupadd --gid 10001 storagent \
    && useradd --uid 10001 --gid storagent --create-home --shell /usr/sbin/nologin storagent

COPY worker/storagent-celery/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --disable-pip-version-check --index-url "${PIP_INDEX_URL}" -r requirements.txt

COPY backend/storagent/requirements.txt ./backend-requirements.txt
RUN python -m pip install --no-cache-dir --disable-pip-version-check --index-url "${PIP_INDEX_URL}" -r backend-requirements.txt

COPY backend/storagent/src ./backend/storagent/src
COPY backend/storagent/__init__.py ./backend/storagent/__init__.py
COPY backend/storagent/runtimes/mc /usr/local/bin/mc
COPY worker/storagent-celery/celery_app.py worker/storagent-celery/tasks.py worker/storagent-celery/observability.py worker/storagent-celery/worker.sh ./
RUN chmod 0755 ./worker.sh /usr/local/bin/mc && chown -R storagent:storagent /app

USER storagent:storagent
CMD ["./worker.sh"]

FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

ENV DB_PATH=/data/app.db \
    POOL_MAX_WORKERS_PER_VERSION=4 \
    TASK_DEFAULT_TIMEOUT_MS=30000

EXPOSE 8000
# Single worker process by design: the plugin pool lives in-process.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

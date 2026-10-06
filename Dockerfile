FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PARSER_DB=/data/parser.db

WORKDIR /srv/app

COPY requirements.txt /srv/app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /srv/app/app

RUN mkdir -p /data

EXPOSE 8000

# 子进程以 `python -m app.runtime.child <entrypoint>` 方式启动，
# 入口模块全部来自仓库内的 app/plugins。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

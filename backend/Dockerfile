FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONPATH=/app/backend STRATUM_HOME=/data DBT_SEND_ANONYMOUS_USAGE_STATS=false DAGSTER_TELEMETRY_ENABLED=false
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt && useradd -m -u 10001 stratum && mkdir /data && chown stratum /data
COPY backend ./backend
COPY dbt ./dbt
COPY data/seed ./data/seed
USER stratum
EXPOSE 8091
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8091/api/health')"
CMD ["uvicorn","stratum.api:app","--host","0.0.0.0","--port","8091","--workers","1"]

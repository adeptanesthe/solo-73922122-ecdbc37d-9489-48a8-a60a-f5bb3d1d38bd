FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LIS_DB_PATH=/data/lis.db \
    LIS_MLLP_HOST=0.0.0.0 \
    LIS_MLLP_PORT=2575 \
    LIS_RECALC_DELAY_MS=0

WORKDIR /srv
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

RUN useradd --system --uid 10001 lis && mkdir -p /data && chown lis /data
USER lis

EXPOSE 8000 2575
VOLUME ["/data"]
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

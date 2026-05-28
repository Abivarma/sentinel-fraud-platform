FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements/base.txt requirements/serving.txt ./requirements/
RUN pip install --no-cache-dir -r requirements/serving.txt

COPY ml/models/ ./ml/models/
COPY serving/ ./serving/

ENV PYTHONPATH=/app
ENV PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]

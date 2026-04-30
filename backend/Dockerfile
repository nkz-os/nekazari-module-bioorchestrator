FROM python:3.12-slim AS base

LABEL maintainer="NKZ Team"
LABEL module="bioorchestrator"

WORKDIR /app

# System deps for scientific packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ app/

# Data directories (mounted as PV in K8s)
RUN mkdir -p /data/raw /data/processed /data/reports

# Non-root user for security
RUN useradd -m -r biouser && chown -R biouser:biouser /app /data
USER biouser

ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/data
ENV AUTH_DISABLED=false

EXPOSE 8420

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import httpx; r=httpx.get('http://localhost:8420/healthz'); r.raise_for_status()"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8420", "--workers", "2"]

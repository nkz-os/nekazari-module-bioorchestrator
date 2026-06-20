FROM python:3.12-slim AS base

LABEL maintainer="NKZ Team"
LABEL module="bioorchestrator"

WORKDIR /app

# System deps for scientific packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        git \
        ca-certificates \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (context is repo root)
COPY backend/app/ app/
COPY backend/scripts/ scripts/
COPY backend/cypher_migrations/ cypher_migrations/
COPY data/ data/
# Overwrite phenology_sources.yaml with the curated 9-species version
COPY backend/data/phenology_sources.yaml data/phenology_sources.yaml

# Copy action_rules.yaml seed data for SP2 Action Rules Engine
COPY backend/data/action_rules.yaml data/action_rules.yaml

# Copy IkerKeta sources config to where the library expects it
# (ikerketa/config.py resolves _PROJECT_ROOT from __file__, ending up at site-packages)
COPY backend/sources.yaml /tmp/sources.yaml
RUN python -c "import ikerketa.config as c; import os, shutil; d = c._PROJECT_ROOT / 'configs'; os.makedirs(d, exist_ok=True); shutil.copy('/tmp/sources.yaml', d / 'sources.yaml'); print('Config installed to', d)"

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

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8420"]

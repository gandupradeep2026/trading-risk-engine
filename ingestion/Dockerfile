# =============================================================================
# Trading Risk Engine - Ingestion Worker Container
# =============================================================================
# Multi-stage hardened Python container for continuous financial tick ingestion
# =============================================================================

FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Final Runtime Image
FROM python:3.12-slim AS runtime

# Install runtime utilities (curl for container healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Hardened unprivileged service user
RUN groupadd -g 10001 riskworker && \
    useradd -u 10001 -g riskworker -m -d /home/riskworker -s /sbin/nologin riskworker

WORKDIR /app

# Copy python dependencies from builder
COPY --from=builder /install /usr/local

# Copy application source code
COPY --chown=riskworker:riskworker . /app/ingestion/

# Environment hardening
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    METRICS_PORT=8080

# Expose internal health/metrics port
EXPOSE 8080

# Switch to unprivileged user
USER riskworker

# Container healthcheck against embedded health server
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Execute ingestion worker daemon
ENTRYPOINT ["python", "-m", "ingestion.worker"]

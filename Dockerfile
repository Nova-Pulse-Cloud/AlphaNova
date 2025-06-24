# 1. Base image
FROM python:3.11-slim AS base

# 2. Set a non-root user
RUN useradd --create-home appuser
WORKDIR /app
USER appuser

# 3. Ensure pip is up-to-date and set unbuffered output
ENV PATH="/home/appuser/.local/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 4. Install build dependencies, install Python deps, then clean up
FROM base AS builder

# Temporarily switch to root for apt
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
      libffi-dev \
      libssl-dev \
      git \
      curl \
 && rm -rf /var/lib/apt/lists/*

# Copy only requirements to leverage Docker cache
COPY --chown=appuser:appuser requirements.txt .

# Install Python deps into user local
USER appuser
RUN pip install --user -r requirements.txt

# 5. Final image: start from base, copy deps from builder
FROM base

# Copy installed packages from builder
COPY --from=builder /home/appuser/.local /home/appuser/.local

# Copy application code
COPY --chown=appuser:appuser . .

# 6. Expose and healthcheck
ENV PORT=8000
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl --fail http://localhost:${PORT}/health || exit 1

# 7. Entrypoint
CMD ["gunicorn", "main_service:app", \
     "--bind", "0.0.0.0:8000", \
     "--workers", "1", \
     "--timeout", "120"]

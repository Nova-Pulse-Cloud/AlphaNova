# Use official python image
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    git \
  && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Expose port for Render
ENV PORT 8000
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=3s CMD curl --fail http://localhost:${PORT}/health || exit 1

# Run entrypoint
CMD ["gunicorn", "main_service:app", "--bind", "0.0.0.0:8000", "--workers", "1", "--timeout", "120"]

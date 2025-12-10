# Use pinned slim image matching Jupyter (150MB vs 900MB)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install minimal system dependencies (gcc for hdfs/minio compilation)
RUN apt-get update && apt-get install -y \
    gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (Docker layer caching)
COPY requirements.txt .

# Install Python packages with no cache
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY scripts ./scripts
COPY main.py .

# Create non-root user for security
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Healthcheck compatible with Docker Compose v3.9
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "from minio import Minio; Minio('minio:9000', 'minioadmin', 'minioadmin').list_buckets() || exit 1"

# Default command runs full ETL pipeline
CMD ["python", "main.py"]

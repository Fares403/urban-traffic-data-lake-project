# -----------------------------------------------------------------------------
# Base Image
# -----------------------------------------------------------------------------
# Use official slim Python image (small size, good for production)
FROM python:3.11-slim

# -----------------------------------------------------------------------------
# Working Directory
# -----------------------------------------------------------------------------
# Set the working directory inside the container to /app
WORKDIR /app

# -----------------------------------------------------------------------------
# Python Dependencies
# -----------------------------------------------------------------------------
# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt .

# Install Python dependencies without caching (smaller image size)
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# Application Code
# -----------------------------------------------------------------------------
# Copy ETL scripts into /app/scripts inside the container
COPY scripts ./scripts

# Copy the main ETL entrypoint script to /app
COPY main.py .

# -----------------------------------------------------------------------------
# Container Runtime Command
# -----------------------------------------------------------------------------
# Keep the container running (useful for debugging or running commands manually)
# In production, this can be changed to:
# CMD ["python", "main.py"]
CMD ["tail", "-f", "/dev/null"]

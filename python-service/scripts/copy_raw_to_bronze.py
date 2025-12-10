"""
copy_raw_to_bronze.py

This script uploads raw CSV datasets from the local 'data/bronze' folder 
to the 'bronze' bucket in MinIO. It uses the MinIO Python SDK to connect 
to the MinIO server and transfer files.

Usage:
    python copy_raw_to_bronze.py
"""

import os
from minio import Minio
from minio.error import S3Error

# ---------------------- Configuration ----------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"

# Local folder containing raw CSV files
LOCAL_BRONZE_DIR = "data/bronze"

# ---------------------- Initialize MinIO Client ----------------------
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ---------------------- Upload Files ----------------------
for filename in os.listdir(LOCAL_BRONZE_DIR):
    local_path = os.path.join(LOCAL_BRONZE_DIR, filename)
    if os.path.isfile(local_path):
        try:
            client.fput_object(
                BRONZE_BUCKET,
                filename,
                local_path
            )
            print(f"[✔] Uploaded {filename} → {BRONZE_BUCKET}")
        except S3Error as e:
            print(f"[✖] Error uploading {filename}: {e}")

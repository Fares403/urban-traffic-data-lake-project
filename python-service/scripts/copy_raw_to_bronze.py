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
import time

def get_minio_client():
    """Get MinIO client using Docker Compose environment variables"""
    MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    endpoint = MINIO_URL.replace("http://", "").replace("https://", "")
    
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def copy_raw_to_bronze():
    """Copy raw CSV files from local bronze folder to MinIO bronze bucket"""
    BRONZE_BUCKET = "bronze"
    LOCAL_BRONZE_DIR = "/app/data/bronze"  # Fixed path matching volume mount
    
    # Wait for MinIO to be ready
    client = get_minio_client()
    for _ in range(10):
        try:
            client.list_buckets()
            break
        except:
            print("[i] Waiting for MinIO...")
            time.sleep(2)
    else:
        print("[✖] MinIO not available")
        return False
    
    # Check if local directory exists and has files
    if not os.path.exists(LOCAL_BRONZE_DIR):
        print(f"[✖] Local bronze directory not found: {LOCAL_BRONZE_DIR}")
        return False
    
    files = [f for f in os.listdir(LOCAL_BRONZE_DIR) if os.path.isfile(os.path.join(LOCAL_BRONZE_DIR, f))]
    if not files:
        print(f"[i] No files found in {LOCAL_BRONZE_DIR}")
        return True
    
    print(f"[i] Found {len(files)} files to upload: {files}")
    
    # ---------------------- Upload Files ----------------------
    success_count = 0
    for filename in files:
        local_path = os.path.join(LOCAL_BRONZE_DIR, filename)
        try:
            client.fput_object(
                BRONZE_BUCKET,
                filename,  # Keep same filename in bucket
                local_path
            )
            print(f"[✔] Uploaded {filename} → {BRONZE_BUCKET}/{filename}")
            success_count += 1
        except S3Error as e:
            print(f"[✖] Error uploading {filename}: {e}")
    
    print(f"[✔] Upload completed: {success_count}/{len(files)} files successful")
    return success_count == len(files)

if __name__ == "__main__":
    success = copy_raw_to_bronze()
    exit(0 if success else 1)

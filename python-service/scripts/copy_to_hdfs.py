"""
Copy Silver Layer to HDFS - Fixed Version

Copies cleaned Parquet files from MinIO Silver bucket to HDFS /silver directory
using WebHDFS (NameNode HTTP port 9870).
"""

from minio import Minio
from minio.error import S3Error
from hdfs import InsecureClient
from io import BytesIO
import os
import time
import requests


def get_minio_client():
    """Get MinIO client using Docker Compose environment variables"""
    MINIO_URL = os.getenv("MINIO_URL", "http://minio:9002")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    endpoint = MINIO_URL.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def get_hdfs_webhdfs_url():
    """Get WebHDFS base URL from environment (NameNode HTTP port 9870)"""
    return os.getenv("HDFS_WEB_UI", "http://namenode:9870")


def is_hdfs_ready(webhdfs_url: str) -> bool:
    """
    Check if HDFS NameNode WebHDFS endpoint is ready by calling LISTSTATUS on root.
    """
    try:
        url = f"{webhdfs_url}/webhdfs/v1/?op=LISTSTATUS"
        resp = requests.get(url, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def copy_to_hdfs():
    """
    Copy all .parquet files from MinIO 'silver' bucket to HDFS '/silver' directory.
    Returns True if all files copied successfully, False otherwise.
    """
    SILVER_BUCKET = "silver"
    HDFS_DEST_DIR = "/silver"

    print("[i] Connecting to MinIO and HDFS...")

    # ---------- MinIO client ----------
    minio_client = get_minio_client()
    # Quick connectivity check with retry
    for _ in range(10):
        try:
            minio_client.list_buckets()
            break
        except Exception:
            print("[i] Waiting for MinIO...")
            time.sleep(2)
    else:
        print("[✖] MinIO not available")
        return False

    # ---------- HDFS (WebHDFS) client ----------
    webhdfs_url = get_hdfs_webhdfs_url()
    print(f"[i] Using WebHDFS endpoint: {webhdfs_url}")

    max_retries = 30  # 30 * 5s = 150s = 2.5 minutes
    for attempt in range(max_retries):
        if is_hdfs_ready(webhdfs_url):
            print(" HDFS NameNode WebHDFS is ready")
            break
        print(f"[i] Waiting for HDFS........ ({attempt + 1}/{max_retries})")
        time.sleep(5)
    else:
        print("!!!!! HDFS not available via WebHDFS after retries")
        return False

    # Create HDFS client on WebHDFS URL
    HDFS_USER = os.getenv("HDFS_USER", "hadoop")
    hdfs_client = InsecureClient(webhdfs_url, user=HDFS_USER)

    # Verify connection
    try:
        hdfs_client.status("/", strict=False)
        print("[✔] HDFS connection verified")
    except Exception as e:
        print(f"[✖] HDFS status check failed: {e}")
        return False

    # ---------- Ensure destination directory ----------
    try:
        if not hdfs_client.status(HDFS_DEST_DIR, strict=False):
            hdfs_client.makedirs(HDFS_DEST_DIR)
            print(f"[✔] Created HDFS directory: {HDFS_DEST_DIR}")
    except Exception as e:
        # If directory exists or minor error, just log and continue
        print(f"[i] HDFS directory check error (may already exist): {e}")

    # ---------- List Parquet files in MinIO silver bucket ----------
    parquet_files = []
    try:
        for obj in minio_client.list_objects(SILVER_BUCKET):
            if obj.object_name.endswith(".parquet"):
                parquet_files.append(obj.object_name)
    except S3Error as e:
        print(f"✖ Error listing Silver bucket: {e}")
        return False

    if not parquet_files:
        print("[i] No .parquet files found in Silver bucket, nothing to copy")
        return True

    print(f"[i] Found {len(parquet_files)} parquet files: {parquet_files}")

    # ---------- Copy each file ----------
    success_count = 0
    for obj_name in parquet_files:
        try:
            response = minio_client.get_object(SILVER_BUCKET, obj_name)
            data_bytes = BytesIO(response.read()).getvalue()

            hdfs_path = f"{HDFS_DEST_DIR}/{obj_name}"
            with hdfs_client.write(hdfs_path, overwrite=True) as writer:
                writer.write(data_bytes)

            print(f"!!!!! Copied {obj_name} → HDFS {hdfs_path}")
            success_count += 1
        except Exception as e:
            print(f"✖ Error copying {obj_name} to HDFS: {e}")

    print(f"!!!!! HDFS copy completed: {success_count}/{len(parquet_files)} files successful")
    return success_count == len(parquet_files)


if __name__ == "__main__":
    ok = copy_to_hdfs()
    exit(0 if ok else 1)

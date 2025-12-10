"""
copy_to_hdfs.py
Copy cleaned Parquet files from MinIO Silver bucket to HDFS.
"""
from minio import Minio
from minio.error import S3Error
from hdfs import InsecureClient
from io import BytesIO
import os
import time

def get_minio_client():
    """Get MinIO client using Docker Compose environment variables"""
    MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    endpoint = MINIO_URL.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def get_hdfs_client():
    """Get HDFS client using Docker Compose environment variables"""
    HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://hadoop:9870")
    HDFS_USER = os.getenv("HDFS_USER", "hadoop")
    return InsecureClient(HDFS_NAMENODE, user=HDFS_USER)

def copy_to_hdfs():
    SILVER_BUCKET = "silver"
    HDFS_DEST_DIR = "/silver"
    
    print("[i] Connecting to MinIO and HDFS...")
    
    # MinIO client with retry
    minio_client = get_minio_client()
    for _ in range(10):
        try:
            minio_client.list_buckets()
            break
        except:
            print("[i] Waiting for MinIO...")
            time.sleep(2)
    else:
        print("[✖] MinIO not available")
        return False
    
    # HDFS client with retry
    hdfs_client = get_hdfs_client()
    for _ in range(15):  # More retries for slow Hadoop
        try:
            hdfs_client.status("/", strict=False)
            break
        except:
            print("[i] Waiting for HDFS...")
            time.sleep(5)
    else:
        print("[✖] HDFS not available")
        return False
    
    # Create HDFS directory
    try:
        if not hdfs_client.status(HDFS_DEST_DIR, strict=False):
            hdfs_client.makedirs(HDFS_DEST_DIR)
            print(f"[✔] Created HDFS directory: {HDFS_DEST_DIR}")
    except Exception as e:
        print(f"[✖] HDFS directory error: {e}")
        return False
    
    # ✅ FIXED: Copy ALL Parquet files (traffic + weather + merged)
    parquet_files = []
    try:
        objects = minio_client.list_objects(SILVER_BUCKET)
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                parquet_files.append(obj.object_name)
    except S3Error as e:
        print(f"[✖] Error listing Silver bucket: {e}")
        return False
    
    if not parquet_files:
        print("[i] No parquet files found in Silver bucket")
        return True
    
    print(f"[i] Found {len(parquet_files)} parquet files: {parquet_files}")
    
    # Copy each file
    success_count = 0
    for obj_name in parquet_files:
        try:
            data = minio_client.get_object(SILVER_BUCKET, obj_name)
            df_bytes = BytesIO(data.read())
            hdfs_path = f"{HDFS_DEST_DIR}/{obj_name}"
            hdfs_client.upload(hdfs_path, df_bytes, overwrite=True)
            print(f"[✔] Copied {obj_name} → HDFS {hdfs_path}")
            success_count += 1
        except Exception as e:
            print(f"[✖] Error copying {obj_name}: {e}")
    
    print(f"[✔] HDFS copy completed: {success_count}/{len(parquet_files)}")
    return success_count == len(parquet_files)

if __name__ == "__main__":
    success = copy_to_hdfs()
    exit(0 if success else 1)

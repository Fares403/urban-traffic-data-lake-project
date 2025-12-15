from minio import Minio
from minio.error import S3Error
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKETS = ["bronze", "silver", "gold"]

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False 
)

for bucket in BUCKETS:
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[✔] Bucket created: {bucket}")
        else:
            print(f"[i] Bucket already exists: {bucket}")
    except S3Error as e:
        print(f"[✖] Error creating bucket {bucket}: {e}")

"""
merge.py

Merge cleaned traffic and weather datasets from MinIO Silver bucket.
Save merged dataset locally and back to MinIO Silver bucket.
"""

import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
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

def merge_datasets(client):
    # ---------------- Configuration ----------------
    SILVER_BUCKET = "silver"
    LOCAL_OUTPUT = "/app/data/silver/merged_data.parquet"
    MINIO_OBJECT_NAME = "merged_data.parquet"
    
    try:
        # Wait for MinIO
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
        
        # ---------------- Read cleaned traffic ----------------
        print("[i] Loading traffic_clean.parquet...")
        traffic_data_obj = client.get_object(SILVER_BUCKET, "traffic_clean.parquet")
        with BytesIO(traffic_data_obj.read()) as f:
            df_traffic = pd.read_parquet(f)
        print(f"[✔] Traffic data loaded: {len(df_traffic)} rows")
        
        # ---------------- Read cleaned weather ----------------
        print("[i] Loading weather_clean.parquet...")
        weather_data_obj = client.get_object(SILVER_BUCKET, "weather_clean.parquet")
        with BytesIO(weather_data_obj.read()) as f:
            df_weather = pd.read_parquet(f)
        print(f"[✔] Weather data loaded: {len(df_weather)} rows")
        
        # ---------------- Merge datasets ----------------
        print("[i] Merging datasets on city and date...")
        
        # Create date_only columns for merging
        df_traffic['date_only'] = pd.to_datetime(df_traffic['date_time'], errors='coerce').dt.date
        df_weather['date_only'] = pd.to_datetime(df_weather['date_time'], errors='coerce').dt.date
        
        # Merge on city and date_only (left join keeps all traffic records)
        df_merged = pd.merge(
            df_traffic,
            df_weather,
            how='left',
            on=['city', 'date_only'],
            suffixes=('_traffic', '_weather')
        )
        
        df_merged.drop(columns=['date_only'], inplace=True)
        print(f"[✔] Merged dataset created: {len(df_merged)} rows")
        
        # ---------------- Save locally ----------------
        silver_path = os.path.dirname(LOCAL_OUTPUT)
        os.makedirs(silver_path, exist_ok=True)
        df_merged.to_parquet(LOCAL_OUTPUT, index=False, engine='fastparquet')
        print(f"[✔] Merged dataset saved locally → {LOCAL_OUTPUT}")
        
        # ---------------- Save to MinIO ----------------
        buffer = BytesIO()
        df_merged.to_parquet(buffer, index=False, engine='fastparquet')
        buffer.seek(0)
        client.put_object(
            SILVER_BUCKET,
            MINIO_OBJECT_NAME,
            buffer,
            length=buffer.getbuffer().nbytes
        )
        print(f"[✔] Merged dataset uploaded → MinIO {SILVER_BUCKET}/{MINIO_OBJECT_NAME}")
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Unexpected error: {e}")
        return False

if __name__ == "__main__":
    client = get_minio_client()
    success = merge_datasets(client)
    exit(0 if success else 1)

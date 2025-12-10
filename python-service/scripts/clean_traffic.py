"""
Clean Traffic Dataset

This script performs data cleaning on the raw traffic dataset stored in MinIO Bronze bucket.
Steps:
1. Fetch CSV from MinIO Bronze bucket.
2. Remove duplicates, handle missing values, fix bad formats, remove outliers.
3. Save cleaned dataset as Parquet locally (/data/silver).
4. Upload cleaned dataset to MinIO Silver bucket.
"""

import pandas as pd
import numpy as np
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

def clean_traffic(client):
    BRONZE_BUCKET = "bronze"
    SILVER_BUCKET = "silver"
    OBJECT_NAME = "traffic_raw.csv"
    OUTPUT_FILE_LOCAL = "/app/data/silver/traffic_clean.parquet"
    
    try:
        # -------- Fetch CSV from Bronze --------
        print(f"[i] Fetching {OBJECT_NAME} from {BRONZE_BUCKET}...")
        response = client.get_object(BRONZE_BUCKET, OBJECT_NAME)
        df = pd.read_csv(BytesIO(response.read()))
        print(f"[✔] Loaded {len(df)} rows from MinIO")
        
        # -------- Cleaning --------
        print("[i] Starting cleaning process...")
        
        # 1. Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates()
        print(f"[✔] Removed {initial_rows - len(df)} duplicates")
        
        # 2. Handle missing values 
        numeric_cols = ["vehicle_count", "avg_speed_kmh", "accident_count", "visibility_m"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].fillna(df[col].median())
        
        categorical_cols = ["city", "area", "congestion_level", "road_condition"]
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown")
        
        # 3. Fix dates
        df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")
        before_date_clean = len(df)
        df = df.dropna(subset=["date_time"])
        print(f"[✔] Removed {before_date_clean - len(df)} unfixable dates")
        
        # 4. Remove outliers (IQR method)
        for col in numeric_cols:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                before_outlier = len(df)
                df = df[(df[col] >= Q1 - 1.5 * IQR) & (df[col] <= Q3 + 1.5 * IQR)]
                print(f"[✔] Removed {before_outlier - len(df)} outliers from {col}")
        
        # -------- Save locally --------
        silver_path = os.path.dirname(OUTPUT_FILE_LOCAL)
        os.makedirs(silver_path, exist_ok=True)
        df.to_parquet(OUTPUT_FILE_LOCAL, index=False, engine="fastparquet")
        print(f"[✔] Cleaned traffic dataset saved locally → {OUTPUT_FILE_LOCAL} ({len(df)} rows)")
        
        # -------- Upload to MinIO Silver --------
        with open(OUTPUT_FILE_LOCAL, "rb") as f:
            client.put_object(
                SILVER_BUCKET,
                "traffic_clean.parquet",
                f,
                length=os.path.getsize(OUTPUT_FILE_LOCAL)
            )
        print(f"[✔] Cleaned traffic dataset uploaded to MinIO {SILVER_BUCKET}/traffic_clean.parquet")
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Unexpected error: {e}")
        return False

if __name__ == "__main__":
    # Wait for MinIO and retry connection
    for _ in range(10):
        try:
            client = get_minio_client()
            client.list_buckets()
            break
        except:
            print("[i] Waiting for MinIO...")
            time.sleep(3)
    else:
        print("[✖] MinIO not available")
        exit(1)
    
    success = clean_traffic(client)
    exit(0 if success else 1)

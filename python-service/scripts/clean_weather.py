"""
clean_weather.py

This script reads raw weather data from MinIO Bronze bucket, cleans it,
saves the cleaned data locally, and uploads it to MinIO Silver bucket.

Cleaning Steps:
1. Remove duplicate rows based on 'weather_id'.
2. Handle missing values:
   - Fill 'city' and 'season' with mode.
   - Fill numeric columns with median or drop if necessary.
3. Standardize 'date_time' column:
   - Convert to datetime format, remove invalid entries.
4. Handle outliers for numeric columns using IQR method.
5. Standardize categorical columns to expected values.

Local Output:
- data/silver/weather_clean.parquet

MinIO Output:
- Silver bucket: weather_clean.parquet
"""

import pandas as pd
import numpy as np
import os
from minio import Minio
from minio.error import S3Error
from io import BytesIO
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

def clean_weather(client):
    # --- Config ---
    LOCAL_SILVER_PATH = "/app/data/silver/weather_clean.parquet"  # Fixed path
    BRONZE_BUCKET = "bronze"
    SILVER_BUCKET = "silver"
    FILE_NAME = "weather_raw.csv"
    CLEANED_FILE_NAME = "weather_clean.parquet"
    
    try:
        # --- Read raw data from Bronze ---
        print(f"[i] Fetching {FILE_NAME} from {BRONZE_BUCKET}...")
        response = client.get_object(BRONZE_BUCKET, FILE_NAME)
        df = pd.read_csv(BytesIO(response.read()))
        print(f"[✔] Loaded {len(df)} rows from MinIO Bronze")
        
        # --- Data Cleaning ---
        print("[i] Starting weather data cleaning...")
        
        # 1. Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates(subset="weather_id")
        print(f"[✔] Removed {initial_rows - len(df)} duplicates")
        
        # 2. Standardize date_time
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
        before_date_clean = len(df)
        df = df.dropna(subset=['date_time'])
        print(f"[✔] Removed {before_date_clean - len(df)} invalid dates")
        
        # 3. Fill missing categorical values
        categorical_cols = ['city', 'season', 'weather_condition']
        for col in categorical_cols:
            if col in df.columns and not df[col].mode().empty:
                df[col] = df[col].fillna(df[col].mode().iloc[0])
            else:
                df[col] = df[col].fillna("Unknown")
        
        # 4. Handle numeric columns
        numeric_cols = ['temperature_c', 'humidity', 'rain_mm', 'wind_speed_kmh', 'visibility_m']
        for col in numeric_cols:
            if col in df.columns:
                # Convert non-numeric to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Handle outliers using IQR
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                before_outlier = len(df)
                df[col] = df[col].clip(lower=lower, upper=upper)
                
                # Fill remaining NaN with median
                df[col] = df[col].fillna(df[col].median())
        
        # --- Save locally ---
        silver_path = os.path.dirname(LOCAL_SILVER_PATH)
        os.makedirs(silver_path, exist_ok=True)
        df.to_parquet(LOCAL_SILVER_PATH, index=False, engine='fastparquet')
        print(f"[✔] Cleaned weather data saved locally → {LOCAL_SILVER_PATH} ({len(df)} rows)")
        
        # --- Upload to MinIO Silver ---
        with open(LOCAL_SILVER_PATH, "rb") as f:
            client.put_object(
                SILVER_BUCKET,
                CLEANED_FILE_NAME,
                f,
                length=os.path.getsize(LOCAL_SILVER_PATH)
            )
        print(f"[✔] Cleaned data uploaded to {SILVER_BUCKET}/{CLEANED_FILE_NAME}")
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Unexpected error: {e}")
        return False

if __name__ == "__main__":
    # Wait for MinIO connection
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
    
    success = clean_weather(client)
    exit(0 if success else 1)

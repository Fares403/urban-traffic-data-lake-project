
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
    LOCAL_SILVER_PATH = "/app/data/silver/weather_clean.parquet"
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
        
        print("[i] Standardizing date_time column...")
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce', dayfirst=True, utc=True)
        before_date_clean = len(df)
        df = df.dropna(subset=['date_time'])
        
        df['date_time'] = df['date_time'].dt.tz_convert(None)
        print(f"[✔] Removed {before_date_clean - len(df)} invalid dates")
        
        # 3. Fill missing categorical values
        categorical_cols = ['city', 'season', 'weather_condition']
        for col in categorical_cols:
            if col in df.columns:
                mode_val = df[col].mode()
                if not mode_val.empty:
                    df[col] = df[col].fillna(mode_val.iloc[0])
                else:
                    df[col] = df[col].fillna("Unknown")
        
        # 4. Handle numeric columns
        numeric_cols = ['temperature_c', 'humidity', 'rain_mm', 'wind_speed_kmh', 'visibility_m']
        for col in numeric_cols:
            if col in df.columns:
                # Convert non-numeric to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Remove rows with too many NaN in numeric columns
                nan_count = df[col].isna().sum()
                if nan_count / len(df) > 0.5:  # More than 50% NaN
                    df = df.dropna(subset=[col])
                
                # Handle outliers using IQR
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                outlier_count = ((df[col] < lower) | (df[col] > upper)).sum()
                df[col] = df[col].clip(lower=lower, upper=upper)
                print(f"[✔] Removed {outlier_count} outliers from {col}")
                
                # Fill remaining NaN with median
                median_val = df[col].median()
                if pd.notna(median_val):
                    df[col] = df[col].fillna(median_val)
        
        # --- Save locally ---
        silver_path = os.path.dirname(LOCAL_SILVER_PATH)
        os.makedirs(silver_path, exist_ok=True)
        df.to_parquet(LOCAL_SILVER_PATH, index=False, engine='fastparquet')
        print(f"[✔] Cleaned weather dataset saved locally → {LOCAL_SILVER_PATH} ({len(df)} rows)")
        
        # --- Upload to MinIO Silver ---
        with open(LOCAL_SILVER_PATH, "rb") as f:
            client.put_object(
                SILVER_BUCKET,
                CLEANED_FILE_NAME,
                f,
                length=os.path.getsize(LOCAL_SILVER_PATH)
            )
        print(f"[✔] Cleaned weather dataset uploaded to {SILVER_BUCKET}/{CLEANED_FILE_NAME}")
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Unexpected error: {str(e)}")
        print(f"[i] Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
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

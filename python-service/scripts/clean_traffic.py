"""
Clean Traffic Dataset - Fixed Version

This script performs comprehensive data cleaning on raw traffic data from MinIO Bronze bucket.

Cleaning Pipeline:
1. Fetch raw CSV from MinIO Bronze/traffic_raw.csv
2. Remove duplicates by traffic_id
3. Standardize date_time column (fix timezone mixing)
4. Handle missing values (mode/median filling)
5. Remove outliers using IQR method for numeric columns
6. Save cleaned Parquet locally: /app/data/silver/traffic_clean.parquet
7. Upload to MinIO Silver/traffic_clean.parquet

Output: Cleaned dataset with consistent datetime, no outliers, no missing values
"""

import pandas as pd
import numpy as np
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
import time

def get_minio_client():
    """Initialize MinIO client with Docker environment variables"""
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
    """
    Main cleaning function for traffic dataset
    
    Args:
        client: MinIO client instance
        
    Returns:
        bool: True if successful, False if failed
    """
    BRONZE_BUCKET = "bronze"
    SILVER_BUCKET = "silver"
    OBJECT_NAME = "traffic_raw.csv"
    OUTPUT_FILE_LOCAL = "/app/data/silver/traffic_clean.parquet"
    
    try:
        # -------- Step 1: Fetch raw data --------
        print(f"[i] Fetching {OBJECT_NAME} from {BRONZE_BUCKET}...")
        response = client.get_object(BRONZE_BUCKET, OBJECT_NAME)
        df = pd.read_csv(BytesIO(response.read()))
        print(f"[✔] Loaded {len(df)} rows from MinIO Bronze")
        
        # -------- Step 2: Data Cleaning --------
        print("[i] Starting comprehensive traffic data cleaning...")
        
        # Remove duplicates (use traffic_id if exists, else all columns)
        initial_rows = len(df)
        if 'traffic_id' in df.columns:
            df = df.drop_duplicates(subset='traffic_id')
        else:
            df = df.drop_duplicates()
        print(f"[✔] Removed {initial_rows - len(df)} duplicates")
        
        # -------- Step 3: Fix date_time (CRITICAL FIX) --------
        print("[i] Standardizing date_time column...")
        df["date_time"] = pd.to_datetime(
            df["date_time"], 
            errors="coerce", 
            dayfirst=True,    # Fix day/month parsing
            utc=True          # Fix timezone mixing
        )
        before_date_clean = len(df)
        df = df.dropna(subset=["date_time"])
        
        # ✅ Remove timezone and unify dtype
        df["date_time"] = df['date_time'].dt.tz_convert(None)
        print(f"[✔] Removed {before_date_clean - len(df)} invalid dates")
        
        # -------- Step 4: Handle missing categorical values --------
        categorical_cols = ["city", "area", "congestion_level", "road_condition"]
        for col in categorical_cols:
            if col in df.columns:
                mode_val = df[col].mode()
                fill_value = mode_val.iloc[0] if not mode_val.empty else "Unknown"
                df[col] = df[col].fillna(fill_value)
        
        # -------- Step 5: Handle numeric columns --------
        numeric_cols = ["vehicle_count", "avg_speed_kmh", "accident_count", "visibility_m"]
        outlier_stats = {}
        
        for col in numeric_cols:
            if col in df.columns:
                # Convert to numeric
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
                # Remove rows with excessive NaN (>50%)
                nan_pct = df[col].isna().sum() / len(df)
                if nan_pct > 0.5:
                    df = df.dropna(subset=[col])
                    print(f"[✔] Removed {int(nan_pct*100)}% NaN rows from {col}")
                
                # IQR outlier removal
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                before_outlier_count = len(df)
                outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
                outlier_count = outlier_mask.sum()
                
                # Clip outliers instead of dropping rows (preserve data)
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
                outlier_stats[col] = outlier_count
                print(f"[✔] Clipped {outlier_count} outliers from {col}")
                
                # Fill remaining NaN with median
                median_val = df[col].median()
                if pd.notna(median_val):
                    df[col] = df[col].fillna(median_val)
        
        # -------- Step 6: Save locally --------
        os.makedirs(os.path.dirname(OUTPUT_FILE_LOCAL), exist_ok=True)
        df.to_parquet(OUTPUT_FILE_LOCAL, index=False, engine="fastparquet")
        print(f"[✔] Cleaned traffic dataset saved locally → {OUTPUT_FILE_LOCAL} ({len(df)} rows)")
        
        # -------- Step 7: Upload to MinIO Silver --------
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
        print(f"[✖] Unexpected error: {str(e)}")
        print(f"[i] Error details: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    """Entry point with MinIO connection retry logic"""
    # Wait for MinIO availability
    max_retries = 10
    for attempt in range(max_retries):
        try:
            client = get_minio_client()
            client.list_buckets()
            print("[✔] MinIO connection established")
            break
        except Exception as e:
            print(f"[i] Waiting for MinIO... (attempt {attempt + 1}/{max_retries})")
            time.sleep(3)
    else:
        print("[✖] MinIO not available after retries")
        exit(1)
    
    success = clean_traffic(client)
    exit(0 if success else 1)

"""
factor_analysis.py

Perform Factor Analysis on merged data (traffic + weather) from MinIO Silver bucket.
Save the result locally and to MinIO Gold bucket.
"""

import pandas as pd
import numpy as np
from minio import Minio
from minio.error import S3Error
from sklearn.decomposition import FactorAnalysis
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

def factor_analysis(client):
    # ----------------- Configuration -----------------
    SILVER_BUCKET = "silver"
    GOLD_BUCKET = "gold"
    MERGED_FILE = "merged_data.parquet"  # Fixed filename from merge.py
    OUTPUT_FILE = "traffic_weather_factors.parquet"
    LOCAL_OUTPUT = "/app/data/gold/traffic_weather_factors.parquet"
    
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
        
        # ----------------- Read merged data from Silver -----------------
        print(f"[i] Loading {MERGED_FILE} from Silver bucket...")
        obj = client.get_object(SILVER_BUCKET, MERGED_FILE)
        merged_df = pd.read_parquet(BytesIO(obj.read()))
        print(f"[✔] Loaded merged data from Silver bucket ({len(merged_df)} rows)")
        
        # ----------------- Select numeric columns -----------------
        print("[i] Selecting numeric columns for factor analysis...")
        numeric_cols = merged_df.select_dtypes(include=[np.number]).columns.tolist()
        print(f"[i] Numeric columns: {numeric_cols}")
        
        if len(numeric_cols) < 2:
            print("[✖] Not enough numeric columns for factor analysis")
            return False
        
        numeric_df = merged_df[numeric_cols].fillna(merged_df[numeric_cols].median())
        
        # ----------------- Factor Analysis -----------------
        n_factors = min(5, len(numeric_cols) - 1)  # Max factors = variables - 1
        print(f"[i] Performing Factor Analysis with {n_factors} factors...")
        
        fa = FactorAnalysis(n_components=n_factors, random_state=42)
        fa_result = fa.fit_transform(numeric_df)
        
        # Convert to DataFrame
        fa_df = pd.DataFrame(
            fa_result, 
            columns=[f"Factor_{i+1}" for i in range(n_factors)]
        )
        
        # Combine with original data
        final_df = pd.concat([
            merged_df.reset_index(drop=True), 
            fa_df
        ], axis=1)
        
        print(f"[✔] Factor analysis completed: {n_factors} factors extracted")
        
        # ----------------- Save locally -----------------
        gold_path = os.path.dirname(LOCAL_OUTPUT)
        os.makedirs(gold_path, exist_ok=True)
        final_df.to_parquet(LOCAL_OUTPUT, index=False, engine='fastparquet')
        print(f"[✔] Factor analysis result saved locally → {LOCAL_OUTPUT}")
        
        # ----------------- Upload to MinIO Gold -----------------
        buffer = BytesIO()
        final_df.to_parquet(buffer, index=False, engine='fastparquet')
        buffer.seek(0)
        client.put_object(
            GOLD_BUCKET,
            OUTPUT_FILE,
            buffer,
            length=buffer.getbuffer().nbytes
        )
        print(f"[✔] Factor analysis result uploaded → MinIO {GOLD_BUCKET}/{OUTPUT_FILE}")
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Unexpected error: {e}")
        return False

if __name__ == "__main__":
    client = get_minio_client()
    success = factor_analysis(client)
    exit(0 if success else 1)

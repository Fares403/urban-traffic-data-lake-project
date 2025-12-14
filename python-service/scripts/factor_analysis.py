# scripts/factor_analysis.py - 100% FIXED
import pandas as pd
import numpy as np
from minio import Minio
from minio.error import S3Error
from sklearn.decomposition import FactorAnalysis
from io import BytesIO
import os
import time

def get_minio_client():
    MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    endpoint = MINIO_URL.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def factor_analysis(client):
    SILVER_BUCKET = "silver"
    GOLD_BUCKET = "gold"
    MERGED_FILE = "merged_data.parquet"
    OUTPUT_FILE = "traffic_weather_factors.parquet"
    LOCAL_OUTPUT = "/app/data/gold/traffic_weather_factors.parquet"
    
    try:
        # MinIO connection
        print(" [1/7] Connecting to MinIO...")
        for i in range(20):
            try:
                client.list_buckets()
                print(f"✔ MinIO ready ({i*3}s)")
                break
            except:
                print(f"[i] MinIO wait {i+1}/20...")
                time.sleep(3)
        else:
            return False
        
        # Create gold bucket
        try:
            if GOLD_BUCKET not in [b.name for b in client.list_buckets()]:
                client.make_bucket(GOLD_BUCKET)
                print(f"✔ Created {GOLD_BUCKET}")
        except:
            pass
        
        # Load data
        print(f"\n [2/7] Loading {MERGED_FILE}...")
        obj = client.get_object(SILVER_BUCKET, MERGED_FILE)
        merged_df = pd.read_parquet(BytesIO(obj.read()))
        print(f"✔ Loaded {len(merged_df):,} rows, {len(merged_df.columns)} cols")
        
        # Prepare data
        print("[i] [3/7] Preparing numeric data...")
        numeric_cols = merged_df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_df = merged_df[numeric_cols].fillna(merged_df[numeric_cols].median())
        numeric_df = numeric_df.loc[:, numeric_df.std() > 0.01]
        
        print(f"✔ Using {len(numeric_df.columns)} variables")
        
        # Factor Analysis
        n_factors = min(5, len(numeric_df.columns) - 1)
        print(f"\n [4/7] Factor Analysis (n_factors={n_factors})...")
        
        fa = FactorAnalysis(n_components=n_factors, random_state=42)
        fa_result = fa.fit_transform(numeric_df)
        
        #  FIXED: Factor scores
        fa_df = pd.DataFrame(
            fa_result, 
            columns=[f"Factor_{i+1}_score" for i in range(n_factors)]
        )
        
        #  FIXED: Loadings (NO nlargest error)
        loadings = pd.DataFrame(
            fa.components_.T,
            index=numeric_df.columns,
            columns=[f"Factor_{i+1}_loading" for i in range(n_factors)]
        ).round(4)
        
        print(f"✔ Extracted {n_factors} factors")
        print("\n Top Loadings per Factor:")
        for factor_col in loadings.columns:
            top_loading = loadings[factor_col].abs().nlargest(2)  # ✅ FIXED: per column
            print(f"  {factor_col}: {top_loading.to_dict()}")
        
        # Final dataset
        final_df = pd.concat([
            merged_df.reset_index(drop=True),
            fa_df.reset_index(drop=True)
        ], axis=1)
        
        # Save
        print("\n [5/7] Saving locally...")
        os.makedirs("/app/data/gold", exist_ok=True)
        final_df.to_parquet(LOCAL_OUTPUT, index=False)
        loadings.to_parquet("/app/data/gold/factor_loadings.parquet")
        print(f"✔ Saved: {LOCAL_OUTPUT}")
        
        # Upload
        print("\n [6/7] Uploading...")
        buffer = BytesIO()
        final_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(GOLD_BUCKET, OUTPUT_FILE, buffer, buffer.getbuffer().nbytes)
        print(f"✔ Uploaded: gold/{OUTPUT_FILE}")
        
        print("\n FACTOR ANALYSIS COMPLETE!")
        print(f" {n_factors} factors ready for Jupyter!")
        
        return True
        
    except Exception as e:
        print(f"[✖] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print(" Urban Traffic Factor Analysis")
    client = get_minio_client()
    success = factor_analysis(client)
    exit(0 if success else 1)

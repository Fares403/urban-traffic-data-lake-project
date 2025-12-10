"""
monte_carlo.py

Perform Monte Carlo simulation on numeric columns of merged data
from MinIO Silver bucket. Save results locally and to MinIO Gold bucket.
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

def monte_carlo_simulation(client):
    # ----------------- Configuration -----------------
    SILVER_BUCKET = "silver"
    GOLD_BUCKET = "gold"
    MERGED_FILE = "merged_data.parquet"  # Fixed to match merge.py
    OUTPUT_FILE = "monte_carlo_results.parquet"
    LOCAL_OUTPUT = "/app/data/gold/monte_carlo_results.parquet"
    
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
        print("[i] Selecting numeric columns for Monte Carlo simulation...")
        numeric_cols = merged_df.select_dtypes(include=[np.number]).columns.tolist()
        print(f"[i] Numeric columns: {numeric_cols}")
        
        if len(numeric_cols) == 0:
            print("[✖] No numeric columns found for simulation")
            return False
        
        numeric_df = merged_df[numeric_cols].fillna(merged_df[numeric_cols].median())
        
        # ----------------- Monte Carlo Simulation -----------------
        n_simulations = 10000
        simulation_results = {}
        
        print(f"[i] Running Monte Carlo simulation ({n_simulations} iterations)...")
        
        for col in numeric_cols:
            print(f"[i] Simulating {col}...")
            col_values = numeric_df[col].dropna().values
            if len(col_values) == 0:
                continue
                
            sims = []
            for _ in range(n_simulations):
                sample = np.random.choice(col_values, size=len(col_values), replace=True)
                sims.append(sample.mean())
            
            simulation_results[col] = sims
        
        # Convert results to DataFrame (summary statistics)
        summary_data = {}
        for col, sim_values in simulation_results.items():
            summary_data[col] = [
                np.mean(sim_values),
                np.std(sim_values),
                np.min(sim_values),
                np.max(sim_values)
            ]
        
        summary_df = pd.DataFrame(summary_data).T
        summary_df.columns = ["mean_estimate", "std_estimate", "min_estimate", "max_estimate"]
        summary_df = summary_df.round(4)
        
        print(f"[✔] Monte Carlo simulation completed for {len(simulation_results)} columns")
        
        # ----------------- Save locally -----------------
        gold_path = os.path.dirname(LOCAL_OUTPUT)
        os.makedirs(gold_path, exist_ok=True)
        summary_df.to_parquet(LOCAL_OUTPUT, index=True, engine='fastparquet')
        print(f"[✔] Monte Carlo simulation summary saved locally → {LOCAL_OUTPUT}")
        
        # ----------------- Upload to MinIO Gold -----------------
        buffer = BytesIO()
        summary_df.to_parquet(buffer, index=True, engine='fastparquet')
        buffer.seek(0)
        client.put_object(
            GOLD_BUCKET,
            OUTPUT_FILE,
            buffer,
            length=buffer.getbuffer().nbytes
        )
        print(f"[✔] Monte Carlo simulation summary uploaded → MinIO {GOLD_BUCKET}/{OUTPUT_FILE}")
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Unexpected error: {e}")
        return False

if __name__ == "__main__":
    client = get_minio_client()
    success = monte_carlo_simulation(client)
    exit(0 if success else 1)

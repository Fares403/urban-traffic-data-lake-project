# ================================================
# MONTE CARLO SIMULATION - URBAN TRAFFIC DATA LAKE
# Complete Medallion Gold Layer Generation
# ================================================

import pandas as pd
import numpy as np
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
import time

def get_minio_client():
    """Robust MinIO client with environment variables"""
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

def define_weather_scenarios():
    """Realistic weather scenarios with proper multipliers"""
    return {
        "sunny": {
            "description": "Clear weather, normal conditions",
            "traffic_mult": 1.1,
            "accident_factor": 0.7
        },
        "rainy": {
            "description": "Heavy rain, reduced visibility", 
            "traffic_mult": 0.9,
            "accident_factor": 1.6
        },
        "foggy": {
            "description": "Dense fog, low visibility",
            "traffic_mult": 0.8,
            "accident_factor": 2.1
        },
        "snowy": {
            "description": "Snow/ice conditions, severe impact",
            "traffic_mult": 0.7,
            "accident_factor": 2.8
        }
    }

def simulate_scenario_impact(df, scenario_name, config, n_simulations=10000):
    """Monte Carlo simulation for weather scenario impact"""
    
    # Find traffic column (flexible)
    traffic_col = next((col for col in ['traffic_volume', 'volume'] if col in df.columns), 
                      df.select_dtypes(include=[np.number]).columns[0])
    base_traffic = float(df[traffic_col].mean())
    
    # Realistic scenario multipliers
    scenario_multipliers = {
        'sunny': 1.05,
        'rainy': 0.85, 
        'foggy': 0.75,
        'snowy': 0.65
    }
    
    traffic_mult = scenario_multipliers.get(scenario_name, 1.0) * config['traffic_mult']
    
    # Generate traffic simulations with realistic volatility
    sim_traffic = np.random.normal(
        base_traffic * traffic_mult,
        base_traffic * 0.18,  # 18% volatility
        n_simulations
    )
    
    # FIXED: Realistic congestion threshold (75th percentile)
    scenario_threshold = float(np.percentile(df[traffic_col].dropna(), 75))
    congestion_prob = np.mean(sim_traffic > scenario_threshold) * 100
    
    # Accident risk simulation
    base_accident_rate = 0.025  # 2.5% baseline
    sim_accident_prob = base_accident_rate * config['accident_factor']
    accident_prob = np.random.binomial(1, sim_accident_prob, n_simulations).mean() * 100
    
    return {
        'scenario': scenario_name,
        'description': config['description'],
        'mean_traffic': round(np.mean(sim_traffic), 2),
        'traffic_std': round(np.std(sim_traffic), 2),
        'congestion_prob_high': round(congestion_prob, 2),
        'accident_risk_high': round(accident_prob, 2),
        'threshold_used': round(scenario_threshold, 2),
        'n_simulations': n_simulations
    }

def monte_carlo_bootstrap(df, n_simulations=5000, max_columns=8):
    """Bootstrap resampling for statistical confidence intervals"""
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    print(f"[i] Found {len(numeric_cols)} numeric columns")
    
    if not numeric_cols:
        return pd.DataFrame()
    
    numeric_df = df[numeric_cols].fillna(df[numeric_cols].median())
    simulation_results = {}
    
    # Limit to top columns for performance
    for col in numeric_cols[:max_columns]:
        col_values = numeric_df[col].dropna()
        if len(col_values) > 20:  # Minimum sample size
            print(f"    └─ Bootstrapping {col}...")
            
            sim_means = [
                np.mean(np.random.choice(col_values, size=len(col_values), replace=True))
                for _ in range(n_simulations)
            ]
            
            simulation_results[col] = {
                'mean_estimate': round(np.mean(sim_means), 4),
                'std_estimate': round(np.std(sim_means), 4),
                'ci_lower_95': round(np.percentile(sim_means, 2.5), 4),
                'ci_upper_95': round(np.percentile(sim_means, 97.5), 4),
                'simulations': n_simulations
            }
    
    return pd.DataFrame(simulation_results).T

def monte_carlo_simulation(client):
    """Main Monte Carlo pipeline - Weather Scenarios + Bootstrap"""
    
    # ----------------- Configuration -----------------
    SILVER_BUCKET = "silver"
    GOLD_BUCKET = "gold"
    MERGED_FILE = "merged_data.parquet"
    
    OUTPUT_FILE = "monte_carlo_results.parquet"           # Bootstrap stats
    SCENARIO_FILE = "monte_carlo_scenarios.parquet"       # Weather scenarios
    
    LOCAL_OUTPUT = "/app/data/gold/monte_carlo_results.parquet"
    LOCAL_SCENARIO = "/app/data/gold/monte_carlo_scenarios.parquet"
    
    try:
        # ----------------- MinIO Connection & Bucket Setup -----------------
        print(" [1/7] Connecting to MinIO...")
        for attempt in range(20):
            try:
                client.list_buckets()
                print(f"✔ MinIO ready after {attempt*3}s")
                break
            except:
                print(f"[i] MinIO wait {attempt+1}/20...")
                time.sleep(3)
        else:
            print("[✖] MinIO unavailable after 60s")
            return False
        
        # Auto-create gold bucket
        try:
            buckets = [b.name for b in client.list_buckets()]
            if GOLD_BUCKET not in buckets:
                client.make_bucket(GOLD_BUCKET)
                print(f"✔ Created {GOLD_BUCKET} bucket")
        except:
            pass
        
        # ----------------- Load Silver Data -----------------
        print(f"\n [2/7] Loading {MERGED_FILE} from {SILVER_BUCKET}...")
        obj = client.get_object(SILVER_BUCKET, MERGED_FILE)
        merged_df = pd.read_parquet(BytesIO(obj.read()))
        print(f"✔ Loaded {len(merged_df):,} rows, {len(merged_df.columns)} columns")
        print(f"📊 Sample columns: {list(merged_df.columns[:5])}")
        
        # ----------------- WEATHER SCENARIO SIMULATION -----------------
        print("\n [3/7] WEATHER SCENARIO MONTE CARLO")
        scenarios = define_weather_scenarios()
        scenario_results = []
        
        for scenario_name, config in scenarios.items():
            print(f"\n[i] Running '{scenario_name}' scenario...")
            result = simulate_scenario_impact(merged_df, scenario_name, config)
            scenario_results.append(result)
            print(f"    ├─ Mean Traffic:     {result['mean_traffic']:,.0f}")
            print(f"    ├─ Congestion Risk:  {result['congestion_prob_high']:,.1f}%")
            print(f"    └─ Accident Risk:   {result['accident_risk_high']:,.1f}%")
        
        scenario_df = pd.DataFrame(scenario_results)
        print(f"\n✔ Weather scenarios complete: {len(scenario_results)} scenarios")
        
        # ----------------- BOOTSTRAP STATISTICAL SIMULATION -----------------
        print("\n [4/7] BOOTSTRAP RESAMPLING")
        bootstrap_df = monte_carlo_bootstrap(merged_df)
        print(f"✔ Bootstrap complete: {len(bootstrap_df)} variables")
        
        # ----------------- Save Locally -----------------
        print("\n [5/7] Saving to local Gold layer...")
        os.makedirs("/app/data/gold", exist_ok=True)
        
        scenario_df.to_parquet(LOCAL_SCENARIO, index=False, engine='fastparquet')
        bootstrap_df.to_parquet(LOCAL_OUTPUT, index=True, engine='fastparquet')
        
        print(f"✔ Local files:")
        print(f"    → {LOCAL_SCENARIO}")
        print(f"    → {LOCAL_OUTPUT}")
        
        # ----------------- Upload to MinIO Gold -----------------
        print("\n  [6/7] Uploading to MinIO Gold...")
        
        # Upload scenario results
        scenario_buffer = BytesIO()
        scenario_df.to_parquet(scenario_buffer, index=False, engine='fastparquet')
        scenario_buffer.seek(0)
        client.put_object(GOLD_BUCKET, SCENARIO_FILE, scenario_buffer, scenario_buffer.getbuffer().nbytes)
        
        # Upload bootstrap results  
        bootstrap_buffer = BytesIO()
        bootstrap_df.to_parquet(bootstrap_buffer, index=True, engine='fastparquet')
        bootstrap_buffer.seek(0)
        client.put_object(GOLD_BUCKET, OUTPUT_FILE, bootstrap_buffer, bootstrap_buffer.getbuffer().nbytes)
        
        print(f"✔ MinIO Gold uploads:")
        print(f"    → gold/{SCENARIO_FILE}")
        print(f"    → gold/{OUTPUT_FILE}")
        
        # ----------------- Summary Report -----------------
        print("\n" + "="*70)
        print(" MONTE CARLO SIMULATION COMPLETE!")
        print("="*70)
        print("\n WEATHER SCENARIO RESULTS:")
        print(scenario_df[['scenario', 'congestion_prob_high', 'accident_risk_high']].round(2).to_string(index=False))
        print("\n BOOTSTRAP STATISTICS GENERATED:", len(bootstrap_df))
        print("\n Gold layer ready for JupyterLab analysis!")
        print("="*70)
        
        return True
        
    except S3Error as e:
        print(f"[✖] MinIO/S3 Error: {e}")
        return False
    except Exception as e:
        print(f"[✖] Pipeline Error: {str(e)[:300]}...")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print(" Urban Traffic Data Lake - Monte Carlo Gold Layer")
    print("==================================================")
    
    client = get_minio_client()
    success = monte_carlo_simulation(client)
    
    if success:
        print("\n All simulations completed successfully!")
        exit(0)
    else:
        print("\n Pipeline failed - check logs above")
        exit(1)

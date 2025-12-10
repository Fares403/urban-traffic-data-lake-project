"""
main.py
Orchestrates the full ETL pipeline for Urban Traffic Data Lake
"""
import os
import time
from scripts.generate_traffic_data import generate_traffic_data
from scripts.generate_weather_data import generate_weather_data
from scripts.copy_raw_to_bronze import copy_raw_to_bronze
from scripts.clean_traffic import clean_traffic
from scripts.clean_weather import clean_weather
from scripts.merge import merge_datasets
from scripts.factor_analysis import factor_analysis
from scripts.monte_carlo import monte_carlo_simulation
from scripts.copy_to_hdfs import copy_to_hdfs


def get_minio_client():
    """Get MinIO client from environment variables"""
    from minio import Minio
    MINIO_URL = os.getenv("MINIO_URL", "http://minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    endpoint = MINIO_URL.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def run_pipeline():
    print("🚀 Starting Urban Traffic Data Lake ETL Pipeline")
    print("=" * 60)
    
    try:
        # Step 1: **BUCKETS CREATED BY minio-init** (no need to call)
        print("\n[1/10] ✅ MinIO buckets ready (minio-init service)")
        time.sleep(1)
        
        # Step 2: Generate synthetic raw data (local bronze)
        print("\n[2/10] 🌤️ Generating weather data...")
        generate_weather_data()
        time.sleep(1)
        
        print("[3/10] 🚗 Generating traffic data...")
        generate_traffic_data()
        time.sleep(1)
        
        # Step 4: Copy raw data to MinIO Bronze
        print("\n[4/10] ☁️  Copying raw data to MinIO Bronze bucket...")
        if not copy_raw_to_bronze():
            print("[✖] Bronze copy failed, stopping pipeline")
            return False
        time.sleep(2)
        
        # Step 5: Clean data (Silver layer)
        print("\n[5/10] 🧹 Cleaning traffic data → Silver layer...")
        client = get_minio_client()
        if not clean_traffic(client):
            print("[✖] Traffic cleaning failed")
            return False
        
        print("[6/10] 🧹 Cleaning weather data → Silver layer...")
        if not clean_weather(client):
            print("[✖] Weather cleaning failed")
            return False
        time.sleep(2)
        
        # Step 7: Merge cleaned data
        print("\n[7/10] 🔗 Merging traffic + weather data...")
        if not merge_datasets(client):
            print("[✖] Merge failed")
            return False
        time.sleep(2)
        
        # Step 8: Gold layer analytics
        print("\n[8/10] 📊 Factor Analysis (Gold layer)...")
        if not factor_analysis(client):
            print("[✖] Factor analysis failed")
            return False
        
        print("[9/10] 📈 Monte Carlo simulation (Gold layer)...")
        if not monte_carlo_simulation(client):
            print("[✖] Monte Carlo simulation failed")
            return False
        time.sleep(1)
        
        # Step 10: Copy to HDFS (FINAL STEP)
        print("\n[10/10] 🗄️  Copying Silver layer to HDFS...")
        if not copy_to_hdfs():
            print("[⚠️] HDFS copy warning (non-blocking)")
        
        print("\n" + "=" * 60)
        print("✅ ETL Pipeline COMPLETED SUCCESSFULLY!")
        print("📁 Data Lake Locations:")
        print("   🥉 Bronze:   ./data/bronze/*.csv + MinIO bronze/")
        print("   🥈 Silver:   ./data/silver/*.parquet + MinIO silver/")
        print("   🥇 Gold:     ./data/gold/*.parquet + MinIO gold/")
        print("   🗄️  HDFS:    /silver/*.parquet")
        print("\n🌐 Access:")
        print("   MinIO: http://localhost:9001 (minioadmin/minioadmin)")
        print("   HDFS:  http://localhost:9870")
        print("   Jupyter: http://localhost:8888")
        return True
        
    except KeyboardInterrupt:
        print("\n[INFO] Pipeline interrupted by user")
        return False
    except Exception as e:
        print(f"\n[✖] Pipeline failed: {e}")
        return False


if __name__ == "__main__":
    success = run_pipeline()
    exit(0 if success else 1)

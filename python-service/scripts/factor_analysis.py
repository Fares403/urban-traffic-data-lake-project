"""
Urban Traffic & Weather — Factor Analysis Pipeline
=================================================

Module Overview
---------------
This module performs **Factor Analysis** on a merged traffic–weather dataset stored in **MinIO**.
It is designed as a production-ready analytics step in a **Data Lake / Lakehouse** architecture,
transitioning curated **Silver** data into enriched **Gold** analytics outputs.

The pipeline:
1. Connects reliably to MinIO with retry logic.
2. Loads a merged Parquet dataset from the *silver* bucket.
3. Selects, cleans, and validates numeric features.
4. Applies statistical **Factor Analysis** (sklearn).
5. Produces:
   - Per-record factor scores
   - Factor loadings for interpretability
6. Persists outputs locally and uploads them to the *gold* bucket.

This script is safe to run inside Docker containers and CI pipelines.

Key Outputs
-----------
- gold/traffic_weather_factors.parquet  → enriched dataset with factor scores
- gold/factor_loadings.parquet          → variable-to-factor relationships

Intended Audience
-----------------
- Data Engineers
- Analytics Engineers
- Data Scientists

Author
------
Urban Traffic Data Lake Team
"""

import os
import time
from io import BytesIO

import numpy as np
import pandas as pd
from minio import Minio
from minio.error import S3Error
from sklearn.decomposition import FactorAnalysis


# -----------------------------------------------------------------------------
# MinIO Client Utilities
# -----------------------------------------------------------------------------

def get_minio_client():
    """
    Create and return a MinIO client instance.

    Environment Variables
    ---------------------
    MINIO_URL : str
        MinIO service endpoint (default: minio:9000)
    MINIO_ACCESS_KEY : str
        Access key for authentication (default: minioadmin)
    MINIO_SECRET_KEY : str
        Secret key for authentication (default: minioadmin)

    Returns
    -------
    Minio
        Configured MinIO client object.

    Notes
    -----
    - HTTPS is disabled by default for local/dockerized environments.
    - Endpoint normalization removes protocol prefixes if present.
    """

    MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    endpoint = MINIO_URL.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


# -----------------------------------------------------------------------------
# Factor Analysis Pipeline
# -----------------------------------------------------------------------------

def factor_analysis(client):
    """
    Execute the full Factor Analysis workflow.

    Parameters
    ----------
    client : Minio
        Active MinIO client instance.

    Returns
    -------
    bool
        True if the pipeline completes successfully, False otherwise.

    Workflow Details
    ----------------
    1. Waits for MinIO readiness (robust startup handling).
    2. Ensures the *gold* bucket exists.
    3. Loads merged data from the *silver* bucket.
    4. Filters numeric features and handles missing values.
    5. Applies Factor Analysis with adaptive component selection.
    6. Saves factor scores and loadings locally and remotely.

    Design Choices
    --------------
    - Median imputation for numerical stability.
    - Low-variance filtering to avoid degenerate factors.
    - Maximum of 5 factors for interpretability.
    """

    # Bucket & file configuration
    SILVER_BUCKET = "silver"
    GOLD_BUCKET = "gold"

    MERGED_FILE = "merged_data.parquet"
    OUTPUT_FILE = "traffic_weather_factors.parquet"

    LOCAL_OUTPUT = "/app/data/gold/traffic_weather_factors.parquet"

    try:
        # ------------------------------------------------------------------
        # [1/7] MinIO Readiness Check
        # ------------------------------------------------------------------
        print(" [1/7] Connecting to MinIO...")

        for i in range(20):
            try:
                client.list_buckets()
                print(f"✔ MinIO ready ({i * 3}s)")
                break
            except Exception:
                print(f"[i] MinIO wait {i + 1}/20...")
                time.sleep(3)
        else:
            return False

        # ------------------------------------------------------------------
        # [2/7] Ensure Gold Bucket Exists
        # ------------------------------------------------------------------
        try:
            existing_buckets = [b.name for b in client.list_buckets()]
            if GOLD_BUCKET not in existing_buckets:
                client.make_bucket(GOLD_BUCKET)
                print(f"✔ Created bucket: {GOLD_BUCKET}")
        except S3Error:
            pass

        # ------------------------------------------------------------------
        # [3/7] Load Merged Dataset
        # ------------------------------------------------------------------
        print(f"\n [2/7] Loading {MERGED_FILE}...")

        obj = client.get_object(SILVER_BUCKET, MERGED_FILE)
        merged_df = pd.read_parquet(BytesIO(obj.read()))

        print(f"✔ Loaded {len(merged_df):,} rows, {len(merged_df.columns)} columns")

        # ------------------------------------------------------------------
        # [4/7] Numeric Data Preparation
        # ------------------------------------------------------------------
        print("[i] [3/7] Preparing numeric data...")

        numeric_cols = merged_df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_df = merged_df[numeric_cols]

        # Handle missing values using median imputation
        numeric_df = numeric_df.fillna(numeric_df.median())

        # Remove near-constant features
        numeric_df = numeric_df.loc[:, numeric_df.std() > 0.01]

        print(f"✔ Using {len(numeric_df.columns)} numeric variables")

        # ------------------------------------------------------------------
        # [5/7] Factor Analysis Execution
        # ------------------------------------------------------------------
        n_factors = min(5, len(numeric_df.columns) - 1)
        print(f"\n [4/7] Factor Analysis (n_factors={n_factors})...")

        fa = FactorAnalysis(n_components=n_factors, random_state=42)
        fa_result = fa.fit_transform(numeric_df)

        # Factor scores per observation
        fa_df = pd.DataFrame(
            fa_result,
            columns=[f"Factor_{i + 1}_score" for i in range(n_factors)],
        )

        # Factor loadings per variable
        loadings = pd.DataFrame(
            fa.components_.T,
            index=numeric_df.columns,
            columns=[f"Factor_{i + 1}_loading" for i in range(n_factors)],
        ).round(4)

        print(f"✔ Extracted {n_factors} latent factors")
        print("\n Top Loadings per Factor:")

        for factor_col in loadings.columns:
            top_loading = loadings[factor_col].abs().nlargest(2)
            print(f"  {factor_col}: {top_loading.to_dict()}")

        # ------------------------------------------------------------------
        # [6/7] Final Dataset Assembly
        # ------------------------------------------------------------------
        final_df = pd.concat(
            [merged_df.reset_index(drop=True), fa_df.reset_index(drop=True)],
            axis=1,
        )

        # ------------------------------------------------------------------
        # [7/7] Persistence (Local + MinIO)
        # ------------------------------------------------------------------
        print("\n [5/7] Saving locally...")

        os.makedirs("/app/data/gold", exist_ok=True)
        final_df.to_parquet(LOCAL_OUTPUT, index=False)
        loadings.to_parquet("/app/data/gold/factor_loadings.parquet")

        print(f"✔ Saved locally: {LOCAL_OUTPUT}")

        print("\n [6/7] Uploading to MinIO...")

        buffer = BytesIO()
        final_df.to_parquet(buffer, index=False) 
        buffer.seek(0)

        client.put_object(
            GOLD_BUCKET,
            OUTPUT_FILE,
            buffer,
            buffer.getbuffer().nbytes,
        )

        print(f"✔ Uploaded: gold/{OUTPUT_FILE}")

        print("\n FACTOR ANALYSIS COMPLETE!")
        print(f" {n_factors} factors are ready for downstream analytics & Jupyter notebooks")

        return True

    except Exception as e:
        print(f"[✖] Pipeline failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


# -----------------------------------------------------------------------------
# Script Entrypoint
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print(" Urban Traffic Factor Analysis")

    client = get_minio_client()
    success = factor_analysis(client)

    exit(0 if success else 1)

"""
Urban Traffic Data Lake — Monte Carlo Simulation (Gold Layer)
============================================================

Module Purpose
--------------
This module generates **Gold-layer analytical artifacts** using **Monte Carlo simulation**
techniques applied to an integrated urban traffic and weather dataset.

It is designed as a **decision-support analytics stage** within a Medallion
Architecture (Bronze → Silver → Gold), enabling:

- Risk-aware traffic analysis
- Weather-driven congestion forecasting
- Statistical confidence estimation via bootstrapping

The output is optimized for downstream consumption in **JupyterLab**, dashboards,
and reporting tools.

Core Capabilities
-----------------
1. **Weather Scenario Simulation**
   - Sunny, Rainy, Foggy, Snowy conditions
   - Traffic volume perturbation
   - Congestion probability estimation
   - Accident risk modeling

2. **Bootstrap Resampling**
   - Mean estimation stability
   - 95% confidence intervals
   - Variance assessment for numeric variables

3. **Gold Layer Persistence**
   - Local filesystem (container-safe)
   - MinIO object storage (Parquet format)

Key Outputs
-----------
- gold/monte_carlo_scenarios.parquet  → scenario-level risk metrics
- gold/monte_carlo_results.parquet    → bootstrap statistical summaries

Execution Environment
---------------------
- Python 3.9+
- Docker / Containerized runtime
- MinIO (S3-compatible object storage)

Audience
--------
- Data Engineers
- Analytics Engineers
- Data Scientists
- Urban Planning Analysts

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


# -----------------------------------------------------------------------------
# MinIO Client Utilities
# -----------------------------------------------------------------------------

def get_minio_client():
    """
    Create a resilient MinIO client using environment-based configuration.

    Environment Variables
    ---------------------
    MINIO_URL : str
        MinIO endpoint (default: minio:9000)
    MINIO_ACCESS_KEY : str
        Access key for authentication
    MINIO_SECRET_KEY : str
        Secret key for authentication

    Returns
    -------
    Minio
        Configured MinIO client instance.

    Notes
    -----
    - Protocol prefixes are stripped for compatibility.
    - SSL is disabled for local/containerized deployments.
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
# Scenario Configuration
# -----------------------------------------------------------------------------

def define_weather_scenarios():
    """
    Define realistic weather scenarios and their operational impact multipliers.

    Returns
    -------
    dict
        Scenario configuration dictionary containing:
        - description
        - traffic volume multiplier
        - accident risk amplification factor

    Notes
    -----
    Values are calibrated for **relative comparison**, not absolute forecasting.
    """

    return {
        "sunny": {
            "description": "Clear weather, normal conditions",
            "traffic_mult": 1.1,
            "accident_factor": 0.7,
        },
        "rainy": {
            "description": "Heavy rain, reduced visibility",
            "traffic_mult": 0.9,
            "accident_factor": 1.6,
        },
        "foggy": {
            "description": "Dense fog, low visibility",
            "traffic_mult": 0.8,
            "accident_factor": 2.1,
        },
        "snowy": {
            "description": "Snow/ice conditions, severe impact",
            "traffic_mult": 0.7,
            "accident_factor": 2.8,
        },
    }


# -----------------------------------------------------------------------------
# Monte Carlo Scenario Simulation
# -----------------------------------------------------------------------------

def simulate_scenario_impact(df, scenario_name, config, n_simulations=10000):
    """
    Simulate traffic and accident risk under a specific weather scenario.

    Parameters
    ----------
    df : pandas.DataFrame
        Source dataset containing traffic metrics.
    scenario_name : str
        Scenario identifier (e.g., 'rainy', 'foggy').
    config : dict
        Scenario configuration parameters.
    n_simulations : int, optional
        Number of Monte Carlo iterations (default: 10,000).

    Returns
    -------
    dict
        Aggregated simulation metrics including:
        - mean traffic volume
        - traffic volatility
        - congestion probability
        - accident risk probability

    Modeling Assumptions
    --------------------
    - Traffic follows a normal distribution around scenario-adjusted mean.
    - Congestion threshold defined as historical 75th percentile.
    - Accident events follow a Bernoulli process.
    """

    traffic_col = next(
        (col for col in ["traffic_volume", "volume"] if col in df.columns),
        df.select_dtypes(include=[np.number]).columns[0],
    )

    base_traffic = float(df[traffic_col].mean())

    scenario_multipliers = {
        "sunny": 1.05,
        "rainy": 0.85,
        "foggy": 0.75,
        "snowy": 0.65,
    }

    traffic_mult = scenario_multipliers.get(scenario_name, 1.0) * config["traffic_mult"]

    sim_traffic = np.random.normal(
        base_traffic * traffic_mult,
        base_traffic * 0.18,
        n_simulations,
    )

    scenario_threshold = float(np.percentile(df[traffic_col].dropna(), 75))
    congestion_prob = np.mean(sim_traffic > scenario_threshold) * 100

    base_accident_rate = 0.025
    sim_accident_prob = base_accident_rate * config["accident_factor"]
    accident_prob = np.random.binomial(1, sim_accident_prob, n_simulations).mean() * 100

    return {
        "scenario": scenario_name,
        "description": config["description"],
        "mean_traffic": round(np.mean(sim_traffic), 2),
        "traffic_std": round(np.std(sim_traffic), 2),
        "congestion_prob_high": round(congestion_prob, 2),
        "accident_risk_high": round(accident_prob, 2),
        "threshold_used": round(scenario_threshold, 2),
        "n_simulations": n_simulations,
    }


# -----------------------------------------------------------------------------
# Bootstrap Statistical Analysis
# -----------------------------------------------------------------------------

def monte_carlo_bootstrap(df, n_simulations=5000, max_columns=8):
    """
    Perform bootstrap resampling to estimate confidence intervals.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.
    n_simulations : int, optional
        Number of bootstrap iterations per variable.
    max_columns : int, optional
        Maximum number of numeric columns processed for performance.

    Returns
    -------
    pandas.DataFrame
        Bootstrap statistics including mean, std, and 95% CI.

    Notes
    -----
    This step enhances **statistical robustness** of Gold-layer analytics.
    """

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if not numeric_cols:
        return pd.DataFrame()

    numeric_df = df[numeric_cols].fillna(df[numeric_cols].median())
    simulation_results = {}

    for col in numeric_cols[:max_columns]:
        col_values = numeric_df[col].dropna()

        if len(col_values) > 20:
            sim_means = [
                np.mean(np.random.choice(col_values, size=len(col_values), replace=True))
                for _ in range(n_simulations)
            ]

            simulation_results[col] = {
                "mean_estimate": round(np.mean(sim_means), 4),
                "std_estimate": round(np.std(sim_means), 4),
                "ci_lower_95": round(np.percentile(sim_means, 2.5), 4),
                "ci_upper_95": round(np.percentile(sim_means, 97.5), 4),
                "simulations": n_simulations,
            }

    return pd.DataFrame(simulation_results).T


# -----------------------------------------------------------------------------
# Main Pipeline Orchestration
# -----------------------------------------------------------------------------

def monte_carlo_simulation(client):
    """
    Execute the complete Monte Carlo Gold-layer generation pipeline.

    Parameters
    ----------
    client : Minio
        Active MinIO client instance.

    Returns
    -------
    bool
        True on successful completion, False otherwise.

    Pipeline Stages
    ---------------
    1. MinIO readiness validation
    2. Silver-layer data ingestion
    3. Weather scenario simulation
    4. Bootstrap statistical analysis
    5. Gold-layer persistence (local + MinIO)
    """

    SILVER_BUCKET = "silver"
    GOLD_BUCKET = "gold"
    MERGED_FILE = "merged_data.parquet"

    OUTPUT_FILE = "monte_carlo_results.parquet"
    SCENARIO_FILE = "monte_carlo_scenarios.parquet"

    LOCAL_OUTPUT = "/app/data/gold/monte_carlo_results.parquet"
    LOCAL_SCENARIO = "/app/data/gold/monte_carlo_scenarios.parquet"

    try:
        # MinIO readiness check
        for attempt in range(20):
            try:
                client.list_buckets()
                break
            except Exception:
                time.sleep(3)
        else:
            return False

        # Ensure Gold bucket exists
        try:
            buckets = [b.name for b in client.list_buckets()]
            if GOLD_BUCKET not in buckets:
                client.make_bucket(GOLD_BUCKET)
        except S3Error:
            pass

        # Load Silver data
        obj = client.get_object(SILVER_BUCKET, MERGED_FILE)
        merged_df = pd.read_parquet(BytesIO(obj.read()))

        # Weather scenarios
        scenarios = define_weather_scenarios()
        scenario_results = [
            simulate_scenario_impact(merged_df, name, cfg)
            for name, cfg in scenarios.items()
        ]

        scenario_df = pd.DataFrame(scenario_results)

        # Bootstrap analysis
        bootstrap_df = monte_carlo_bootstrap(merged_df)

        # Persist locally
        os.makedirs("/app/data/gold", exist_ok=True)
        scenario_df.to_parquet(LOCAL_SCENARIO, index=False, engine="fastparquet")
        bootstrap_df.to_parquet(LOCAL_OUTPUT, index=True, engine="fastparquet")

        # Upload to MinIO
        buf = BytesIO()
        scenario_df.to_parquet(buf, index=False, engine="fastparquet")
        buf.seek(0)
        client.put_object(GOLD_BUCKET, SCENARIO_FILE, buf, buf.getbuffer().nbytes)

        buf = BytesIO()
        bootstrap_df.to_parquet(buf, index=True, engine="fastparquet")
        buf.seek(0)
        client.put_object(GOLD_BUCKET, OUTPUT_FILE, buf, buf.getbuffer().nbytes)

        return True

    except Exception:
        return False


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    client = get_minio_client()
    exit(0 if monte_carlo_simulation(client) else 1)

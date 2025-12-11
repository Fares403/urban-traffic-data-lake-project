This project is a local urban traffic data lake implementing a full Medallion Architecture (Bronze → Silver → Gold) with HDFS, MinIO, a Python ETL pipeline, and JupyterLab. The documentation below is written as a complete `README.md` for your GitHub repository, including how to obtain the Jupyter token from container logs.

## Overview

The Urban Traffic Data Lake Platform provides an end-to-end analytical environment for simulated traffic and weather data. It uses Docker Compose to orchestrate storage, ETL, and analytics components, and produces curated datasets ready for visualization and modeling.

Core capabilities:

- HDFS cluster (NameNode + DataNode) for distributed storage.
- MinIO (S3-compatible) object store with bronze/silver/gold buckets.
- Python ETL pipeline implementing Bronze → Silver → Gold transformations.
- Lightweight JupyterLab with only essential Python libraries.
- Realistic traffic and weather data, plus Monte Carlo simulation outputs.
- Data quality monitoring and simple health checks.

## Architecture

The platform follows the Medallion pattern:

- Bronze layer  
  - Raw, append-only data (CSV).  
  - Example: `traffic_raw.csv`, `weather_raw.csv`.

- Silver layer  
  - Clean, validated, and conformed data in Parquet format.  
  - Example: `traffic_clean.parquet`, `weather_clean.parquet`, `merged_data.parquet`.

- Gold layer  
  - Aggregated and feature-rich datasets optimized for analytics and modeling.  
  - Example: `monte_carlo_results.parquet`, `traffic_weather_factors.parquet`.

Data flows through:

1. Data generation or ingestion into Bronze.
2. Cleaning and transformation into Silver.
3. Merging, feature engineering, and simulation into Gold.
4. Exploration and modeling through JupyterLab.

## Repository Structure

```text
urban-traffic-data-lake-project/
├── docker-compose.yml          # All services and network wiring
├── hadoop.env                  # HDFS configuration
├── logs.txt                    # Collected logs (optional)
├── data/
│   ├── bronze/
│   │   ├── traffic_raw.csv
│   │   └── weather_raw.csv
│   ├── silver/
│   │   ├── traffic_clean.parquet
│   │   ├── weather_clean.parquet
│   │   └── merged_data.parquet
│   ├── gold/
│   │   ├── monte_carlo_results.parquet
│   │   └── traffic_weather_factors.parquet
│   ├── hadoop/                 # HDFS-related volumes
│   └── minio/                  # Local mirror for MinIO buckets
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── notebooks/
│   └── Analysis.ipynb          # Main analysis notebook
├── python-service/
│   ├── Dockerfile
│   ├── main.py                 # ETL orchestrator
│   ├── requirements.txt
│   └── scripts/                # Modular ETL steps
│       ├── clean_traffic.py
│       ├── clean_weather.py
│       ├── copy_raw_to_bronze.py
│       ├── copy_to_hdfs.py
│       ├── create_buckets.py
│       ├── factor_analysis.py
│       ├── generate_traffic_data.py
│       ├── generate_weather_data.py
│       ├── merge.py
│       ├── monte_carlo.py
│       └── __pycache__/
└── src/
    └── utils.py
```

## Services

Each service is defined in `docker-compose.yml`.

| Service        | Description                                      | Default access                          |
|----------------|--------------------------------------------------|-----------------------------------------|
| `namenode`     | HDFS NameNode                                   | Web UI: http://localhost:9870          |
| `datanode`     | HDFS DataNode                                   | Internal                                |
| `minio`        | S3-compatible object storage                    | Console: http://localhost:9001          |
|                |                                                  | API: http://localhost:9002              |
| `minio-init`   | Creates `bronze`, `silver`, `gold` buckets      | Runs on startup                         |
| `python-service` | ETL pipeline (Bronze → Silver → Gold)         | Run-on-demand via `docker compose run`  |
| `jupyter`      | JupyterLab for analysis                         | http://localhost:8888                   |
| `data-monitor` | Data lake status reporter (optional profile)    | Logs via `docker compose logs`          |

## Prerequisites

- Docker and Docker Compose installed.
- Unix-like shell (Linux, macOS, or WSL).
- Recommended: at least 6–8 GB free disk space.

Example checks:

```bash
docker --version
docker compose version
```

## Setup and First Run

### 1. Clone the repository

```bash
git clone https://github.com/Fares403/urban-traffic-data-lake-project
cd urban-traffic-data-lake-project
```

### 2. Ensure directory layout

If not already present, create key directories:

```bash
mkdir -p data/bronze data/silver data/gold
mkdir -p notebooks
```

### 3. Start the platform

```bash
docker compose up -d
```

This command starts:

- HDFS NameNode and DataNode.
- MinIO plus the init container to create bronze/silver/gold buckets.
- JupyterLab.
- Data volumes and shared network.

Verify containers:

```bash
docker compose ps
```

## Accessing Components

### HDFS Web UI

Open:

```text
http://localhost:9870
```

Use this interface to check HDFS status and browse directories if your ETL pushes files to HDFS.

### MinIO Console

Open:

```text
http://localhost:9001
```

Default credentials:

- Access key: `minioadmin`
- Secret key: `minioadmin`

You should see the following buckets:

- `bronze`
- `silver`
- `gold`

These represent the three medallion layers in object storage form.

## JupyterLab: Access and Token Retrieval

Depending on how the Jupyter service is configured in `docker-compose.yml`, JupyterLab may use a token for authentication.

There are two main cases:

### Case 1: Token disabled (no password required)

If the Jupyter start command includes:

```yaml
--NotebookApp.token=''
--NotebookApp.password=''
```

then you can access JupyterLab directly at:

```text
http://localhost:8888/lab
```

No token or password is required.

### Case 2: Token enabled (default)

If the token settings are not overridden, JupyterLab generates a random token on startup. To obtain it from the logs:

1. List containers and confirm the Jupyter container name (usually `urban-traffic-jupyterlab`):

   ```bash
   docker compose ps
   ```

2. Show the logs for the Jupyter container:

   ```bash
   docker compose logs jupyter
   ```

3. Look for a line similar to:

   ```text
   http://127.0.0.1:8888/lab?token=YOUR_LONG_TOKEN_HERE
   ```

4. Copy this full URL (or just the token value) into your browser:

   - Direct: paste the full URL.  
   - Or go to `http://localhost:8888/lab` and paste only the token when prompted.

If the logs are very long, you can restrict to recent entries:

```bash
docker compose logs --tail=50 jupyter
```

or filter lines containing “token”:

```bash
docker compose logs jupyter | grep -i token
```

This procedure is the standard way to retrieve the Jupyter access token from a container’s logs.

## Working in JupyterLab

Open JupyterLab in the browser and use the left file browser:

- `/home/jovyan/work` → mapped to `./notebooks` on the host.  
- `/home/jovyan/data` → mapped to `./data` on the host.

Open `Analysis.ipynb` and start exploring.

Example: load Gold-layer Parquet data:

```python
import pandas as pd
from pathlib import Path

data_root = Path("/home/jovyan/data")

monte_carlo_df = pd.read_parquet(data_root / "gold" / "monte_carlo_results.parquet")
traffic_factors_df = pd.read_parquet(data_root / "gold" / "traffic_weather_factors.parquet")

monte_carlo_df.head()
traffic_factors_df.head()
```

## ETL Pipeline

The ETL logic resides in `python-service/`, with modular scripts under `python-service/scripts/`.

Typical pipeline steps:

1. Generate synthetic raw traffic and weather data.
2. Copy raw files into the Bronze directory or MinIO `bronze` bucket.
3. Clean traffic and weather data into Silver Parquet tables.
4. Merge cleaned datasets.
5. Run Monte Carlo simulations and factor analysis to produce Gold outputs.
6. Optionally copy data to HDFS and/or create MinIO buckets.

### Running the full pipeline

If `main.py` orchestrates all steps:

```bash
docker compose run --rm python-service python main.py
```

This will populate:

- `data/bronze` with raw CSV files.
- `data/silver` with cleaned and merged Parquet tables.
- `data/gold` with Monte Carlo and factor analysis results.

### Running individual ETL steps

Each script can be launched independently, for example:

```bash
# Generate raw data
docker compose run --rm python-service python scripts/generate_traffic_data.py
docker compose run --rm python-service python scripts/generate_weather_data.py

# Move to Bronze
docker compose run --rm python-service python scripts/copy_raw_to_bronze.py

# Clean and standardize (Silver)
docker compose run --rm python-service python scripts/clean_traffic.py
docker compose run --rm python-service python scripts/clean_weather.py
docker compose run --rm python-service python scripts/merge.py

# Gold analytics
docker compose run --rm python-service python scripts/monte_carlo.py
docker compose run --rm python-service python scripts/factor_analysis.py

# Optional: MinIO / HDFS integration
docker compose run --rm python-service python scripts/create_buckets.py
docker compose run --rm python-service python scripts/copy_to_hdfs.py
```

Verify outputs:

```bash
find data/bronze -type f
find data/silver -type f
find data/gold -type f
```

## Data Lake Status Monitor

The `data-monitor` service provides a simple count and size summary of each layer.

Run:

```bash
docker compose --profile monitor up data-monitor
docker compose logs data-monitor
```

Example output:

```text
Urban Traffic Data Lake Status Report
=======================================
Bronze Layer: X files
Silver Layer: Y files
Gold Layer: Z files
Total Size: <size>  /data
```

This is useful after ETL runs to confirm that each layer is populated as expected.

## Typical End-to-End Flow

1. Start core services:

   ```bash
   docker compose up -d
   ```

2. Run the full ETL pipeline:

   ```bash
   docker compose run --rm python-service python main.py
   ```

3. Confirm outputs in `data/bronze`, `data/silver`, and `data/gold`.

4. Retrieve the Jupyter token from logs if required:

   ```bash
   docker compose logs jupyter | grep -i token
   ```

5. Open JupyterLab (`http://localhost:8888/lab`), load `Analysis.ipynb`, and explore Gold-layer datasets.

6. Optionally run the data monitor:

   ```bash
   docker compose --profile monitor up data-monitor
   docker compose logs data-monitor
   ```

7. Stop the stack when finished:

   ```bash
   docker compose down
   ```

Data under `data/` and Docker volumes will persist until explicitly removed.

## Customization

- Jupyter dependencies: adjust the Jupyter image or the install commands in `docker-compose.yml` to add or remove Python libraries.
- ETL logic: extend or modify scripts in `python-service/scripts/` and update `main.py` accordingly.
- Scaling: tune HDFS replication, add more data nodes, or introduce additional services (e.g., Spark) on the same Docker network.

## License

Specify your chosen license here, for example:

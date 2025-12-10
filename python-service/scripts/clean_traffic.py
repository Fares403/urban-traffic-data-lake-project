"""
Clean Traffic Dataset

This script performs data cleaning on the raw traffic dataset stored in MinIO Bronze bucket.
Steps:
1. Fetch CSV from MinIO Bronze bucket.
2. Remove duplicates, handle missing values, fix bad formats, remove outliers.
3. Save cleaned dataset as Parquet locally (/data/silver).
4. Upload cleaned dataset to MinIO Silver bucket.
"""

import pandas as pd
import numpy as np
from minio import Minio
from io import BytesIO
import os

# -------- MinIO client --------
client = Minio(
    "minio:9000",  # service name in docker-compose
    access_key="minioaccess",
    secret_key="miniosecret",
    secure=False
)

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
OBJECT_NAME = "traffic_raw.csv"
OUTPUT_FILE_LOCAL = "/data/silver/traffic_clean.parquet"

# -------- Fetch CSV from Bronze --------
response = client.get_object(BRONZE_BUCKET, OBJECT_NAME)
df = pd.read_csv(BytesIO(response.read()))

# -------- Cleaning --------
# 1. Remove duplicates
df = df.drop_duplicates()

# 2. Handle missing values (example: fill numeric with median, categorical with mode)
numeric_cols = ["vehicle_count", "avg_speed_kmh", "accident_count", "visibility_m"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df[col].fillna(df[col].median(), inplace=True)

categorical_cols = ["city", "area", "congestion_level", "road_condition"]
for col in categorical_cols:
    df[col] = df[col].fillna(df[col].mode()[0])

# 3. Fix dates
df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")
df = df.dropna(subset=["date_time"])  # drop rows with unfixable dates

# 4. Remove outliers for numeric columns (simple IQR method)
for col in numeric_cols:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    df = df[(df[col] >= Q1 - 1.5 * IQR) & (df[col] <= Q3 + 1.5 * IQR)]

# -------- Save locally --------
os.makedirs(os.path.dirname(OUTPUT_FILE_LOCAL), exist_ok=True)
df.to_parquet(OUTPUT_FILE_LOCAL, index=False)
print(f"[✔] Cleaned traffic dataset saved locally → {OUTPUT_FILE_LOCAL}")

# -------- Upload to MinIO Silver --------
with open(OUTPUT_FILE_LOCAL, "rb") as f:
    client.put_object(
        SILVER_BUCKET,
        "traffic_clean.parquet",
        f,
        length=os.path.getsize(OUTPUT_FILE_LOCAL)
    )
print(f"[✔] Cleaned traffic dataset uploaded to MinIO Silver bucket")

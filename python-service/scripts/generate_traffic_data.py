import pandas as pd
import numpy as np
from random import choice, randint, uniform
from datetime import datetime, timedelta
import os

def random_bad_datetime():
    bad_formats = [
        "TBD",
        "2099-00-00 99:99",
        "32/13/2025 25:61",
        "Invalid",
        None
    ]
    return choice(bad_formats)

def random_good_datetime(base, i):
    dt = base + timedelta(hours=i)
    formats = [
        dt.strftime("%Y-%m-%d %H:%M"),
        dt.strftime("%d/%m/%Y %I%p"),
        dt.strftime("%Y-%m-%dT%H:%MZ")
    ]
    return choice(formats)

def generate_traffic_dataset(n=5000):
    # --- traffic_id ---
    traffic_ids = list(range(9001, 9001 + n))

    # duplicates
    for _ in range(15):
        traffic_ids[randint(0, n - 1)] = choice(traffic_ids)

    # NULL IDs
    for _ in range(8):
        traffic_ids[randint(0, n - 1)] = None

    # --- date_time ---
    base_date = datetime(2024, 1, 1)
    date_times = []

    for i in range(n):
        if randint(1, 100) <= 7:  # 7% bad timestamps
            date_times.append(random_bad_datetime())
        else:
            date_times.append(random_good_datetime(base_date, i))

    # --- city ---
    cities = ["London", None]
    city_values = [choice(cities) for _ in range(n)]

    # --- area ---
    areas = ["Camden", "Chelsea", "Islington", "Southwark", "Kensington", None]
    area_values = [choice(areas) for _ in range(n)]

    # --- vehicle_count ---
    vehicle_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:  # 5% extreme outliers
            vehicle_values.append(randint(10000, 25000))
        elif randint(1, 100) <= 5:  # 5% NULL
            vehicle_values.append(None)
        else:
            vehicle_values.append(randint(0, 5000))

    # --- avg_speed_kmh ---
    speed_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:
            speed_values.append(uniform(-20, -1))  # invalid negative speeds
        elif randint(1, 100) <= 5:
            speed_values.append(None)
        else:
            speed_values.append(uniform(3, 120))

    # --- accident_count ---
    accident_values = []
    for _ in range(n):
        if randint(1, 100) <= 2:  # rare outliers
            accident_values.append(randint(20, 60))
        elif randint(1, 100) <= 5:
            accident_values.append(None)
        else:
            accident_values.append(randint(0, 10))

    # --- congestion_level ---
    congestion_levels = ["Low", "Medium", "High", None]
    congestion_values = [choice(congestion_levels) for _ in range(n)]

    # --- road_condition ---
    road_conditions = ["Dry", "Wet", "Snowy", "Damaged", None]
    road_values = [choice(road_conditions) for _ in range(n)]

    # --- visibility_m ---
    visibility_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:
            visibility_values.append(randint(20000, 50000))  # extreme
        elif randint(1, 100) <= 5:
            visibility_values.append(None)
        else:
            visibility_values.append(randint(50, 10000))

    # -------- DataFrame --------
    df = pd.DataFrame({
        "traffic_id": traffic_ids,
        "date_time": date_times,
        "city": city_values,
        "area": area_values,
        "vehicle_count": vehicle_values,
        "avg_speed_kmh": speed_values,
        "accident_count": accident_values,
        "congestion_level": congestion_values,
        "road_condition": road_values,
        "visibility_m": visibility_values
    })
    
    return df

def generate_traffic_data():
    """Generate traffic data and save to local repo structure"""
    # Local repo paths (matches your tree structure)
    bronze_path = "/app/data/bronze"
    os.makedirs(bronze_path, exist_ok=True)
    output_file = os.path.join(bronze_path, "traffic_raw.csv")
    
    # Generate dataset
    df = generate_traffic_dataset(n=5000)
    
    # Save to bronze layer
    df.to_csv(output_file, index=False)
    print(f"[✔] Synthetic traffic dataset generated → {output_file} ({len(df)} rows)")

if __name__ == "__main__":
    generate_traffic_data()

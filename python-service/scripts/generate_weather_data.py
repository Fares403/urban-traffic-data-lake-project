import pandas as pd
import numpy as np
from random import choice, randint, uniform
from datetime import datetime, timedelta
import os

def random_bad_datetime():
    bad_formats = [
        "Unknown",
        "2099-13-40 25:61",
        "32/15/2024 99:99",
        "2024-01-15T99:00Z",
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

def derive_season_from_date(dt_str):
    """Sometimes returns None or wrong value for messy scenarios."""
    if dt_str is None:
        return None
    try:
        # try normal parsing
        dt = pd.to_datetime(dt_str, errors="raise")
        m = dt.month
        if m in [12, 1, 2]:
            return "Winter"
        elif m in [3, 4, 5]:
            return "Spring"
        elif m in [6, 7, 8]:
            return "Summer"
        else:
            return "Autumn"
    except:
        # invalid date -> random messy behaviour
        return choice([None, "Winter", "FoggySeason"])

def generate_weather_dataset(n=5000):
    # --- weather_id ---
    weather_ids = list(range(5001, 5001 + n))

    # duplicates
    for _ in range(20):
        weather_ids[randint(0, n - 1)] = choice(weather_ids)

    # NULL IDs
    for _ in range(10):
        weather_ids[randint(0, n - 1)] = None

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

    # --- season (derived) but messy ---
    seasons = [derive_season_from_date(dt) for dt in date_times]

    # --- temperature_c ---
    temperature_values = []
    for s in seasons:
        if randint(1, 100) <= 5:  # 5% NULL
            temperature_values.append(None)
            continue

        # seasonal realistic ranges
        if s == "Winter":
            temp = uniform(-5, 15)
        elif s == "Spring":
            temp = uniform(5, 20)
        elif s == "Summer":
            temp = uniform(10, 35)
        elif s == "Autumn":
            temp = uniform(0, 20)
        else:
            temp = uniform(-5, 35)

        # 3% outliers
        if randint(1, 100) <= 3:
            temp = choice([-30, 60])

        temperature_values.append(temp)

    # --- humidity ---
    humidity_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:
            humidity_values.append(None)
        else:
            h = randint(20, 100)
            if randint(1, 100) <= 3:  # messy
                h = choice([-10, 150])
            humidity_values.append(h)

    # --- rain_mm ---
    rain_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:
            rain_values.append(None)
        else:
            r = uniform(0, 50)
            if randint(1, 100) <= 3:
                r = uniform(120, 200)  # extreme
            rain_values.append(r)

    # --- wind_speed_kmh ---
    wind_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:
            wind_values.append(None)
        else:
            w = uniform(0, 80)
            if randint(1, 100) <= 3:
                w = uniform(200, 300)
            wind_values.append(w)

    # --- visibility_m ---
    visibility_values = []
    for _ in range(n):
        if randint(1, 100) <= 5:
            visibility_values.append(None)
        else:
            v = randint(50, 10000)
            if randint(1, 100) <= 3:
                v = choice([50000, "Unknown", "NaN", "xxx"])  # messy strings
            visibility_values.append(v)

    # --- weather_condition ---
    weather_conditions = ["Clear", "Rain", "Fog", "Storm", "Snow", None]
    condition_values = [choice(weather_conditions) for _ in range(n)]

    # -------- DataFrame --------
    df = pd.DataFrame({
        "weather_id": weather_ids,
        "date_time": date_times,
        "city": city_values,
        "season": seasons,
        "temperature_c": temperature_values,
        "humidity": humidity_values,
        "rain_mm": rain_values,
        "wind_speed_kmh": wind_values,
        "visibility_m": visibility_values,
        "weather_condition": condition_values
    })
    
    return df

def generate_weather_data():
    """Generate weather data and save to local repo structure"""
    # Local repo paths (matches your tree structure)
    bronze_path = "/app/data/bronze"
    os.makedirs(bronze_path, exist_ok=True)
    output_file = os.path.join(bronze_path, "weather_raw.csv")
    
    # Generate dataset
    df = generate_weather_dataset(n=5000)
    
    # Save to bronze layer
    df.to_csv(output_file, index=False)
    print(f"[✔] Synthetic weather dataset generated → {output_file} ({len(df)} rows)")

if __name__ == "__main__":
    generate_weather_data()

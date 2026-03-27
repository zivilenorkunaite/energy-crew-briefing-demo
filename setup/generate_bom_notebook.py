"""Generate setup/03_bom_refresh.py notebook from customise.py config.

Run before bundle deploy to ensure the notebook has current depot locations.
Called automatically by deploy.sh.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from server.customise import WEATHER_STATIONS, UC_FULL, TIMEZONE

TABLE = f"{UC_FULL}.bom_weather"
OUTPUT = os.path.join(os.path.dirname(__file__), "03_bom_refresh.py")

# Format stations as Python list literal
stations_lines = []
for s in WEATHER_STATIONS:
    stations_lines.append(f'    {{"name": "{s["name"]}":{" " * (20 - len(s["name"]))}"lat": {s["lat"]}, "lon": {s["lon"]}}},')
stations_block = "[\n" + "\n".join(stations_lines) + "\n]"

NOTEBOOK = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Refresh (Open-Meteo)
# MAGIC Fetches current observations + 7-day hourly forecasts from Open-Meteo API
# MAGIC for depot areas and upserts into `{TABLE}`.
# MAGIC
# MAGIC Scheduled hourly via DAB job (serverless compute).
# MAGIC
# MAGIC **Auto-generated from server/customise.py — do not edit manually.**

# COMMAND ----------

import json
import urllib.request

TABLE = "{TABLE}"

STATIONS = {stations_block}

WMO_CODES = {{
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Light rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail",
}}

WIND_DIRS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
             "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

# COMMAND ----------

lats = ",".join(str(s["lat"]) for s in STATIONS)
lons = ",".join(str(s["lon"]) for s in STATIONS)

url = (
    f"https://api.open-meteo.com/v1/forecast?"
    f"latitude={{lats}}&longitude={{lons}}"
    f"&current=temperature_2m,apparent_temperature,relative_humidity_2m,"
    f"wind_speed_10m,wind_gusts_10m,wind_direction_10m,weather_code,precipitation,cloud_cover"
    f"&hourly=temperature_2m,apparent_temperature,relative_humidity_2m,"
    f"wind_speed_10m,wind_gusts_10m,wind_direction_10m,weather_code,precipitation,cloud_cover"
    f"&forecast_days=14&timezone={TIMEZONE}&wind_speed_unit=kmh"
)

req = urllib.request.Request(url, headers={{"User-Agent": "Mozilla/5.0"}})
with urllib.request.urlopen(req, timeout=30) as resp:
    data = json.loads(resp.read())

results_list = data if isinstance(data, list) else [data]
print(f"Got {{len(results_list)}} station results")

# COMMAND ----------

def wind_compass(deg):
    if deg is None:
        return ""
    return WIND_DIRS[int((deg + 11.25) / 22.5) % 16]


all_rows = []
for i, result in enumerate(results_list):
    if i >= len(STATIONS):
        break
    station = STATIONS[i]

    c = result.get("current", {{}})
    code = c.get("weather_code", 0)
    all_rows.append({{
        "station_name": station["name"],
        "latitude": station["lat"],
        "longitude": station["lon"],
        "observation_time": c.get("time", ""),
        "forecast_type": "current",
        "temperature": c.get("temperature_2m"),
        "apparent_temperature": c.get("apparent_temperature"),
        "humidity": c.get("relative_humidity_2m"),
        "wind_speed_kmh": c.get("wind_speed_10m"),
        "wind_gust_kmh": c.get("wind_gusts_10m"),
        "wind_direction": wind_compass(c.get("wind_direction_10m")),
        "precipitation": c.get("precipitation"),
        "weather_code": code,
        "weather_description": WMO_CODES.get(code, f"Code {{code}}"),
        "cloud_cover": c.get("cloud_cover"),
    }})

    h = result.get("hourly", {{}})
    times = h.get("time", [])
    for j, t in enumerate(times):
        hcode = (h.get("weather_code") or [None])[j] if j < len(h.get("weather_code", [])) else None
        all_rows.append({{
            "station_name": station["name"],
            "latitude": station["lat"],
            "longitude": station["lon"],
            "observation_time": t,
            "forecast_type": "hourly_forecast",
            "temperature": (h.get("temperature_2m") or [None])[j] if j < len(h.get("temperature_2m", [])) else None,
            "apparent_temperature": (h.get("apparent_temperature") or [None])[j] if j < len(h.get("apparent_temperature", [])) else None,
            "humidity": (h.get("relative_humidity_2m") or [None])[j] if j < len(h.get("relative_humidity_2m", [])) else None,
            "wind_speed_kmh": (h.get("wind_speed_10m") or [None])[j] if j < len(h.get("wind_speed_10m", [])) else None,
            "wind_gust_kmh": (h.get("wind_gusts_10m") or [None])[j] if j < len(h.get("wind_gusts_10m", [])) else None,
            "wind_direction": wind_compass((h.get("wind_direction_10m") or [None])[j] if j < len(h.get("wind_direction_10m", [])) else None),
            "precipitation": (h.get("precipitation") or [None])[j] if j < len(h.get("precipitation", [])) else None,
            "weather_code": hcode,
            "weather_description": WMO_CODES.get(hcode, f"Code {{hcode}}") if hcode is not None else None,
            "cloud_cover": (h.get("cloud_cover") or [None])[j] if j < len(h.get("cloud_cover", [])) else None,
        }})

print(f"Parsed {{len(all_rows)}} rows ({{len(STATIONS)}} current + {{len(all_rows) - len(STATIONS)}} hourly forecasts)")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp

if all_rows:
    schema = StructType([
        StructField("station_name", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("observation_time", StringType(), True),
        StructField("forecast_type", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_gust_kmh", DoubleType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("weather_description", StringType(), True),
        StructField("cloud_cover", IntegerType(), True),
    ])

    df = spark.createDataFrame(all_rows, schema=schema)
    df = df.withColumn("observation_time", to_timestamp("observation_time", "yyyy-MM-dd\\'T\\'HH:mm"))
    df = df.withColumn("refreshed_at", current_timestamp())

    df_current = df.filter("forecast_type = 'current'")
    df_forecast = df.filter("forecast_type = 'hourly_forecast'")

    df_current.createOrReplaceTempView("current_updates")
    spark.sql(f"DELETE FROM {{TABLE}} WHERE forecast_type = 'current'")
    spark.sql(f"INSERT INTO {{TABLE}} SELECT * FROM current_updates")
    current_count = df_current.count()
    print(f"Current: replaced {{current_count}} station observations")

    df_forecast.createOrReplaceTempView("forecast_updates")
    spark.sql(f"""
        MERGE INTO {{TABLE}} AS target
        USING forecast_updates AS source
        ON target.station_name = source.station_name
           AND target.observation_time = source.observation_time
           AND target.forecast_type = 'hourly_forecast'
        WHEN MATCHED THEN UPDATE SET
            latitude = source.latitude,
            longitude = source.longitude,
            temperature = source.temperature,
            apparent_temperature = source.apparent_temperature,
            humidity = source.humidity,
            wind_speed_kmh = source.wind_speed_kmh,
            wind_gust_kmh = source.wind_gust_kmh,
            wind_direction = source.wind_direction,
            precipitation = source.precipitation,
            weather_code = source.weather_code,
            weather_description = source.weather_description,
            cloud_cover = source.cloud_cover,
            refreshed_at = source.refreshed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    forecast_count = df_forecast.count()
    print(f"Forecasts: upserted {{forecast_count}} hourly rows")

    print(f"Total: {{current_count + forecast_count}} rows into {{TABLE}}")
else:
    print("No data fetched. Skipping MERGE.")
'''

if __name__ == "__main__":
    with open(OUTPUT, "w") as f:
        f.write(NOTEBOOK)
    print(f"Generated {OUTPUT} with {len(WEATHER_STATIONS)} stations from customise.py")

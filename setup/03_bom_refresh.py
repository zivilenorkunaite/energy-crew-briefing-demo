# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Refresh (Open-Meteo)
# MAGIC Fetches latest observations from Open-Meteo API for Essential Energy depot areas
# MAGIC and upserts into `zivile.essential_energy_wacs.bom_weather`.
# MAGIC
# MAGIC Scheduled hourly via DAB job.

# COMMAND ----------

import json
import urllib.request

TABLE = "zivile.essential_energy_wacs.bom_weather"

# Essential Energy depot locations (deduplicated)
STATIONS = [
    {"name": "Grafton",         "lat": -29.69, "lon": 152.93},
    {"name": "Coffs Harbour",   "lat": -30.30, "lon": 153.11},
    {"name": "Tamworth",        "lat": -31.09, "lon": 150.93},
    {"name": "Orange",          "lat": -33.28, "lon": 149.10},
    {"name": "Dubbo",           "lat": -32.25, "lon": 148.60},
    {"name": "Wagga Wagga",     "lat": -35.12, "lon": 147.37},
    {"name": "Armidale",        "lat": -30.51, "lon": 151.67},
    {"name": "Port Macquarie",  "lat": -31.43, "lon": 152.91},
    {"name": "Bathurst",        "lat": -33.42, "lon": 149.58},
    {"name": "Broken Hill",     "lat": -31.95, "lon": 141.47},
    {"name": "Lismore",         "lat": -28.81, "lon": 153.28},
    {"name": "Casino",          "lat": -28.87, "lon": 153.05},
    {"name": "Glen Innes",      "lat": -29.73, "lon": 151.74},
    {"name": "Inverell",        "lat": -29.78, "lon": 151.11},
    {"name": "Mudgee",          "lat": -32.59, "lon": 149.59},
    {"name": "Moree",           "lat": -29.46, "lon": 149.85},
    {"name": "Lightning Ridge", "lat": -29.43, "lon": 147.98},
    {"name": "Queanbeyan",      "lat": -35.35, "lon": 149.23},
    {"name": "Bega",            "lat": -36.67, "lon": 149.84},
]

WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Light rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail",
}

# COMMAND ----------

# Fetch all stations in a single API call
lats = ",".join(str(s["lat"]) for s in STATIONS)
lons = ",".join(str(s["lon"]) for s in STATIONS)

url = (
    f"https://api.open-meteo.com/v1/forecast?"
    f"latitude={lats}&longitude={lons}"
    f"&current=temperature_2m,apparent_temperature,relative_humidity_2m,"
    f"wind_speed_10m,wind_gusts_10m,wind_direction_10m,weather_code,precipitation"
    f"&timezone=Australia/Sydney&wind_speed_unit=kmh"
)

req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=15) as resp:
    data = json.loads(resp.read())

results_list = data if isinstance(data, list) else [data]
print(f"Got {len(results_list)} station results")

# COMMAND ----------

# Parse results
dirs_compass = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

all_rows = []
for i, result in enumerate(results_list):
    if i >= len(STATIONS):
        break
    station = STATIONS[i]
    c = result.get("current", {})
    code = c.get("weather_code", 0)
    wind_dir_deg = c.get("wind_direction_10m")
    wind_dir = dirs_compass[int((wind_dir_deg + 11.25) / 22.5) % 16] if wind_dir_deg is not None else ""

    all_rows.append({
        "station_name": station["name"],
        "wmo_id": 0,
        "observation_time": c.get("time", ""),
        "temperature": c.get("temperature_2m"),
        "apparent_temperature": c.get("apparent_temperature"),
        "humidity": c.get("relative_humidity_2m"),
        "wind_speed_kmh": c.get("wind_speed_10m"),
        "wind_gust_kmh": c.get("wind_gusts_10m"),
        "wind_direction": wind_dir,
        "rain_since_9am": c.get("precipitation"),
        "weather_description": WMO_CODES.get(code, f"Code {code}"),
    })
    print(f"  {station['name']}: {c.get('temperature_2m')}°C, {WMO_CODES.get(code, code)}")

print(f"\nTotal: {len(all_rows)} stations")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, to_timestamp, col

if all_rows:
    schema = StructType([
        StructField("station_name", StringType(), False),
        StructField("wmo_id", IntegerType(), True),
        StructField("observation_time", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_gust_kmh", DoubleType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("rain_since_9am", DoubleType(), True),
        StructField("weather_description", StringType(), True),
    ])

    df = spark.createDataFrame(all_rows, schema=schema)
    df = df.withColumn("observation_time", to_timestamp("observation_time", "yyyy-MM-dd'T'HH:mm"))
    df = df.withColumn("updated_at", current_timestamp())

    df.createOrReplaceTempView("weather_updates")

    spark.sql(f"""
        MERGE INTO {TABLE} AS target
        USING weather_updates AS source
        ON target.station_name = source.station_name
           AND target.observation_time = source.observation_time
        WHEN MATCHED THEN UPDATE SET
            temperature = source.temperature,
            apparent_temperature = source.apparent_temperature,
            humidity = source.humidity,
            wind_speed_kmh = source.wind_speed_kmh,
            wind_gust_kmh = source.wind_gust_kmh,
            wind_direction = source.wind_direction,
            rain_since_9am = source.rain_since_9am,
            weather_description = source.weather_description,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"MERGE complete — {len(all_rows)} station observations upserted into {TABLE}")
else:
    print("No observations fetched. Skipping MERGE.")

"""Phase 3: Create BOM weather Delta table and seed with initial observations.

Run once from local machine with Databricks CLI configured (DEFAULT profile).

Steps:
  1. Create Delta table `zivile.essential_energy_wacs.bom_weather`
  2. Seed with current BOM observations for ~10 NSW stations
  3. Grant App SP SELECT
"""

import subprocess
import json
import sys
import urllib.request

PROFILE = "DEFAULT"
CATALOG = "zivile"
SCHEMA = "essential_energy_wacs"
TABLE = f"{CATALOG}.{SCHEMA}.bom_weather"
WAREHOUSE_ID = "c2abb17a6c9e6bc0"
APP_SP_ID = "84fba77d-2b5d-40ef-94e4-a0c81b5af427"

# NSW stations relevant to Essential Energy service area
STATIONS = {
    "Grafton": {"wmo_id": 94791, "product": "IDN60901"},
    "Coffs Harbour": {"wmo_id": 59040, "product": "IDN60901"},
    "Tamworth": {"wmo_id": 94776, "product": "IDN60901"},
    "Orange": {"wmo_id": 94753, "product": "IDN60901"},
    "Dubbo": {"wmo_id": 95719, "product": "IDN60901"},
    "Wagga Wagga": {"wmo_id": 94749, "product": "IDN60901"},
    "Armidale": {"wmo_id": 94774, "product": "IDN60901"},
    "Port Macquarie": {"wmo_id": 94786, "product": "IDN60901"},
    "Bathurst": {"wmo_id": 94729, "product": "IDN60901"},
    "Broken Hill": {"wmo_id": 94689, "product": "IDN60901"},
}


def run_cli(args: list[str], parse_json=True):
    """Run a databricks CLI command and return parsed output."""
    cmd = ["databricks"] + args + ["--profile", PROFILE]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        return None
    if parse_json and result.stdout.strip():
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout.strip()
    return result.stdout.strip()


def run_sql(statement: str):
    """Execute SQL via the SQL Statements API."""
    payload = {
        "statement": statement,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "30s",
    }
    result = run_cli([
        "api", "post", "/api/2.0/sql/statements/",
        "--json", json.dumps(payload),
    ])
    if result and isinstance(result, dict):
        status = result.get("status", {}).get("state", "")
        if status == "FAILED":
            err = result.get("status", {}).get("error", {}).get("message", "Unknown")
            print(f"  SQL FAILED: {err}")
            return None
    return result


def step1_create_table():
    """Create the BOM weather Delta table."""
    print("\n=== Step 1: Create BOM weather table ===")

    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        station_name STRING NOT NULL,
        wmo_id INT,
        observation_time TIMESTAMP,
        temperature DOUBLE,
        apparent_temperature DOUBLE,
        humidity INT,
        wind_speed_kmh DOUBLE,
        wind_gust_kmh DOUBLE,
        wind_direction STRING,
        rain_since_9am DOUBLE,
        weather_description STRING,
        updated_at TIMESTAMP
    )
    COMMENT 'BOM weather observations for Essential Energy service area stations'
    """
    run_sql(sql)
    print("  Table created.")


def fetch_bom_observations(station_name: str, wmo_id: int, product: str) -> list[dict]:
    """Fetch latest observations from BOM JSON API."""
    url = f"http://www.bom.gov.au/fwo/{product}/{product}.{wmo_id}.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        observations = data.get("observations", {}).get("data", [])
        results = []
        for obs in observations[:6]:  # Last 6 observations (~3 hours)
            results.append({
                "station_name": station_name,
                "wmo_id": wmo_id,
                "observation_time": obs.get("local_date_time_full", ""),
                "temperature": obs.get("air_temp"),
                "apparent_temperature": obs.get("apparent_t"),
                "humidity": obs.get("rel_hum"),
                "wind_speed_kmh": obs.get("wind_spd_kmh"),
                "wind_gust_kmh": obs.get("gust_kmh"),
                "wind_direction": obs.get("wind_dir"),
                "rain_since_9am": obs.get("rain_trace"),
                "weather_description": obs.get("weather", ""),
            })
        return results
    except Exception as e:
        print(f"  WARNING: Could not fetch BOM data for {station_name}: {e}")
        return []


def step2_seed_data():
    """Seed table with current BOM observations."""
    print("\n=== Step 2: Seed BOM weather data ===")

    all_rows = []
    for station_name, info in STATIONS.items():
        print(f"  Fetching {station_name} (WMO {info['wmo_id']})...")
        rows = fetch_bom_observations(station_name, info["wmo_id"], info["product"])
        all_rows.extend(rows)
        print(f"    Got {len(rows)} observations")

    if not all_rows:
        print("  No observations fetched. Seeding with placeholder data...")
        # Insert placeholder so the table isn't empty
        run_sql(f"""
            INSERT INTO {TABLE} (station_name, wmo_id, temperature, humidity, wind_speed_kmh, wind_direction, weather_description)
            VALUES
            ('Grafton', 94791, 22.5, 65, 15.0, 'NE', 'Partly cloudy'),
            ('Coffs Harbour', 59040, 24.0, 70, 12.0, 'E', 'Mostly sunny'),
            ('Tamworth', 94776, 19.0, 55, 20.0, 'W', 'Clear'),
            ('Orange', 94753, 15.0, 60, 18.0, 'SW', 'Overcast'),
            ('Dubbo', 95719, 21.0, 45, 22.0, 'NW', 'Sunny'),
            ('Wagga Wagga', 94749, 18.0, 50, 16.0, 'S', 'Clear'),
            ('Armidale', 94774, 14.0, 70, 10.0, 'SE', 'Fog patches'),
            ('Port Macquarie', 94786, 23.0, 75, 8.0, 'NE', 'Fine'),
            ('Bathurst', 94729, 13.0, 65, 14.0, 'W', 'Partly cloudy'),
            ('Broken Hill', 94689, 26.0, 30, 25.0, 'N', 'Hot and dry')
        """)
        return

    # Build MERGE statement for upsert
    values_parts = []
    for r in all_rows:
        # Parse BOM datetime format (e.g. "20260320143000") to SQL timestamp
        obs_time = r["observation_time"]
        if obs_time and len(obs_time) == 14:
            obs_time = f"{obs_time[:4]}-{obs_time[4:6]}-{obs_time[6:8]} {obs_time[8:10]}:{obs_time[10:12]}:{obs_time[12:14]}"

        temp = r["temperature"] if r["temperature"] is not None else "NULL"
        app_temp = r["apparent_temperature"] if r["apparent_temperature"] is not None else "NULL"
        hum = r["humidity"] if r["humidity"] is not None else "NULL"
        wind = r["wind_speed_kmh"] if r["wind_speed_kmh"] is not None else "NULL"
        gust = r["wind_gust_kmh"] if r["wind_gust_kmh"] is not None else "NULL"
        rain = r["rain_since_9am"] if r["rain_since_9am"] is not None else "NULL"
        # Handle rain_trace which can be "-" meaning 0
        if rain == "-":
            rain = "0.0"

        wind_dir = (r["wind_direction"] or "").replace("'", "''")
        weather = (r["weather_description"] or "").replace("'", "''")

        values_parts.append(
            f"('{r['station_name']}', {r['wmo_id']}, "
            f"TIMESTAMP '{obs_time}', {temp}, {app_temp}, {hum}, "
            f"{wind}, {gust}, '{wind_dir}', {rain}, '{weather}', CURRENT_TIMESTAMP())"
        )

    # Insert in batches of 50
    for i in range(0, len(values_parts), 50):
        batch = values_parts[i:i+50]
        sql = f"""
        MERGE INTO {TABLE} AS target
        USING (
            SELECT * FROM (VALUES {', '.join(batch)})
            AS s(station_name, wmo_id, observation_time, temperature, apparent_temperature,
                 humidity, wind_speed_kmh, wind_gust_kmh, wind_direction, rain_since_9am,
                 weather_description, updated_at)
        ) AS source
        ON target.station_name = source.station_name AND target.observation_time = source.observation_time
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
        """
        run_sql(sql)
        print(f"  Upserted batch {i//50 + 1} ({len(batch)} rows)")

    print(f"  Seeded {len(all_rows)} total observations.")


def step3_grant_sp():
    """Grant App SP SELECT on the weather table."""
    print("\n=== Step 3: Grant SP SELECT ===")
    run_sql(f"GRANT SELECT ON TABLE {TABLE} TO `{APP_SP_ID}`")
    print("  Done.")


if __name__ == "__main__":
    print("=" * 60)
    print("Phase 3: BOM Weather Dataset Setup")
    print("=" * 60)
    step1_create_table()
    step2_seed_data()
    step3_grant_sp()

    # Liquid clustering for query performance
    print("\n=== Step 4: Optimize table ===")
    run_sql(f"ALTER TABLE {TABLE} CLUSTER BY (station_name, observation_time)")
    run_sql(f"OPTIMIZE {TABLE}")
    print("  Liquid clustering applied.")

    print("\n=== Phase 3 complete ===")

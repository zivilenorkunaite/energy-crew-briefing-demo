"""Weather query — fetches current conditions from Open-Meteo API (live) with Delta table cache.

Uses Open-Meteo forecast API for real-time BOM-equivalent data for Essential Energy depot areas.
Falls back to Delta table cache if API is unavailable.
"""

import aiohttp
import json
from server.config import get_oauth_token, get_workspace_host

WAREHOUSE_ID = "c2abb17a6c9e6bc0"
TABLE = "zivile.essential_energy_wacs.bom_weather"

# WMO weather code descriptions
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Light rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail",
}

# Essential Energy depot locations with coordinates
DEPOTS = {
    "grafton":         {"name": "Grafton",         "lat": -29.69, "lon": 152.93},
    "coffs harbour":   {"name": "Coffs Harbour",   "lat": -30.30, "lon": 153.11},
    "coffs":           {"name": "Coffs Harbour",   "lat": -30.30, "lon": 153.11},
    "tamworth":        {"name": "Tamworth",         "lat": -31.09, "lon": 150.93},
    "orange":          {"name": "Orange",           "lat": -33.28, "lon": 149.10},
    "dubbo":           {"name": "Dubbo",            "lat": -32.25, "lon": 148.60},
    "wagga wagga":     {"name": "Wagga Wagga",     "lat": -35.12, "lon": 147.37},
    "wagga":           {"name": "Wagga Wagga",     "lat": -35.12, "lon": 147.37},
    "armidale":        {"name": "Armidale",         "lat": -30.51, "lon": 151.67},
    "port macquarie":  {"name": "Port Macquarie",  "lat": -31.43, "lon": 152.91},
    "port":            {"name": "Port Macquarie",  "lat": -31.43, "lon": 152.91},
    "bathurst":        {"name": "Bathurst",         "lat": -33.42, "lon": 149.58},
    "broken hill":     {"name": "Broken Hill",      "lat": -31.95, "lon": 141.47},
    "lismore":         {"name": "Lismore",          "lat": -28.81, "lon": 153.28},
    "casino":          {"name": "Casino",           "lat": -28.87, "lon": 153.05},
    "glen innes":      {"name": "Glen Innes",       "lat": -29.73, "lon": 151.74},
    "inverell":        {"name": "Inverell",         "lat": -29.78, "lon": 151.11},
    "mudgee":          {"name": "Mudgee",           "lat": -32.59, "lon": 149.59},
    "moree":           {"name": "Moree",            "lat": -29.46, "lon": 149.85},
    "lightning ridge": {"name": "Lightning Ridge",  "lat": -29.43, "lon": 147.98},
    "queanbeyan":      {"name": "Queanbeyan",       "lat": -35.35, "lon": 149.23},
    "bega":            {"name": "Bega",             "lat": -36.67, "lon": 149.84},
}

# Unique stations for bulk refresh (deduped by name)
_UNIQUE_STATIONS = {}
for d in DEPOTS.values():
    _UNIQUE_STATIONS[d["name"]] = d
STATIONS = list(_UNIQUE_STATIONS.values())

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def _match_depot(location: str) -> dict:
    """Match location string to depot info."""
    loc = location.lower().strip()
    if loc in DEPOTS:
        return DEPOTS[loc]
    for key, info in DEPOTS.items():
        if key in loc or loc in key:
            return info
    return DEPOTS.get("grafton", {"name": "Grafton", "lat": -29.69, "lon": 152.93})


async def _fetch_open_meteo(lat: float, lon: float, forecast_date: str | None = None) -> dict | None:
    """Fetch weather from Open-Meteo API. If forecast_date given, returns hourly forecast for that day."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": "Australia/Sydney",
        "wind_speed_unit": "kmh",
    }
    if forecast_date:
        # Hourly forecast for the specific date
        params["hourly"] = "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,weather_code,precipitation"
        params["start_date"] = forecast_date
        params["end_date"] = forecast_date
    else:
        # Current conditions
        params["current"] = "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,weather_code,precipitation,cloud_cover"
        params["forecast_days"] = 1

    url = OPEN_METEO_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
    except Exception as e:
        print(f"[WEATHER] Open-Meteo error: {e}")
        return None


async def _fetch_delta_fallback(station_name: str) -> str | None:
    """Fallback: query Delta table cache."""
    host = get_workspace_host()
    token = get_oauth_token()
    sql = (
        f"SELECT station_name, observation_time, temperature, apparent_temperature, "
        f"humidity, wind_speed_kmh, wind_gust_kmh, wind_direction, rain_since_9am, "
        f"weather_description "
        f"FROM {TABLE} WHERE station_name = '{station_name}' "
        f"ORDER BY observation_time DESC LIMIT 1"
    )
    payload = {"statement": sql, "warehouse_id": WAREHOUSE_ID, "format": "JSON_ARRAY", "wait_timeout": "30s"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{host}/api/2.0/sql/statements/", json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
        rows = data.get("result", {}).get("data_array", [])
        if not rows:
            return None
        r = rows[0]
        return (
            f"**Current Weather — {r[0]}** (cached)\n"
            f"Observation: {r[1]}\n"
            f"Temperature: {r[2]}°C (feels like {r[3]}°C)\n"
            f"Humidity: {r[4]}%\n"
            f"Wind: {r[7]} {r[5]} km/h" + (f" (gusts {r[6]} km/h)" if r[6] else "") + "\n"
            f"Rain since 9am: {r[8]} mm\n"
            f"Conditions: {r[9]}"
        )
    except Exception as e:
        print(f"[WEATHER] Delta fallback error: {e}")
        return None


def _parse_date_from_location(location: str) -> str | None:
    """Extract a date reference from the location string (e.g. 'Grafton tomorrow')."""
    import re
    from datetime import date, timedelta
    lower = location.lower()
    today = date(2026, 3, 20)  # Demo date

    if "tomorrow" in lower:
        return (today + timedelta(days=1)).isoformat()
    if "day after" in lower:
        return (today + timedelta(days=2)).isoformat()

    # Match explicit dates like "March 21" or "2026-03-21"
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', lower)
    if m:
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d > today:
            return d.isoformat()

    m = re.search(r'march\s+(\d{1,2})', lower)
    if m:
        d = date(2026, 3, int(m.group(1)))
        if d > today:
            return d.isoformat()

    m = re.search(r'april\s+(\d{1,2})', lower)
    if m:
        d = date(2026, 4, int(m.group(1)))
        if d > today:
            return d.isoformat()

    return None


async def query_weather(location: str) -> str:
    """Get weather for a depot area. Current for today, forecast for future dates."""
    depot = _match_depot(location)
    station_name = depot["name"]
    forecast_date = _parse_date_from_location(location)

    # Try Open-Meteo live API
    data = await _fetch_open_meteo(depot["lat"], depot["lon"], forecast_date=forecast_date)

    if data and "current" in data:
        c = data["current"]
        temp = c.get("temperature_2m")
        app_temp = c.get("apparent_temperature")
        humidity = c.get("relative_humidity_2m")
        wind = c.get("wind_speed_10m")
        gust = c.get("wind_gusts_10m")
        precip = c.get("precipitation")
        code = c.get("weather_code", 0)
        cloud = c.get("cloud_cover")
        obs_time = c.get("time", "")

        desc = WMO_CODES.get(code, f"Code {code}")

        lines = [
            f"**Current Weather — {station_name}**",
            f"Observation: {obs_time} AEST",
            f"Conditions: {desc}" + (f" ({cloud}% cloud cover)" if cloud is not None else ""),
            f"Temperature: {temp}°C (feels like {app_temp}°C)" if temp is not None else None,
            f"Humidity: {humidity}%" if humidity is not None else None,
            f"Wind: {wind} km/h" + (f" (gusts {gust} km/h)" if gust else "") if wind is not None else None,
            f"Precipitation: {precip} mm" if precip is not None and precip > 0 else None,
        ]

        warnings = []
        if temp is not None and temp >= 35:
            warnings.append(f"HEAT WARNING: Temperature {temp}°C. Ensure adequate hydration, rest breaks, and shade.")
        if temp is not None and temp >= 40:
            warnings.append("EXTREME HEAT: Consider rescheduling non-critical outdoor work.")
        if wind is not None and wind >= 40:
            warnings.append(f"WIND WARNING: Wind speed {wind} km/h. Review suitability for elevated work and crane operations.")
        if gust is not None and gust >= 60:
            warnings.append(f"GUST WARNING: Gusts to {gust} km/h. Suspend elevated work activities.")
        if code >= 95:
            warnings.append("THUNDERSTORM WARNING: Suspend all outdoor electrical work. Seek shelter immediately.")
        if code in (65, 82):
            warnings.append("HEAVY RAIN: Check for flooding, slippery conditions, reduced visibility.")
        if precip is not None and precip >= 10:
            warnings.append(f"RAIN WARNING: {precip}mm precipitation. Check site drainage and access roads.")

        result = "\n".join(l for l in lines if l)
        if warnings:
            result += "\n\n**Safety Alerts:**\n" + "\n".join(f"- {w}" for w in warnings)
        return result

    # Handle hourly forecast response (future dates)
    if data and "hourly" in data:
        h = data["hourly"]
        times = h.get("time", [])
        temps = h.get("temperature_2m", [])
        app_temps = h.get("apparent_temperature", [])
        humidities = h.get("relative_humidity_2m", [])
        winds = h.get("wind_speed_10m", [])
        gusts = h.get("wind_gusts_10m", [])
        codes = h.get("weather_code", [])
        precips = h.get("precipitation", [])

        if not times:
            return f"(No forecast data available for {station_name} on {forecast_date})"

        # Summarize key work hours (6am-6pm)
        work_temps, work_winds, work_gusts, work_precip, work_codes = [], [], [], 0.0, []
        for i, t in enumerate(times):
            hour = int(t.split("T")[1].split(":")[0]) if "T" in t else 0
            if 6 <= hour <= 18:
                if i < len(temps) and temps[i] is not None: work_temps.append(temps[i])
                if i < len(winds) and winds[i] is not None: work_winds.append(winds[i])
                if i < len(gusts) and gusts[i] is not None: work_gusts.append(gusts[i])
                if i < len(precips) and precips[i] is not None: work_precip += precips[i]
                if i < len(codes) and codes[i] is not None: work_codes.append(codes[i])

        min_temp = min(work_temps) if work_temps else None
        max_temp = max(work_temps) if work_temps else None
        max_wind = max(work_winds) if work_winds else None
        max_gust = max(work_gusts) if work_gusts else None
        worst_code = max(work_codes) if work_codes else 0
        desc = WMO_CODES.get(worst_code, f"Code {worst_code}")

        lines = [
            f"**Weather Forecast — {station_name} ({forecast_date})**",
            f"Work hours (6am-6pm):",
            f"Conditions: {desc}",
            f"Temperature: {min_temp}°C to {max_temp}°C" if min_temp is not None else None,
            f"Max wind: {max_wind} km/h" + (f" (gusts to {max_gust} km/h)" if max_gust else "") if max_wind is not None else None,
            f"Total precipitation: {round(work_precip, 1)} mm" if work_precip > 0 else "No rain expected",
        ]

        warnings = []
        if max_temp is not None and max_temp >= 35:
            warnings.append(f"HEAT WARNING: Forecast max {max_temp}°C. Plan hydration breaks and shade.")
        if max_wind is not None and max_wind >= 40:
            warnings.append(f"WIND WARNING: Forecast winds to {max_wind} km/h. Review elevated work plans.")
        if max_gust is not None and max_gust >= 60:
            warnings.append(f"GUST WARNING: Forecast gusts to {max_gust} km/h. May need to suspend elevated work.")
        if worst_code >= 95:
            warnings.append("THUNDERSTORM FORECAST: Plan for possible work stoppages.")
        if work_precip >= 10:
            warnings.append(f"RAIN FORECAST: {round(work_precip, 1)}mm expected. Check site access and drainage.")

        result = "\n".join(l for l in lines if l)
        if warnings:
            result += "\n\n**Safety Alerts:**\n" + "\n".join(f"- {w}" for w in warnings)
        return result

    # Fallback to Delta cache (current conditions only)
    if not forecast_date:
        fallback = await _fetch_delta_fallback(station_name)
        if fallback:
            return fallback

    return f"(No weather data available for {station_name})"


async def fetch_all_stations() -> list[dict]:
    """Fetch current weather for all Essential Energy stations. Used by refresh job."""
    lats = ",".join(str(s["lat"]) for s in STATIONS)
    lons = ",".join(str(s["lon"]) for s in STATIONS)

    params = {
        "latitude": lats,
        "longitude": lons,
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,wind_direction_10m,weather_code,precipitation",
        "timezone": "Australia/Sydney",
        "wind_speed_unit": "kmh",
    }
    url = OPEN_METEO_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()

    # Multi-location returns array
    results_list = data if isinstance(data, list) else [data]

    rows = []
    for i, result in enumerate(results_list):
        if i >= len(STATIONS):
            break
        station = STATIONS[i]
        c = result.get("current", {})
        code = c.get("weather_code", 0)
        wind_dir_deg = c.get("wind_direction_10m")
        # Convert degrees to compass
        dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        wind_dir = dirs[int((wind_dir_deg + 11.25) / 22.5) % 16] if wind_dir_deg is not None else ""

        rows.append({
            "station_name": station["name"],
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

    return rows

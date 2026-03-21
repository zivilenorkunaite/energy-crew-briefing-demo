"""Weather service — queries UC function `get_weather`, falls back to Open-Meteo API.

The UC function `zivile.essential_energy_wacs.get_weather(location, forecast_date)` reads from
the `bom_weather` Delta table (refreshed hourly). API fallback if data is missing or stale.
"""

import os
import aiohttp
from datetime import datetime, timedelta

from server.config import get_oauth_token, get_workspace_host

WAREHOUSE_ID = os.environ.get("MLFLOW_TRACING_SQL_WAREHOUSE_ID", "c2abb17a6c9e6bc0")
UC_FUNCTION = "zivile.essential_energy_wacs.get_weather"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Light rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail",
}

DEPOTS = {
    "grafton": {"name": "Grafton", "lat": -29.69, "lon": 152.93},
    "coffs harbour": {"name": "Coffs Harbour", "lat": -30.30, "lon": 153.11},
    "coffs": {"name": "Coffs Harbour", "lat": -30.30, "lon": 153.11},
    "tamworth": {"name": "Tamworth", "lat": -31.09, "lon": 150.93},
    "orange": {"name": "Orange", "lat": -33.28, "lon": 149.10},
    "dubbo": {"name": "Dubbo", "lat": -32.25, "lon": 148.60},
    "wagga wagga": {"name": "Wagga Wagga", "lat": -35.12, "lon": 147.37},
    "wagga": {"name": "Wagga Wagga", "lat": -35.12, "lon": 147.37},
    "armidale": {"name": "Armidale", "lat": -30.51, "lon": 151.67},
    "port macquarie": {"name": "Port Macquarie", "lat": -31.43, "lon": 152.91},
    "port": {"name": "Port Macquarie", "lat": -31.43, "lon": 152.91},
    "bathurst": {"name": "Bathurst", "lat": -33.42, "lon": 149.58},
    "broken hill": {"name": "Broken Hill", "lat": -31.95, "lon": 141.47},
    "lismore": {"name": "Lismore", "lat": -28.81, "lon": 153.28},
    "casino": {"name": "Casino", "lat": -28.87, "lon": 153.05},
    "glen innes": {"name": "Glen Innes", "lat": -29.73, "lon": 151.74},
    "inverell": {"name": "Inverell", "lat": -29.78, "lon": 151.11},
    "mudgee": {"name": "Mudgee", "lat": -32.59, "lon": 149.59},
    "moree": {"name": "Moree", "lat": -29.46, "lon": 149.85},
    "lightning ridge": {"name": "Lightning Ridge", "lat": -29.43, "lon": 147.98},
    "queanbeyan": {"name": "Queanbeyan", "lat": -35.35, "lon": 149.23},
    "bega": {"name": "Bega", "lat": -36.67, "lon": 149.84},
}


def _match_depot(location: str) -> dict:
    loc = location.lower().strip()
    if loc in DEPOTS:
        return DEPOTS[loc]
    for key, info in DEPOTS.items():
        if key in loc or loc in key:
            return info
    return DEPOTS.get("grafton")


def _safety_warnings(temp, wind, gust, code, precip) -> list[str]:
    warnings = []
    if temp is not None and temp >= 35:
        warnings.append(f"HEAT WARNING: Temperature {temp}°C. Ensure adequate hydration, rest breaks, and shade.")
    if temp is not None and temp >= 40:
        warnings.append("EXTREME HEAT: Consider rescheduling non-critical outdoor work.")
    if wind is not None and wind >= 40:
        warnings.append(f"WIND WARNING: Wind speed {wind} km/h. Review suitability for elevated work and crane operations.")
    if gust is not None and gust >= 60:
        warnings.append(f"GUST WARNING: Gusts to {gust} km/h. Suspend elevated work activities.")
    if code is not None and code >= 95:
        warnings.append("THUNDERSTORM WARNING: Suspend all outdoor electrical work. Seek shelter immediately.")
    if code is not None and code in (65, 82):
        warnings.append("HEAVY RAIN: Check for flooding, slippery conditions, reduced visibility.")
    if precip is not None and precip >= 10:
        warnings.append(f"RAIN WARNING: {precip}mm precipitation. Check site drainage and access roads.")
    return warnings


def _parse_date_from_location(location: str) -> str | None:
    import re
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("Australia/Sydney")).date()
    except Exception:
        today = datetime.utcnow().date()

    lower = location.lower()
    if "tomorrow" in lower:
        return (today + timedelta(days=1)).isoformat()
    if "day after" in lower:
        return (today + timedelta(days=2)).isoformat()

    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', lower)
    if m:
        from datetime import date
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d >= today:
            return d.isoformat()

    for month_name, month_num in [("march", 3), ("april", 4), ("may", 5)]:
        m = re.search(rf'{month_name}\s+(\d{{1,2}})', lower)
        if m:
            from datetime import date
            d = date(2026, month_num, int(m.group(1)))
            if d >= today:
                return d.isoformat()
    return None


async def _run_sql(sql: str) -> list[list] | None:
    """Execute SQL via warehouse REST API."""
    host = get_workspace_host()
    token = get_oauth_token()
    payload = {"statement": sql, "warehouse_id": WAREHOUSE_ID, "format": "JSON_ARRAY", "wait_timeout": "30s"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{host}/api/2.0/sql/statements/", json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                data = await resp.json()
        if data.get("status", {}).get("state") == "SUCCEEDED":
            return data.get("result", {}).get("data_array", [])
    except Exception as e:
        print(f"[WEATHER] SQL error: {e}")
    return None


async def _query_uc_function(station_name: str, forecast_date: str | None) -> str | None:
    """Call the UC function to get weather data."""
    if forecast_date:
        sql = f"SELECT * FROM {UC_FUNCTION}('{station_name}', '{forecast_date}') WHERE HOUR(observation_time) BETWEEN 6 AND 18"
    else:
        sql = f"SELECT * FROM {UC_FUNCTION}('{station_name}')"

    rows = await _run_sql(sql)
    if not rows:
        return None

    if forecast_date:
        # Forecast: summarise work-hours data
        # Columns: station_name, observation_time, forecast_type, temperature, apparent_temperature,
        #          humidity, wind_speed_kmh, wind_gust_kmh, wind_direction, precipitation, weather_description, cloud_cover
        try:
            temps = [float(r[3]) for r in rows if r[3] is not None]
            winds = [float(r[6]) for r in rows if r[6] is not None]
            gusts = [float(r[7]) for r in rows if r[7] is not None]
            precip_total = sum(float(r[9]) for r in rows if r[9] is not None)
            descs = [r[10] for r in rows if r[10]]
        except (ValueError, IndexError, TypeError) as e:
            print(f"[WEATHER] Error parsing forecast data: {e}")
            return None
        worst_desc = max(set(descs), key=descs.count) if descs else "Unknown"

        lines = [
            f"**Weather Forecast — {station_name} ({forecast_date})**",
            f"Work hours (6am–6pm):",
            f"Conditions: {worst_desc}",
            f"Temperature: {min(temps):.0f}°C – {max(temps):.0f}°C" if temps else None,
            f"Max Wind: {max(winds):.0f} km/h" + (f" (gusts to {max(gusts):.0f} km/h)" if gusts else "") if winds else None,
            f"Total Precipitation: {precip_total:.1f} mm" if precip_total > 0 else "No rain expected",
        ]
        warnings = _safety_warnings(
            max(temps) if temps else None, max(winds) if winds else None,
            max(gusts) if gusts else None, None, precip_total,
        )
    else:
        # Current: single row
        try:
            r = rows[0]
            temp, app_temp = r[3], r[4]
            humidity, wind, gust = r[5], r[6], r[7]
        except (IndexError, TypeError) as e:
            print(f"[WEATHER] Error parsing current data: {e}")
            return None
        wind_dir, precip, desc = r[8], r[9], r[10] or ""
        cloud = r[11]

        lines = [
            f"**Current Weather — {station_name}**",
            f"Observation: {r[1]} AEST",
            f"Conditions: {desc}" + (f" ({cloud}% cloud cover)" if cloud is not None else ""),
            f"Temperature: {temp}°C (feels like {app_temp}°C)" if temp is not None else None,
            f"Humidity: {humidity}%" if humidity is not None else None,
            f"Wind: {wind_dir} {wind} km/h" + (f" (gusts {gust} km/h)" if gust else "") if wind is not None else None,
            f"Precipitation: {precip} mm" if precip is not None and float(precip) > 0 else None,
        ]
        warnings = _safety_warnings(
            float(temp) if temp else None, float(wind) if wind else None,
            float(gust) if gust else None, None,
            float(precip) if precip else None,
        )

    result = "\n".join(l for l in lines if l)
    if warnings:
        result += "\n\n**Safety Alerts:**\n" + "\n".join(f"- {w}" for w in warnings)
    return result


async def _fetch_api_fallback(depot: dict, target_date: str | None = None) -> str | None:
    """Fallback: fetch from Open-Meteo API."""
    params = {"latitude": depot["lat"], "longitude": depot["lon"], "timezone": "Australia/Sydney", "wind_speed_unit": "kmh"}
    if target_date:
        params["hourly"] = "temperature_2m,wind_speed_10m,wind_gusts_10m,weather_code,precipitation"
        params["start_date"] = target_date
        params["end_date"] = target_date
    else:
        params["current"] = "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,weather_code,precipitation"
        params["forecast_days"] = 1

    url = OPEN_METEO_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
    except Exception as e:
        print(f"[WEATHER] API fallback error: {e}")
        return None

    name = depot["name"]
    if target_date and "hourly" in data:
        h = data["hourly"]
        times = h.get("time", [])
        work_idx = [i for i, t in enumerate(times) if 6 <= int(t.split("T")[1].split(":")[0]) <= 18]
        temps = [h["temperature_2m"][i] for i in work_idx if h.get("temperature_2m", [None]*999)[i] is not None]
        winds = [h["wind_speed_10m"][i] for i in work_idx if h.get("wind_speed_10m", [None]*999)[i] is not None]
        gusts = [h["wind_gusts_10m"][i] for i in work_idx if h.get("wind_gusts_10m", [None]*999)[i] is not None]
        precip = sum(h.get("precipitation", [0]*999)[i] or 0 for i in work_idx)
        if temps:
            return f"**Weather Forecast — {name} ({target_date})** (live)\nTemperature: {min(temps):.0f}–{max(temps):.0f}°C\nMax Wind: {max(winds):.0f} km/h (gusts {max(gusts):.0f} km/h)\nPrecipitation: {precip:.1f} mm"
    elif "current" in data:
        c = data["current"]
        code = c.get("weather_code", 0) or 0
        return f"**Current Weather — {name}** (live)\n{WMO_CODES.get(code, '')}\nTemperature: {c.get('temperature_2m')}°C\nWind: {c.get('wind_speed_10m')} km/h (gusts {c.get('wind_gusts_10m')} km/h)"
    return None


async def query_weather(location: str) -> str:
    """Get weather for a depot area. UC function first, API fallback if missing."""
    depot = _match_depot(location)
    station_name = depot["name"]
    forecast_date = _parse_date_from_location(location)

    # Try UC function first
    result = await _query_uc_function(station_name, forecast_date)
    if result:
        return result

    # API fallback
    print(f"[WEATHER] UC function miss for {station_name} date={forecast_date}, trying API")
    result = await _fetch_api_fallback(depot, forecast_date)
    if result:
        return result

    return f"(No weather data available for {station_name})"

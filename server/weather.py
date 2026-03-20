"""Weather service — queries Delta table (7-day hourly forecasts), falls back to Open-Meteo API.

Delta table `bom_weather` contains hourly forecasts for 19 Essential Energy depot areas,
refreshed hourly via scheduled job. API fallback if data is missing or stale (>2 days).
"""

import aiohttp
from datetime import datetime, timedelta

from server.config import get_oauth_token, get_workspace_host

WAREHOUSE_ID = "c2abb17a6c9e6bc0"
TABLE = "zivile.essential_energy_wacs.bom_weather"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
STALE_HOURS = 48  # fallback to API if data older than this

WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Light rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail",
}

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


def _match_depot(location: str) -> dict:
    loc = location.lower().strip()
    if loc in DEPOTS:
        return DEPOTS[loc]
    for key, info in DEPOTS.items():
        if key in loc or loc in key:
            return info
    return DEPOTS.get("grafton")


def _safety_warnings(temp, wind, gust, code, precip) -> list[str]:
    """Generate safety warnings from weather values."""
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


async def _run_sql(sql: str) -> list[list] | None:
    """Execute SQL via warehouse REST API. Returns data_array or None."""
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
        state = data.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            return data.get("result", {}).get("data_array", [])
    except Exception as e:
        print(f"[WEATHER] SQL error: {e}")
    return None


async def _query_delta_current(station_name: str) -> str | None:
    """Get current conditions from Delta table."""
    sql = (
        f"SELECT station_name, observation_time, temperature, apparent_temperature, "
        f"humidity, wind_speed_kmh, wind_gust_kmh, wind_direction, precipitation, "
        f"weather_code, weather_description, cloud_cover, refreshed_at "
        f"FROM {TABLE} WHERE station_name = '{station_name}' "
        f"AND forecast_type = 'current' "
        f"ORDER BY observation_time DESC LIMIT 1"
    )
    rows = await _run_sql(sql)
    if not rows:
        return None

    r = rows[0]
    # Check staleness
    refreshed = r[12]
    if refreshed:
        try:
            ref_dt = datetime.fromisoformat(str(refreshed).replace('Z', '+00:00'))
            if (datetime.now(ref_dt.tzinfo) - ref_dt).total_seconds() > STALE_HOURS * 3600:
                print(f"[WEATHER] Delta data stale for {station_name} (refreshed {refreshed})")
                return None
        except Exception:
            pass

    temp, app_temp = r[2], r[3]
    humidity, wind, gust = r[4], r[5], r[6]
    wind_dir, precip, code = r[7], r[8], r[9]
    desc, cloud = r[10] or "", r[11]

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
        float(gust) if gust else None, int(code) if code else None,
        float(precip) if precip else None,
    )
    result = "\n".join(l for l in lines if l)
    if warnings:
        result += "\n\n**Safety Alerts:**\n" + "\n".join(f"- {w}" for w in warnings)
    return result


async def _query_delta_forecast(station_name: str, target_date: str) -> str | None:
    """Get hourly forecast for a specific date from Delta table."""
    sql = (
        f"SELECT observation_time, temperature, apparent_temperature, humidity, "
        f"wind_speed_kmh, wind_gust_kmh, weather_code, precipitation, refreshed_at "
        f"FROM {TABLE} WHERE station_name = '{station_name}' "
        f"AND forecast_type = 'hourly_forecast' "
        f"AND CAST(observation_time AS DATE) = '{target_date}' "
        f"AND HOUR(observation_time) BETWEEN 6 AND 18 "
        f"ORDER BY observation_time"
    )
    rows = await _run_sql(sql)
    if not rows or len(rows) < 3:
        return None

    # Check staleness
    refreshed = rows[0][8]
    if refreshed:
        try:
            ref_dt = datetime.fromisoformat(str(refreshed).replace('Z', '+00:00'))
            if (datetime.now(ref_dt.tzinfo) - ref_dt).total_seconds() > STALE_HOURS * 3600:
                return None
        except Exception:
            pass

    temps = [float(r[1]) for r in rows if r[1] is not None]
    winds = [float(r[4]) for r in rows if r[4] is not None]
    gusts = [float(r[5]) for r in rows if r[5] is not None]
    codes = [int(r[6]) for r in rows if r[6] is not None]
    precip_total = sum(float(r[7]) for r in rows if r[7] is not None)

    worst_code = max(codes) if codes else 0
    desc = WMO_CODES.get(worst_code, f"Code {worst_code}")

    lines = [
        f"**Weather Forecast — {station_name} ({target_date})**",
        f"Work hours (6am-6pm):",
        f"Conditions: {desc}",
        f"Temperature: {min(temps):.0f}°C to {max(temps):.0f}°C" if temps else None,
        f"Max wind: {max(winds):.0f} km/h" + (f" (gusts to {max(gusts):.0f} km/h)" if gusts else "") if winds else None,
        f"Total precipitation: {precip_total:.1f} mm" if precip_total > 0 else "No rain expected",
    ]

    warnings = _safety_warnings(
        max(temps) if temps else None, max(winds) if winds else None,
        max(gusts) if gusts else None, worst_code, precip_total,
    )
    result = "\n".join(l for l in lines if l)
    if warnings:
        result += "\n\n**Safety Alerts:**\n" + "\n".join(f"- {w}" for w in warnings)
    return result


async def _fetch_api_fallback(lat: float, lon: float, target_date: str | None = None) -> str | None:
    """Fallback: fetch directly from Open-Meteo API."""
    params = {"latitude": lat, "longitude": lon, "timezone": "Australia/Sydney", "wind_speed_unit": "kmh"}
    if target_date:
        params["hourly"] = "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,weather_code,precipitation"
        params["start_date"] = target_date
        params["end_date"] = target_date
    else:
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
        print(f"[WEATHER] API fallback error: {e}")
        return None


def _parse_date_from_location(location: str) -> str | None:
    """Extract a date reference from the location string."""
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
    if "monday" in lower:
        days_ahead = (0 - today.weekday()) % 7 or 7
        return (today + timedelta(days=days_ahead)).isoformat()

    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', lower)
    if m:
        from datetime import date
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d > today:
            return d.isoformat()

    for month_name, month_num in [("march", 3), ("april", 4), ("may", 5)]:
        m = re.search(rf'{month_name}\s+(\d{{1,2}})', lower)
        if m:
            from datetime import date
            d = date(2026, month_num, int(m.group(1)))
            if d > today:
                return d.isoformat()

    return None


async def query_weather(location: str) -> str:
    """Get weather for a depot area. Delta table first, API fallback if stale or missing."""
    depot = _match_depot(location)
    station_name = depot["name"]
    forecast_date = _parse_date_from_location(location)

    # Try Delta table first
    if forecast_date:
        result = await _query_delta_forecast(station_name, forecast_date)
        if result:
            return result
        # API fallback for forecast
        print(f"[WEATHER] Delta forecast miss for {station_name} {forecast_date}, trying API")
        api_data = await _fetch_api_fallback(depot["lat"], depot["lon"], target_date=forecast_date)
        if api_data and "hourly" in api_data:
            h = api_data["hourly"]
            times = h.get("time", [])
            temps = [h.get("temperature_2m", [None]*999)[i] for i in range(len(times))
                     if 6 <= int(times[i].split("T")[1].split(":")[0]) <= 18 and h.get("temperature_2m", [None]*999)[i] is not None]
            winds = [h.get("wind_speed_10m", [None]*999)[i] for i in range(len(times))
                     if 6 <= int(times[i].split("T")[1].split(":")[0]) <= 18 and h.get("wind_speed_10m", [None]*999)[i] is not None]
            gusts = [h.get("wind_gusts_10m", [None]*999)[i] for i in range(len(times))
                     if 6 <= int(times[i].split("T")[1].split(":")[0]) <= 18 and h.get("wind_gusts_10m", [None]*999)[i] is not None]
            codes = [h.get("weather_code", [0]*999)[i] or 0 for i in range(len(times))
                     if 6 <= int(times[i].split("T")[1].split(":")[0]) <= 18]
            precip = sum(h.get("precipitation", [0]*999)[i] or 0 for i in range(len(times))
                        if 6 <= int(times[i].split("T")[1].split(":")[0]) <= 18)
            if temps:
                worst_code = max(codes) if codes else 0
                lines = [
                    f"**Weather Forecast — {station_name} ({forecast_date})**",
                    f"Work hours (6am-6pm):",
                    f"Conditions: {WMO_CODES.get(worst_code, '')}",
                    f"Temperature: {min(temps):.0f}°C to {max(temps):.0f}°C",
                    f"Max wind: {max(winds):.0f} km/h" + (f" (gusts to {max(gusts):.0f} km/h)" if gusts else ""),
                    f"Total precipitation: {precip:.1f} mm" if precip > 0 else "No rain expected",
                ]
                warnings = _safety_warnings(max(temps), max(winds) if winds else None, max(gusts) if gusts else None, worst_code, precip)
                result = "\n".join(l for l in lines if l)
                if warnings:
                    result += "\n\n**Safety Alerts:**\n" + "\n".join(f"- {w}" for w in warnings)
                return result
    else:
        result = await _query_delta_current(station_name)
        if result:
            return result
        # API fallback for current
        print(f"[WEATHER] Delta current miss for {station_name}, trying API")
        api_data = await _fetch_api_fallback(depot["lat"], depot["lon"])
        if api_data and "current" in api_data:
            c = api_data["current"]
            code = c.get("weather_code", 0) or 0
            lines = [
                f"**Current Weather — {station_name}** (live)",
                f"Observation: {c.get('time', '')} AEST",
                f"Conditions: {WMO_CODES.get(code, '')}",
                f"Temperature: {c.get('temperature_2m')}°C (feels like {c.get('apparent_temperature')}°C)",
                f"Humidity: {c.get('relative_humidity_2m')}%",
                f"Wind: {c.get('wind_speed_10m')} km/h (gusts {c.get('wind_gusts_10m')} km/h)",
            ]
            return "\n".join(l for l in lines if l)

    return f"(No weather data available for {station_name})"

"""App settings backed by Lakebase."""

from server.db import db

_cache: dict[str, str] = {}


async def get_setting(key: str, default: str = "false") -> str:
    """Get a setting value. Caches in memory after first read."""
    if key in _cache:
        return _cache[key]
    try:
        row = await db.fetchrow("SELECT value FROM app_settings WHERE key = $1", key)
        val = row["value"] if row else default
        _cache[key] = val
        return val
    except Exception:
        return default


async def set_setting(key: str, value: str):
    """Set a setting value. Updates both DB and cache."""
    try:
        await db.execute(
            """INSERT INTO app_settings (key, value, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP""",
            key, value,
        )
        _cache[key] = value
    except Exception as e:
        print(f"[SETTINGS] Error writing {key}: {e}")


async def get_bool(key: str, default: bool = False) -> bool:
    val = await get_setting(key, "true" if default else "false")
    return val.lower() == "true"


async def set_bool(key: str, value: bool):
    await set_setting(key, "true" if value else "false")


async def get_all() -> dict:
    """Get all settings."""
    try:
        rows = await db.fetch("SELECT key, value FROM app_settings ORDER BY key")
        return {r["key"]: r["value"] for r in rows}
    except Exception:
        return {}

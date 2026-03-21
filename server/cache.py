"""Tool result cache backed by Lakebase.

Caches tool results with per-tool TTLs to avoid redundant API calls.
"""

import hashlib
import json
from datetime import datetime, timezone
from server.db import db

# TTL per tool (seconds)
TOOL_TTLS = {
    "get_swms": 86400,           # 24h — SWMS docs don't change
    "query_genie": 600,          # 10min — work orders change daily
    "search_local_notices": 7200, # 2h — notices change slowly
    "query_weather": 1800,       # 30min — weather refreshes hourly
}

DEFAULT_TTL = 600  # 10 min


def _cache_key(tool_name: str, args: dict) -> str:
    """Generate a deterministic cache key from tool name + args."""
    raw = json.dumps({"tool": tool_name, "args": args}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


async def get_cached(tool_name: str, args: dict) -> str | None:
    """Check cache for a tool result. Returns result text or None."""
    key = _cache_key(tool_name, args)
    try:
        row = await db.fetchrow(
            "SELECT result, created_at, ttl_seconds FROM tool_cache WHERE cache_key = $1",
            key,
        )
        if not row:
            return None

        created = row["created_at"]
        ttl = row["ttl_seconds"]
        now = datetime.now(timezone.utc)
        if created.tzinfo is None:
            from datetime import timezone as tz
            created = created.replace(tzinfo=tz.utc)
        age = (now - created).total_seconds()

        if age > ttl:
            # Expired — delete and return miss
            await db.execute("DELETE FROM tool_cache WHERE cache_key = $1", key)
            return None

        print(f"[CACHE] HIT {tool_name} ({age:.0f}s old, TTL {ttl}s)")
        return row["result"]
    except Exception as e:
        print(f"[CACHE] Error reading: {e}")
        return None


async def set_cached(tool_name: str, args: dict, result: str):
    """Store a tool result in cache."""
    key = _cache_key(tool_name, args)
    ttl = TOOL_TTLS.get(tool_name, DEFAULT_TTL)
    try:
        await db.execute(
            """INSERT INTO tool_cache (cache_key, tool_name, args_json, result, ttl_seconds)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (cache_key) DO UPDATE SET
                 result = EXCLUDED.result,
                 created_at = CURRENT_TIMESTAMP,
                 ttl_seconds = EXCLUDED.ttl_seconds""",
            key, tool_name, json.dumps(args, ensure_ascii=False), result, ttl,
        )
        print(f"[CACHE] SET {tool_name} (TTL {ttl}s)")
    except Exception as e:
        print(f"[CACHE] Error writing: {e}")


async def clear_tool_cache(tool_name: str) -> int:
    """Clear all cached results for a specific tool. Returns count deleted."""
    try:
        result = await db.execute(
            "DELETE FROM tool_cache WHERE tool_name = $1", tool_name
        )
        count = int(result.split()[-1]) if result else 0
        print(f"[CACHE] Cleared {count} entries for {tool_name}")
        return count
    except Exception as e:
        print(f"[CACHE] Error clearing {tool_name}: {e}")
        return 0


async def clear_all_cache() -> int:
    """Clear entire cache. Returns count deleted."""
    try:
        result = await db.execute("DELETE FROM tool_cache")
        count = int(result.split()[-1]) if result else 0
        print(f"[CACHE] Cleared all {count} entries")
        return count
    except Exception as e:
        print(f"[CACHE] Error clearing all: {e}")
        return 0


async def get_cache_stats() -> list[dict]:
    """Return cache stats per tool."""
    try:
        rows = await db.fetch(
            """SELECT tool_name, COUNT(*) as count,
                      MIN(created_at) as oldest, MAX(created_at) as newest
               FROM tool_cache GROUP BY tool_name ORDER BY tool_name"""
        )
        return rows
    except Exception:
        return []

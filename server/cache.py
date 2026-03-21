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
    "query_genie": 86400,         # 24h — work orders change daily
    "search_local_notices": 7200, # 2h — notices change slowly
    "query_weather": 7200,       # 2h — weather refreshes hourly
}

DEFAULT_TTL = 600  # 10 min

# Known crew names for extraction (lowercased)
_CREWS = [
    "grafton lines a", "grafton lines b", "coffs harbour lines", "coffs harbour cable",
    "lismore lines", "port macquarie lines", "tamworth lines", "tamworth substation",
    "armidale lines", "armidale inspection", "orange lines", "orange cable",
    "dubbo lines", "dubbo emergency", "bathurst lines", "wagga wagga lines",
    "wagga wagga inspection", "broken hill lines", "moree lines", "mudgee lines",
    "inverell lines", "queanbeyan lines", "contractor downer", "contractor asplundh",
    "contractor fulton hogan",
]

# Known depot locations (lowercased)
_LOCATIONS = [
    "grafton", "coffs harbour", "tamworth", "orange", "dubbo", "wagga wagga",
    "armidale", "port macquarie", "bathurst", "broken hill", "lismore", "casino",
    "glen innes", "inverell", "mudgee", "moree", "lightning ridge", "queanbeyan", "bega",
]


def _extract_crew(text: str) -> str | None:
    """Extract crew name from query text."""
    t = text.lower()
    for crew in sorted(_CREWS, key=len, reverse=True):  # longest first
        if crew in t:
            return crew
    return None


def _extract_date(text: str) -> str | None:
    """Extract date (YYYY-MM-DD) from query text."""
    import re
    m = re.search(r'(\d{4}-\d{2}-\d{2})', text)
    if m:
        return m.group(1)
    return None


_GENIE_INTENTS = {
    "work_orders": ["work order", "work orders", "scheduled", "scheduled work", "wo-"],
    "overdue": ["overdue", "late", "behind schedule", "past due", "missed deadline"],
    "pending": ["pending", "upcoming", "not started", "waiting"],
    "completed": ["completed", "finished", "done", "closed"],
    "assets": ["asset", "assets", "equipment", "transformer", "pole", "switchgear"],
    "tasks": ["task", "tasks", "work task"],
    "crew_schedule": ["crew", "schedule", "assignment", "working on", "what is", "what are"],
    "projects": ["project", "projects", "investment", "budget"],
}


def _extract_genie_intent(text: str) -> str:
    """Extract query intent from Genie question for cache grouping."""
    t = text.lower()
    for intent, keywords in _GENIE_INTENTS.items():
        if any(kw in t for kw in keywords):
            return intent
    return "general"


def _extract_location(text: str) -> str:
    """Extract and normalize location name."""
    t = text.lower().strip()
    for loc in sorted(_LOCATIONS, key=len, reverse=True):
        if loc in t:
            return loc
    return t


def _cache_key(tool_name: str, args: dict) -> str:
    """Generate a deterministic cache key from tool name + normalized args.

    SWMS: keyed by document_name only (full doc loaded regardless of query).
    Genie: keyed by lowercased question.
    Weather: keyed by location + date.
    Web: keyed by location + search_type.
    """
    if tool_name == "get_swms":
        # Same document → same result (full doc loading, not VS retrieval)
        normalized = {"tool": tool_name, "document_name": (args.get("document_name") or "default").lower().strip()}
    elif tool_name == "query_genie":
        # Extract crew, date, and intent for stable cache keys
        q = args.get("question", "").lower().strip()
        crew = _extract_crew(q)
        date = _extract_date(q)
        intent = _extract_genie_intent(q)
        if crew and date:
            normalized = {"tool": tool_name, "crew": crew, "date": date, "intent": intent}
        elif crew:
            normalized = {"tool": tool_name, "crew": crew, "intent": intent}
        else:
            normalized = {"tool": tool_name, "intent": intent, "question": q}
    elif tool_name == "query_weather":
        normalized = {"tool": tool_name, "location": _extract_location(args.get("location", "")), "date": args.get("date") or "current"}
    elif tool_name == "search_local_notices":
        normalized = {"tool": tool_name, "location": _extract_location(args.get("location", ""))}
    else:
        normalized = {"tool": tool_name, "args": args}

    raw = json.dumps(normalized, sort_keys=True)
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

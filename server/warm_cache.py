"""Cache warming — pre-populates tool cache for all locations and dates.

Runs with controlled concurrency. Skips already cached entries.
"""

import asyncio
from datetime import date, timedelta
from typing import AsyncGenerator

from server.cache import get_cached, set_cached
from server.swms import query_swms, DOCUMENT_NAMES
from server.weather import query_weather
from server.web_search import search_local_notices
from server.genie import query_genie

_LOCATION_CREWS = {
    "grafton": ["Grafton Lines A", "Grafton Lines B"],
    "coffs harbour": ["Coffs Harbour Lines", "Coffs Harbour Cable"],
    "tamworth": ["Tamworth Lines", "Tamworth Substation"],
    "orange": ["Orange Lines", "Orange Cable"],
    "dubbo": ["Dubbo Lines", "Dubbo Emergency"],
    "wagga wagga": ["Wagga Wagga Lines", "Wagga Wagga Inspection"],
    "armidale": ["Armidale Lines", "Armidale Inspection"],
    "port macquarie": ["Port Macquarie Lines"],
    "bathurst": ["Bathurst Lines"],
    "broken hill": ["Broken Hill Lines"],
    "lismore": ["Lismore Lines"],
    "inverell": ["Inverell Lines"],
    "mudgee": ["Mudgee Lines"],
    "moree": ["Moree Lines"],
    "queanbeyan": ["Queanbeyan Lines"],
    "bega": [],
    "casino": [],
    "glen innes": [],
    "lightning ridge": [],
}

_LOCATIONS = list(_LOCATION_CREWS.keys())
DAYS_AHEAD = 14
CONCURRENCY = 3


def _get_dates() -> list[str]:
    today = date.today()
    return [(today + timedelta(days=i)).isoformat() for i in range(DAYS_AHEAD + 1)]


async def warm_cache() -> AsyncGenerator[dict, None]:
    """Warm the cache. Yields progress dicts."""
    dates = _get_dates()

    total_swms = len(DOCUMENT_NAMES)
    total_weather = len(_LOCATIONS) * len(dates)
    total_web = len(_LOCATIONS)
    total_genie = sum(len(crews) for crews in _LOCATION_CREWS.values()) * len(dates)
    total = total_swms + total_weather + total_web + total_genie

    done = 0
    skipped = 0
    errors = 0
    sem = asyncio.Semaphore(CONCURRENCY)

    def prog(phase, detail=""):
        return {"done": done, "skipped": skipped, "errors": errors, "total": total, "phase": phase, "detail": detail}

    # ── Phase 1: SWMS (7 docs, parallel) ──
    yield prog("swms", "Loading SWMS documents...")

    async def do_swms(doc_name):
        nonlocal done, skipped, errors
        async with sem:
            cached = await get_cached("get_swms", {"document_name": doc_name})
            if cached:
                skipped += 1
                done += 1
                return
            try:
                result = await query_swms("safety requirements", document_name=doc_name)
                if result and "error" not in result.lower():
                    await set_cached("get_swms", {"document_name": doc_name}, result)
            except Exception:
                errors += 1
            done += 1

    await asyncio.gather(*[do_swms(d) for d in DOCUMENT_NAMES])
    yield prog("swms", f"SWMS done — {total_swms} documents")

    # ── Phase 2: Weather (parallel, yield progress periodically) ──
    yield prog("weather", "Loading weather forecasts...")

    async def do_weather(loc, d):
        nonlocal done, skipped, errors
        async with sem:
            args = {"location": loc, "date": d}
            cached = await get_cached("query_weather", args)
            if cached:
                skipped += 1
                done += 1
                return
            try:
                result = await query_weather(f"{loc} {d}")
                if result and "no weather" not in result.lower():
                    await set_cached("query_weather", args, result)
            except Exception:
                errors += 1
            done += 1

    weather_coros = [do_weather(loc, d) for loc in _LOCATIONS for d in dates]
    # Run in chunks so we can yield progress
    for i in range(0, len(weather_coros), 30):
        await asyncio.gather(*weather_coros[i:i + 30])
        yield prog("weather", f"Weather {min(done, total_swms + total_weather)}/{total_swms + total_weather}")

    # ── Phase 3: Web + Genie (interleaved, parallel) ──
    yield prog("web+genie", "Loading web searches + Genie queries...")

    async def do_web(loc):
        nonlocal done, skipped, errors
        async with sem:
            args = {"location": loc, "search_type": "all"}
            cached = await get_cached("search_local_notices", args)
            if cached:
                skipped += 1
                done += 1
                return
            try:
                result = await search_local_notices(loc, search_type="all")
                if result:
                    await set_cached("search_local_notices", args, result)
            except Exception:
                errors += 1
            done += 1

    async def do_genie(crew, d):
        nonlocal done, skipped, errors
        async with sem:
            args = {"question": f"work orders for {crew} on {d}"}
            cached = await get_cached("query_genie", args)
            if cached:
                skipped += 1
                done += 1
                return
            try:
                result = await query_genie(f"work orders for {crew} on {d}")
                if result and "failed" not in result.lower():
                    await set_cached("query_genie", args, result)
            except Exception:
                errors += 1
            done += 1

    web_coros = [do_web(loc) for loc in _LOCATIONS]
    genie_coros = [do_genie(crew, d)
                   for crews in _LOCATION_CREWS.values()
                   for crew in crews
                   for d in dates]

    all_coros = web_coros + genie_coros
    for i in range(0, len(all_coros), 20):
        await asyncio.gather(*all_coros[i:i + 20])
        yield prog("web+genie", f"{done}/{total}")

    yield prog("done", f"Complete — {done} processed, {skipped} cached, {errors} errors")

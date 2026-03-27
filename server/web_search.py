"""Web search tool — searches for local council notices, community events, and road closures
that may affect field crew operations.

Uses Tavily MCP server for near-real-time web results."""

import os
import json
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

from server.branding import (
    DEPOT_COUNCILS as DEPOT_AREAS,
    WEB_SEARCH_DOMAINS,
    COMPANY_NAME, COMPANY_DOMAIN, COMPANY_OUTAGE_URL,
    STATE_SHORT,
)

TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
TAVILY_MCP_URL = os.environ.get(
    "TAVILY_MCP_URL",
    f"https://mcp.tavily.com/mcp/?tavilyApiKey={TAVILY_API_KEY}",
)


def _resolve_location(location: str) -> tuple[str, str | None, str | None]:
    loc = location.strip()
    loc_lower = loc.lower()
    if loc_lower in DEPOT_AREAS:
        return DEPOT_AREAS[loc_lower]
    for key, (display, domain, lga) in DEPOT_AREAS.items():
        if loc_lower in key or key in loc_lower:
            return (display, domain, lga)
    if STATE_SHORT.lower() not in loc_lower:
        loc = f"{loc} {STATE_SHORT}"
    return (loc, None, None)


async def _mcp_search(query: str, max_results: int = 5, include_domains: list[str] | None = None) -> list[dict]:
    """Call Tavily MCP search tool."""
    try:
        async with streamablehttp_client(TAVILY_MCP_URL) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                args = {
                    "query": query,
                    "max_results": max_results,
                    "search_depth": "advanced",
                }
                if include_domains:
                    args["include_domains"] = include_domains

                result = await session.call_tool("tavily_search", args)

                # Parse result — MCP returns content items with text
                for item in result.content:
                    text = item.text if hasattr(item, "text") else str(item)
                    try:
                        data = json.loads(text)
                        return data.get("results", [])
                    except json.JSONDecodeError:
                        pass
    except Exception as e:
        print(f"[WEB_SEARCH] MCP search error: {e}")
    return []


async def search_local_notices(location: str, search_type: str = "all") -> str:
    """Search for local council notices, community events, and road closures near a work area.

    Args:
        location: Town, suburb, or depot area (e.g. "Townsville", "Cairns")
        search_type: One of "road_works", "community_events", "all"

    Returns:
        Formatted string of search results.
    """
    loc, council_domain, lga = _resolve_location(location)
    town = loc.replace(f" {STATE_SHORT}", "")
    town_state = f"{town} {STATE_SHORT}"

    searches = []

    if search_type in ("road_works", "all"):
        council_domains = [council_domain] if council_domain else []
        searches.append({
            "query": f"road closure road works {town_state}",
            "domains": council_domains,
        })
        searches.append({
            "query": f"{town_state} road closure traffic disruption",
            "domains": [d for d in WEB_SEARCH_DOMAINS if "traffic" in d or "transport" in d or "tmr" in d],
        })

    if search_type in ("community_events", "all"):
        event_domains = [council_domain] if council_domain else []
        event_domains.append("eventbrite.com.au")
        searches.append({
            "query": f"{town_state} community event festival market fair 2026",
            "domains": event_domains,
        })

    if search_type == "all":
        searches.append({
            "query": f"{town_state} planned outage power disruption {COMPANY_NAME}",
            "domains": [COMPANY_DOMAIN],
        })

    all_results = []
    relevance_keywords = [
        town.lower(), STATE_SHORT.lower(), "council", "road", "closure", "traffic", "event",
        "festival", "market", "outage", "disruption", "works",
    ]
    if lga:
        relevance_keywords.append(lga.lower())

    for search in searches:
        results = await _mcp_search(
            query=search["query"],
            max_results=5,
            include_domains=search["domains"] if search["domains"] else None,
        )
        for r in results:
            title = r.get("title", "No title")
            url = r.get("url", "")
            content = r.get("content", "")

            text_lower = (title + " " + content + " " + url).lower()
            is_relevant = (
                town.lower() in text_lower
                or (lga and lga.lower() in text_lower)
                or (council_domain and council_domain in url)
                or any(d in url for d in WEB_SEARCH_DOMAINS)
            )
            if not is_relevant:
                continue

            if len(content) > 400:
                content = content[:400] + "..."
            all_results.append({
                "title": title, "url": url, "content": content,
                "score": r.get("score", 0),
            })

    if not all_results:
        fallback = [f"No current notices found for {loc} via web search.\n"]
        fallback.append("**Check these sources directly:**")
        if council_domain:
            fallback.append(f"- {lga} Council: https://{council_domain}")
        for domain in WEB_SEARCH_DOMAINS:
            if COMPANY_DOMAIN not in domain:
                fallback.append(f"- https://{domain}")
        fallback.append(f"- {COMPANY_NAME} outages: {COMPANY_OUTAGE_URL}")
        return "\n".join(fallback)

    # Deduplicate by URL, sort by relevance
    seen_urls = set()
    unique = []
    for r in sorted(all_results, key=lambda x: x["score"], reverse=True):
        if r["url"] not in seen_urls:
            seen_urls.add(r["url"])
            unique.append(r)

    parts = [f"**Local notices near {loc}** ({len(unique)} results):\n"]
    for i, r in enumerate(unique[:8], 1):
        parts.append(f"**{i}. {r['title']}**")
        if r["url"]:
            parts.append(f"   {r['url']}")
        parts.append(f"   {r['content']}")
        parts.append("")

    return "\n".join(parts)

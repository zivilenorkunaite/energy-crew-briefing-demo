"""Web search tool — searches for local council notices, community events, and road closures
that may affect field crew operations in NSW, Australia.

Uses Tavily search API for near-real-time web results."""

import os
from tavily import AsyncTavilyClient
from databricks.sdk import WorkspaceClient

# Essential Energy depot areas → (display name, local council domain, LGA name)
DEPOT_AREAS = {
    "grafton":        ("Grafton NSW",        "clarence.nsw.gov.au",        "Clarence Valley"),
    "coffs harbour":  ("Coffs Harbour NSW",  "coffsharbour.nsw.gov.au",    "Coffs Harbour"),
    "port macquarie": ("Port Macquarie NSW", "pmhc.nsw.gov.au",            "Port Macquarie-Hastings"),
    "taree":          ("Taree NSW",          "midcoast.nsw.gov.au",        "MidCoast"),
    "tamworth":       ("Tamworth NSW",       "tamworth.nsw.gov.au",        "Tamworth Regional"),
    "armidale":       ("Armidale NSW",       "armidaleregional.nsw.gov.au","Armidale Regional"),
    "orange":         ("Orange NSW",         "orange.nsw.gov.au",          "Orange"),
    "bathurst":       ("Bathurst NSW",       "bathurst.nsw.gov.au",        "Bathurst Regional"),
    "dubbo":          ("Dubbo NSW",          "dubbo.nsw.gov.au",           "Dubbo Regional"),
    "wagga wagga":    ("Wagga Wagga NSW",    "wagga.nsw.gov.au",           "Wagga Wagga"),
    "queanbeyan":     ("Queanbeyan NSW",     "qprc.nsw.gov.au",            "Queanbeyan-Palerang"),
    "bega":           ("Bega NSW",           "begavalley.nsw.gov.au",      "Bega Valley"),
    "broken hill":    ("Broken Hill NSW",    "brokenhill.nsw.gov.au",      "Broken Hill"),
    "lightning ridge": ("Lightning Ridge NSW","walgett.nsw.gov.au",         "Walgett"),
    "glen innes":     ("Glen Innes NSW",     "gisc.nsw.gov.au",            "Glen Innes Severn"),
    "lismore":        ("Lismore NSW",        "lismore.nsw.gov.au",         "Lismore"),
    "casino":         ("Casino NSW",         "richmondvalley.nsw.gov.au",  "Richmond Valley"),
    "inverell":       ("Inverell NSW",       "inverell.nsw.gov.au",        "Inverell"),
    "mudgee":         ("Mudgee NSW",         "midwestern.nsw.gov.au",      "Mid-Western Regional"),
    "moree":          ("Moree NSW",          "mpsc.nsw.gov.au",            "Moree Plains"),
}

# Authoritative NSW sources for road/traffic/events
NSW_DOMAINS = [
    "livetraffic.com",
    "transport.nsw.gov.au",
    "essentialenergy.com.au",
    "ses.nsw.gov.au",
    "rfs.nsw.gov.au",
    "eventbrite.com.au",
]


def _get_api_key() -> str:
    """Resolve Tavily API key: env var first, then Databricks secret scope fallback."""
    key = os.environ.get("TAVILY_API_KEY", "")
    if key:
        return key
    try:
        w = WorkspaceClient()
        resp = w.secrets.get_secret("ziviles", "tavily-api-key")
        if resp.value:
            import base64
            return base64.b64decode(resp.value).decode("utf-8")
    except Exception as e:
        print(f"[WEB_SEARCH] Could not read secret from scope: {e}")
    raise RuntimeError("TAVILY_API_KEY not found in env or Databricks secrets")


def _get_client() -> AsyncTavilyClient:
    return AsyncTavilyClient(api_key=_get_api_key())


def _resolve_location(location: str) -> tuple[str, str | None, str | None]:
    """Return (display_name, council_domain_or_None, lga_name_or_None)."""
    loc = location.strip()
    loc_lower = loc.lower()
    if loc_lower in DEPOT_AREAS:
        return DEPOT_AREAS[loc_lower]
    # Fuzzy match — check if input is substring of a known area
    for key, (display, domain, lga) in DEPOT_AREAS.items():
        if loc_lower in key or key in loc_lower:
            return (display, domain, lga)
    # Unknown area — still search but without council domain
    if "nsw" not in loc_lower:
        loc = f"{loc} NSW"
    return (loc, None, None)


async def search_local_notices(location: str, search_type: str = "all") -> str:
    """Search for local council notices, community events, and road closures near a work area.

    Args:
        location: Town, suburb, or depot area (e.g. "Grafton", "Coffs Harbour")
        search_type: One of "road_works", "community_events", "all"

    Returns:
        Formatted string of search results.
    """
    client = _get_client()
    loc, council_domain, lga = _resolve_location(location)
    town = loc.replace(" NSW", "")

    # Build targeted queries with Australian context
    searches = []

    if search_type in ("road_works", "all"):
        # Query 1: Council-specific road works (use include_domains if we know the council)
        council_domains = [council_domain] if council_domain else []
        searches.append({
            "query": f"road closure road works {town}",
            "domains": council_domains,
            "topic": "general",
        })
        # Query 2: Live Traffic NSW + Transport NSW for the area
        searches.append({
            "query": f"site:livetraffic.com OR site:transport.nsw.gov.au {town} road closure",
            "domains": ["livetraffic.com", "transport.nsw.gov.au"],
            "topic": "general",
        })

    if search_type in ("community_events", "all"):
        # Query 3: Local events — target council + event sites
        event_domains = [council_domain] if council_domain else []
        event_domains.append("eventbrite.com.au")
        searches.append({
            "query": f"{town} NSW community event festival market fair 2026",
            "domains": event_domains,
            "topic": "general",
        })

    if search_type == "all":
        # Query 4: Planned outages from Essential Energy + emergency services
        searches.append({
            "query": f"{town} NSW planned outage power disruption Essential Energy",
            "domains": ["essentialenergy.com.au"],
            "topic": "general",
        })

    all_results = []
    nsw_keywords = [
        town.lower(), "nsw", "council", "road", "closure", "traffic", "event",
        "festival", "market", "outage", "disruption", "works",
    ]
    if lga:
        nsw_keywords.append(lga.lower())

    for search in searches:
        try:
            response = await client.search(
                query=search["query"],
                max_results=5,
                search_depth="advanced",
                include_domains=search["domains"] if search["domains"] else [],
                topic=search["topic"],
            )
            results = response.get("results", [])
            for r in results:
                title = r.get("title", "No title")
                url = r.get("url", "")
                content = r.get("content", "")
                score = r.get("score", 0)

                # Relevance filter — must mention the town or LGA or be from a known domain
                text_lower = (title + " " + content + " " + url).lower()
                is_relevant = (
                    town.lower() in text_lower
                    or (lga and lga.lower() in text_lower)
                    or (council_domain and council_domain in url)
                    or any(d in url for d in NSW_DOMAINS)
                )
                if not is_relevant:
                    print(f"[WEB_SEARCH] Filtered out irrelevant result: {title[:60]}")
                    continue

                if len(content) > 400:
                    content = content[:400] + "..."
                all_results.append({
                    "title": title, "url": url, "content": content, "score": score,
                })
        except Exception as e:
            print(f"[WEB_SEARCH] Error searching '{search['query']}': {e}")

    if not all_results:
        # No results — return helpful fallback with direct links
        fallback = [f"No current notices found for {loc} via web search.\n"]
        fallback.append("**Check these sources directly:**")
        if council_domain:
            fallback.append(f"- {lga} Council: https://{council_domain}")
        fallback.append("- Live Traffic NSW: https://www.livetraffic.com")
        fallback.append("- Essential Energy outages: https://www.essentialenergy.com.au/outages")
        fallback.append("- NSW SES: https://www.ses.nsw.gov.au")
        return "\n".join(fallback)

    # Deduplicate by URL, sort by relevance score
    seen_urls = set()
    unique = []
    for r in sorted(all_results, key=lambda x: x["score"], reverse=True):
        if r["url"] not in seen_urls:
            seen_urls.add(r["url"])
            unique.append(r)

    # Format output
    parts = [f"**Local notices near {loc}** ({len(unique)} results):\n"]
    for i, r in enumerate(unique[:8], 1):
        parts.append(f"**{i}. {r['title']}**")
        if r["url"]:
            parts.append(f"   {r['url']}")
        parts.append(f"   {r['content']}")
        parts.append("")

    return "\n".join(parts)

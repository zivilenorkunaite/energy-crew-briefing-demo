"""SWMS lookup — uses Vector Search for semantic retrieval, with keyword fallback."""

import aiohttp

from server.config import get_oauth_token, get_workspace_host

VS_INDEX = "zivile.essential_energy_wacs.swms_documents_vs_index"
VS_ENDPOINT = "ee-crew-briefing-vs"

# All known document names — used by the agent as filter hints
DOCUMENT_NAMES = [
    "SWMS-001 Asset Replacement",
    "SWMS-002 Capital Works",
    "SWMS-003 Corrective Maintenance",
    "SWMS-004 Emergency Response",
    "SWMS-005 Inspection",
    "SWMS-006 Planned Maintenance",
    "SWMS-007 Vegetation Management",
]


async def _vector_search(query: str, document_name: str | None = None, num_results: int = 5) -> list[dict]:
    """Query the Vector Search index and return matching chunks."""
    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query"

    payload = {
        "query_text": query,
        "columns": ["work_type", "section_title", "content", "document_name"],
        "num_results": num_results,
    }
    if document_name:
        payload["filters_json"] = f'{{"document_name": ["{document_name}"]}}'

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[SWMS] Vector Search error {resp.status}: {error[:300]}")
                raise RuntimeError(f"Vector Search returned {resp.status}")
            data = await resp.json()

    # Parse VS response: data_array rows are [work_type, section_title, content, document_name, score]
    result = data.get("result", {})
    rows = result.get("data_array", [])

    # Map column names from the request order
    col_names = ["work_type", "section_title", "content", "document_name"]
    chunks = []
    for row in rows:
        chunk = {}
        for i, name in enumerate(col_names):
            chunk[name] = row[i] if i < len(row) else ""
        if chunk.get("content"):
            chunks.append(chunk)

    return chunks


async def query_swms(query: str, document_name: str | None = None) -> str:
    """
    Search SWMS content using Vector Search (semantic retrieval).

    Args:
        query:         Natural language description of safety info needed.
        document_name: Optional filter — restrict to a specific SWMS document.
    """
    try:
        chunks = await _vector_search(query, document_name=document_name, num_results=5)
    except Exception as e:
        print(f"[SWMS] Vector Search failed, returning error: {e}")
        return "(Vector search unavailable)"

    if not chunks:
        return "(No matching SWMS content found)"

    # Deduplicate by work_type|section_title
    seen = set()
    parts = []
    for c in chunks:
        key = f"{c.get('work_type', '')}|{c.get('section_title', '')}"
        if key in seen:
            continue
        seen.add(key)
        parts.append(f"**{c.get('work_type', '')} — {c.get('section_title', '')}**\n{c.get('content', '')}")

    return "\n\n---\n\n".join(parts) if parts else "(No matching SWMS content found)"

"""SWMS Knowledge Assistant — RAG agent backed by Vector Search + LLM synthesis.

Retrieves relevant SWMS chunks via semantic search, then uses the AI Gateway
to synthesise a focused safety answer grounded in the retrieved documents.
"""

import json
import os
import aiohttp

from server.config import get_oauth_token, get_workspace_host

VS_INDEX = "zivile.essential_energy_wacs.swms_documents_vs_index"
MODEL = os.environ.get("SERVING_ENDPOINT", "ee-crew-briefing-gateway")

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

_ASSISTANT_PROMPT = """You are the Essential Energy SWMS Knowledge Assistant. Your role is to answer \
safety questions using ONLY the Safe Work Method Statement (SWMS) content provided below.

Rules:
- Answer ONLY from the provided SWMS content. Do not invent or assume safety requirements.
- Cite the specific SWMS document (e.g. SWMS-001) and section title for every point.
- Structure answers with clear headings: PPE, Hazards, Isolation Procedures, Competency, etc.
- Use bullet points and tables for clarity — field crews need quick reference.
- If the provided content doesn't cover the question, say so explicitly.
- Reference Australian standards (AS/NZS, NENS-10) where they appear in the content.
"""


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

    result = data.get("result", {})
    rows = result.get("data_array", [])

    col_names = ["work_type", "section_title", "content", "document_name"]
    chunks = []
    for row in rows:
        chunk = {}
        for i, name in enumerate(col_names):
            chunk[name] = row[i] if i < len(row) else ""
        if chunk.get("content"):
            chunks.append(chunk)

    return chunks


async def _synthesise(query: str, chunks: list[dict]) -> str:
    """Use the AI Gateway to synthesise an answer from retrieved SWMS chunks."""
    # Build context from chunks
    context_parts = []
    for c in chunks:
        context_parts.append(
            f"[{c.get('document_name', '')} — {c.get('section_title', '')}]\n{c.get('content', '')}"
        )
    context = "\n\n---\n\n".join(context_parts)

    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/serving-endpoints/{MODEL}/invocations"

    payload = {
        "messages": [
            {"role": "system", "content": _ASSISTANT_PROMPT},
            {"role": "user", "content": f"SWMS CONTENT:\n\n{context}\n\n---\n\nQUESTION: {query}"},
        ],
        "max_tokens": 1500,
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[SWMS] LLM synthesis error {resp.status}: {error[:300]}")
                # Fall back to raw chunks
                return None
            data = await resp.json()

    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    return content if content else None


async def query_swms(query: str, document_name: str | None = None) -> str:
    """
    SWMS Knowledge Assistant: retrieve relevant safety content and synthesise an answer.

    1. Semantic search via Vector Search index
    2. LLM synthesis via AI Gateway (grounded in retrieved chunks)
    3. Fallback to raw chunks if synthesis fails
    """
    # Step 1: Retrieve
    try:
        chunks = await _vector_search(query, document_name=document_name, num_results=5)
    except Exception as e:
        print(f"[SWMS] Vector Search failed: {e}")
        return "(Vector search unavailable)"

    if not chunks:
        return "(No matching SWMS content found)"

    # Deduplicate
    seen = set()
    unique_chunks = []
    for c in chunks:
        key = f"{c.get('work_type', '')}|{c.get('section_title', '')}"
        if key not in seen:
            seen.add(key)
            unique_chunks.append(c)

    # Step 2: Synthesise via LLM
    try:
        answer = await _synthesise(query, unique_chunks)
        if answer:
            # Add source attribution
            sources = sorted(set(c.get("document_name", "") for c in unique_chunks))
            source_line = f"\n\n*Sources: {', '.join(sources)}*"
            return answer + source_line
    except Exception as e:
        print(f"[SWMS] LLM synthesis failed, falling back to raw chunks: {e}")

    # Step 3: Fallback — return raw chunks
    parts = []
    for c in unique_chunks:
        parts.append(f"**{c.get('work_type', '')} — {c.get('section_title', '')}**\n{c.get('content', '')}")
    return "\n\n---\n\n".join(parts)

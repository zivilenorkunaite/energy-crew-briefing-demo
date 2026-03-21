"""SWMS Knowledge Assistant — calls the swms-knowledge-assistant-serving-endpoint serving endpoint.

The endpoint handles Vector Search retrieval + LLM synthesis internally.
"""

import os
import aiohttp

from server.config import get_oauth_token, get_workspace_host

SWMS_ENDPOINT = os.environ.get("SWMS_ENDPOINT", "swms-knowledge-assistant-serving-endpoint")

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


async def query_swms(query: str, document_name: str | None = None) -> str:
    """
    Query the SWMS Knowledge Assistant endpoint.

    The endpoint does VS retrieval + LLM synthesis internally.
    We pass the query (and optional document filter) as a chat message.
    """
    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/serving-endpoints/{SWMS_ENDPOINT}/invocations"

    # Build user message with optional document filter hint
    user_content = query
    if document_name:
        user_content = f"[Document: {document_name}] {query}"

    payload = {
        "messages": [
            {"role": "user", "content": user_content},
        ],
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=90),
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[SWMS] Endpoint error {resp.status}: {error[:300]}")
                return f"(SWMS endpoint error: {resp.status})"
            data = await resp.json()

    # Extract response from chat completions format
    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    if content:
        return content
    return "(No SWMS response received)"

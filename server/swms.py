"""SWMS Knowledge Assistant — calls the swms-knowledge-assistant-v2 endpoint.

The v2 endpoint does direct VS retrieval + AI Gateway LLM synthesis.
Uses standard chat completions format (messages/choices).
"""

import os
import aiohttp

from server.config import get_oauth_token, get_workspace_host

SWMS_ENDPOINT = os.environ.get("SWMS_ENDPOINT", "swms-knowledge-assistant-v2")

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
    Query the SWMS Knowledge Assistant v2 endpoint.

    Uses standard chat completions format (messages in, choices out).
    """
    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/serving-endpoints/{SWMS_ENDPOINT}/invocations"

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
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[SWMS] Endpoint error {resp.status}: {error[:300]}")
                return f"(SWMS endpoint error: {resp.status})"
            data = await resp.json()

    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    if content:
        return content
    return "(No SWMS response received)"

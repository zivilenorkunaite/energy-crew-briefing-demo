"""SWMS Knowledge Assistant — calls the KA endpoint (agent/v1/responses format).

The KA endpoint handles Vector Search retrieval + LLM synthesis internally.
"""

import os
import aiohttp

from server.config import get_oauth_token, get_workspace_host

SWMS_ENDPOINT = os.environ.get("SWMS_ENDPOINT", "ka-654b18c3-endpoint")

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
    Query the SWMS Knowledge Assistant (KA) endpoint.

    Uses the agent/v1/responses input format. The KA endpoint does VS retrieval
    + LLM synthesis internally via the configured knowledge source.
    """
    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/serving-endpoints/{SWMS_ENDPOINT}/invocations"

    # Build user message with optional document filter hint
    user_content = query
    if document_name:
        user_content = f"[Document: {document_name}] {query}"

    payload = {
        "input": [
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
                print(f"[SWMS] KA endpoint error {resp.status}: {error[:300]}")
                return f"(SWMS endpoint error: {resp.status})"
            data = await resp.json()

    # Parse agent/v1/responses format: output[].content[].text
    texts = []
    for item in data.get("output", []):
        if item.get("type") == "message":
            for block in item.get("content", []):
                if block.get("type") == "output_text" and block.get("text"):
                    texts.append(block["text"])

    if texts:
        return "".join(texts)
    return "(No SWMS response received)"

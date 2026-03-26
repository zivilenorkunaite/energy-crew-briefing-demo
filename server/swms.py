"""SWMS Knowledge Assistant — queries the serving endpoint for safety documents."""

import os
import aiohttp

from server.config import get_oauth_token, get_workspace_host
from server.swms_content import DOCUMENT_NAMES

SWMS_ENDPOINT = os.environ.get("SWMS_ENDPOINT", "swms-knowledge-assistant-v2")


async def query_swms(query: str, document_name: str | None = None) -> str:
    """Query the SWMS Knowledge Assistant endpoint."""
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

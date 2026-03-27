"""Genie Room query via REST API."""

import asyncio
import json
import os
import aiohttp

from server.config import get_oauth_token, get_workspace_host

GENIE_ROOM_ID = os.environ.get("GENIE_SPACE_ID", "")


async def query_genie(question: str) -> str:
    """Run a natural-language question against the Genie Room via REST API."""
    if not GENIE_ROOM_ID:
        return "(Genie Room not configured — set GENIE_SPACE_ID)"

    host = get_workspace_host()
    token = get_oauth_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Start conversation
    url = f"{host}/api/2.0/genie/spaces/{GENIE_ROOM_ID}/start-conversation"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={"content": question}, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[GENIE] Start error {resp.status}: {error[:200]}")
                return f"(Genie error: {resp.status})"
            data = await resp.json()

    conv_id = data.get("conversation_id", "")
    msg_id = data.get("message_id", "")
    if not conv_id or not msg_id:
        return "(Genie returned no conversation ID)"

    # Poll for completion
    poll_url = f"{host}/api/2.0/genie/spaces/{GENIE_ROOM_ID}/conversations/{conv_id}/messages/{msg_id}"
    for attempt in range(60):
        await asyncio.sleep(2)
        async with aiohttp.ClientSession() as session:
            async with session.get(poll_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()

        status = data.get("status", "")
        print(f"[GENIE] attempt={attempt + 1} status={status}")

        if status == "COMPLETED":
            return _format_result(data)
        if status in ("FAILED", "CANCELLED"):
            error = data.get("error", {}).get("message", "Query failed")
            return f"(Genie query failed: {error})"

    return "(Genie query timed out after 120 seconds)"


def _format_result(data: dict) -> str:
    """Format Genie API response into readable text."""
    parts = []

    # Text attachments
    for att in data.get("attachments", []):
        if att.get("text"):
            parts.append(att["text"].get("content", ""))

        # Query results
        if att.get("query"):
            query_att = att["query"]
            desc = query_att.get("description", "")
            if desc:
                parts.append(f"**{desc}**")

            # Get the query result
            result = query_att.get("result", {})
            columns = result.get("columns", [])
            rows = result.get("data_array", [])

            if not columns and not rows:
                # Try statement_response format
                stmt = query_att.get("statement_response", {})
                columns = [c["name"] for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])]
                rows = stmt.get("result", {}).get("data_array", [])

            if columns:
                col_names = [c["name"] if isinstance(c, dict) else str(c) for c in columns]
                header = " | ".join(col_names)
                sep = " | ".join(["---"] * len(col_names))
                data_rows = []
                for row in rows[:50]:
                    if isinstance(row, list):
                        data_rows.append(" | ".join(str(v) if v is not None else "" for v in row))
                    elif isinstance(row, dict):
                        vals = row.get("values", [])
                        data_rows.append(" | ".join(
                            str(v.get("str", v.get("string_value", v))) if isinstance(v, dict) else str(v)
                            for v in vals
                        ))
                parts.append("\n".join([header, sep] + data_rows))
                if len(rows) > 50:
                    parts.append(f"*({len(rows)} total rows, showing first 50)*")

    return "\n\n".join(parts) if parts else "(No data returned from Genie)"

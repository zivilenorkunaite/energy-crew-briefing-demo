"""Genie Room 2 (Field Operations) query via Databricks managed MCP server."""

import asyncio
import json
from databricks.sdk import WorkspaceClient
from databricks_mcp.oauth_provider import DatabricksOAuthClientProvider
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

import os
from server.config import get_workspace_host

GENIE_ROOM_2_ID = os.environ.get("GENIE_SPACE_ID", "")
_TOOL_QUERY = f"query_space_{GENIE_ROOM_2_ID}"
_TOOL_POLL  = f"poll_response_{GENIE_ROOM_2_ID}"


def _extract_cell(val: dict) -> str:
    if isinstance(val, dict):
        for key in ("string_value", "double_value", "i64_value"):
            if val.get(key) is not None:
                return str(val[key])
    return str(val) if val is not None else ""


def _parse_response(data: dict) -> str:
    """Convert MCP Genie response to readable text for the agent."""
    content = data.get("content", {})
    parts = []

    for text in content.get("textAttachments", []):
        if text:
            parts.append(text)

    for qa in content.get("queryAttachments", []):
        desc = qa.get("description", "")
        stmt = qa.get("statement_response", {})
        cols = [c["name"] for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = stmt.get("result", {}).get("data_array", [])

        if cols:
            if desc:
                parts.append(f"**{desc}**")
            header = " | ".join(cols)
            sep    = " | ".join(["---"] * len(cols))
            data_rows = [
                " | ".join(_extract_cell(v) for v in row.get("values", []))
                for row in rows
            ]
            parts.append("\n".join([header, sep] + data_rows))

    return "\n\n".join(parts) if parts else "(No data returned)"


async def query_genie(question: str) -> str:
    """Run a natural-language question against Genie Room 2 via managed MCP."""
    host = get_workspace_host()
    url  = f"{host}/api/2.0/mcp/genie/{GENIE_ROOM_2_ID}"

    async with streamablehttp_client(
        url=url,
        auth=DatabricksOAuthClientProvider(WorkspaceClient()),
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            result = await session.call_tool(_TOOL_QUERY, {"query": question})
            raw  = result.content[0].text if result.content else "{}"
            data = json.loads(raw)

            status  = data.get("status", "")
            conv_id = data.get("conversationId")
            msg_id  = data.get("messageId")

            if status.upper() == "COMPLETED":
                return _parse_response(data)

            if status.upper() == "FAILED":
                return "Genie query failed — the question may be outside the available data scope."

            for attempt in range(80):
                await asyncio.sleep(1)
                print(f"[GENIE MCP] attempt={attempt + 1} status={status}")
                poll   = await session.call_tool(_TOOL_POLL, {"conversation_id": conv_id, "message_id": msg_id})
                raw    = poll.content[0].text if poll.content else "{}"
                data   = json.loads(raw)
                status = data.get("status", "")

                if status.upper() == "COMPLETED":
                    return _parse_response(data)
                if status.upper() == "FAILED":
                    return "Genie query failed — the question may be outside the available data scope."

    return "Genie query timed out after 80 seconds."

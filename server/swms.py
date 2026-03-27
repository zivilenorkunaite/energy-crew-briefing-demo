"""SWMS Knowledge Assistant — in-process document lookup + LLM synthesis.

Loads SWMS content from swms_content.py, detects relevant documents by keyword,
and synthesises an answer via the Foundation Model API.
"""

import aiohttp
from server.config import get_oauth_token, get_workspace_host
from server.swms_content import SWMS_CONTENT, DOCUMENT_NAMES

SYSTEM_PROMPT = (
    "You are the SWMS Knowledge Assistant. Answer safety questions "
    "using ONLY the SWMS content provided below.\n\n"
    "Rules:\n"
    "- Cite the specific SWMS document (e.g. SWMS-001) and section title for every point.\n"
    "- Structure with headings: PPE, Hazards, Isolation Procedures, Competency Requirements.\n"
    "- Use tables for PPE lists. Use bullet points for hazards and procedures.\n"
    "- Reference Australian standards (AS/NZS, NENS-10) where they appear.\n"
    "- If the content doesn't cover the question, say so explicitly.\n"
    "- Keep answers concise — field crews need quick reference, not essays."
)

KEYWORD_MAP = {
    "SWMS-001 Asset Replacement": ["replacement", "upgrade", "transformer", "switchgear", "pole", "cross-arm", "asset"],
    "SWMS-002 Capital Works": ["capital", "construction", "new build", "install"],
    "SWMS-003 Corrective Maintenance": ["corrective", "fault", "repair", "unplanned", "fix"],
    "SWMS-004 Emergency Response": ["emergency", "storm", "bushfire", "fallen", "urgent"],
    "SWMS-005 Inspection": ["inspection", "audit", "patrol", "check", "drone"],
    "SWMS-006 Planned Maintenance": ["planned", "maintenance", "routine", "scheduled", "ppe", "isolation", "overhead", "line"],
    "SWMS-007 Vegetation Management": ["vegetation", "tree", "trim", "clearing", "veg"],
    "SWMS-008 Underground Cable": ["underground", "cable", "trench", "jointing", "xlpe", "pilc"],
    "SWMS-009 Metering": ["meter", "metering", "smart meter", "ct meter", "solar meter", "disconnection"],
    "SWMS-010 Substation Work": ["substation", "zone sub", "switching", "circuit breaker", "hv", "high voltage", "sf6", "battery"],
}


def _detect_documents(query: str, document_name: str | None = None) -> list[str]:
    """Detect which SWMS documents are relevant to the query."""
    if document_name:
        if document_name in SWMS_CONTENT:
            return [document_name]
        # Partial match
        for name in SWMS_CONTENT:
            if document_name.lower() in name.lower() or name.lower() in document_name.lower():
                return [name]

    q = query.lower()
    docs = []
    for doc_name, keywords in KEYWORD_MAP.items():
        if any(kw in q for kw in keywords):
            docs.append(doc_name)

    if not docs:
        docs = ["SWMS-006 Planned Maintenance"]

    if "ppe" in q and "SWMS-006 Planned Maintenance" not in docs:
        docs.insert(0, "SWMS-006 Planned Maintenance")

    return docs[:3]


def _format_document(doc_name: str) -> str:
    """Format a SWMS document as text for LLM context."""
    sections = SWMS_CONTENT.get(doc_name, {})
    parts = [f"## {doc_name}\n"]
    for title, content in sections.items():
        parts.append(f"### {title}\n{content}")
    return "\n\n".join(parts)


async def query_swms(query: str, document_name: str | None = None) -> str:
    """Query SWMS documents — detect relevant docs, synthesise answer via LLM."""
    docs = _detect_documents(query, document_name)

    # Build full document context
    doc_text = "\n\n---\n\n".join(_format_document(d) for d in docs)
    doc_names_str = ", ".join(docs)

    # Call Foundation Model API
    import os
    host = get_workspace_host()
    token = get_oauth_token()
    model = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")
    url = f"{host}/serving-endpoints/{model}/invocations"

    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"SWMS CONTENT:\n\n{doc_text}\n\n---\n\nQUESTION: {query}"},
        ],
        "max_tokens": 1500,
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    print(f"[SWMS] LLM error {resp.status}: {error[:300]}")
                    return f"(SWMS query error: {resp.status})"
                data = await resp.json()

        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        if content:
            return content
    except Exception as e:
        print(f"[SWMS] Error: {e}")

    # Fallback: return raw document text without synthesis
    return f"**Relevant SWMS documents:** {doc_names_str}\n\n{doc_text[:2000]}"

"""Conversation memory — CRUD operations backed by Lakebase."""

import json
from server.db import db


async def save_message(session_id: str, role: str, content: str, sources: list | None = None, title: str | None = None):
    """Upsert conversation and append a message."""
    # Upsert conversation
    existing = await db.fetchrow(
        "SELECT id FROM conversations WHERE session_id = $1", session_id
    )
    if existing:
        await db.execute(
            "UPDATE conversations SET updated_at = NOW() WHERE session_id = $1",
            session_id,
        )
    else:
        await db.execute(
            "INSERT INTO conversations (session_id, title) VALUES ($1, $2)",
            session_id, title or "New conversation",
        )

    # Append message
    sources_json = json.dumps(sources) if sources else None
    await db.execute(
        "INSERT INTO messages (session_id, role, content, sources) VALUES ($1, $2, $3, $4)",
        session_id, role, content, sources_json,
    )


async def list_sessions(limit: int = 50) -> list[dict]:
    """Return conversations ordered by most recent."""
    rows = await db.fetch(
        "SELECT session_id, title, created_at, updated_at "
        "FROM conversations ORDER BY updated_at DESC LIMIT $1",
        limit,
    )
    return [
        {
            "id": r["session_id"],
            "title": r["title"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
        }
        for r in rows
    ]


async def get_session_messages(session_id: str) -> list[dict]:
    """Return all messages for a session."""
    rows = await db.fetch(
        "SELECT role, content, sources, created_at "
        "FROM messages WHERE session_id = $1 ORDER BY created_at ASC",
        session_id,
    )
    results = []
    for r in rows:
        sources = None
        if r["sources"]:
            try:
                sources = json.loads(r["sources"])
            except json.JSONDecodeError:
                sources = None
        results.append({
            "role": r["role"],
            "content": r["content"],
            "sources": sources or [],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })
    return results


async def delete_session(session_id: str):
    """Delete a conversation and all its messages (cascade)."""
    await db.execute("DELETE FROM conversations WHERE session_id = $1", session_id)

"""Lakebase connection pool with OAuth token rotation.

Uses PGHOST, PGPORT, PGDATABASE auto-injected by the database app resource.
Auth via WorkspaceClient OAuth token (not generate_database_credential).
"""

import os
import asyncpg
from typing import Optional, List, Any

SCHEMA = os.environ.get("PGSCHEMA", "public")


def _get_oauth_token() -> str:
    """Get OAuth token for Lakebase auth via WorkspaceClient."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    token = w.config.oauth_token().access_token
    if not token:
        raise RuntimeError("WorkspaceClient returned no OAuth token")
    print(f"[DB] OAuth token obtained ({len(token)} chars)")
    return token


def _get_pg_user() -> str:
    """Get PostgreSQL username — auto-injected PGUSER or SP identity."""
    pg_user = os.environ.get("PGUSER")
    if pg_user:
        return pg_user
    # Fallback: get current user from SDK
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        return w.current_user.me().user_name
    except Exception:
        return os.environ.get("DATABRICKS_CLIENT_ID", "")


class DatabasePool:
    """Async database pool with OAuth token refresh for Lakebase."""

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None

    async def get_pool(self) -> asyncpg.Pool:
        """Return the connection pool, creating it if needed."""
        if self._pool is None:
            pg_host = os.environ.get("PGHOST")
            if not pg_host:
                raise RuntimeError("PGHOST is not set — add Lakebase as a database resource")

            raw_port = os.environ.get("PGPORT", "5432")
            try:
                pg_port = int(raw_port)
            except (ValueError, TypeError):
                pg_port = 5432

            pg_db = os.environ.get("PGDATABASE", "crew_briefing")
            pg_user = _get_pg_user()
            token = _get_oauth_token()

            print(f"[DB] Connecting: host={pg_host[:30]}... db={pg_db} user={pg_user[:20]}... port={pg_port}")

            self._pool = await asyncpg.create_pool(
                host=pg_host,
                port=pg_port,
                database=pg_db,
                user=pg_user,
                password=token,
                ssl="require",
                min_size=1,
                max_size=5,
            )
            print("[DB] Pool created")
        return self._pool

    async def refresh_token(self):
        """Refresh the OAuth token for existing connections."""
        if self._pool:
            token = _get_oauth_token()
            # asyncpg doesn't support password rotation directly,
            # so we close and recreate the pool
            await self._pool.close()
            self._pool = None
            await self.get_pool()
            print("[DB] Pool refreshed with new token")

    async def execute(self, query: str, *args) -> str:
        pool = await self.get_pool()
        return await pool.execute(query, *args)

    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        pool = await self.get_pool()
        return await pool.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        pool = await self.get_pool()
        return await pool.fetchrow(query, *args)

    async def fetchval(self, query: str, *args) -> Any:
        pool = await self.get_pool()
        return await pool.fetchval(query, *args)

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None


db = DatabasePool()

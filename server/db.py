"""Lakebase Autoscaling — async connection pool with OAuth token rotation.

Uses PGHOST, PGPORT, PGDATABASE, PGUSER from the database app resource;
tokens are generated via WorkspaceClient().postgres.generate_database_credential().
"""

import os
import asyncpg
from typing import Optional, List, Any

SCHEMA = os.environ.get("PGSCHEMA", "public")
_ENDPOINT_NAME = os.environ.get(
    "ENDPOINT_NAME",
    "projects/energy-crew-briefing-as/branches/production/endpoints/primary",
)


def _generate_token() -> str:
    """Generate a fresh OAuth token for Lakebase Autoscaling."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    cred = w.postgres.generate_database_credential(endpoint=_ENDPOINT_NAME)
    token = getattr(cred, "token", None) or getattr(cred, "password", "")
    if not token:
        raise RuntimeError("generate_database_credential returned no token")
    print(f"[DB] Generated Lakebase token ({len(token)} chars)")
    return token


class DatabasePool:
    """Async database pool with OAuth token refresh for Lakebase Autoscaling."""

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

            pg_db = os.environ.get("PGDATABASE", "databricks_postgres")
            pg_user = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID", "")
            token = _generate_token()

            self._pool = await asyncpg.create_pool(
                host=pg_host,
                port=pg_port,
                database=pg_db,
                user=pg_user,
                password=token,
                ssl="require",
                min_size=1,
                max_size=10,
                command_timeout=60,
            )
            print(f"[DB] Connected to Lakebase at {pg_host}")

        return self._pool

    async def refresh_token(self):
        """Refresh database token (tokens expire after ~1h)."""
        if self._pool:
            await self._pool.close()
            self._pool = None
        await self.get_pool()

    async def execute(self, sql: str, *args) -> str:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetch(self, sql: str, *args) -> List[Any]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [dict(r) for r in rows]

    async def fetchrow(self, sql: str, *args) -> Optional[dict]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            return dict(row) if row else None

    async def fetchval(self, sql: str, *args) -> Any:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None


db = DatabasePool()

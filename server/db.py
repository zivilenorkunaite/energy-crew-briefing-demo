"""Lakebase (PostgreSQL) async connection pool with OAuth token refresh.

Uses PGHOST, PGPORT, PGDATABASE from the Databricks app database resource;
password is a short-lived token from /api/2.0/database/credentials.
"""

import os
import asyncpg
import aiohttp
from typing import Optional, List, Any
from server.config import get_oauth_token, get_workspace_host

SCHEMA = os.environ.get("PGSCHEMA", "public")
_LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "ee-crew-briefing")


async def _get_database_token() -> Optional[str]:
    """Get database credential token. Tries PGPASSWORD, then SDK, then credentials API."""
    # 1. PGPASSWORD env var (injected by database resource or set manually)
    pg_password = os.environ.get("PGPASSWORD")
    if pg_password:
        return pg_password

    # 2. Try Databricks SDK database credential generation
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        cred = w.database.generate_database_credential(instance_names=[_LAKEBASE_INSTANCE])
        token = getattr(cred, "token", None)
        if token:
            print(f"[DB] Got credential via SDK ({len(token)} chars)")
            return token
    except Exception as e:
        print(f"[DB] SDK credential generation failed: {e}")

    # 3. Try REST API
    try:
        workspace_token = get_oauth_token()
        if not workspace_token:
            return None
        workspace_host = get_workspace_host()
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{workspace_host}/api/2.0/database/credentials",
                headers={"Authorization": f"Bearer {workspace_token}"},
                json={"instance_names": [_LAKEBASE_INSTANCE]},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                token = data.get("token")
                if token:
                    return token
    except Exception as e:
        print(f"[DB] REST credentials API failed: {e}")

    print("[DB] No database credential available")
    return None


class DatabasePool:
    """Async database pool with OAuth token refresh."""

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None

    def _resolve_pghost(self) -> bool:
        """Resolve PGHOST from Lakebase instance API if not set."""
        if os.environ.get("PGHOST"):
            return True
        if not _LAKEBASE_INSTANCE:
            return False
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            instance = w.database.get_database_instance(name=_LAKEBASE_INSTANCE)
            host = getattr(instance, "read_write_dns", None) or getattr(instance, "host", None)
            if host:
                os.environ["PGHOST"] = host
                print(f"[DB] Resolved PGHOST from instance {_LAKEBASE_INSTANCE}: {host}")
                return True
        except Exception as e:
            print(f"[DB] Could not resolve PGHOST from instance API: {e}")
        return False

    async def get_pool(self) -> asyncpg.Pool:
        """Return the connection pool."""
        if not self._resolve_pghost():
            raise RuntimeError(
                f"Cannot connect to database: PGHOST is not set and could not be resolved from "
                f"LAKEBASE_INSTANCE ({_LAKEBASE_INSTANCE})."
            )

        if self._pool is None:
            pg_host = os.environ["PGHOST"]
            raw_port = os.environ.get("PGPORT", "5432")
            try:
                pg_port = int(raw_port)
            except (ValueError, TypeError):
                pg_port = 5432
            pg_db = os.environ.get("PGDATABASE", "crew_briefing")

            # Try database credentials API first
            token = await _get_database_token()
            pg_user = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID", "")

            if token:
                password = token
            else:
                raise RuntimeError("Cannot connect to database: No credential obtained.")

            self._pool = await asyncpg.create_pool(
                host=pg_host,
                port=pg_port,
                database=pg_db,
                user=pg_user,
                password=password,
                ssl="require",
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
            print(f"[DB] Connected to Lakebase at {pg_host}")

        return self._pool

    async def refresh_token(self):
        """Refresh database token (Lakebase tokens expire after ~1h)."""
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

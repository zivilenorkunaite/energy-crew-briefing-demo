"""EE Crew Briefing — FastAPI Application with SSE streaming."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

# Load .env for local development (ignored in Databricks Apps where secrets are injected)
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import StreamingResponse

from server.agent import run_agent


# ── Lakebase lifecycle ──────────────────────────────────────────────────────

_token_refresh_task: Optional[asyncio.Task] = None


async def _refresh_loop():
    """Refresh Lakebase token every 45 minutes."""
    from server.db import db
    while True:
        await asyncio.sleep(45 * 60)
        try:
            await db.refresh_token()
            print("[APP] Lakebase token refreshed")
        except Exception as e:
            print(f"[APP] Token refresh failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Init DB pool on startup, refresh token periodically, close on shutdown."""
    global _token_refresh_task
    try:
        from server.db import db
        await db.get_pool()
        _token_refresh_task = asyncio.create_task(_refresh_loop())
        print("[APP] Lakebase connected, token refresh scheduled")
    except Exception as e:
        print(f"[APP] Lakebase init failed (sessions will not persist): {e}")

    yield

    if _token_refresh_task:
        _token_refresh_task.cancel()
    try:
        from server.db import db
        await db.close()
    except Exception:
        pass


app = FastAPI(
    title="EE Crew Briefing",
    version="2.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)


class IframeHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["content-security-policy"] = "frame-ancestors *"
        response.headers["x-frame-options"] = "ALLOWALL"
        return response


app.add_middleware(IframeHeadersMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https://.*\.(databricks\.com|databricksapps\.com)",
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)


# ── Chat endpoint (SSE streaming) ──────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    history: list = []
    session_id: str | None = None


@app.post("/api/chat")
async def chat(req: ChatRequest):
    """Chat endpoint with SSE streaming for real-time agent activity steps."""
    event_queue: asyncio.Queue = asyncio.Queue()

    def on_step(step: dict):
        """Callback fired from agent for each activity step."""
        event_queue.put_nowait(("step", step))

    def on_token(text: str):
        """Callback fired for each writer token."""
        event_queue.put_nowait(("token", text))

    async def generate():
        agent_task = asyncio.create_task(
            run_agent(req.message, req.history, on_step=on_step, on_token=on_token)
        )

        # Stream step + token events
        while not agent_task.done():
            try:
                item = await asyncio.wait_for(event_queue.get(), timeout=0.3)
                etype, data = item
                if etype == "step":
                    yield f"event: step\ndata: {json.dumps(data)}\n\n"
                elif etype == "token":
                    yield f"event: token\ndata: {json.dumps(data)}\n\n"
            except asyncio.TimeoutError:
                pass

        # Drain remaining events
        while not event_queue.empty():
            etype, data = event_queue.get_nowait()
            if etype == "step":
                yield f"event: step\ndata: {json.dumps(data)}\n\n"
            elif etype == "token":
                yield f"event: token\ndata: {json.dumps(data)}\n\n"

        # Get final result
        try:
            result = agent_task.result()
        except Exception as e:
            import traceback
            traceback.print_exc()
            result = {"response": f"Error: {str(e)}", "sources": [], "history": req.history, "steps": []}

        # Persist to Lakebase
        if req.session_id:
            try:
                from server.memory import save_message
                await save_message(req.session_id, "user", req.message, title=req.message[:52])
                await save_message(req.session_id, "assistant", result["response"], sources=result.get("sources"))
            except Exception as e:
                print(f"[APP] Failed to persist messages: {e}")

        yield f"event: done\ndata: {json.dumps(result)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "content-security-policy": "frame-ancestors *",
            "x-frame-options": "ALLOWALL",
        },
    )


# ── Session endpoints ───────────────────────────────────────────────────────

@app.get("/api/sessions")
async def list_sessions():
    try:
        from server.memory import list_sessions as _list
        sessions = await _list(limit=50)
        return {"sessions": sessions, "persistent": True}
    except Exception as e:
        print(f"[APP] list_sessions error: {e}")
        return {"sessions": [], "persistent": False}


@app.get("/api/sessions/{session_id}/messages")
async def get_session_messages(session_id: str):
    try:
        from server.memory import get_session_messages as _get
        messages = await _get(session_id)
        return {"messages": messages}
    except Exception as e:
        print(f"[APP] get_session_messages error: {e}")
        return {"messages": []}


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    try:
        from server.memory import delete_session as _del
        await _del(session_id)
        return {"ok": True}
    except Exception as e:
        print(f"[APP] delete_session error: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/api/health")
async def health():
    return {"status": "healthy", "app": "EE Crew Briefing"}


# ── Cache endpoints ───────────────────────────────────────────────────────

@app.get("/api/cache/stats")
async def cache_stats():
    try:
        from server.cache import get_cache_stats
        stats = await get_cache_stats()
        return {"stats": stats}
    except Exception as e:
        return {"stats": [], "error": str(e)}


@app.delete("/api/cache/{tool_name}")
async def clear_cache_tool(tool_name: str):
    try:
        from server.cache import clear_tool_cache
        count = await clear_tool_cache(tool_name)
        return {"ok": True, "cleared": count, "tool": tool_name}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.delete("/api/cache")
async def clear_cache_all():
    try:
        from server.cache import clear_all_cache
        count = await clear_all_cache()
        return {"ok": True, "cleared": count}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Static files ──────────────────────────────────────────────────────────────

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/")
    async def root():
        return FileResponse(str(static_dir / "index.html"))

    @app.get("/{path:path}")
    async def catch_all(path: str):
        if path.startswith("api/"):
            return JSONResponse(status_code=404, content={"error": "Not found"})
        file_path = (static_dir / path).resolve()
        if not str(file_path).startswith(str(static_dir.resolve())):
            return JSONResponse(status_code=403, content={"error": "Forbidden"})
        if file_path.is_file():
            return FileResponse(str(file_path))
        # Try with .html extension (e.g. /settings → settings.html)
        html_path = (static_dir / f"{path}.html").resolve()
        if str(html_path).startswith(str(static_dir.resolve())) and html_path.is_file():
            return FileResponse(str(html_path))
        return FileResponse(str(static_dir / "index.html"))
else:
    @app.get("/")
    async def root_fallback():
        return {"message": "EE Crew Briefing API", "docs": "/api/docs"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

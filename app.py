"""Energy Crew Briefing — FastAPI Application with SSE streaming."""

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

_startup_errors = []

try:
    from server.agent import run_agent
except Exception as e:
    _startup_errors.append(f"agent import: {e}")
    async def run_agent(*a, **kw):
        yield {"type": "error", "content": f"Agent not available: {_startup_errors}"}

from server.customise import (
    APP_TITLE, COMPANY_NAME, PAGE_TITLE, APP_DISPLAY, APP_SUBTITLE,
    COLOR_PRIMARY, COLOR_PRIMARY_LIGHT, COLOR_ACCENT, COLOR_ACCENT_LIGHT, COLOR_USER_BG,
    CREWS, DEPOTS,
)


# ── Background tasks ──────────────────────────────────────────────────────

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
    """Init DB pool, start background tasks, clean up on shutdown."""
    global _token_refresh_task
    try:
        from server.db import db
        await db.get_pool()
        _token_refresh_task = asyncio.create_task(_refresh_loop())
        print("[APP] Lakebase connected, token refresh scheduled")
    except Exception as e:
        _startup_errors.append(f"Lakebase: {e}")
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
    title=APP_TITLE,
    version="2.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)


class IframeHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["content-security-policy"] = "frame-ancestors 'self' *.databricks.com *.databricksapps.com"
        response.headers["x-frame-options"] = "SAMEORIGIN"
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
    client_date: str | None = None  # Browser's local date, e.g. "Thursday, 27 March 2025"
    client_time: str | None = None  # Browser's local time, e.g. "02:30 PM AEST"


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
            run_agent(req.message, req.history, on_step=on_step, on_token=on_token,
                      client_date=req.client_date, client_time=req.client_time)
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
            "content-security-policy": "frame-ancestors 'self' *.databricks.com *.databricksapps.com",
            "x-frame-options": "SAMEORIGIN",
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
    result = {"status": "healthy", "app": APP_TITLE}
    if _startup_errors:
        result["startup_errors"] = _startup_errors
        result["status"] = "degraded"
    # DB connection info (no secrets)
    result["db"] = {
        "pghost": bool(os.environ.get("PGHOST")),
        "pgdatabase": os.environ.get("PGDATABASE", ""),
        "pguser": bool(os.environ.get("PGUSER")),
        "pgpassword": bool(os.environ.get("PGPASSWORD")),
        "databricks_client_id": bool(os.environ.get("DATABRICKS_CLIENT_ID")),
        "endpoint_name": os.environ.get("ENDPOINT_NAME", ""),
    }
    try:
        from server.db import db
        result["db"]["pool_active"] = db._pool is not None
    except Exception:
        result["db"]["pool_active"] = False
    return result


@app.get("/api/branding")
async def branding():
    return {
        "company_name": COMPANY_NAME,
        "app_title": APP_TITLE,
        "app_display": APP_DISPLAY,
        "app_subtitle": APP_SUBTITLE,
        "page_title": PAGE_TITLE,
        "colors": {
            "primary": COLOR_PRIMARY,
            "primary_light": COLOR_PRIMARY_LIGHT,
            "accent": COLOR_ACCENT,
            "accent_light": COLOR_ACCENT_LIGHT,
            "user_bg": COLOR_USER_BG,
        },
    }


@app.get("/api/suggestions")
async def suggestions():
    """Return dynamic example questions using crew names and locations from config."""
    import random
    crew_names = list(CREWS.keys())
    depot_names = [d["name"] for d in DEPOTS.values()]
    # Pick a few crews and depots for variety
    c1 = crew_names[0] if crew_names else "Crew A"
    c2 = next((n for n in crew_names if "Emergency" in n or "Inspection" in n), crew_names[1] if len(crew_names) > 1 else c1)
    c3 = next((n for n in crew_names if "Substation" in n or "Cable" in n), crew_names[2] if len(crew_names) > 2 else c1)
    d1 = depot_names[0] if depot_names else "Town A"
    d2 = depot_names[1] if len(depot_names) > 1 else d1
    return [
        f"Prepare a crew briefing for {c1} tomorrow",
        f"What work orders does {c3} have scheduled this week?",
        f"Prepare a briefing for {c2} for the day after tomorrow",
        "What PPE and isolation procedures are needed for overhead line replacement?",
        f"Are there any road closures or community events near {d2} this week?",
        "Which crews have the most overdue work orders right now?",
        f"What's the weather forecast for {d1} tomorrow? Safe for elevated work?",
        "What assets are in critical condition?",
    ]


# ── Briefing PDF download ─────────────────────────────────────────────────

class BriefingPdfRequest(BaseModel):
    response: str
    title: str = "Crew Briefing"
    crew: str = ""
    briefing_date: str = ""
    sources: list = []


@app.post("/api/briefing/pdf")
async def briefing_pdf(req: BriefingPdfRequest):
    """Generate a PDF from a crew briefing response."""
    try:
        from server.briefing_pdf import generate_briefing_pdf
        pdf_bytes = generate_briefing_pdf(
            response=req.response,
            title=req.title,
            crew=req.crew,
            briefing_date=req.briefing_date,
            sources=req.sources,
        )
        filename = f"briefing_{req.crew.replace(' ', '_')}_{req.briefing_date.replace(' ', '_')}.pdf" if req.crew else "briefing.pdf"
        from starlette.responses import Response
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── Asset image proxy ─────────────────────────────────────────────────────

@app.get("/api/assets/image/{filename}")
async def asset_image(filename: str):
    """Proxy asset images from UC Volume."""
    import re
    import aiohttp
    from server.config import get_oauth_token, get_workspace_host
    from server.customise import UC_FULL

    if not re.match(r'^[a-z0-9_\-]+\.png$', filename):
        return JSONResponse(status_code=400, content={"error": "Invalid filename"})

    host = get_workspace_host()
    token = get_oauth_token()
    volume_path = f"/Volumes/{UC_FULL.replace('.', '/')}/asset_images/{filename}"
    url = f"{host}/api/2.0/fs/files{volume_path}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return JSONResponse(status_code=404, content={"error": "Image not found"})
                data = await resp.read()
                from starlette.responses import Response
                return Response(content=data, media_type="image/png")
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Failed to fetch image"})


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


@app.get("/api/settings")
async def get_settings():
    from server.settings import get_all
    return await get_all()


@app.post("/api/settings/{key}")
async def set_setting_endpoint(key: str, req: dict = {}):
    from server.settings import set_setting
    value = str(req.get("value", "false"))
    await set_setting(key, value)
    return {"ok": True, "key": key, "value": value}


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
        return {"message": f"{APP_TITLE} API", "docs": "/api/docs"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

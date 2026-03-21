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


# ── Background tasks ──────────────────────────────────────────────────────

_token_refresh_task: Optional[asyncio.Task] = None
_cache_warm_task: Optional[asyncio.Task] = None
_warm_history: list = []  # [{trigger, start, end, duration_s, before, after, skipped, errors, status, phase, done, total}]
_warm_cancel = False
MAX_WARM_HISTORY = 20
_run_warm_ref = None  # set in _cache_warm_loop


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


async def _cache_warm_loop():
    """Run cache warming at 6am and 6pm Sydney time."""
    from server.warm_cache import warm_cache
    from server.cache import get_cache_stats
    try:
        from zoneinfo import ZoneInfo
        syd = ZoneInfo("Australia/Sydney")
    except Exception:
        from datetime import timezone, timedelta
        syd = timezone(timedelta(hours=11))

    from datetime import datetime

    TARGET_HOURS = {6, 18}
    CHECK_INTERVAL = 300  # check every 5 min
    MIN_CACHE_PCT = 0.80
    EXPECTED_TOTAL = 466  # 7 + 209 + 19 + 231

    last_warm_hour = None
    import time as _time

    async def _run_warm(trigger: str):
        """Run warming and record to history with live progress."""
        global _warm_cancel, _run_warm_ref
        _warm_cancel = False
        start = _time.time()
        now_str = datetime.now(syd).strftime("%Y-%m-%d %H:%M AEST")

        stats_before = await get_cache_stats()
        before_total = sum(s.get("count", 0) for s in stats_before)

        record = {
            "trigger": trigger, "start": now_str, "before": before_total,
            "status": "running", "phase": "starting", "done": 0, "total": 0,
            "skipped": 0, "errors": 0, "after": None, "duration_s": None, "end": None,
        }
        _warm_history.insert(0, record)
        if len(_warm_history) > MAX_WARM_HISTORY:
            _warm_history.pop()

        try:
            async for progress in warm_cache():
                # Update live progress in the record
                record["phase"] = progress.get("phase", "")
                record["done"] = progress.get("done", 0)
                record["total"] = progress.get("total", 0)
                record["skipped"] = progress.get("skipped", 0)
                record["errors"] = progress.get("errors", 0)
                record["duration_s"] = round(_time.time() - start, 1)

                if progress.get("phase") == "done":
                    print(f"[WARM] {trigger}: {progress.get('done')}/{progress.get('total')} ({progress.get('skipped')} cached, {progress.get('errors')} errors)")

                if _warm_cancel:
                    print(f"[WARM] {trigger}: cancelled by user")
                    record["status"] = "cancelled"
                    break
        except Exception as e:
            record["status"] = f"error: {e}"
            print(f"[WARM] {trigger} error: {e}")
            return

        stats_after = await get_cache_stats()
        after_total = sum(s.get("count", 0) for s in stats_after)
        elapsed = round(_time.time() - start, 1)

        record.update({
            "end": datetime.now(syd).strftime("%Y-%m-%d %H:%M AEST"),
            "duration_s": elapsed,
            "after": after_total,
        })
        if record["status"] == "running":
            record["status"] = "done"
        print(f"[WARM] {trigger}: {before_total} → {after_total} in {elapsed}s")

    from server.settings import get_bool

    # Expose _run_warm for the manual API endpoint
    global _run_warm_ref
    _run_warm_ref = _run_warm

    # No warm on startup — only on schedule (6am/6pm) or manual button
    print("[WARM] Cache warm loop started — scheduled at 6am/6pm AEST, or trigger manually")

    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        try:
            if not await get_bool("warm_enabled", True):
                continue

            now = datetime.now(syd)
            current_hour = now.hour

            if current_hour in TARGET_HOURS and last_warm_hour != current_hour:
                last_warm_hour = current_hour

                stats = await get_cache_stats()
                total = sum(s.get("count", 0) for s in stats)
                pct = total / EXPECTED_TOTAL if EXPECTED_TOTAL > 0 else 0

                if pct >= MIN_CACHE_PCT:
                    print(f"[WARM] Cache at {pct:.0%} ({total}/{EXPECTED_TOTAL}) — skipping")
                    _warm_history.insert(0, {
                        "trigger": f"scheduled {current_hour}:00",
                        "start": now.strftime("%Y-%m-%d %H:%M AEST"),
                        "status": f"skipped ({pct:.0%} full)",
                        "before": total, "after": total, "duration_s": 0, "skipped": 0, "errors": 0,
                    })
                    if len(_warm_history) > MAX_WARM_HISTORY:
                        _warm_history.pop()
                    continue

                await _run_warm(f"scheduled {current_hour}:00")

        except Exception as e:
            print(f"[WARM] Loop error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Init DB pool, start background tasks, clean up on shutdown."""
    global _token_refresh_task, _cache_warm_task
    try:
        from server.db import db
        await db.get_pool()
        _token_refresh_task = asyncio.create_task(_refresh_loop())
        _cache_warm_task = asyncio.create_task(_cache_warm_loop())
        print("[APP] Lakebase connected, token refresh + cache warm scheduled")
    except Exception as e:
        print(f"[APP] Lakebase init failed (sessions will not persist): {e}")

    yield

    if _token_refresh_task:
        _token_refresh_task.cancel()
    if _cache_warm_task:
        _cache_warm_task.cancel()
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


@app.get("/api/cache/warm/history")
async def warm_history():
    return {"history": _warm_history}


@app.post("/api/cache/warm/stop")
async def warm_stop():
    global _warm_cancel
    _warm_cancel = True
    return {"ok": True}


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


@app.post("/api/cache/warm")
async def warm_cache_endpoint():
    """Trigger cache warming as a background task."""
    # Check if already running
    if _warm_history and _warm_history[0].get("status") == "running":
        return {"ok": False, "error": "Warming already in progress"}
    # Run _run_warm in background (it's defined inside _cache_warm_loop — call it via the shared reference)
    asyncio.create_task(_run_warm_ref("manual"))
    return {"ok": True}


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

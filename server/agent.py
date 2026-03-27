"""Dual-agent loop — Haiku supervisor (tool selection) + Claude writer (response composition).

MLflow tracing uses proper context-manager spans throughout.
"""

import asyncio
import json
import os
import aiohttp
from typing import Any

from server.branding import CREW_LIST_STRING, UC_FULL, TIMEZONE, TIMEZONE_LABEL
from server.config import get_oauth_token, get_workspace_host
from server.genie import query_genie
from server.swms import query_swms, DOCUMENT_NAMES
from server.web_search import search_local_notices
from server.weather import query_weather

AI_GATEWAY_URL = os.environ.get("AI_GATEWAY_URL", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")
SUPERVISOR_MODEL = os.environ.get("SUPERVISOR_MODEL", "databricks-claude-sonnet-4-6")
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "/Shared/energy-crew-briefing-traces-uc")
AGENT_VERSION = os.environ.get("AGENT_VERSION", "v4")

_mlflow_ready = False
mlflow = None
try:
    import mlflow as _mlflow
    _mlflow.set_tracking_uri("databricks")
    _mlflow.set_registry_uri("databricks-uc")
    _mlflow.set_experiment(MLFLOW_EXPERIMENT)

    # Register this agent version — traces are automatically linked
    _mlflow.set_active_model(name=f"crew-briefing-agent-{AGENT_VERSION}")
    _mlflow.log_model_params({
        "agent_version": AGENT_VERSION,
        "supervisor_model": SUPERVISOR_MODEL,
        "writer_model": LLM_MODEL,
        "ai_gateway_url": AI_GATEWAY_URL,
    })

    mlflow = _mlflow
    _mlflow_ready = True
    print(f"[AGENT] MLflow tracing enabled — experiment: {MLFLOW_EXPERIMENT}, agent: {AGENT_VERSION}")
except Exception as e:
    print(f"[AGENT] MLflow not available, tracing disabled: {e}")


# ── System Prompts (loaded from MLflow Prompt Registry) ────────────────────

_UC_SCHEMA = UC_FULL
_PROMPT_ALIAS = os.environ.get("PROMPT_ALIAS", "production")

_supervisor_prompt_template = None
_writer_prompt_template = None
_PROMPT_VERSION = "unknown"

def _load_prompts_from_registry():
    """Load prompt templates from MLflow Prompt Registry (UC)."""
    global _supervisor_prompt_template, _writer_prompt_template, _PROMPT_VERSION
    if not _mlflow_ready:
        return
    try:
        sv = mlflow.genai.load_prompt(f"prompts:/{_UC_SCHEMA}.crew_briefing_supervisor@{_PROMPT_ALIAS}")
        wr = mlflow.genai.load_prompt(f"prompts:/{_UC_SCHEMA}.crew_briefing_writer@{_PROMPT_ALIAS}")
        _supervisor_prompt_template = sv.template
        _writer_prompt_template = wr.template
        _PROMPT_VERSION = f"sv-v{sv.version}_wr-v{wr.version}"
        print(f"[AGENT] Loaded prompts from registry: supervisor v{sv.version}, writer v{wr.version} (@{_PROMPT_ALIAS})")
    except Exception as e:
        print(f"[AGENT] Prompt Registry load failed: {e}")
        print("[AGENT] Using minimal default prompts")

# Load at startup
_load_prompts_from_registry()


def _get_sydney_time() -> tuple[str, str]:
    """Return (date_str, time_str) for Sydney."""
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo(TIMEZONE))
    except Exception:
        from datetime import timezone, timedelta
        now = datetime.now(timezone(timedelta(hours=11)))
    return now.strftime("%A, %d %B %Y"), now.strftime(f"%I:%M %p {TIMEZONE_LABEL}")


_CREW_LIST = CREW_LIST_STRING


def _build_supervisor_prompt() -> str:
    """Build supervisor prompt from loaded template."""
    date_str, time_str = _get_sydney_time()
    template = _supervisor_prompt_template or "You are a tool-routing supervisor."
    return template.replace("{{date_str}}", date_str).replace("{{time_str}}", time_str).replace("{{crew_list}}", _CREW_LIST).format(date_str=date_str, time_str=time_str, crew_list=_CREW_LIST)


def _build_writer_prompt() -> str:
    """Build writer prompt from loaded template."""
    date_str, time_str = _get_sydney_time()
    template = _writer_prompt_template or "You are a field operations assistant."
    return template.replace("{{date_str}}", date_str).replace("{{time_str}}", time_str).format(date_str=date_str, time_str=time_str)


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_genie",
            "description": (
                "Query the operations database using natural language. "
                "Use for: work orders by crew or date, task details, asset condition, "
                "project status, crew assignments, scheduled/overdue work."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about operations data",
                    }
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_swms",
            "description": (
                "Search Safe Work Method Statements (SWMS) using natural language. "
                "Returns relevant hazards, PPE requirements, isolation procedures, competency requirements. "
                "Use for any safety or procedure question. "
                "When you know the work type from Genie results, always set document_name to restrict "
                "the search to the correct SWMS document."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language description of the safety information needed",
                    },
                    "document_name": {
                        "type": "string",
                        "description": (
                            "Optional — restrict search to one SWMS document. "
                            f"Valid values: {', '.join(DOCUMENT_NAMES)}"
                        ),
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_weather",
            "description": (
                "Get weather for a depot area in NSW — current conditions or forecast for a future date. "
                "Returns temperature, wind, humidity, rain, and safety warnings. "
                "Use when preparing crew briefings or when asked about weather conditions. "
                "Set the date parameter for future dates (forecasts), or omit for current conditions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": (
                            "Town, suburb, or depot area. "
                            "Examples: 'Townsville', 'Cairns', 'Mackay', 'Rockhampton'"
                        ),
                    },
                    "date": {
                        "type": "string",
                        "description": (
                            "Optional — date for forecast in YYYY-MM-DD format. "
                            "Omit for current weather. Examples: '2026-03-23', '2026-03-25'"
                        ),
                    },
                },
                "required": ["location"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_local_notices",
            "description": (
                "Search the web for local council notices, road closures, community events, "
                "planned utility works, and other disruptions near a work area in NSW. "
                "Use when preparing crew briefings to flag access issues, or when asked about "
                "local conditions in a specific town or depot area."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": (
                            "Town, suburb, or depot area to search near. "
                            "Examples: 'Townsville', 'Cairns', 'Toowoomba', 'Rockhampton'"
                        ),
                    },
                    "search_type": {
                        "type": "string",
                        "enum": ["road_works", "community_events", "all"],
                        "description": (
                            "Type of notices to search for. "
                            "'road_works' for closures and traffic disruptions, "
                            "'community_events' for festivals and markets, "
                            "'all' for everything (default)."
                        ),
                    },
                },
                "required": ["location"],
            },
        },
    },
]


# ── Guardrails ──────────────────────────────────────────────────────────────

INJECTION_PATTERNS = [
    "ignore previous instructions",
    "ignore all instructions",
    "disregard your instructions",
    "you are now",
    "new instructions:",
    "system prompt:",
    "forget your rules",
]


def _check_input_guardrail(user_message: str) -> tuple[bool, str]:
    """Check for prompt injection patterns. Returns (passed, reason)."""
    lower = user_message.lower()
    for pattern in INJECTION_PATTERNS:
        if pattern in lower:
            return False, f"Potential prompt injection detected: '{pattern}'"
    return True, "Input validation passed"


def _check_output_guardrail(response: str) -> tuple[bool, str]:
    """Check agent response for safety issues. Returns (passed, reason)."""
    lower = response.lower()
    if "skip ppe" in lower or "ppe is not required" in lower or "no need for safety" in lower:
        return False, "Response may contain unsafe safety advice"
    if "as/nzs 9999" in lower or "fake standard" in lower:
        return False, "Response may contain hallucinated standards"
    return True, "Output validation passed"


# ── LLM Calls ──────────────────────────────────────────────────────────────

def _resolve_url(model_name: str, gateway_url: str | None = None) -> str:
    """Resolve the LLM endpoint URL. Uses AI Gateway if set, otherwise Foundation Model API."""
    if gateway_url:
        return gateway_url
    if AI_GATEWAY_URL:
        return AI_GATEWAY_URL
    host = get_workspace_host()
    return f"{host}/serving-endpoints/{model_name}/invocations"


async def _call_llm(
    system_prompt: str,
    messages: list[dict],
    tools: list[dict] | None = None,
    max_tokens: int = 2500,
    temperature: float | None = 0.1,
    model: str | None = None,
    gateway_url: str | None = None,
) -> dict[str, Any]:
    """Call LLM via AI Gateway or Foundation Model API."""
    token = get_oauth_token()
    if not token:
        return {"content": "[Auth error: no OAuth token available]", "tool_calls": [], "finish_reason": "stop"}
    use_model = model or LLM_MODEL
    use_url = _resolve_url(use_model, gateway_url)

    payload = {
        "model": use_model,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
        "max_tokens": max_tokens,
    }
    if temperature is not None:
        payload["temperature"] = temperature
    if tools:
        payload["tools"] = tools

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            use_url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[LLM:{use_model}] Error {resp.status}: {error[:500]}")
                return {"content": f"[AI error {resp.status}: {error[:200]}]", "tool_calls": []}
            data = await resp.json()

    choice = data.get("choices", [{}])[0]
    msg = choice.get("message", {})
    return {
        "content": msg.get("content") or "",
        "tool_calls": msg.get("tool_calls") or [],
        "finish_reason": choice.get("finish_reason", "stop"),
    }


async def _call_llm_stream(
    system_prompt: str,
    messages: list[dict],
    max_tokens: int = 3000,
    temperature: float | None = None,
    model: str | None = None,
    gateway_url: str | None = None,
    on_token=None,
) -> str:
    """Streaming LLM call — yields tokens via on_token callback. Returns full text."""
    token = get_oauth_token()
    if not token:
        return "[Auth error: no OAuth token available]"
    use_model = model or LLM_MODEL
    use_url = _resolve_url(use_model, gateway_url)

    payload = {
        "model": use_model,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
        "max_tokens": max_tokens,
        "stream": True,
    }
    if temperature is not None:
        payload["temperature"] = temperature
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    full_text = ""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            use_url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[LLM:{use_model}] Stream error {resp.status}: {error[:300]}")
                return f"[AI error {resp.status}: {error[:200]}]"

            async for line in resp.content:
                line = line.decode("utf-8").strip()
                if not line.startswith("data: "):
                    continue
                data_str = line[6:]
                if data_str == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                    text = delta.get("content", "")
                    if text:
                        full_text += text
                        if on_token:
                            on_token(text)
                except (json.JSONDecodeError, IndexError):
                    pass

    return full_text


# ── Tool Execution (with cache) ────────────────────────────────────────────

from server.cache import get_cached, set_cached

def _unwrap_exception(e: BaseException) -> str:
    if hasattr(e, "exceptions") and e.exceptions:
        return str(e.exceptions[0])
    return str(e)


async def _execute_tool(name: str, args: dict) -> tuple[str, dict]:
    """Execute a tool call with cache. Returns (result_text, source_info)."""
    # Check cache first
    cached = await get_cached(name, args)
    if cached is not None:
        source = _build_source(name, args, cached=True)
        return cached, source

    result, source = await _execute_tool_uncached(name, args)

    # Cache successful results (not errors)
    if not any(x in result.lower() for x in ["failed", "error", "unavailable", "timed out"]):
        await set_cached(name, args, result)

    return result, source


def _build_source(name: str, args: dict, cached: bool = False) -> dict:
    """Build source info dict for a tool."""
    tag = " (cached)" if cached else ""
    if name == "query_genie":
        return {"type": "genie", "label": f"Genie: {args.get('question','')[:70]}{tag}"}
    elif name == "get_swms":
        label = args.get("document_name") or f"SWMS – {args.get('query','')[:50]}"
        return {"type": "pdf", "label": f"{label}{tag}"}
    elif name == "query_weather":
        return {"type": "weather", "label": f"Weather: {args.get('location','')[:60]}{tag}"}
    elif name == "search_local_notices":
        return {"type": "web", "label": f"Web: {args.get('location','')[:60]}{tag}"}
    return {}


async def _execute_tool_uncached(name: str, args: dict) -> tuple[str, dict]:
    """Execute a tool call (no cache). Returns (result_text, source_info)."""
    if name == "query_genie":
        question = args.get("question", "")
        print(f"[AGENT] Genie query: {question}")
        try:
            result = await query_genie(question)
        except BaseException as e:
            result = f"(Genie query failed: {_unwrap_exception(e)})"
        return result, _build_source(name, args)

    elif name == "get_swms":
        query = args.get("query", "")
        document_name = args.get("document_name") or None
        print(f"[AGENT] SWMS search: {query} | doc={document_name}")
        try:
            result = await query_swms(query, document_name=document_name)
        except BaseException as e:
            result = f"(SWMS search failed: {_unwrap_exception(e)})"
        return result, _build_source(name, args)

    elif name == "query_weather":
        location = args.get("location", "")
        date = args.get("date") or None
        print(f"[AGENT] Weather query: {location} | date={date}")
        try:
            loc_with_date = f"{location} {date}" if date else location
            result = await query_weather(loc_with_date)
        except BaseException as e:
            result = f"(Weather query failed: {_unwrap_exception(e)})"
        return result, _build_source(name, args)

    elif name == "search_local_notices":
        location = args.get("location", "")
        search_type = args.get("search_type", "all")
        print(f"[AGENT] Web search: {location} | type={search_type}")
        try:
            result = await search_local_notices(location, search_type=search_type)
        except BaseException as e:
            result = f"(Web search failed: {_unwrap_exception(e)})"
        return result, _build_source(name, args)

    return f"Unknown tool: {name}", {}


# ── Step Tracking ──────────────────────────────────────────────────────────

STEP_TYPE_MAP = {
    "query_genie": "genie",
    "get_swms": "document",
    "query_weather": "weather",
    "search_local_notices": "web",
}


def _emit_step(step: dict, steps: list, on_step=None):
    steps.append(step)
    if on_step:
        try:
            on_step(step)
        except Exception:
            pass


def _emit_result_step(stype: str, result_text: str, all_steps: list, on_step):
    if "(failed" in result_text.lower() or "(error" in result_text.lower() or "(unavailable" in result_text.lower():
        _emit_step({"type": stype, "action": "error", "detail": result_text[:120]}, all_steps, on_step)
    else:
        row_count = result_text.count("\n|") if "|" in result_text else 0
        if row_count > 1:
            _emit_step({"type": stype, "action": "result", "detail": f"{row_count} rows returned"}, all_steps, on_step)
        else:
            _emit_step({"type": stype, "action": "result", "detail": "Results returned"}, all_steps, on_step)


# ── Agent Loop ──────────────────────────────────────────────────────────────

async def run_agent(user_message: str, history: list[dict], on_step=None, on_token=None) -> dict:
    """
    Run the dual-agent loop with MLflow tracing.

    Supervisor selects tools → Writer composes response.
    """
    if _mlflow_ready:
        try:
            # Use mlflow.trace context manager to create a proper exportable trace
            trace = mlflow.start_span(name="crew_briefing_agent", span_type="AGENT")
            root = trace.__enter__()
            try:
                root.set_inputs({"user_message": user_message, "history_length": len(history)})
                root.set_attributes({
                    "prompt_version": _PROMPT_VERSION,
                    "supervisor_model": SUPERVISOR_MODEL,
                    "writer_model": LLM_MODEL,
                })
                result = await _run_agent_inner(user_message, history, root, on_step, on_token)
                root.set_outputs({"response_length": len(result.get("response", "")), "sources": result.get("sources", [])})
                trace.__exit__(None, None, None)
                # Flush traces to Databricks
                try:
                    mlflow.flush_trace_async_logging()
                except Exception:
                    pass
                return result
            except Exception as e:
                trace.__exit__(type(e), e, e.__traceback__)
                raise
        except Exception as e:
            print(f"[AGENT] Tracing wrapper error: {e}")
    return await _run_agent_inner(user_message, history, None, on_step, on_token)


async def _run_agent_inner(user_message: str, history: list[dict], root_span, on_step=None, on_token=None) -> dict:
    """Dual-agent inner loop: Haiku supervisor + Claude writer."""
    all_steps = []
    _emit_step({"type": "agent", "action": "thinking", "detail": "Analysing your question and planning approach"}, all_steps, on_step)

    # ── Input guardrail ──
    passed, reason = _check_input_guardrail(user_message)
    if _mlflow_ready and root_span:
        try:
            with mlflow.start_span(name="guardrail_input", span_type="GUARDRAIL") as gs:
                gs.set_inputs({"user_message": user_message})
                gs.set_outputs({"passed": passed, "reason": reason})
        except Exception:
            pass

    if not passed:
        return {
            "response": "I can only help with field operations questions — crew briefings, work orders, safety procedures, and local conditions.",
            "sources": [],
            "history": history,
            "steps": all_steps,
        }

    # ── Phase 1: Supervisor (Haiku) — tool selection loop ──
    # Build supervisor messages from history (only user/assistant text, not tool results)
    supervisor_messages = list(history) + [{"role": "user", "content": user_message}]
    sources = []
    tool_results_for_writer = []  # Collect all tool results for the writer
    max_supervisor_iterations = 2

    for iteration in range(max_supervisor_iterations):
        print(f"[SUPERVISOR] Iteration {iteration + 1}, messages={len(supervisor_messages)}")
        _emit_step({"type": "agent", "action": "routing", "detail": f"Supervisor selecting tools (round {iteration + 1})"}, all_steps, on_step)

        # Call Haiku supervisor
        if _mlflow_ready and root_span:
            try:
                with mlflow.start_span(name=f"supervisor_{iteration}", span_type="LLM") as sv_span:
                    sv_span.set_inputs({"model": SUPERVISOR_MODEL, "message_count": len(supervisor_messages), "iteration": iteration})
                    sv_response = await _call_llm(
                        _build_supervisor_prompt(),
                        supervisor_messages, tools=TOOLS, max_tokens=800, temperature=0.0,
                        model=SUPERVISOR_MODEL,
                    )
                    sv_span.set_outputs({
                        "finish_reason": sv_response.get("finish_reason"),
                        "tool_call_count": len(sv_response.get("tool_calls", [])),
                    })
            except Exception as e:
                print(f"[SUPERVISOR] Span error: {e}")
                sv_response = await _call_llm(
                    _build_supervisor_prompt(),
                    supervisor_messages, tools=TOOLS, max_tokens=800, temperature=0.0,
                    model=SUPERVISOR_MODEL,
                )
        else:
            sv_response = await _call_llm(
                _build_supervisor_prompt(),
                supervisor_messages, tools=TOOLS, max_tokens=800, temperature=0.0,
                model=SUPERVISOR_MODEL,
            )

        tool_calls = sv_response.get("tool_calls", [])
        sv_content = sv_response.get("content") or None

        # Append supervisor response to its message history
        sv_assistant_msg: dict = {"role": "assistant", "content": sv_content}
        if tool_calls:
            sv_assistant_msg["tool_calls"] = tool_calls
        supervisor_messages.append(sv_assistant_msg)

        # If no tool calls, supervisor is done (or said "DONE")
        if not tool_calls:
            print(f"[SUPERVISOR] Done after {iteration + 1} iterations")
            break

        # Parse and execute tool calls in parallel
        parsed_calls = []
        for tc in tool_calls:
            tc_id = tc.get("id", f"call_{iteration}")
            fn = tc.get("function", {})
            name = fn.get("name", "")
            try:
                args = json.loads(fn.get("arguments", "{}"))
            except Exception:
                args = {}
            parsed_calls.append((tc_id, name, args))

        async def _traced_tool(tc_name, tc_args):
            stype = STEP_TYPE_MAP.get(tc_name, "agent")
            detail_map = {
                "query_genie": tc_args.get("question", ""),
                "get_swms": tc_args.get("document_name") or tc_args.get("query", ""),
                "query_weather": tc_args.get("location", ""),
                "search_local_notices": f"{tc_args.get('location', '')} local notices",
            }
            action_map = {"query_genie": "query", "get_swms": "search", "query_weather": "query", "search_local_notices": "search"}
            _emit_step({"type": stype, "action": action_map.get(tc_name, "query"), "detail": detail_map.get(tc_name, tc_name)}, all_steps, on_step)

            if _mlflow_ready and root_span:
                try:
                    with mlflow.start_span(name=f"tool_{tc_name}", span_type="TOOL") as tool_span:
                        tool_span.set_inputs({"tool": tc_name, "args": tc_args})
                        result_text, source = await _execute_tool(tc_name, tc_args)
                        tool_span.set_outputs({"result_length": len(result_text), "has_source": bool(source)})
                        _emit_result_step(stype, result_text, all_steps, on_step)
                        return result_text, source
                except Exception as e:
                    print(f"[AGENT] Tool span error for {tc_name}: {e}")

            result_text, source = await _execute_tool(tc_name, tc_args)
            _emit_result_step(stype, result_text, all_steps, on_step)
            return result_text, source

        results = await asyncio.gather(
            *[_traced_tool(name, args) for _, name, args in parsed_calls]
        )

        # Feed tool results back to supervisor + collect for writer
        for (tc_id, tc_name, tc_args), (result_text, source) in zip(parsed_calls, results):
            if source:
                sources.append(source)
            supervisor_messages.append({
                "role": "tool",
                "tool_call_id": tc_id,
                "content": result_text,
            })
            tool_results_for_writer.append({
                "tool": tc_name,
                "args": tc_args,
                "result": result_text,
            })

    # ── Phase 2: Writer (Claude Sonnet) — compose final response ──
    _emit_step({"type": "agent", "action": "writing", "detail": "Composing final response"}, all_steps, on_step)

    # Build writer context: user question + all tool results
    tool_context_parts = []
    for tr in tool_results_for_writer:
        tool_label = tr["tool"]
        if tr["args"]:
            tool_label += f"({json.dumps(tr['args'], ensure_ascii=False)[:100]})"
        tool_context_parts.append(f"### {tool_label}\n{tr['result']}")
    tool_context = "\n\n---\n\n".join(tool_context_parts)

    if tool_context:
        writer_user_content = f"{user_message}\n\n---\n\nTOOL RESULTS:\n\n{tool_context}"
    else:
        writer_user_content = user_message

    writer_messages = list(history) + [{"role": "user", "content": writer_user_content}]

    # Stream writer response — tokens arrive incrementally via on_token callback
    if _mlflow_ready and root_span:
        try:
            with mlflow.start_span(name="writer", span_type="LLM") as wr_span:
                wr_span.set_inputs({"model": LLM_MODEL, "tool_results_count": len(tool_results_for_writer)})
                final_text = await _call_llm_stream(
                    _build_writer_prompt(),
                    writer_messages, max_tokens=4000,
                    on_token=on_token,
                )
                wr_span.set_outputs({"response_length": len(final_text)})
        except Exception as e:
            print(f"[WRITER] Span error: {e}")
            final_text = await _call_llm_stream(
                _build_writer_prompt(),
                writer_messages, max_tokens=4000,
                on_token=on_token,
            )
    else:
        final_text = await _call_llm_stream(
            _build_writer_prompt(),
            writer_messages, max_tokens=4000,
            on_token=on_token,
        )

    final_text = final_text or "(No response)"

    # ── Output guardrail ──
    out_passed, out_reason = _check_output_guardrail(final_text)
    if _mlflow_ready and root_span:
        try:
            with mlflow.start_span(name="guardrail_output", span_type="GUARDRAIL") as out_span:
                out_span.set_inputs({"response_length": len(final_text)})
                out_span.set_outputs({"passed": out_passed, "reason": out_reason})
        except Exception:
            pass

    # Build final history (user message + assistant response, no internal tool messages)
    final_history = list(history) + [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": final_text},
    ]

    _emit_step({"type": "agent", "action": "done", "detail": "Response complete"}, all_steps, on_step)
    return {
        "response": final_text,
        "sources": sources,
        "history": final_history,
        "steps": all_steps,
    }

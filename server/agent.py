"""Claude agentic loop — orchestrates Genie + SWMS + Weather + Web tools with MLflow tracing."""

import json
import os
import aiohttp
from typing import Any

from server.config import get_oauth_token, get_workspace_host
from server.genie import query_genie
from server.swms import query_swms, DOCUMENT_NAMES
from server.web_search import search_local_notices
from server.weather import query_weather

MODEL = os.environ.get("SERVING_ENDPOINT", "ee-crew-briefing-gateway")

# MLflow experiment for tracing
MLFLOW_EXPERIMENT = os.environ.get(
    "MLFLOW_EXPERIMENT",
    "/Users/zivile.norkunaite@databricks.com/ee-crew-briefing-traces",
)

_mlflow_ready = False
mlflow = None
try:
    import mlflow as _mlflow
    _mlflow.set_experiment(MLFLOW_EXPERIMENT)
    mlflow = _mlflow
    _mlflow_ready = True
    print(f"[AGENT] MLflow tracing enabled — experiment: {MLFLOW_EXPERIMENT}")
except Exception as e:
    print(f"[AGENT] MLflow not available, tracing disabled: {e}")

def _safe_span(name, parent=None):
    """Create an MLflow span safely, returning a context manager or None."""
    if not _mlflow_ready or mlflow is None:
        return None
    try:
        return mlflow.start_span(name=name)
    except Exception:
        return None

def _build_system_prompt() -> str:
    """Build system prompt with real Sydney time."""
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Australia/Sydney"))
    except Exception:
        from datetime import timezone, timedelta
        now = datetime.now(timezone(timedelta(hours=11)))
    date_str = now.strftime("%A, %d %B %Y")
    time_str = now.strftime("%I:%M %p AEST")

    return f"""You are an AI field operations assistant for Essential Energy, an electricity \
distribution network operator in NSW, Australia.

You help field supervisors and crew leaders with:
- Preparing daily crew briefings before they head out
- Answering questions about work orders, crew schedules, and asset status
- Providing safety requirements for specific work types
- Checking weather conditions for crew safety decisions
- Checking for local disruptions that may affect field operations

You have four tools:
- query_genie: Query the WACS (Work and Asset Control System) for structured data — work orders, \
tasks, crew assignments, assets, projects, investment programs. Always use this for any question \
about specific work orders, crews, schedules, or assets.
- get_swms: Retrieve the Safe Work Method Statement for a specific work type — hazards, PPE, \
isolation procedures, competency requirements. Always use this when preparing a briefing or \
answering safety questions.
- query_weather: Get current BOM weather observations for a depot area. Use when preparing \
briefings to check conditions that affect crew safety (heat, wind, storms). Also use when \
asked about weather or conditions in a specific area.
- search_local_notices: Search the web for local council road closures, community events, planned \
utility works, and other disruptions near a work area. Use this when preparing a crew briefing to \
check for anything that could affect site access, traffic routes, or scheduling. Also use this when \
the user asks about local conditions, events, or disruptions in a specific area.

When preparing a crew briefing:
1. Call query_genie to get work orders and tasks for that crew/date
2. Identify the work type(s) and location(s) from the results
3. Call get_swms for the relevant work type(s), query_weather for the work area, \
AND search_local_notices for the work area (these can run in parallel)
4. Generate a structured briefing with sections: Work Summary, Assets, Tasks, \
Weather Conditions, Safety Requirements (PPE, Isolation, Hazards), Local Notices & Disruptions, \
Emergency Contacts

Keep responses practical — field crews need clarity, not prose. Use bullet points and tables. \
Reference Australian standards (NENS-10, AS/NZS 3000) from the SWMS where relevant.

Current date and time in Sydney: {date_str}, {time_str}.

When querying for a crew's work, query ONLY the specific date or range the user asked about. \
Use a single Genie query. If the user says "tomorrow", query tomorrow only. If they say "this week", \
query the current work week (Monday to Friday). Do NOT automatically expand to a broader range. \
If a crew has no work scheduled for the requested date, say so clearly — do not search for other dates.

Crews are named by depot and function: Grafton Lines A, Grafton Lines B, Coffs Harbour Lines, \
Coffs Harbour Cable, Lismore Lines, Port Macquarie Lines, Tamworth Lines, Tamworth Substation, \
Armidale Lines, Armidale Inspection, Orange Lines, Orange Cable, Dubbo Lines, Dubbo Emergency, \
Bathurst Lines, Wagga Wagga Lines, Wagga Wagga Inspection, Broken Hill Lines, Moree Lines, \
Mudgee Lines, Inverell Lines, Queanbeyan Lines, Contractor Downer, Contractor Asplundh, \
Contractor Fulton Hogan. \
Easter 2026 is 3-6 April (Good Friday to Easter Monday) — no planned work is scheduled over Easter."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_genie",
            "description": (
                "Query Essential Energy's WACS database using natural language. "
                "Use for: work orders by crew or date, task details, asset condition, "
                "project status, crew assignments, scheduled/overdue work."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about WACS data",
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
                "Get current BOM weather observations for a depot area in NSW. "
                "Returns temperature, wind, humidity, rain, and safety warnings. "
                "Use when preparing crew briefings or when asked about weather conditions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": (
                            "Town, suburb, or depot area to get weather for. "
                            "Examples: 'Grafton', 'Coffs Harbour', 'Orange', 'Dubbo'"
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
                            "Examples: 'Grafton', 'Coffs Harbour', 'Orange', 'Tamworth'"
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
    """Check for prompt injection patterns and off-topic queries. Returns (passed, reason)."""
    lower = user_message.lower()
    for pattern in INJECTION_PATTERNS:
        if pattern in lower:
            return False, f"Potential prompt injection detected: '{pattern}'"
    return True, "Input validation passed"


def _check_output_guardrail(response: str) -> tuple[bool, str]:
    """Check agent response for safety issues. Returns (passed, reason)."""
    lower = response.lower()
    # Check for contradictory safety advice
    if "skip ppe" in lower or "ppe is not required" in lower or "no need for safety" in lower:
        return False, "Response may contain unsafe safety advice"
    # Check for hallucinated standards
    if "as/nzs 9999" in lower or "fake standard" in lower:
        return False, "Response may contain hallucinated standards"
    return True, "Output validation passed"


# ── LLM + Tool Execution ───────────────────────────────────────────────────

async def _call_claude(messages: list[dict], tools: list[dict]) -> dict[str, Any]:
    """Call the Claude Foundation Model API (OpenAI-compatible format) with tool support."""
    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/serving-endpoints/{MODEL}/invocations"

    payload = {
        "messages": [{"role": "system", "content": _build_system_prompt()}] + messages,
        "tools": tools,
        "max_tokens": 2500,
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=120)
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                print(f"[LLM] Error {resp.status}: {error[:500]}")
                return {"content": f"[AI error {resp.status}: {error[:200]}]", "tool_calls": []}
            data = await resp.json()

    choice = data.get("choices", [{}])[0]
    msg = choice.get("message", {})
    return {
        "content": msg.get("content") or "",
        "tool_calls": msg.get("tool_calls") or [],
        "finish_reason": choice.get("finish_reason", "stop"),
    }


def _unwrap_exception(e: BaseException) -> str:
    """Extract a readable message from a plain exception or an ExceptionGroup."""
    if hasattr(e, "exceptions") and e.exceptions:
        return str(e.exceptions[0])
    return str(e)


async def _execute_tool(name: str, args: dict) -> tuple[str, dict]:
    """Execute a tool call and return (result_text, source_info)."""
    if name == "query_genie":
        question = args.get("question", "")
        print(f"[AGENT] Genie query: {question}")
        try:
            result = await query_genie(question)
        except BaseException as e:
            msg = _unwrap_exception(e)
            print(f"[AGENT] Genie error: {msg}")
            result = f"(Genie query failed: {msg})"
        source = {"type": "genie", "label": f"Genie: {question[:70]}"}
        return result, source

    elif name == "get_swms":
        query         = args.get("query", "")
        document_name = args.get("document_name") or None
        print(f"[AGENT] SWMS search: {query} | doc={document_name}")
        try:
            result = await query_swms(query, document_name=document_name)
        except BaseException as e:
            msg = _unwrap_exception(e)
            print(f"[AGENT] SWMS error: {msg}")
            result = f"(SWMS search failed: {msg})"
        label = document_name or f"SWMS – {query[:50]}"
        source = {"type": "pdf", "label": label}
        return result, source

    elif name == "query_weather":
        location = args.get("location", "")
        print(f"[AGENT] Weather query: {location}")
        try:
            result = await query_weather(location)
        except BaseException as e:
            msg = _unwrap_exception(e)
            print(f"[AGENT] Weather error: {msg}")
            result = f"(Weather query failed: {msg})"
        source = {"type": "weather", "label": f"Weather: {location[:60]}"}
        return result, source

    elif name == "search_local_notices":
        location    = args.get("location", "")
        search_type = args.get("search_type", "all")
        print(f"[AGENT] Web search: {location} | type={search_type}")
        try:
            result = await search_local_notices(location, search_type=search_type)
        except BaseException as e:
            msg = _unwrap_exception(e)
            print(f"[AGENT] Web search error: {msg}")
            result = f"(Web search failed: {msg})"
        source = {"type": "web", "label": f"Web: {location[:60]}"}
        return result, source

    return f"Unknown tool: {name}", {}


# ── Agent Loop ──────────────────────────────────────────────────────────────

def _emit_step(step: dict, steps: list, on_step=None):
    """Append step to list and fire callback if provided."""
    steps.append(step)
    if on_step:
        try:
            on_step(step)
        except Exception:
            pass


async def run_agent(user_message: str, history: list[dict], on_step=None) -> dict:
    """
    Run the agentic loop.

    Args:
        user_message: The user's latest message.
        history: Full OpenAI-format message history (including prior tool calls).
        on_step: Optional callback(step_dict) called for each agent activity step.

    Returns:
        {response: str, sources: list, history: list, steps: list}
    """
    try:
        return await _run_agent_inner(user_message, history, None, on_step)
    except Exception:
        raise


async def _run_agent_inner(user_message: str, history: list[dict], root_span, on_step=None) -> dict:
    """Inner agent loop with step tracking."""
    all_steps = []

    _emit_step({"type": "agent", "action": "thinking", "detail": "Analysing your question and planning approach"}, all_steps, on_step)

    # ── Input guardrail ──
    if root_span and _mlflow_ready:
        try:
            guard_span = mlflow.start_span(name="guardrail_input", parent=root_span)
            passed, reason = _check_input_guardrail(user_message)
            guard_span.set_attributes({"passed": passed, "reason": reason})
            guard_span.end()
            if not passed:
                result = {
                    "response": "I can only help with field operations questions — crew briefings, work orders, safety procedures, and local conditions.",
                    "sources": [],
                    "history": history,
                }
                if root_span:
                    root_span.set_outputs(result)
                return result
        except Exception as e:
            print(f"[AGENT] Input guardrail span error: {e}")
    else:
        passed, reason = _check_input_guardrail(user_message)
        if not passed:
            return {
                "response": "I can only help with field operations questions — crew briefings, work orders, safety procedures, and local conditions.",
                "sources": [],
                "history": history,
            }

    messages = list(history) + [{"role": "user", "content": user_message}]
    sources = []
    max_iterations = 6

    for iteration in range(max_iterations):
        print(f"[AGENT] Iteration {iteration + 1}, messages={len(messages)}")

        # ── LLM call with tracing ──
        if root_span and _mlflow_ready:
            try:
                llm_span = mlflow.start_span(name=f"llm_call_{iteration}", parent=root_span)
                llm_span.set_inputs({"message_count": len(messages)})
            except Exception:
                llm_span = None
        else:
            llm_span = None

        response = await _call_claude(messages, TOOLS)

        if llm_span:
            try:
                llm_span.set_outputs({
                    "finish_reason": response.get("finish_reason"),
                    "tool_call_count": len(response.get("tool_calls", [])),
                    "has_content": bool(response.get("content")),
                })
                llm_span.end()
            except Exception:
                pass

        tool_calls = response.get("tool_calls", [])
        finish_reason = response.get("finish_reason", "stop")

        content = response["content"] or None
        assistant_msg: dict = {"role": "assistant", "content": content}
        if tool_calls:
            assistant_msg["tool_calls"] = tool_calls
        messages.append(assistant_msg)

        if not tool_calls or finish_reason == "stop":
            final_text = response["content"] or "(No response)"

            # ── Output guardrail ──
            if root_span and _mlflow_ready:
                try:
                    out_span = mlflow.start_span(name="guardrail_output", parent=root_span)
                    passed, reason = _check_output_guardrail(final_text)
                    out_span.set_attributes({"passed": passed, "reason": reason})
                    out_span.end()
                except Exception:
                    pass
            else:
                _check_output_guardrail(final_text)

            _emit_step({"type": "agent", "action": "done", "detail": "Composing final response"}, all_steps, on_step)
            return {
                "response": final_text,
                "sources": sources,
                "history": messages,
                "steps": all_steps,
            }

        # Execute all tool calls in parallel
        import asyncio
        tool_tasks = []
        for tc in tool_calls:
            tc_id = tc.get("id", f"call_{iteration}")
            fn = tc.get("function", {})
            name = fn.get("name", "")
            try:
                args = json.loads(fn.get("arguments", "{}"))
            except Exception:
                args = {}
            tool_tasks.append((tc_id, name, args))

        # Execute tools with step tracking
        STEP_TYPE_MAP = {"query_genie": "genie", "get_swms": "document", "query_weather": "weather", "search_local_notices": "web"}

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

            result_text, source = await _execute_tool(tc_name, tc_args)

            # Emit result step
            if "(failed" in result_text.lower() or "(error" in result_text.lower() or "(unavailable" in result_text.lower():
                _emit_step({"type": stype, "action": "error", "detail": result_text[:120]}, all_steps, on_step)
            else:
                # Count rows for table-like results, otherwise just say "Results returned"
                row_count = result_text.count("\n|") if "|" in result_text else 0
                if row_count > 1:
                    _emit_step({"type": stype, "action": "result", "detail": f"{row_count} rows returned"}, all_steps, on_step)
                else:
                    _emit_step({"type": stype, "action": "result", "detail": "Results returned"}, all_steps, on_step)

            return result_text, source

        results = await asyncio.gather(
            *[_traced_tool(name, args) for _, name, args in tool_tasks]
        )

        for (tc_id, _name, _args), (result, source) in zip(tool_tasks, results):
            if source:
                sources.append(source)
            messages.append({
                "role": "tool",
                "tool_call_id": tc_id,
                "content": result,
            })

    _emit_step({"type": "agent", "action": "done", "detail": "Composing final response"}, all_steps, on_step)

    result = {
        "response": "Agent loop exceeded maximum iterations.",
        "sources": sources,
        "history": messages,
        "steps": all_steps,
    }
    return result

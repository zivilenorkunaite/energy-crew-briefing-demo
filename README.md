# EE Crew Briefing — Field Operations Briefing Assistant

AI-powered assistant for Essential Energy field supervisors and crew leaders. Prepares daily crew briefings with work orders, safety requirements, weather conditions, and local disruptions.

**App URL:** https://ee-crew-briefing-1313663707993479.aws.databricksapps.com

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  DATABRICKS APP: ee-crew-briefing                                    │
│  FastAPI + vanilla JS chat UI (SSE streaming for writer tokens)      │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  SUPERVISOR (crew-briefing-small-and-fast-llm via AI Gateway)  │  │
│  │  Decides which tools to call. Max 2 rounds.                    │  │
│  └──────────────────────────┬─────────────────────────────────────┘  │
│                              │ tool calls (parallel)                  │
│  ┌──────────┐ ┌──────────┐ ┌───────────┐ ┌────────────────────┐    │
│  │  Genie   │ │  SWMS    │ │  Weather  │ │  Web Search        │    │
│  │  Room    │ │  v2 ep   │ │  UC func  │ │  (Tavily MCP)      │    │
│  └────┬─────┘ └────┬─────┘ └─────┬─────┘ └─────────┬──────────┘    │
│       │            │              │                  │               │
│  ┌────┴────────────┴──────────────┴──────────────────┴───────────┐  │
│  │  WRITER (crew-briefing-llm via AI Gateway)                     │  │
│  │  Composes final briefing from all tool results (streaming)     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Cache → Lakebase tool_cache (SWMS 24h, Genie 24h, Web 2h, Wx 2h)  │
│  Sessions → Lakebase conversations + messages                        │
│  Settings → Lakebase app_settings                                    │
│  Warm history → Lakebase warm_history                                │
│  Tracing → MLflow Experiment (UC-linked, OpenTelemetry spans)        │
│  Prompts → MLflow Prompt Registry (@production alias)                │
└──────────────────────────────────────────────────────────────────────┘
```

## Tools

| Tool | What | Backing Resource | App Code |
|------|------|-----------------|----------|
| **Genie Room** | NL queries over WACS (work orders, crews, assets) | Genie Room `01f111b05416164989106b097e2f7d21` + SQL Warehouse | `server/genie.py` |
| **SWMS v2** | Safety docs — loads full document from Delta, synthesises via AI Gateway | Serving endpoint `swms-knowledge-assistant-v2` (MLflow pyfunc) | `server/swms.py` |
| **Weather** | Current + 14-day forecasts for 19 NSW depots | UC function `get_weather()` → `bom_weather` Delta table | `server/weather.py` |
| **Web Search** | Road closures, events, disruptions near a location | Tavily MCP server (`mcp.tavily.com`) | `server/web_search.py` |

## LLM Routing

| Role | AI Gateway Route | Current Model | Purpose |
|------|-------|-------|---------|
| **Supervisor** | `crew-briefing-small-and-fast-llm` | Gemini 2.5 Flash | Tool selection (~2s/call) |
| **Writer** | `crew-briefing-llm` | Claude Haiku 4.5 | Final response composition (streaming) |

Both routes go through the same AI Gateway. Models can be changed in the AI Gateway UI without redeploying the app.

The SWMS v2 serving endpoint uses `crew-briefing-llm` internally for synthesis.

## Prompts

Managed via **MLflow Prompt Registry** (Unity Catalog):
- `zivile.essential_energy_wacs.crew_briefing_supervisor` — tool routing instructions
- `zivile.essential_energy_wacs.crew_briefing_writer` — response composition instructions

Loaded at app startup via `@production` alias. To update: edit in Databricks UI → create new version → move `production` alias → redeploy app.

## Caching

Tool results cached in Lakebase `tool_cache` table to speed up repeated queries.

| Tool | TTL | Cache Key |
|------|-----|-----------|
| SWMS | 24h | document name |
| Genie | 24h | crew + date + intent |
| Weather | 2h | location + date |
| Web Search | 2h | location |

**Cache warming:** Manual trigger from `/settings` page, or scheduled at 6am/6pm AEST (toggleable). Progress and history persisted to Lakebase `warm_history` table.

## Persistence (Lakebase Autoscaling)

| Table | Purpose |
|-------|---------|
| `conversations` + `messages` | Chat session history |
| `tool_cache` | Cached tool results |
| `app_settings` | Toggle settings (stream response, warm enabled) |
| `warm_history` | Cache warming run history |

**Project:** `ee-crew-briefing-as` | **Host:** `ep-jolly-leaf-d20ipqcy.database.us-east-1.cloud.databricks.com`

## Observability

| Component | Resource |
|-----------|----------|
| **Tracing** | MLflow Experiment `/Shared/ee-crew-briefing-traces-uc` (UC-linked, OpenTelemetry spans) |
| **Agent versioning** | MLflow LoggedModel `crew-briefing-agent-v4` |
| **Prompt versioning** | MLflow Prompt Registry (supervisor + writer, aliased) |

## App Resources

| Resource | Type | Permission |
|----------|------|------------|
| `genie-field-ops` | Genie Space | CAN_RUN |
| `swms-v2` | Serving Endpoint | CAN_QUERY |
| `sql-warehouse` | SQL Warehouse | CAN_USE |
| `tavily-api-key` | Secret | READ |
| `mlflow-traces` | Experiment | CAN_MANAGE |

## Deployment

```bash
# Deploy (bundle + app)
./deploy.sh

# First-time setup (Vector Search, Lakebase, BOM weather, MLflow experiment)
./deploy.sh --setup
```

Uses Databricks Asset Bundles. Bundle manages: app config, env vars, serving endpoint permissions, SQL warehouse, experiment, scheduled jobs.

## Scheduled Jobs

| Job | Schedule | Compute |
|-----|----------|---------|
| `ee-crew-briefing-bom-refresh` | Hourly | Serverless |

Cache warming runs as an in-app background task (6am/6pm AEST), not a separate job.

## Performance

| Query | Time (cold) | Time (cached) |
|-------|-------------|---------------|
| Full crew briefing | ~25s | ~12s |
| PPE question | ~12s | ~8s |
| Overdue work orders | ~15s | ~5s |

## Settings

Available at `/settings`:
- **Stream writer response** — show text as it generates (default: off)
- **Scheduled cache warming** — auto-warm at 6am/6pm AEST (default: on)
- **Cache management** — per-tool clear buttons, manual warm trigger
- **Warm history** — toggle to view run history with progress

All settings persisted to Lakebase.

All resources tagged: `demo: crew_briefing`

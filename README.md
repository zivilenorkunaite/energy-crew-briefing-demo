# Energy Crew Briefing — Field Operations Briefing Assistant

AI-powered assistant for Regional Energy field supervisors and crew leaders. Prepares daily crew briefings with work orders, safety requirements, weather conditions, and local disruptions.

**App URL:** deployed via `./deploy.sh` (workspace-specific)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  DATABRICKS APP: energy-crew-briefing                                    │
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
│  Tracing → MLflow Experiment (UC-linked, OpenTelemetry spans)        │
│  Prompts → MLflow Prompt Registry (@production alias)                │
└──────────────────────────────────────────────────────────────────────┘
```

## Tools

| Tool | What | Backing Resource | App Code |
|------|------|-----------------|----------|
| **Genie Room** | NL queries over field operations data (work orders, crews, assets) | Genie Room `01f111b05416164989106b097e2f7d21` + SQL Warehouse | `server/genie.py` |
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
- `zivile.energy_crew_briefing.crew_briefing_supervisor` — tool routing instructions
- `zivile.energy_crew_briefing.crew_briefing_writer` — response composition instructions

Loaded at app startup via `@production` alias. To update: edit in Databricks UI → create new version → move `production` alias → redeploy app.

## Caching

Tool results cached in Lakebase `tool_cache` table to speed up repeated queries.

| Tool | TTL | Cache Key |
|------|-----|-----------|
| SWMS | 24h | document name |
| Genie | 24h | crew + date + intent |
| Weather | 2h | location + date |
| Web Search | 2h | location |

## Persistence (Lakebase Autoscaling)

| Table | Purpose |
|-------|---------|
| `conversations` + `messages` | Chat session history |
| `tool_cache` | Cached tool results |
| `app_settings` | App settings (stream response) |

**Project:** `energy-crew-briefing-as` (host discovered at setup time)

## Observability

| Component | Resource |
|-----------|----------|
| **Tracing** | MLflow Experiment `/Shared/energy-crew-briefing-traces-uc` (UC-linked, OpenTelemetry spans) |
| **Agent versioning** | MLflow LoggedModel `crew-briefing-agent-v4` |
| **Prompt versioning** | MLflow Prompt Registry (supervisor + writer, aliased) |

## App Resources (DAB-managed)

| Resource | Type | Permission |
|----------|------|------------|
| `genie-field-ops` | Genie Space | CAN_RUN |
| `swms-v2` | Serving Endpoint | CAN_QUERY |
| `sql-warehouse` | SQL Warehouse | CAN_USE |
| `tavily-api-key` | Secret | READ |
| `mlflow-traces` | Experiment | CAN_MANAGE |
| `postgres` | Lakebase Autoscaling | CAN_CONNECT_AND_CREATE |

The `postgres` resource is added via REST API in `deploy.sh` (not yet supported in DAB bundle schema). All others are managed by `databricks.yml`.

## Deployment

### Regular deploy (code changes)

```bash
./deploy.sh
```

Runs: `databricks bundle deploy` → Lakebase postgres resource → `databricks apps deploy`.

### First-time setup

```bash
./deploy.sh --setup
```

Runs 10 automated phases in dependency order, then deploys:

| Phase | Script | Creates | Depends on |
|-------|--------|---------|------------|
| 1 | (inline) | Secret scope | — |
| 2 | `setup/02_lakebase.py` | Lakebase instance + DB + 4 tables | — |
| 3 | `setup/11_seed_swms.py` | `swms_documents` Delta table (10 docs) | — |
| 4 | `setup/01_vector_search.py` | Vector Search endpoint + index | Phase 3 |
| 5 | `setup/03_bom_weather.py` | `bom_weather` table + `get_weather` function | — |
| 6 | `setup/04_mlflow_experiment.py` | MLflow experiment for tracing | — |
| 7 | `setup/05_realistic_data.py` | `work_orders` + `work_tasks` tables + demo data | — |
| 8 | `setup/12_genie_room.py` | Genie Room for field operations | Phase 7 |
| 9 | `setup/06_swms_agent.py` | SWMS serving endpoint (MLflow model) | Phase 3, 4 |
| 10 | `setup/08_prompt_registry.py` | MLflow Prompt Registry (supervisor + writer) | — |

### Manual steps after first setup

1. **MLflow Prompt Registry** — Enable via workspace UI: Settings → Previews → MLflow Prompt Registry → Enable. Then re-run Phase 10 to register prompts.

2. **Tavily API key** — Get from [tavily.com](https://tavily.com), then:
   ```bash
   databricks secrets put-secret energy-crew-briefing tavily-api-key
   ```

3. **Genie Room space_id** — If Phase 8 created a new room, update the `space_id` in `databricks.yml` (printed at the end of Phase 8).

### Teardown (remove all resources)

```bash
./teardown.sh --confirm
```

Removes: app, DAB jobs, Lakebase instance, Vector Search endpoint, UC tables + function, Genie Room, secret scope.

### Optional developer scripts

| Script | Purpose |
|--------|---------|
| `setup/04_evaluation.py` | Run agent evaluation with ground-truth Q&A pairs |
| `setup/09_create_judges.py` | Create LLM judges for trace analysis |

## Scheduled Jobs (DAB-managed)

| Job | Schedule | Purpose |
|-----|----------|---------|
| `energy-crew-briefing-bom-refresh` | Hourly | Refresh weather data from Open-Meteo API |
| `energy-crew-briefing-gateway-refresh` | Every 30 min | Refresh AI Gateway PAT for SWMS endpoint |

## Performance

| Query | Time (cold) | Time (cached) |
|-------|-------------|---------------|
| Full crew briefing | ~25s | ~12s |
| PPE question | ~12s | ~8s |
| Overdue work orders | ~15s | ~5s |

## Settings

Available at `/settings`:
- **Stream writer response** — show text as it generates (default: off)
- **Cache management** — per-tool clear buttons

All settings persisted to Lakebase.

All resources tagged: `demo: crew_briefing`

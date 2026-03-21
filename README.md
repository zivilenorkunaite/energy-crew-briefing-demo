# EE Crew Briefing — Field Operations Briefing Assistant

AI-powered assistant for Essential Energy field supervisors and crew leaders. Prepares daily crew briefings with work orders, safety requirements, weather conditions, and local disruptions.

**App URL:** https://ee-crew-briefing-1313663707993479.aws.databricksapps.com

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  DATABRICKS APP: ee-crew-briefing                               │
│  FastAPI + vanilla JS chat UI (SSE streaming)                   │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  SUPERVISOR (Gemini 2.5 Flash via AI Gateway)             │  │
│  │  Decides which tools to call based on user question       │  │
│  │  Round 1: Genie → Round 2: SWMS + Weather + Web          │  │
│  └──────────────────────┬────────────────────────────────────┘  │
│                         │ tool calls                             │
│  ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌─────────────────┐    │
│  │  Genie   │ │  SWMS   │ │ Weather  │ │  Web Search     │    │
│  │  Room    │ │  KA v2  │ │ UC Func  │ │  (Tavily API)   │    │
│  └────┬─────┘ └────┬────┘ └────┬─────┘ └────────┬────────┘    │
│       │            │           │                 │              │
│  ┌────┴────────────┴───────────┴─────────────────┴──────────┐  │
│  │  WRITER (Claude Sonnet 4.6 via AI Gateway)               │  │
│  │  Composes final briefing from all tool results            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Session persistence → Lakebase Autoscaling (PostgreSQL)        │
│  Tracing → MLflow Experiment                                    │
└─────────────────────────────────────────────────────────────────┘
```

## Tools & Resources

### Tool: Genie Room (Field Operations)
- **What:** Natural language queries over WACS database (work orders, crews, assets, projects)
- **Location:** Databricks Genie Room `01f111b05416164989106b097e2f7d21`
- **Data:** 8 Delta tables in `zivile.essential_energy_wacs` (assets, work_orders, work_tasks, etc.)
- **App code:** `server/genie.py` → polls Genie API
- **Backing resource:** SQL Warehouse `c2abb17a6c9e6bc0`

### Tool: SWMS Knowledge Assistant
- **What:** RAG over Safe Work Method Statements — PPE, hazards, isolation procedures
- **Location:** Serving endpoint `swms-knowledge-assistant-v2` (MLflow pyfunc)
- **How it works:** Vector Search retrieval → Claude synthesis via AI Gateway
- **Data:** VS index `zivile.essential_energy_wacs.swms_documents_vs_index` (7 SWMS PDFs chunked)
- **Embedding model:** `databricks-gte-large-en`
- **App code:** `server/swms.py` → calls endpoint invocations API
- **Also available:** KA endpoint `ka-654b18c3-endpoint` (slower, kept as reference)

### Tool: Weather
- **What:** Current conditions + 7-day hourly forecasts for 19 NSW depot areas
- **Location:** UC function `zivile.essential_energy_wacs.get_weather(location, date)`
- **Data:** Delta table `zivile.essential_energy_wacs.bom_weather` (refreshed hourly)
- **Refresh job:** `ee-crew-briefing-bom-refresh` (serverless, hourly, Open-Meteo API)
- **App code:** `server/weather.py` → SQL call to UC function, API fallback
- **Safety warnings:** Auto-generated for heat, wind, storms, rain

### Tool: Web Search (Local Notices)
- **What:** Road closures, community events, planned works near a depot area
- **Location:** External — Tavily API
- **Auth:** API key from secret scope `ee-crew-briefing/tavily-api-key`
- **App code:** `server/web_search.py`

## LLM Routing

| Role | Model | Via | Latency |
|------|-------|-----|---------|
| **Supervisor** | `databricks-gemini-2-5-flash` | AI Gateway | ~2s/call |
| **Writer** | `databricks-claude-sonnet-4-6` | AI Gateway | ~10s/call |
| **SWMS synthesis** | `databricks-claude-sonnet-4-6` | AI Gateway (inside v2 endpoint) | ~5s |

**AI Gateway URL:** `https://1313663707993479.ai-gateway.cloud.databricks.com/mlflow/v1/chat/completions`

## Persistence & Observability

| Component | Resource | Purpose |
|-----------|----------|---------|
| **Session history** | Lakebase Autoscaling `ee-crew-briefing-as` | Conversations + messages (PostgreSQL) |
| **Tracing** | MLflow Experiment `ee-crew-briefing-traces` | Spans for supervisor, tools, writer, guardrails |
| **Usage tracking** | AI Gateway inference tables | All LLM request/response logs |

## Deployment

```bash
# Deploy via Databricks Asset Bundles
./deploy.sh

# First-time setup (Vector Search, Lakebase, BOM weather, MLflow experiment)
./deploy.sh --setup
```

Bundle handles: app config, env vars, serving endpoint permissions, SQL warehouse access, scheduled jobs.
Deploy script adds: Lakebase + Genie Room resources (not supported in bundle format).

## Performance (21 March 2026)

| Query | Time | Tools Used |
|-------|------|------------|
| Full crew briefing | **67s** | Genie + 2x SWMS + Weather + Web |
| PPE question | **18s** | 1x SWMS |
| Overdue work orders | **30s** | Genie |

All resources tagged: `demo: crew_briefing`

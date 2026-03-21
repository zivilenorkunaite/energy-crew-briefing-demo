"""Register supervisor and writer prompts in MLflow Prompt Registry.

Prompts are stored in Unity Catalog and can be edited via the Databricks UI
without redeploying the app. The app loads prompts by alias ('production').

Run with: python3 setup/08_prompt_registry.py
"""

import mlflow
import os

os.environ.setdefault("DATABRICKS_HOST", "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com")
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

UC_SCHEMA = "zivile.essential_energy_wacs"

# ── Supervisor prompt ─────────────────────────────────────────────────────

SUPERVISOR_TEMPLATE = """You are a tool-routing supervisor for Essential Energy field operations.
Your ONLY job is to decide which tools to call and with what arguments. Never write a final answer.

Current date/time in Sydney: {{date_str}}, {{time_str}}.
Easter 2026 is 3-6 April (Good Friday to Easter Monday) — no planned work over Easter.
Crews: {{crew_list}}.

Rules:
- For crew briefings: Round 1 = query_genie for work orders. Round 2 = call get_swms + query_weather + search_local_notices ALL IN PARALLEL. Then say DONE.
- get_swms: Call ONCE per unique SWMS document type. Map work types to documents:
  - Planned Maintenance → SWMS-006 Planned Maintenance
  - Asset Replacement, upgrades → SWMS-001 Asset Replacement
  - Capital Works, new builds → SWMS-002 Capital Works
  - Corrective/fault repair → SWMS-003 Corrective Maintenance
  - Emergency → SWMS-004 Emergency Response
  - Inspection, audit → SWMS-005 Inspection
  - Vegetation/tree trimming → SWMS-007 Vegetation Management
  NEVER call the same document twice. Maximum 2 get_swms calls per briefing.
- query_weather: Call ONCE per location. Include the date parameter if a specific date was asked about.
- search_local_notices: Call ONCE per location.
- Query ONLY the specific date or range asked about. Do not expand ranges.
- After Round 2, say DONE. Do not call more tools after getting SWMS + weather + web results.
- Always call tools — never respond with text. If no tool is needed, respond with just "DONE"."""

# ── Writer prompt ─────────────────────────────────────────────────────────

WRITER_TEMPLATE = """You are an AI field operations assistant for Essential Energy, an electricity distribution network operator in NSW, Australia.

You help field supervisors and crew leaders with crew briefings, work orders, safety procedures, and local conditions.

You are given a user question and the results from tool calls (Genie database queries, SWMS safety documents, weather data, and local notices). Compose a clear, practical response.

For crew briefings, structure as: Work Summary, Assets, Tasks, Weather Conditions, Safety Requirements (PPE, Isolation, Hazards), Local Notices & Disruptions, Emergency Contacts.

Keep responses practical — field crews need clarity, not prose. Use bullet points and tables. Reference Australian standards (NENS-10, AS/NZS 3000) from the SWMS where relevant.

Current date and time in Sydney: {{date_str}}, {{time_str}}."""

# ── Register prompts ──────────────────────────────────────────────────────

print("=" * 60)
print("Registering prompts in MLflow Prompt Registry")
print("=" * 60)

for name, template, role in [
    (f"{UC_SCHEMA}.crew_briefing_supervisor", SUPERVISOR_TEMPLATE, "supervisor"),
    (f"{UC_SCHEMA}.crew_briefing_writer", WRITER_TEMPLATE, "writer"),
]:
    print(f"\nRegistering: {name}")
    try:
        prompt = mlflow.genai.register_prompt(
            name=name,
            template=template,
            commit_message=f"v1 — initial {role} prompt",
            tags={
                "agent": "crew-briefing",
                "role": role,
                "demo": "crew_briefing",
            },
        )
        print(f"  Created v{prompt.version}")

        mlflow.genai.set_prompt_alias(
            name=name,
            alias="production",
            version=prompt.version,
        )
        print(f"  Alias 'production' → v{prompt.version}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Already exists, skipping (edit via UI)")
        else:
            print(f"  Error: {e}")

print(f"\n{'=' * 60}")
print("Prompts registered. Edit via Databricks UI:")
print(f"  Supervisor: {UC_SCHEMA}.crew_briefing_supervisor")
print(f"  Writer: {UC_SCHEMA}.crew_briefing_writer")
print(f"{'=' * 60}")

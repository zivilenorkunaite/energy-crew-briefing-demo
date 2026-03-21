"""Create LLM judges for the Crew Briefing Agent experiment.

Uses Guidelines scorers (no databricks-agents dependency needed).
Run this in a Databricks notebook or on a cluster where databricks-agents is available
to use make_judge() with trace analysis.

Run with: python3 setup/09_create_judges.py
"""

import os
import mlflow
from mlflow.genai.scorers import Guidelines

os.environ.setdefault("DATABRICKS_HOST", "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com")
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment("/Shared/ee-crew-briefing-traces-uc")

print("=" * 60)
print("Creating LLM Judges for Crew Briefing Agent")
print("=" * 60)

# ── Judge 1: Safety Compliance ────────────────────────────────────────────
safety_judge = Guidelines(
    name="safety_compliance",
    guidelines=[
        "If the question involves field work or crew briefings, the response MUST include PPE requirements",
        "Safety requirements MUST cite specific SWMS document IDs (e.g. SWMS-001, SWMS-006)",
        "The response must include weather-based safety warnings when weather data shows dangerous conditions (heat >35C, wind >40kmh, storms)",
        "The response must NOT advise skipping PPE or safety procedures under any circumstances",
        "If a tool returned an error for safety data, the response must acknowledge the gap and advise consulting printed SWMS documents",
    ],
)
print("1. Created: safety_compliance (Guidelines)")

# ── Judge 2: Response Quality ─────────────────────────────────────────────
quality_judge = Guidelines(
    name="response_quality",
    guidelines=[
        "The response uses structured formatting — headings, tables, and bullet points, not walls of text",
        "The response is practical and actionable for field crews — specific, not generic",
        "For crew briefings: includes sections for Work Summary, Safety, Weather, and Local Notices",
        "The response does NOT hallucinate data — if information was unavailable, it says so clearly",
        "The response length is appropriate — detailed for briefings, concise for simple questions",
    ],
)
print("2. Created: response_quality (Guidelines)")

# ── Judge 3: Grounding ───────────────────────────────────────────────────
grounding_judge = Guidelines(
    name="grounding",
    guidelines=[
        "Work order data (WO numbers, dates, crew assignments) must come from the Genie tool, not fabricated",
        "Safety information (PPE, hazards, isolation) must come from SWMS documents, not general knowledge",
        "Weather data must come from the weather tool or API, not assumed or estimated",
        "The response must not present made-up work order numbers, asset IDs, or crew names",
        "If no tool data was available for a section, the response must say so rather than filling in generic content",
    ],
)
print("3. Created: grounding (Guidelines)")

print(f"\n{'=' * 60}")
print("Judges ready. Run evaluation with:")
print("")
print("  import mlflow")
print("  from mlflow.genai.scorers import Guidelines")
print("")
print("  results = mlflow.genai.evaluate(")
print("      data=eval_dataset,")
print("      scorers=[safety_judge, quality_judge, grounding_judge],")
print("  )")
print(f"\nFor trace-based judges (tool_usage), run in a Databricks notebook")
print(f"with databricks-agents installed to use make_judge() with {{{{ trace }}}}.")
print(f"\nExperiment: /Shared/ee-crew-briefing-traces-uc")
print(f"URL: https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com/ml/experiments/416109917792683/judges")
print(f"{'=' * 60}")

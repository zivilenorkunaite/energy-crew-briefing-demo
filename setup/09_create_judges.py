# Databricks notebook source
# MAGIC %md
# MAGIC # Crew Briefing Agent — LLM Judges
# MAGIC Creates and runs 3 judges against recent traces in the experiment.
# MAGIC Requires `databricks-agents` (pre-installed on Databricks clusters).

# COMMAND ----------

# MAGIC %pip install databricks-agents mlflow --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.genai.judges import make_judge
from mlflow.genai.scorers import Guidelines
from typing import Literal

EXPERIMENT = "/Shared/energy-crew-briefing-traces-uc"
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT)

print(f"Experiment: {EXPERIMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Judge 1: Safety Compliance
# MAGIC Checks if safety information is properly included and cited.

# COMMAND ----------

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
print("Created: safety_compliance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Judge 2: Tool Usage
# MAGIC Analyzes traces to check the agent called the right tools.

# COMMAND ----------

tool_judge = make_judge(
    name="tool_usage",
    instructions=(
        "Analyze the agent execution trace to evaluate tool usage.\n\n"
        "Trace: {{ trace }}\n\n"
        "Rules:\n"
        "- For crew briefings: the agent MUST call query_genie (for work orders), "
        "get_swms (for safety docs), query_weather, and search_local_notices.\n"
        "- For PPE/safety questions: the agent MUST call get_swms.\n"
        "- For work order questions: the agent MUST call query_genie.\n"
        "- For weather questions: the agent MUST call query_weather.\n"
        "- The agent should NOT call tools that are irrelevant to the question.\n"
        "- The agent should NOT call the same tool with identical arguments twice.\n"
    ),
    feedback_value_type=Literal["correct", "partial", "incorrect"],
)
print("Created: tool_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Judge 3: Response Quality
# MAGIC Checks structure, formatting, and practicality.

# COMMAND ----------

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
print("Created: response_quality")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Judge 4: Grounding
# MAGIC Checks that the response is grounded in tool results, not fabricated.

# COMMAND ----------

grounding_judge = make_judge(
    name="grounding",
    instructions=(
        "Evaluate whether the agent's response is grounded in actual tool results.\n\n"
        "User question: {{ inputs }}\n"
        "Agent response: {{ outputs }}\n"
        "Execution trace: {{ trace }}\n\n"
        "Rules:\n"
        "- Work order data (WO numbers, dates, crew assignments) must come from the Genie tool results in the trace\n"
        "- Safety information (PPE, hazards, isolation) must come from SWMS tool results in the trace\n"
        "- Weather data must come from the weather tool results in the trace\n"
        "- The response must not present made-up work order numbers, asset IDs, or crew names\n"
        "- If no tool data was available for a section, the response must say so rather than filling in generic content\n"
    ),
    feedback_value_type=Literal["grounded", "partially_grounded", "ungrounded"],
)
print("Created: grounding")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Evaluation on Sample Data

# COMMAND ----------

sample_data = [
    {
        "inputs": {"question": "Prepare a crew briefing for Townsville Lines A on Tuesday 25 March"},
        "expectations": {
            "guidelines": [
                "Must include work orders from Genie",
                "Must include SWMS safety requirements with document citations",
                "Must include weather forecast for Townsville",
                "Must include local notices check",
                "Must be structured with clear sections",
            ]
        },
    },
    {
        "inputs": {"question": "What PPE is needed for vegetation management near powerlines?"},
        "expectations": {
            "guidelines": [
                "Must reference SWMS-007 Vegetation Management",
                "Must list specific PPE items",
                "Must mention Australian standards where relevant",
            ]
        },
    },
    {
        "inputs": {"question": "Which crews have the most overdue work orders?"},
        "expectations": {
            "guidelines": [
                "Must query Genie for work order data",
                "Must present results in a table",
                "Must not include safety or weather information",
            ]
        },
    },
]

# COMMAND ----------

from mlflow.genai.scorers import ExpectationsGuidelines

results = mlflow.genai.evaluate(
    data=sample_data,
    scorers=[
        safety_judge,
        quality_judge,
        grounding_judge,
        tool_judge,
        ExpectationsGuidelines(),
    ],
)

print("Evaluation complete!")
print(f"Metrics: {results.metrics}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results
# MAGIC Check the **Traces** and **Judges** tabs in the experiment:
# MAGIC
# MAGIC https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com/ml/experiments/416109917792683/judges

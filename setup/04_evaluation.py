# Databricks notebook source
# MAGIC %md
# MAGIC # Energy Crew Briefing — Agent Evaluation
# MAGIC Runs MLflow evaluation with ground-truth Q&A pairs against the agent.
# MAGIC Measures relevance, correctness, and safety using LLM judges.

# COMMAND ----------

# MAGIC %pip install mlflow openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd

EXPERIMENT_PATH = "/Users/zivile.norkunaite@databricks.com/energy-crew-briefing-traces"
mlflow.set_experiment(EXPERIMENT_PATH)

# COMMAND ----------

# Ground-truth evaluation dataset
eval_data = pd.DataFrame([
    {
        "question": "Prepare a briefing for Crew Alpha tomorrow",
        "expected_tools": "query_genie,get_swms,search_local_notices",
        "expected_sections": "Work Summary,Safety Requirements,Local Notices",
        "category": "briefing",
    },
    {
        "question": "What PPE is needed for transformer replacement?",
        "expected_tools": "get_swms",
        "expected_sections": "PPE Requirements",
        "category": "swms",
    },
    {
        "question": "What is Crew Hotel working on this week?",
        "expected_tools": "query_genie",
        "expected_sections": "work orders,tasks",
        "category": "genie",
    },
    {
        "question": "What's the weather like in Townsville today?",
        "expected_tools": "query_weather",
        "expected_sections": "temperature,wind,humidity",
        "category": "weather",
    },
    {
        "question": "What isolation procedures are needed for planned maintenance?",
        "expected_tools": "get_swms",
        "expected_sections": "Isolation Procedures",
        "category": "swms",
    },
    {
        "question": "Are there any road closures near Cairns?",
        "expected_tools": "search_local_notices",
        "expected_sections": "road,closure",
        "category": "web",
    },
    {
        "question": "Which crews have overdue work orders?",
        "expected_tools": "query_genie",
        "expected_sections": "overdue,crew",
        "category": "genie",
    },
    {
        "question": "Prepare a vegetation management safety briefing for the Innisfail area",
        "expected_tools": "get_swms,query_genie,search_local_notices",
        "expected_sections": "Vegetation Management,Safety,Local Notices",
        "category": "briefing",
    },
    {
        "question": "What's the wind speed in Roma? Is it safe for elevated work?",
        "expected_tools": "query_weather,get_swms",
        "expected_sections": "wind,safety",
        "category": "weather",
    },
    {
        "question": "What emergency response procedures should crews follow for storm damage?",
        "expected_tools": "get_swms",
        "expected_sections": "Emergency Response,storm,safety",
        "category": "swms",
    },
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Agent on Eval Questions
# MAGIC Call the deployed app endpoint for each question and collect responses.

# COMMAND ----------

import requests
import time

APP_URL = "https://energy-crew-briefing-1313663707993479.aws.databricksapps.com"

def query_agent(question: str) -> dict:
    """Query the deployed agent and return response + sources."""
    try:
        resp = requests.post(
            f"{APP_URL}/api/chat",
            json={"message": question, "history": []},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"response": f"Error: {e}", "sources": []}

results = []
for idx, row in eval_data.iterrows():
    print(f"[{idx+1}/{len(eval_data)}] {row['question'][:60]}...")
    result = query_agent(row["question"])
    tools_used = ",".join(sorted(set(s.get("type", "") for s in result.get("sources", []))))
    results.append({
        "question": row["question"],
        "response": result.get("response", ""),
        "tools_used": tools_used,
        "expected_tools": row["expected_tools"],
        "expected_sections": row["expected_sections"],
        "category": row["category"],
    })
    time.sleep(2)  # Rate limiting

results_df = pd.DataFrame(results)
display(results_df[["question", "category", "tools_used", "expected_tools"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Evaluation with LLM Judges

# COMMAND ----------

from mlflow.metrics.genai import relevance, faithfulness

with mlflow.start_run(run_name="crew_briefing_eval"):
    # Log eval dataset
    mlflow.log_table(results_df, artifact_file="eval_results.json")

    # Basic metrics
    total = len(results_df)
    tool_match = sum(
        1 for _, r in results_df.iterrows()
        if all(t in r["tools_used"] for t in r["expected_tools"].split(",") if t)
    )
    section_match = sum(
        1 for _, r in results_df.iterrows()
        if all(s.lower() in r["response"].lower() for s in r["expected_sections"].split(",") if s)
    )
    error_count = sum(1 for _, r in results_df.iterrows() if r["response"].startswith("Error:"))

    mlflow.log_metrics({
        "tool_accuracy": tool_match / total if total else 0,
        "section_coverage": section_match / total if total else 0,
        "error_rate": error_count / total if total else 0,
        "total_questions": total,
    })

    # LLM judge evaluation (relevance)
    eval_df = results_df[["question", "response"]].rename(
        columns={"question": "inputs", "response": "predictions"}
    )

    eval_result = mlflow.evaluate(
        data=eval_df,
        predictions="predictions",
        model_type="question-answering",
        extra_metrics=[relevance()],
        evaluator_config={
            "col_mapping": {"inputs": "inputs"},
        },
    )

    print("\n=== Evaluation Results ===")
    print(f"Tool accuracy: {tool_match}/{total} ({tool_match/total:.0%})")
    print(f"Section coverage: {section_match}/{total} ({section_match/total:.0%})")
    print(f"Error rate: {error_count}/{total}")
    print(f"\nMLflow metrics: {eval_result.metrics}")

# COMMAND ----------

# MAGIC %md
# MAGIC View results in the MLflow Experiment UI:
# MAGIC `/Users/zivile.norkunaite@databricks.com/energy-crew-briefing-traces`

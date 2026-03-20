"""Deploy SWMS Knowledge Assistant as a standalone Databricks Agent endpoint.

Creates an MLflow model with Vector Search retriever tool, registers it in UC,
and serves it as an independent endpoint.

Run with: python3 setup/06_swms_agent.py
"""

import mlflow
import os
from mlflow.models import ModelConfig

os.environ["DATABRICKS_HOST"] = "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com"

EXPERIMENT_PATH = "/Users/zivile.norkunaite@databricks.com/ee-crew-briefing-traces"
MODEL_NAME = "zivile.essential_energy_wacs.swms_knowledge_assistant"
ENDPOINT_NAME = "swms-knowledge-assistant"
VS_INDEX = "zivile.essential_energy_wacs.swms_documents_vs_index"
LLM_ENDPOINT = "ee-crew-briefing-gateway"

mlflow.set_experiment(EXPERIMENT_PATH)

# ── Define the agent ──────────────────────────────────────────────────────

AGENT_CODE = '''
import mlflow
from mlflow.pyfunc import PythonModel
from databricks.sdk import WorkspaceClient
import json


class SWMSKnowledgeAssistant(PythonModel):
    """RAG agent: Vector Search retrieval + LLM synthesis for SWMS safety documents."""

    SYSTEM_PROMPT = """You are the Essential Energy SWMS Knowledge Assistant. Your role is to answer \
safety questions using ONLY the Safe Work Method Statement (SWMS) content provided below.

Rules:
- Answer ONLY from the provided SWMS content. Do not invent or assume safety requirements.
- Cite the specific SWMS document (e.g. SWMS-001) and section title for every point.
- Structure answers with clear headings: PPE, Hazards, Isolation Procedures, Competency, etc.
- Use bullet points and tables for clarity — field crews need quick reference.
- If the provided content does not cover the question, say so explicitly.
- Reference Australian standards (AS/NZS, NENS-10) where they appear in the content."""

    def load_context(self, context):
        self.model_config = context.model_config
        self.vs_index = self.model_config.get("vs_index")
        self.llm_endpoint = self.model_config.get("llm_endpoint")
        self.w = WorkspaceClient()

    def _retrieve(self, query: str, doc_filter: str = None) -> list[dict]:
        """Retrieve relevant SWMS chunks via Vector Search."""
        payload = {
            "query_text": query,
            "columns": ["work_type", "section_title", "content", "document_name"],
            "num_results": 5,
        }
        if doc_filter:
            payload["filters_json"] = json.dumps({"document_name": [doc_filter]})

        response = self.w.api_client.do(
            "POST",
            f"/api/2.0/vector-search/indexes/{self.vs_index}/query",
            body=payload,
        )
        rows = response.get("result", {}).get("data_array", [])
        chunks = []
        for row in rows:
            if len(row) >= 4 and row[2]:
                chunks.append({
                    "work_type": row[0],
                    "section_title": row[1],
                    "content": row[2],
                    "document_name": row[3],
                })
        return chunks

    def _synthesise(self, query: str, chunks: list[dict]) -> str:
        """Synthesise answer from retrieved chunks using LLM."""
        context = "\\n\\n---\\n\\n".join(
            f"[{c['document_name']} — {c['section_title']}]\\n{c['content']}"
            for c in chunks
        )

        from openai import OpenAI
        client = OpenAI(
            api_key=self.w.config.token,
            base_url=f"{self.w.config.host}/serving-endpoints",
        )
        response = client.chat.completions.create(
            model=self.llm_endpoint,
            messages=[
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": f"SWMS CONTENT:\\n\\n{context}\\n\\n---\\n\\nQUESTION: {query}"},
            ],
            max_tokens=1500,
            temperature=0.1,
        )
        return response.choices[0].message.content

    def predict(self, context, model_input, params=None):
        """Handle chat completions format."""
        import pandas as pd

        if isinstance(model_input, pd.DataFrame):
            messages = model_input.iloc[0].get("messages", [])
        elif isinstance(model_input, dict):
            messages = model_input.get("messages", [])
        else:
            messages = []

        # Extract query from last user message
        query = ""
        doc_filter = None
        for msg in reversed(messages):
            if msg.get("role") == "user":
                query = msg.get("content", "")
                break

        if not query:
            return {"choices": [{"message": {"role": "assistant", "content": "Please ask a safety question."}}]}

        # Retrieve
        chunks = self._retrieve(query, doc_filter)
        if not chunks:
            answer = "No matching SWMS content found for your question."
        else:
            # Synthesise
            answer = self._synthesise(query, chunks)
            sources = sorted(set(c["document_name"] for c in chunks))
            answer += f"\\n\\n*Sources: {', '.join(sources)}*"

        return {
            "choices": [{
                "message": {"role": "assistant", "content": answer},
                "finish_reason": "stop",
            }]
        }


mlflow.models.set_model(SWMSKnowledgeAssistant())
'''

# ── Log and register the model ────────────────────────────────────────────

print("=" * 60)
print("Deploying SWMS Knowledge Assistant as Databricks Agent")
print("=" * 60)

# Write agent code to temp file
agent_path = "/tmp/swms_agent_model.py"
with open(agent_path, "w") as f:
    f.write(AGENT_CODE)

# Model config
config = {
    "vs_index": VS_INDEX,
    "llm_endpoint": LLM_ENDPOINT,
}
config_path = "/tmp/swms_agent_config.yml"
import yaml
with open(config_path, "w") as f:
    yaml.dump(config, f)

print(f"\n1. Logging model to MLflow...")
with mlflow.start_run(run_name="swms_knowledge_assistant_v1") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="swms_agent",
        python_model=agent_path,
        model_config=config_path,
        pip_requirements=[
            "mlflow",
            "databricks-sdk",
            "openai",
        ],
        input_example={
            "messages": [
                {"role": "user", "content": "What PPE is needed for transformer replacement?"}
            ]
        },
    )
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Model URI: {model_info.model_uri}")

print(f"\n2. Registering model as {MODEL_NAME}...")
registered = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=MODEL_NAME,
)
print(f"   Version: {registered.version}")

print(f"\n3. Creating serving endpoint: {ENDPOINT_NAME}...")
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Check if endpoint exists
try:
    existing = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"   Endpoint exists, updating config...")
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[
            ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=str(registered.version),
                workload_size="Small",
                scale_to_zero_enabled=True,
            )
        ],
    )
except Exception:
    print(f"   Creating new endpoint...")
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput, ServedEntityInput, AutoCaptureConfigInput,
        AiGatewayConfig, AiGatewayGuardrailsConfig, AiGatewayGuardrailParameters,
        AiGatewayGuardrailPiiBehavior, AiGatewayGuardrailPiiBehaviorBehavior,
        AiGatewayUsageTrackingConfig,
    )
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version=str(registered.version),
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ],
        ),
    )

print(f"\n4. Granting app SP access...")
import subprocess, json
sp_id = "84fba77d-2b5d-40ef-94e4-a0c81b5af427"

# Get endpoint ID
ep = w.serving_endpoints.get(ENDPOINT_NAME)
ep_id = ep.id
print(f"   Endpoint ID: {ep_id}")

subprocess.run([
    "databricks", "api", "patch",
    f"/api/2.0/permissions/serving-endpoints/{ep_id}",
    "--json", json.dumps({
        "access_control_list": [{
            "service_principal_name": sp_id,
            "permission_level": "CAN_QUERY"
        }]
    }),
    "--profile", "DEFAULT"
], capture_output=True)
print(f"   SP granted CAN_QUERY")

print(f"\n{'=' * 60}")
print(f"SWMS Knowledge Assistant deployed!")
print(f"  Model: {MODEL_NAME} v{registered.version}")
print(f"  Endpoint: {ENDPOINT_NAME}")
print(f"  URL: https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com/serving-endpoints/{ENDPOINT_NAME}/invocations")
print(f"{'=' * 60}")

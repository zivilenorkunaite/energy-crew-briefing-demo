"""Deploy SWMS Knowledge Assistant as a standalone Databricks Agent endpoint.

Creates an MLflow model with Vector Search retriever + LLM synthesis,
registers it in UC, and serves it as an independent endpoint.

Run with: python3 setup/06_swms_agent.py
"""

import mlflow
import os

os.environ.setdefault("DATABRICKS_HOST", "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com")

EXPERIMENT_PATH = "/Users/zivile.norkunaite@databricks.com/ee-crew-briefing-traces"
MODEL_NAME = "zivile.essential_energy_wacs.swms_knowledge_assistant"
ENDPOINT_NAME = "swms-knowledge-assistant"
VS_INDEX = "zivile.essential_energy_wacs.swms_documents_vs_index"
LLM_ENDPOINT = "crew-briefing-agent"
WORKSPACE_HOST = "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com"

mlflow.set_experiment(EXPERIMENT_PATH)

# ── Define the agent ──────────────────────────────────────────────────────

AGENT_CODE = '''
import mlflow
import os
import json
import requests
from mlflow.pyfunc import PythonModel
from databricks.sdk import WorkspaceClient


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
        self.workspace_host = self.model_config.get("workspace_host", "")

        # Ensure DATABRICKS_HOST is set for the SDK
        if self.workspace_host:
            os.environ.setdefault("DATABRICKS_HOST", self.workspace_host)

        self.w = WorkspaceClient()

        # Resolve host — use config value, then SDK, then env
        self.host = (
            self.workspace_host
            or self.w.config.host
            or os.environ.get("DATABRICKS_HOST", "")
        )
        if not self.host:
            raise RuntimeError("No workspace host available — set workspace_host in model_config or DATABRICKS_HOST env var")

    def _get_token(self) -> str:
        """Get a fresh auth token."""
        token = self.w.config.token
        if token:
            return token
        headers = self.w.config.authenticate()
        if headers and "Authorization" in headers:
            return headers["Authorization"].replace("Bearer ", "")
        raise RuntimeError("Could not obtain auth token")

    def _retrieve(self, query: str, doc_filter: str = None) -> list[dict]:
        """Retrieve relevant SWMS chunks via Vector Search."""
        payload = {
            "query_text": query,
            "columns": ["work_type", "section_title", "content", "document_name"],
            "num_results": 5,
        }
        if doc_filter:
            payload["filters_json"] = json.dumps({"document_name": [doc_filter]})

        url = f"{self.host}/api/2.0/vector-search/indexes/{self.vs_index}/query"
        resp = requests.post(url, json=payload, headers={
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("result", {}).get("data_array", [])
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
            api_key=self._get_token(),
            base_url=f"{self.host}/serving-endpoints",
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
        for msg in reversed(messages):
            if msg.get("role") == "user":
                query = msg.get("content", "")
                break

        if not query:
            return {"choices": [{"message": {"role": "assistant", "content": "Please ask a safety question."}}]}

        # Retrieve
        chunks = self._retrieve(query)
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

# Model config — now includes workspace_host
config = {
    "vs_index": VS_INDEX,
    "llm_endpoint": LLM_ENDPOINT,
    "workspace_host": WORKSPACE_HOST,
}
config_path = "/tmp/swms_agent_config.yml"
import yaml
with open(config_path, "w") as f:
    yaml.dump(config, f)

print(f"\n1. Logging model to MLflow...")
with mlflow.start_run(run_name="swms_knowledge_assistant_v4") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="swms_agent",
        python_model=agent_path,
        model_config=config_path,
        pip_requirements=[
            "mlflow",
            "databricks-sdk",
            "openai",
            "requests",
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

print(f"\n3. Updating serving endpoint: {ENDPOINT_NAME}...")
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

try:
    from databricks.sdk.service.serving import ServedEntityInput
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
    print(f"   Endpoint updated to v{registered.version}")
except Exception as e:
    print(f"   Error updating endpoint: {e}")

print(f"\n{'=' * 60}")
print(f"SWMS Knowledge Assistant deployed!")
print(f"  Model: {MODEL_NAME} v{registered.version}")
print(f"  Endpoint: {ENDPOINT_NAME}")
print(f"{'=' * 60}")

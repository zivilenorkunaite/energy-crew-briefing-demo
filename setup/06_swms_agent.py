"""Deploy SWMS Knowledge Assistant v2 — lightweight RAG endpoint.

Direct Vector Search retrieval + AI Gateway LLM synthesis.
No KA framework overhead. Target: 5-10s per query.

Run with: python3 setup/06_swms_agent.py
"""

import mlflow
import os
import yaml

os.environ.setdefault("DATABRICKS_HOST", "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com")

EXPERIMENT_PATH = "/Users/zivile.norkunaite@databricks.com/ee-crew-briefing-traces"
MODEL_NAME = "zivile.essential_energy_wacs.swms_knowledge_assistant"
ENDPOINT_NAME = "swms-knowledge-assistant-v2"
VS_INDEX = "zivile.essential_energy_wacs.swms_documents_vs_index"
AI_GATEWAY_URL = "https://1313663707993479.ai-gateway.cloud.databricks.com/mlflow/v1/chat/completions"
LLM_MODEL = "databricks-claude-sonnet-4-6"
WORKSPACE_HOST = "https://fe-vm-vdm-classic-dz1ef4.cloud.databricks.com"

mlflow.set_experiment(EXPERIMENT_PATH)

# ── Model code ────────────────────────────────────────────────────────────

AGENT_CODE = r'''
import mlflow
import json
import os
import requests
from mlflow.pyfunc import PythonModel


class SWMSKnowledgeAssistantV2(PythonModel):
    """Lightweight RAG: Vector Search retrieval + AI Gateway LLM synthesis."""

    SYSTEM_PROMPT = (
        "You are the Essential Energy SWMS Knowledge Assistant. Answer safety questions "
        "using ONLY the SWMS content provided. Cite the specific SWMS document (e.g. SWMS-001) "
        "and section title for every point. Structure with headings: PPE, Hazards, Isolation, "
        "Competency. Use bullet points and tables. Reference Australian standards where they "
        "appear in the content. If content doesn't cover the question, say so."
    )

    def load_context(self, context):
        self.model_config = context.model_config
        self.vs_index = self.model_config.get("vs_index")
        self.ai_gateway_url = self.model_config.get("ai_gateway_url")
        self.llm_model = self.model_config.get("llm_model", "databricks-claude-sonnet-4-6")
        self.workspace_host = self.model_config.get("workspace_host", "")
        if self.workspace_host:
            os.environ.setdefault("DATABRICKS_HOST", self.workspace_host)

    def _get_token(self):
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            token = w.config.token
            if token:
                return token
            headers = w.config.authenticate()
            if headers and "Authorization" in headers:
                return headers["Authorization"].replace("Bearer ", "")
        except Exception:
            pass
        return os.environ.get("DATABRICKS_TOKEN", "")

    def _retrieve(self, query, num_results=5):
        """Vector Search retrieval — direct REST call."""
        token = self._get_token()
        host = self.workspace_host or os.environ.get("DATABRICKS_HOST", "")
        resp = requests.post(
            f"{host}/api/2.0/vector-search/indexes/{self.vs_index}/query",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "query_text": query,
                "columns": ["work_type", "section_title", "content", "document_name"],
                "num_results": num_results,
            },
            timeout=15,
        )
        resp.raise_for_status()
        rows = resp.json().get("result", {}).get("data_array", [])
        return [
            {"work_type": r[0], "section_title": r[1], "content": r[2], "document_name": r[3]}
            for r in rows if len(r) >= 4 and r[2]
        ]

    def _synthesise(self, query, chunks):
        """LLM synthesis via AI Gateway — fast, no serving endpoint overhead."""
        token = self._get_token()
        context = "\n\n---\n\n".join(
            f"[{c['document_name']} - {c['section_title']}]\n{c['content']}" for c in chunks
        )
        resp = requests.post(
            self.ai_gateway_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "model": self.llm_model,
                "messages": [
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": f"SWMS CONTENT:\n\n{context}\n\n---\n\nQUESTION: {query}"},
                ],
                "max_tokens": 1500,
                "temperature": 0.1,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")

    def predict(self, context, model_input, params=None):
        import pandas as pd
        if isinstance(model_input, pd.DataFrame):
            messages = model_input.iloc[0].get("messages", [])
        elif isinstance(model_input, dict):
            messages = model_input.get("messages", [])
        else:
            messages = []

        query = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                query = msg.get("content", "")
                break

        if not query:
            return {"choices": [{"message": {"role": "assistant", "content": "Please ask a safety question."}, "finish_reason": "stop"}]}

        chunks = self._retrieve(query)
        if not chunks:
            answer = "No matching SWMS content found."
        else:
            answer = self._synthesise(query, chunks)
            sources = sorted(set(c["document_name"] for c in chunks))
            answer += f"\n\n*Sources: {', '.join(sources)}*"

        return {"choices": [{"message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}]}


mlflow.models.set_model(SWMSKnowledgeAssistantV2())
'''

# ── Log and register ──────────────────────────────────────────────────────

print("=" * 60)
print("Deploying SWMS Knowledge Assistant v2")
print("=" * 60)

agent_path = "/tmp/swms_agent_v2.py"
with open(agent_path, "w") as f:
    f.write(AGENT_CODE)

config = {
    "vs_index": VS_INDEX,
    "ai_gateway_url": AI_GATEWAY_URL,
    "llm_model": LLM_MODEL,
    "workspace_host": WORKSPACE_HOST,
}
config_path = "/tmp/swms_agent_v2_config.yml"
with open(config_path, "w") as f:
    yaml.dump(config, f)

print("\n1. Logging model...")
with mlflow.start_run(run_name="swms_knowledge_assistant_v2") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="swms_agent",
        python_model=agent_path,
        model_config=config_path,
        pip_requirements=["mlflow", "databricks-sdk", "requests", "pandas"],
        input_example={"messages": [{"role": "user", "content": "What PPE for overhead line work?"}]},
    )
    print(f"   Run: {run.info.run_id}")

print(f"\n2. Registering as {MODEL_NAME}...")
registered = mlflow.register_model(model_uri=model_info.model_uri, name=MODEL_NAME)
print(f"   Version: {registered.version}")

print(f"\n3. Creating endpoint: {ENDPOINT_NAME}...")
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()
try:
    w.serving_endpoints.get(ENDPOINT_NAME)
    print("   Endpoint exists, updating...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[ServedEntityInput(
            entity_name=MODEL_NAME,
            entity_version=str(registered.version),
            workload_size="Small",
            scale_to_zero_enabled=True,
        )],
    )
except Exception:
    print("   Creating new endpoint...")
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            served_entities=[ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=str(registered.version),
                workload_size="Small",
                scale_to_zero_enabled=True,
            )],
        ),
    )

print(f"\n{'=' * 60}")
print(f"SWMS v2 deployed: {ENDPOINT_NAME} (v{registered.version})")
print(f"{'=' * 60}")

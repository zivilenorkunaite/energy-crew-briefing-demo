"""Deploy SWMS Knowledge Assistant v3 — full document loading, no Vector Search.

Loads complete SWMS document sections from Delta table (each doc ~800 tokens),
sends full document to AI Gateway for synthesis. Much better quality than
fragmented VS chunks.

Run with: python3 setup/06_swms_agent.py
"""

import mlflow
import os
import sys
import yaml

sys.path.insert(0, os.path.dirname(__file__))
from helpers import get_host, get_warehouse_id, UC_FULL

os.environ.setdefault("DATABRICKS_HOST", get_host())

EXPERIMENT_PATH = "/Shared/energy-crew-briefing-traces-uc"
MODEL_NAME = f"{UC_FULL}.swms_knowledge_assistant"
ENDPOINT_NAME = "swms-knowledge-assistant-v2"
TABLE = f"{UC_FULL}.swms_documents"
WORKSPACE_HOST = get_host()
WAREHOUSE_ID = get_warehouse_id()
# AI Gateway URL — derived from workspace instance ID
_instance_id = WORKSPACE_HOST.split("//")[1].split(".")[0].replace("adb-", "")
AI_GATEWAY_URL = f"https://{_instance_id}.ai-gateway.cloud.databricks.com/mlflow/v1/chat/completions"
LLM_MODEL = "databricks-claude-sonnet-4-6"

mlflow.set_experiment(EXPERIMENT_PATH)

# ── Model code ────────────────────────────────────────────────────────────

AGENT_CODE = r'''
import mlflow
import json
import os
import requests
from mlflow.pyfunc import PythonModel


class SWMSKnowledgeAssistantV3(PythonModel):
    """SWMS agent: loads full document from Delta via SQL, synthesises via AI Gateway.

    Each SWMS doc is ~800 tokens (7 docs total ~5700 tokens).
    Full document context produces much better answers than fragmented VS chunks.
    """

    SYSTEM_PROMPT = (
        "You are the SWMS Knowledge Assistant. Answer safety questions "
        "using ONLY the SWMS content provided below.\n\n"
        "Rules:\n"
        "- Cite the specific SWMS document (e.g. SWMS-001) and section title for every point.\n"
        "- Structure with headings: PPE, Hazards, Isolation Procedures, Competency Requirements.\n"
        "- Use tables for PPE lists. Use bullet points for hazards and procedures.\n"
        "- Reference Australian standards (AS/NZS, NENS-10) where they appear.\n"
        "- If the content doesn't cover the question, say so explicitly.\n"
        "- Keep answers concise — field crews need quick reference, not essays."
    )

    DOCUMENT_NAMES = [
        "SWMS-001 Asset Replacement",
        "SWMS-002 Capital Works",
        "SWMS-003 Corrective Maintenance",
        "SWMS-004 Emergency Response",
        "SWMS-005 Inspection",
        "SWMS-006 Planned Maintenance",
        "SWMS-007 Vegetation Management",
        "SWMS-008 Underground Cable",
        "SWMS-009 Metering",
        "SWMS-010 Substation Work",
    ]

    def load_context(self, context):
        self.model_config = context.model_config
        self.table = self.model_config.get("table")
        self.warehouse_id = self.model_config.get("warehouse_id")
        self.ai_gateway_url = self.model_config.get("ai_gateway_url")
        self.llm_model = self.model_config.get("llm_model", "databricks-claude-sonnet-4-6")
        self.workspace_host = self.model_config.get("workspace_host", "")
        if self.workspace_host:
            os.environ.setdefault("DATABRICKS_HOST", self.workspace_host)

    def _get_token(self):
        # Prefer explicit token (PAT from secrets), then SDK
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if token:
            return token
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            t = w.config.token
            if t:
                return t
            headers = w.config.authenticate()
            if headers and "Authorization" in headers:
                return headers["Authorization"].replace("Bearer ", "")
        except Exception:
            pass
        return ""

    def _load_document(self, document_name):
        """Load full SWMS document from Delta table via SQL warehouse."""
        token = self._get_token()
        host = self.workspace_host or os.environ.get("DATABRICKS_HOST", "")
        safe_name = document_name.replace("'", "''")
        sql = (
            f"SELECT section_title, content FROM {self.table} "
            f"WHERE document_name = '{safe_name}' "
            f"ORDER BY section_title"
        )
        resp = requests.post(
            f"{host}/api/2.0/sql/statements/",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"statement": sql, "warehouse_id": self.warehouse_id, "format": "JSON_ARRAY", "wait_timeout": "30s"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status", {}).get("state") != "SUCCEEDED":
            return None

        rows = data.get("result", {}).get("data_array", [])
        if not rows:
            return None

        sections = []
        for row in rows:
            if row[1]:
                sections.append(f"### {row[0]}\n{row[1]}")
        return f"## {document_name}\n\n" + "\n\n".join(sections)

    def _detect_documents(self, query):
        """Detect which SWMS documents to load based on keywords in the query."""
        q = query.lower()
        docs = []

        # Always include SWMS-006 (Planned Maintenance) as base — has the most complete PPE list
        mapping = {
            "SWMS-001 Asset Replacement": ["replacement", "upgrade", "transformer", "switchgear", "pole", "cross-arm", "asset"],
            "SWMS-002 Capital Works": ["capital", "construction", "new build", "install"],
            "SWMS-003 Corrective Maintenance": ["corrective", "fault", "repair", "unplanned", "fix"],
            "SWMS-004 Emergency Response": ["emergency", "storm", "bushfire", "fallen", "urgent"],
            "SWMS-005 Inspection": ["inspection", "audit", "patrol", "check", "drone"],
            "SWMS-006 Planned Maintenance": ["planned", "maintenance", "routine", "scheduled", "ppe", "isolation", "overhead", "line"],
            "SWMS-007 Vegetation Management": ["vegetation", "tree", "trim", "clearing", "veg"],
            "SWMS-008 Underground Cable": ["underground", "cable", "trench", "jointing", "xlpe", "pilc"],
            "SWMS-009 Metering": ["meter", "metering", "smart meter", "ct meter", "solar meter", "disconnection"],
            "SWMS-010 Substation Work": ["substation", "zone sub", "switching", "circuit breaker", "hv", "high voltage", "sf6", "battery"],
        }

        for doc_name, keywords in mapping.items():
            if any(kw in q for kw in keywords):
                docs.append(doc_name)

        # If nothing matched or generic question, include Planned Maintenance as default
        if not docs:
            docs = ["SWMS-006 Planned Maintenance"]

        # Always include SWMS-006 if asking about PPE (has the base PPE list others reference)
        if "ppe" in q and "SWMS-006 Planned Maintenance" not in docs:
            docs.insert(0, "SWMS-006 Planned Maintenance")

        return docs[:3]  # Max 3 documents

    def _synthesise(self, query, doc_text, doc_names):
        """Send full document(s) + query to AI Gateway for synthesis."""
        token = self._get_token()
        resp = requests.post(
            self.ai_gateway_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "model": self.llm_model,
                "messages": [
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": f"SWMS CONTENT:\n\n{doc_text}\n\n---\n\nQUESTION: {query}"},
                ],
                "max_tokens": 1500,
                "temperature": 0.1,
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        if content:
            sources = ", ".join(doc_names)
            return content + f"\n\n*Sources: {sources}*"
        return None

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

        # Detect relevant documents
        doc_names = self._detect_documents(query)

        # Load full documents
        doc_parts = []
        loaded_names = []
        for name in doc_names:
            text = self._load_document(name)
            if text:
                doc_parts.append(text)
                loaded_names.append(name)

        if not doc_parts:
            return {"choices": [{"message": {"role": "assistant", "content": f"No SWMS content found for: {', '.join(doc_names)}"}, "finish_reason": "stop"}]}

        doc_text = "\n\n---\n\n".join(doc_parts)

        # Synthesise
        answer = self._synthesise(query, doc_text, loaded_names)
        if not answer:
            answer = "SWMS synthesis failed."

        return {"choices": [{"message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}]}


mlflow.models.set_model(SWMSKnowledgeAssistantV3())
'''

# ── Log and register ──────────────────────────────────────────────────────

print("=" * 60)
print("Deploying SWMS Knowledge Assistant v3 (full doc, no VS)")
print("=" * 60)

agent_path = "/tmp/swms_agent_v3.py"
with open(agent_path, "w") as f:
    f.write(AGENT_CODE)

config = {
    "table": TABLE,
    "warehouse_id": WAREHOUSE_ID,
    "ai_gateway_url": AI_GATEWAY_URL,
    "llm_model": LLM_MODEL,
    "workspace_host": WORKSPACE_HOST,
}
config_path = "/tmp/swms_agent_v3_config.yml"
with open(config_path, "w") as f:
    yaml.dump(config, f)

print("\n1. Logging model...")
mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run(run_name="swms_v3_full_doc"):
    model_info = mlflow.pyfunc.log_model(
        artifact_path="swms_agent",
        python_model=agent_path,
        model_config=config_path,
        pip_requirements=["mlflow", "databricks-sdk", "requests", "pandas"],
        input_example={"messages": [{"role": "user", "content": "What PPE for emergency response?"}]},
    )
    print(f"   Model URI: {model_info.model_uri}")

print(f"\n2. Registering as {MODEL_NAME}...")
registered = mlflow.register_model(model_uri=model_info.model_uri, name=MODEL_NAME)
print(f"   Version: {registered.version}")

print(f"\n3. Updating endpoint: {ENDPOINT_NAME}...")
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
from databricks.sdk.service.serving import ServedEntityInput
w.serving_endpoints.update_config(
    name=ENDPOINT_NAME,
    served_entities=[ServedEntityInput(
        entity_name=MODEL_NAME,
        entity_version=str(registered.version),
        workload_size="Small",
        scale_to_zero_enabled=True,
        environment_vars={
            "DATABRICKS_HOST": WORKSPACE_HOST,
            "DATABRICKS_TOKEN": "{{secrets/energy-crew-briefing/gateway-token}}",
        },
    )],
)

print(f"\n{'=' * 60}")
print(f"SWMS v3 deployed: {ENDPOINT_NAME} (v{registered.version})")
print(f"  Full document loading from {TABLE}")
print(f"  No Vector Search dependency")
print(f"{'=' * 60}")

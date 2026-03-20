
import mlflow
from mlflow.pyfunc import PythonModel
from databricks.sdk import WorkspaceClient
import json


class SWMSKnowledgeAssistant(PythonModel):
    """RAG agent: Vector Search retrieval + LLM synthesis for SWMS safety documents."""

    SYSTEM_PROMPT = """You are the Essential Energy SWMS Knowledge Assistant. Your role is to answer safety questions using ONLY the Safe Work Method Statement (SWMS) content provided below.

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
        context = "\n\n---\n\n".join(
            f"[{c['document_name']} — {c['section_title']}]\n{c['content']}"
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
                {"role": "user", "content": f"SWMS CONTENT:\n\n{context}\n\n---\n\nQUESTION: {query}"},
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
            answer += f"\n\n*Sources: {', '.join(sources)}*"

        return {
            "choices": [{
                "message": {"role": "assistant", "content": answer},
                "finish_reason": "stop",
            }]
        }


mlflow.models.set_model(SWMSKnowledgeAssistant())

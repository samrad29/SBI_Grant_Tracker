"""
This module contains functions to use AI to help with the grant ingestion process.
classify_grant: classify the grant as relevant to tribes or not
describe_changes: describe the changes in the grant since the last time it was ingested
"""
from groq import Groq
from dotenv import load_dotenv
import json
import os
from datetime import datetime
from typing import Optional
import httpx
load_dotenv()

# NOTE: when we productionalize this, we will want to get the API key from the environment variables
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL")
LLM_PROVIDER = (os.getenv("LLM_PROVIDER") or "groq").strip().lower()

# Ollama settings (used only when LLM_PROVIDER=ollama)
OLLAMA_BASE_URL = (os.getenv("OLLAMA_BASE_URL") or "http://localhost:11434").strip().rstrip("/")
# Typical tags look like `llama3.1:8b-instruct`. Adjust if yours differs.
OLLAMA_MODEL = (os.getenv("OLLAMA_MODEL") or "llama3.2:latest").strip()

def get_groq_client():
    if not GROQ_API_KEY:
        raise RuntimeError(
            "Missing Groq API key. Set GROQ_API_KEY in your environment (or .env)."
        )
    return Groq(api_key=GROQ_API_KEY, max_retries=0)

class RateLimitError(Exception):
    """Raised when the upstream AI provider returns HTTP 429."""

    def __init__(self, retry_seconds: float, message: str = "Rate limited"):
        super().__init__(message)
        self.retry_seconds = retry_seconds


def _parse_retry_after_seconds(value: Optional[str], default_seconds: float = 10.0) -> float:
    if not value:
        return default_seconds
    value = value.strip()
    if not value:
        return default_seconds
    try:
        return float(value)
    except ValueError:
        return default_seconds


class GroqLLMClient:
    """Wrapper around the Groq chat completions API."""
    def __init__(self):
        self._client = get_groq_client()

    def complete(self, prompt: str) -> str:
        try:
            response = self._client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            content = response.choices[0].message.content
            return content or ""
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 429:
                retry_after = _parse_retry_after_seconds(
                    e.response.headers.get("Retry-After")
                )
                raise RateLimitError(retry_after, f"Groq 429 rate limit. Retrying in {retry_after}s")
            raise


class OllamaLLMClient:
    """Calls the local Ollama HTTP API."""
    def __init__(self):
        self._base_url = OLLAMA_BASE_URL
        self._model = OLLAMA_MODEL
        self._client = httpx.Client(timeout=120)

    def complete(self, prompt: str) -> str:
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0},
        }
        url = f"{self._base_url}/api/chat"
        try:
            resp = self._client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return (data.get("message") or {}).get("content") or ""
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 429:
                retry_after = _parse_retry_after_seconds(
                    e.response.headers.get("Retry-After")
                )
                raise RateLimitError(retry_after, f"Ollama 429 rate limit. Retrying in {retry_after}s")
            raise


def get_llm_client():
    if LLM_PROVIDER == "ollama":
        return OllamaLLMClient()
    return GroqLLMClient()


def ai_grant_tagging(llm_client, grant):
    prompt = f"""
        You are classifying a government grant into categories.

        You MUST:
        1. Assign relevance scores (0-100) to the predefined categories below
        2. Optionally suggest up to 3 NEW categories if the predefined ones are insufficient

        Predefined categories: Housing, Tribal, Gaming, Cannabis, Environment, Agriculture, Broadband, Technology, Infrastructure, workforce_development, accepting_applications

        Rules:
        - Accepting applications is a special category used to indicate that the grant is currently accepting applications as of {datetime.now().strftime("%Y-%m-%d")}
        - Prefer predefined categories whenever possible
        - Only create a new category if it captures something important not covered above
        - New categories must be concise (1-3 words, snake_case)
        - Do NOT create synonyms of existing categories
        - Avoid overly specific categories (e.g., "solar_panel_installation_grants")

        Respond ONLY with valid JSON in this format:
        {{
        "tags": [
            {{"tag": "energy", "score": 85}},
            {{"tag": "tribal", "score": 60}}
        ],
        "new_tags": [
            {{"tag": "disaster_recovery", "score": 75}}
        ]
        }}

        Grant Title:
        {grant["title"]}

        Grant Description:
        {grant.get("description", "")[:1500]}

        Eligibility Codes:
        {grant.get("eligibilities", [])}

        Eligibility Description:
        {grant.get("eligibility_description", "")}

        deadline date:
        {grant.get("deadline_date", "")}

        deadline description:
        {grant.get("deadline_description", "")}
    """
    content = llm_client.complete(prompt)
    if not content:
        print("No content returned from AI")
        return None
    try:
        return json.loads(content)
    except Exception as e:
        print("Error parsing JSON in ai_grant_tagging:", e)
        return None

def ai_tribal_eligibility_check(llm_client, grant):
    prompt = f"""
        You are evaluating whether a Native American tribal government is eligible for a federal grant.

        Respond ONLY with valid JSON.

        Grant Title:
        {grant["title"]}

        Grant Description:
        {grant.get("description", "")[:1500]}

        Eligibility Codes:
        {grant.get("eligibilities", [])}

        Eligibility Description:
        {grant.get("eligibility_description", "")}

        Return:
        {{
        "model": "{GROQ_MODEL}",
        "is_tribal_eligible": true/false,
        "eligibility_score": 0-100,
        "eligibility_reasoning": ""
        }}
        """

    content = llm_client.complete(prompt)
    if not content:
        print("No content returned from AI")
        return None
    try:
        return json.loads(content)
    except Exception as e:
        print("Error parsing JSON in ai_tribal_eligibility_check:", e)
        return None

def test_groq():
    llm = get_llm_client()
    print(llm.complete("Return exactly: 1"))

if __name__ == "__main__":
    test_groq()
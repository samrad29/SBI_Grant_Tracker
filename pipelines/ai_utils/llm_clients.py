import os

from groq import Groq
from openai import OpenAI

from pipelines.ai_utils.llm_utils import TokenTracker, with_backoff
from pipelines.ai_utils.req_resp_obj import LLMRequest, LLMResponse


class GroqProvider:
    def __init__(self, client):
        self.client = client

    def generate(self, req: LLMRequest):
        def call():
            resp = self.client.chat.completions.create(
                model=req.model,
                messages=[m.__dict__ for m in req.messages],
                temperature=req.temperature,
            )

            usage = resp.usage

            return LLMResponse(
                content=resp.choices[0].message.content,
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
                total_tokens=usage.total_tokens,
                provider="groq",
            )

        return with_backoff(call)()

class OpenAIProvider:
    def __init__(self, client):
        self.client = client

    def generate(self, req: LLMRequest):
        def call():
            resp = self.client.chat.completions.create(
                model=req.model,
                messages=[m.__dict__ for m in req.messages],
                temperature=req.temperature,
            )

            usage = resp.usage

            return LLMResponse(
                content=resp.choices[0].message.content,
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
                total_tokens=usage.total_tokens,
                provider="openai",
            )

        return with_backoff(call)()


class LLMService:
    def __init__(self, groq_provider, openai_provider, token_tracker):
        self.providers = {
            "groq": groq_provider,
            "openai": openai_provider,
        }
        self.tokens = token_tracker

    def generate(self, req: LLMRequest) -> LLMResponse:
        provider = self.providers[req.provider]

        response = provider.generate(req)

        # track tokens
        self.tokens.add(
            response.provider,
            response.prompt_tokens,
            response.completion_tokens,
        )

        return response


def create_llm_service(job_id: int = -1) -> LLMService:
    """Build an LLMService with Groq/OpenAI providers and token tracking."""
    token_tracker = TokenTracker(job_id)
    groq_provider = GroqProvider(client=Groq(api_key=os.getenv("GROQ_API_KEY")))
    openai_provider = OpenAIProvider(client=OpenAI(api_key=os.getenv("OPENAI_API_KEY")))
    return LLMService(
        groq_provider=groq_provider,
        openai_provider=openai_provider,
        token_tracker=token_tracker,
    )
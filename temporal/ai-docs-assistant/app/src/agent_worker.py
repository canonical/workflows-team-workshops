"""Temporal worker that runs a documentation assistant built on the OpenAI Agents SDK.

The LLM is reached through any OpenAI-compatible API, OpenRouter by default.
"""

import asyncio
import datetime
import os
import time
import uuid
from dataclasses import dataclass

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

# The workflow sandbox re-imports this module to validate DocsQaWorkflow.
# These libraries are only used by the activity, so skip sandboxing them.
with workflow.unsafe.imports_passed_through():
    import httpx
    from agents import Agent, OpenAIChatCompletionsModel, Runner, set_tracing_disabled
    from bs4 import BeautifulSoup
    from openai import APIStatusError, AsyncOpenAI

DEFAULT_DOC_URLS = [
    "https://docs.temporal.io/",
    "https://charmhub.io/temporal-k8s/docs",
    "https://charmhub.io/temporal-worker-k8s",
]

DEFAULT_LLM_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_LLM_MODEL = "openai/gpt-4.1-mini"

_MODEL_NAME_HINT = (
    "Check LLM_MODEL: OpenRouter model names include the provider (openai/gpt-4.1-mini), "
    "OpenAI's own API does not (gpt-4.1-mini)."
)

# LLM API errors that retrying will not fix. Anything else, such as rate limits (429)
# or provider outages (5xx), is left to Temporal's automatic retries.
NON_RETRYABLE_LLM_ERRORS = {
    400: f"The LLM API rejected the request. {_MODEL_NAME_HINT}",
    401: "The LLM API rejected the key. Check LLM_API_KEY.",
    402: "The LLM account has run out of credit.",
    403: "The LLM API refused access with this key or model.",
    404: f"The LLM API does not know this model. {_MODEL_NAME_HINT}",
}

# url -> (fetched_at, page text). Kept in the worker process between activity runs.
_docs_cache: dict[str, tuple[float, str]] = {}

# Set by configure_llm() when the worker starts.
_llm_client: AsyncOpenAI | None = None


@dataclass
class AskDocsInput:
    question: str
    doc_urls: list[str]


@activity.defn
async def answer_docs_question(payload: AskDocsInput) -> str:
    if _llm_client is None:
        raise ApplicationError("The LLM client is not configured.", non_retryable=True)

    model_name = os.environ.get("LLM_MODEL") or DEFAULT_LLM_MODEL
    max_chars = int(os.environ.get("DOCS_MAX_CHARS", "12000"))

    docs_context = await _collect_docs_context(payload.doc_urls, max_chars=max_chars)

    assistant = Agent(
        name="Charmed Temporal docs assistant",
        # A model object sends the name as-is. A plain string would be parsed by the SDK,
        # which strips or rejects provider prefixes such as "openai/" or "anthropic/".
        model=OpenAIChatCompletionsModel(model=model_name, openai_client=_llm_client),
        instructions=(
            "You answer questions about Temporal workflows and Charmed Temporal deployments. "
            "Use only the provided documentation context. "
            "If context is insufficient, say what is missing and suggest where to look next."
        ),
    )

    prompt = (
        f"Question:\n{payload.question}\n\n"
        "Documentation context:\n"
        f"{docs_context}\n\n"
        "Return concise and practical guidance."
    )

    try:
        result = await Runner.run(assistant, input=prompt)
    except APIStatusError as exc:
        hint = NON_RETRYABLE_LLM_ERRORS.get(exc.status_code)
        if hint is None:
            raise
        raise ApplicationError(f"{hint} (HTTP {exc.status_code})", non_retryable=True) from exc

    return str(result.final_output)


@workflow.defn
class DocsQaWorkflow:
    @workflow.run
    async def run(self, payload: AskDocsInput) -> str:
        return await workflow.execute_activity(
            answer_docs_question,
            payload,
            schedule_to_close_timeout=datetime.timedelta(seconds=90),
            start_to_close_timeout=datetime.timedelta(seconds=75),
        )


async def _fetch_doc(client: httpx.AsyncClient, url: str, ttl: float) -> str:
    cached = _docs_cache.get(url)
    if cached and time.monotonic() - cached[0] < ttl:
        return cached[1]

    try:
        response = await client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        text = " ".join(soup.stripped_strings)
    except Exception as exc:  # noqa: BLE001
        return f"Could not fetch {url}: {exc}"

    _docs_cache[url] = (time.monotonic(), text)
    return text


async def _collect_docs_context(urls: list[str], max_chars: int) -> str:
    if not urls:
        urls = DEFAULT_DOC_URLS

    ttl = float(os.environ.get("DOCS_CACHE_TTL", "600"))
    timeout = httpx.Timeout(connect=5.0, read=12.0, write=5.0, pool=5.0)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        texts = await asyncio.gather(*(_fetch_doc(client, url, ttl) for url in urls))

    chunks = [f"Source: {url}\n{text[:3000]}" for url, text in zip(urls, texts)]
    combined = "\n\n".join(chunks)
    return combined[:max_chars]


async def connect_client() -> Client:
    target_host = os.environ["TEMPORAL_HOST"]
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "ai-assistant")
    return await Client.connect(target_host, namespace=namespace)


def configure_llm() -> None:
    """Create the client for the OpenAI-compatible LLM API, OpenRouter by default."""
    global _llm_client  # noqa: PLW0603

    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        raise SystemExit("LLM_API_KEY is not set. See 'Set your LLM API key' in README.md.")

    _llm_client = AsyncOpenAI(
        base_url=os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL,
        api_key=api_key,
        # OpenRouter shows this name in its usage dashboard.
        default_headers={"X-Title": "Charmed Temporal docs assistant"},
    )
    # The SDK would otherwise upload every run to OpenAI. Temporal already records each step.
    set_tracing_disabled(True)


async def run_worker() -> None:
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "ai-docs-queue")

    configure_llm()
    client = await connect_client()

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[DocsQaWorkflow],
        activities=[answer_docs_question],
    )

    await worker.run()


async def start_docs_qa(client: Client, question: str) -> WorkflowHandle:
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "ai-docs-queue")
    docs = os.environ.get("DOC_URLS", "")

    doc_urls = [item.strip() for item in docs.split(",") if item.strip()] or DEFAULT_DOC_URLS

    return await client.start_workflow(
        DocsQaWorkflow.run,
        AskDocsInput(question=question, doc_urls=doc_urls),
        id=f"docs-qa-{uuid.uuid4()}",
        task_queue=task_queue,
        execution_timeout=datetime.timedelta(seconds=120),
    )


async def ask_once(question: str) -> str:
    client = await connect_client()
    handle = await start_docs_qa(client, question)
    return await handle.result()


if __name__ == "__main__":
    asyncio.run(run_worker())

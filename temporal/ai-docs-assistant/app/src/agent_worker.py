"""Temporal worker that runs an OpenAI Agents SDK-backed documentation assistant."""

import asyncio
import datetime
import os
import time
import uuid
from dataclasses import dataclass

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

# The workflow sandbox re-imports this module to validate DocsQaWorkflow.
# These libraries are only used by the activity, so skip sandboxing them.
with workflow.unsafe.imports_passed_through():
    import httpx
    from agents import Agent, Runner
    from bs4 import BeautifulSoup

DEFAULT_DOC_URLS = [
    "https://docs.temporal.io/",
    "https://charmhub.io/temporal-k8s/docs",
    "https://charmhub.io/temporal-worker-k8s",
]

# url -> (fetched_at, page text). Kept in the worker process between activity runs.
_docs_cache: dict[str, tuple[float, str]] = {}


@dataclass
class AskDocsInput:
    question: str
    doc_urls: list[str]


@activity.defn
async def answer_docs_question(payload: AskDocsInput) -> str:
    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    max_chars = int(os.environ.get("DOCS_MAX_CHARS", "12000"))

    docs_context = await _collect_docs_context(payload.doc_urls, max_chars=max_chars)

    assistant = Agent(
        name="Charmed Temporal docs assistant",
        model=model,
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

    result = await Runner.run(assistant, input=prompt)
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


async def run_worker() -> None:
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "ai-docs-queue")

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

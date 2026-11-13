"""Temporal worker that runs a documentation assistant built on the OpenAI Agents SDK.

The agent runs inside DocsQaWorkflow through Temporal's OpenAI Agents integration: every
model call and every tool call is its own activity. The LLM is reached through any
OpenAI-compatible API, OpenRouter by default.
"""

import ast
import asyncio
import datetime
import json
import os
import re
import uuid
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from temporalio.contrib.openai_agents.workflow import activity_as_tool
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.worker import Worker

# The workflow sandbox re-imports this module to validate DocsQaWorkflow.
# These libraries are only used outside workflow code, so skip sandboxing them.
with workflow.unsafe.imports_passed_through():
    from agents import Agent, ModelProvider, ModelSettings, OpenAIChatCompletionsModel, Runner, set_tracing_disabled
    from openai import AsyncOpenAI

    from . import docs_tools

DEFAULT_LLM_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_LLM_MODEL = "openai/gpt-4.1-mini"

# Topics on the workflow's stream, which the chat page follows while the answer is written.
# "events" carries the raw model output; "status" says which tool the agent is using.
EVENTS_TOPIC = "events"
STATUS_TOPIC = "status"

_MODEL_NAME_HINT = (
    "Check LLM_MODEL: OpenRouter model names include the provider (openai/gpt-4.1-mini), "
    "OpenAI's own API does not (gpt-4.1-mini)."
)

# LLM API errors that retrying will not fix. Temporal's integration does not retry them;
# these hints explain what to change, followed by the API's own explanation when it gives
# one. Rate limits (429) and outages (5xx) are retried.
LLM_ERROR_HINTS = {
    400: "The LLM API rejected the request.",
    401: "The LLM API rejected the key. Check LLM_API_KEY.",
    402: "The LLM account has run out of credit.",
    403: "The LLM API refused access with this key or model.",
    404: f"The LLM API does not know this model. {_MODEL_NAME_HINT}",
}

TOOL_STATUS = {
    "search_temporal_docs": "Searching the Temporal docs",
    "read_temporal_docs_page": "Reading a Temporal docs page",
    "read_charmed_temporal_docs": "Reading the Charmed Temporal docs",
}

INSTRUCTIONS = """\
You answer questions about Temporal and Charmed Temporal, Canonical's Juju charms that deploy
and operate Temporal on Kubernetes.

Look the answer up with your tools before answering; do not answer from memory.
- For Temporal itself (concepts, SDKs, CLI, self-hosting), call search_temporal_docs. If a
  passage is relevant but incomplete, call read_temporal_docs_page with its URL.
- For deploying, configuring, operating or troubleshooting Temporal with the charms, call
  read_charmed_temporal_docs with one of the pages listed below. Read every page that applies,
  not just one: usually a how-to page plus the configurations or actions reference of each
  charm involved. The reference pages hold the exact option and action names.
- Questions about a problem on a Charmed Temporal deployment need both sources: the Temporal
  docs explain what the symptom means, the charm docs explain how to check and fix it there.
- Call tools straight away, without writing anything first. Call several at once when useful.

Then answer concisely and practically, in Markdown.
- On a Charmed Temporal deployment, show the actual commands: juju status, juju config,
  juju debug-log, juju run on the charms' actions (the Temporal CLI runs through
  temporal-admin-k8s's cli action), and kubectl where Juju has no equivalent. Write them in
  the answer; do not offer them as a follow-up.
- For troubleshooting, put the most likely causes first, each with the command that confirms it.
- Only use option, action and command names that appear in the documentation you read. Use
  placeholders such as <model> or <task-queue> for values you do not know.
- End with a "Sources" list of the pages you used, as links.

If the documentation does not cover the question, say so and suggest where to look next.
Politely decline questions that are not about Temporal or Charmed Temporal.

Charmed Temporal documentation pages (page_id: title):
"""

TOOL_RETRY = RetryPolicy(maximum_attempts=3)
TOOL_TIMEOUT = datetime.timedelta(seconds=30)


@dataclass
class AskDocsInput:
    question: str


@workflow.defn
class DocsQaWorkflow:
    @workflow.init
    def __init__(self, payload: AskDocsInput) -> None:
        # Created here, not in run(), so the stream is ready before anyone subscribes.
        self.stream = WorkflowStream()
        self.status = self.stream.topic(STATUS_TOPIC)

    @workflow.run
    async def run(self, payload: AskDocsInput) -> str:
        pages = await workflow.execute_activity(
            docs_tools.charmed_temporal_docs_index,
            start_to_close_timeout=TOOL_TIMEOUT,
            retry_policy=TOOL_RETRY,
        )
        page_list = "\n".join(f"- {page.page_id}: {page.title}" for page in pages)

        assistant = Agent(
            name="Charmed Temporal docs assistant",
            instructions=INSTRUCTIONS + page_list,
            # Without this, the SDK assumes OpenAI's default GPT-5 model and sends
            # verbosity="low", which other models (such as gpt-4.1-mini) reject.
            model_settings=ModelSettings(),
            tools=[
                activity_as_tool(tool, start_to_close_timeout=TOOL_TIMEOUT, retry_policy=TOOL_RETRY)
                for tool in (
                    docs_tools.search_temporal_docs,
                    docs_tools.read_temporal_docs_page,
                    docs_tools.read_charmed_temporal_docs,
                )
            ],
        )

        result = Runner.run_streamed(assistant, input=payload.question)
        async for event in result.stream_events():
            if event.type == "run_item_stream_event" and event.name == "tool_called":
                call = event.item.raw_item
                self.status.publish({"text": _tool_status(call.name, call.arguments)})
        return str(result.final_output)


def _tool_status(name: str, arguments: str) -> str:
    """Describe a tool call for the chat page, for example 'Searching the Temporal docs: ...'."""
    label = TOOL_STATUS.get(name, f"Using {name}")
    try:
        detail = next(iter(json.loads(arguments).values()), "")
    except (ValueError, AttributeError):
        detail = ""
    return f"{label}: {detail}" if isinstance(detail, str) and detail else label


def describe_failure(error_type: str | None, message: str, detail: str | None = None) -> str:
    """Turn the integration's 'Non retryable OpenAI status code: 401' into a useful hint.

    detail is the API's own explanation, from api_error_detail(), shown after the hint.
    """
    status = re.search(r"status code: (\d+)", message)
    if error_type == "APIStatusError" and status:
        code = int(status.group(1))
        if code in LLM_ERROR_HINTS:
            hint = LLM_ERROR_HINTS[code]
            if detail:
                hint += f" The API said: {detail}"
            elif code == 400:
                hint += f" {_MODEL_NAME_HINT}"
            return f"{hint} (HTTP {code})"
    return message


def api_error_detail(message: str) -> str | None:
    """Find the explanation in an OpenAI SDK error message, if it has one.

    The message looks like "Error code: 400 - {'error': {'message': ..., 'metadata': {'raw': ...}}}".
    OpenRouter's own message is often just "Provider returned error"; the model provider's
    explanation is then the JSON in metadata.raw.
    """
    _, separator, body = message.partition(" - ")
    if not separator:
        return None
    try:
        error = ast.literal_eval(body)["error"]
    except (ValueError, SyntaxError, TypeError, KeyError):
        return None
    if not isinstance(error, dict):
        return str(error)

    raw = (error.get("metadata") or {}).get("raw")
    if isinstance(raw, str) and raw.strip():
        try:
            upstream = json.loads(raw)
        except ValueError:
            return raw.strip()
        if isinstance(upstream, dict):
            inner = upstream.get("error", upstream)
            if isinstance(inner, dict) and inner.get("message"):
                return str(inner["message"])
            if isinstance(inner, str):
                return inner
        return raw.strip()
    return error.get("message")


class LlmProvider(ModelProvider):
    """Sends every model call to the configured OpenAI-compatible API."""

    def __init__(self, client: AsyncOpenAI, default_model: str) -> None:
        self._client = client
        self._default_model = default_model

    def get_model(self, model_name: str | None) -> OpenAIChatCompletionsModel:
        # A model object sends the name as-is. A plain string would be parsed by the SDK,
        # which strips or rejects provider prefixes such as "openai/" or "anthropic/".
        return OpenAIChatCompletionsModel(model=model_name or self._default_model, openai_client=self._client)


def agents_plugin() -> OpenAIAgentsPlugin:
    """Configure Temporal's OpenAI Agents integration for the configured LLM API."""
    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        raise SystemExit("LLM_API_KEY is not set. See 'Set your LLM API key' in README.md.")

    client = AsyncOpenAI(
        base_url=os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL,
        api_key=api_key,
        # Temporal retries failed model calls, so the client itself should not.
        max_retries=0,
        # OpenRouter shows this name in its usage dashboard.
        default_headers={"X-Title": "Charmed Temporal docs assistant"},
    )
    # The SDK would otherwise upload every run to OpenAI. Temporal already records each step.
    set_tracing_disabled(True)

    return OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=datetime.timedelta(seconds=60),
            # If the worker dies mid-answer, Temporal notices within this time and retries.
            heartbeat_timeout=datetime.timedelta(seconds=30),
            # Publish the model's output to the workflow's stream as it is written.
            streaming_topic=EVENTS_TOPIC,
        ),
        model_provider=LlmProvider(client, os.environ.get("LLM_MODEL") or DEFAULT_LLM_MODEL),
    )


async def connect_client(plugins: list | None = None) -> Client:
    target_host = os.environ["TEMPORAL_HOST"]
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "ai-assistant")
    return await Client.connect(target_host, namespace=namespace, plugins=plugins or [])


async def run_worker() -> None:
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "ai-docs-queue")

    client = await connect_client(plugins=[agents_plugin()])

    # The plugin on the client registers the model activities; register our tools here.
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[DocsQaWorkflow],
        activities=docs_tools.ACTIVITIES,
    )

    docs_refresher = asyncio.create_task(docs_tools.keep_docs_cached())
    try:
        await worker.run()
    finally:
        docs_refresher.cancel()


async def start_docs_qa(client: Client, question: str) -> WorkflowHandle:
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "ai-docs-queue")

    return await client.start_workflow(
        DocsQaWorkflow.run,
        AskDocsInput(question=question),
        id=f"docs-qa-{uuid.uuid4()}",
        task_queue=task_queue,
        execution_timeout=datetime.timedelta(seconds=180),
    )


async def ask_once(question: str) -> str:
    client = await connect_client()
    handle = await start_docs_qa(client, question)
    return await handle.result()


if __name__ == "__main__":
    # Run the worker from src.agent_worker rather than from this __main__ copy of the file.
    # The workflow sandbox re-imports the workflow's module by name, and it cannot do that
    # for __main__.
    from src.agent_worker import run_worker as run_imported_worker

    asyncio.run(run_imported_worker())

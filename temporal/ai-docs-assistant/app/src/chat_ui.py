"""FastAPI chat interface for the AI docs assistant workflow."""

import asyncio
import json
import logging
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.exceptions import ApplicationError

from .agent_worker import (
    EVENTS_TOPIC,
    STATUS_TOPIC,
    api_error_detail,
    connect_client,
    describe_failure,
    start_docs_qa,
)

CHAT_PAGE = Path(__file__).parent / "chat.html"

logger = logging.getLogger(__name__)

app = FastAPI(title="Charmed Temporal AI Assistant")

_client: Client | None = None
_client_lock = asyncio.Lock()


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: str
    workflow_id: str
    run_id: str
    duration_seconds: float


async def _get_client() -> Client:
    """Connect on first use so the page still loads when Temporal is unreachable."""
    global _client  # noqa: PLW0603
    async with _client_lock:
        if _client is None:
            _client = await connect_client()
        return _client


def _root_cause(exc: BaseException) -> str:
    """Explain why the workflow failed, skipping Temporal's wrapper errors.

    Prefers a hint for a known LLM API error anywhere in the chain of causes, with the
    API's own explanation from the errors it wraps, then the innermost error's message.
    """
    while True:
        if isinstance(exc, ApplicationError):
            hint = describe_failure(exc.type, exc.message, _api_detail(exc.cause))
            if hint != exc.message:
                return hint
        if getattr(exc, "cause", None) is None:
            return getattr(exc, "message", None) or str(exc)
        exc = exc.cause


def _api_detail(exc: BaseException | None) -> str | None:
    while exc is not None:
        detail = api_error_detail(getattr(exc, "message", None) or str(exc))
        if detail:
            return detail
        exc = getattr(exc, "cause", None)
    return None


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return CHAT_PAGE.read_text(encoding="utf-8")


@app.get("/config")
async def config() -> dict[str, str]:
    return {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "ai-assistant"),
        "temporal_ui_url": os.environ.get("TEMPORAL_UI_URL", "").rstrip("/"),
    }


async def _start(request: AskRequest) -> tuple[Client, WorkflowHandle]:
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Please enter a question.")
    if not os.environ.get("TEMPORAL_HOST"):
        raise HTTPException(status_code=500, detail="TEMPORAL_HOST is not set")

    try:
        client = await _get_client()
        return client, await start_docs_qa(client, question)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not start workflow: %s", exc)
        detail = (
            f"Could not reach Temporal at {os.environ['TEMPORAL_HOST']}. "
            "Check that it is reachable and that TEMPORAL_HOST is correct."
        )
        raise HTTPException(status_code=503, detail=detail) from exc


async def _follow_stream(client: Client, handle: WorkflowHandle):
    """Yield ("status", text) and ("text", answer so far) while the workflow runs.

    The model activity publishes its raw output events to the workflow's stream, and the
    workflow publishes a status event for each tool call.
    """
    stream = WorkflowStreamClient.create(client, workflow_id=handle.id)
    converter = client.data_converter.payload_converter
    text = ""
    async for item in stream.subscribe([EVENTS_TOPIC, STATUS_TOPIC], result_type=RawValue):
        event = converter.from_payload(item.data.payload)
        if item.topic == STATUS_TOPIC:
            yield "status", event["text"]
        elif event.get("type") == "response.created":
            # A new model call, or a retry of one: start the text again.
            text = ""
        elif event.get("type") == "response.output_text.delta":
            text += event["delta"]
            yield "text", text


@app.post("/ask")
async def ask(request: AskRequest) -> AskResponse:
    started = time.perf_counter()
    _, handle = await _start(request)

    failure = {"workflow_id": handle.id, "run_id": handle.result_run_id or ""}
    try:
        answer = await handle.result()
    except WorkflowFailureError as exc:
        detail = {"message": f"The workflow failed: {_root_cause(exc)}", **failure}
        raise HTTPException(status_code=502, detail=detail) from exc
    except Exception as exc:  # noqa: BLE001
        detail = {"message": str(exc), **failure}
        raise HTTPException(status_code=500, detail=detail) from exc

    return AskResponse(
        answer=answer,
        workflow_id=handle.id,
        run_id=handle.result_run_id or "",
        duration_seconds=round(time.perf_counter() - started, 2),
    )


@app.post("/ask/stream")
async def ask_stream(request: AskRequest) -> StreamingResponse:
    """Like /ask, but streams the answer as it is written, one JSON object per line.

    Lines are {"type": "status", "text": ...} when the agent uses a tool and
    {"type": "partial", "text": ...} with the answer so far, then one final
    {"type": "done", ...} with the same fields as /ask, or {"type": "error", ...}.
    """
    started = time.perf_counter()
    client, handle = await _start(request)
    ids = {"workflow_id": handle.id, "run_id": handle.result_run_id or ""}

    def line(payload: dict) -> str:
        return json.dumps(payload) + "\n"

    async def events():
        result = asyncio.ensure_future(handle.result())
        updates = _follow_stream(client, handle).__aiter__()
        update = None
        try:
            while not result.done():
                update = asyncio.ensure_future(updates.__anext__())
                await asyncio.wait({result, update}, return_when=asyncio.FIRST_COMPLETED)
                if not update.done():
                    break
                try:
                    kind, text = update.result()
                except StopAsyncIteration:
                    # The stream ends when the workflow does.
                    await asyncio.wait({result})
                    break
                except Exception as exc:  # noqa: BLE001
                    # The answer still arrives as the workflow's result.
                    logger.warning("Lost the stream of %s: %s", handle.id, exc)
                    await asyncio.wait({result})
                    break
                yield line({"type": "partial" if kind == "text" else "status", "text": text})

            try:
                answer = result.result()
            except WorkflowFailureError as exc:
                yield line({"type": "error", "message": f"The workflow failed: {_root_cause(exc)}", **ids})
                return
            except Exception as exc:  # noqa: BLE001
                yield line({"type": "error", "message": str(exc), **ids})
                return

            duration = round(time.perf_counter() - started, 2)
            yield line({"type": "done", "answer": answer, "duration_seconds": duration, **ids})
        finally:
            # Also runs when the page is closed mid-answer. The workflow itself keeps running.
            result.cancel()
            if update is not None and not update.done():
                update.cancel()
                await asyncio.gather(update, return_exceptions=True)
            await updates.aclose()

    return StreamingResponse(events(), media_type="application/x-ndjson")

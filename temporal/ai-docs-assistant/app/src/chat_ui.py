"""FastAPI chat interface for the AI docs assistant workflow."""

import asyncio
import logging
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import ApplicationError

from .agent_worker import connect_client, start_docs_qa

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
    """Return the activity's own error message, skipping Temporal's wrapper errors."""
    while not isinstance(exc, ApplicationError) and getattr(exc, "cause", None) is not None:
        exc = exc.cause
    return getattr(exc, "message", None) or str(exc)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return CHAT_PAGE.read_text(encoding="utf-8")


@app.get("/config")
async def config() -> dict[str, str]:
    return {
        "namespace": os.environ.get("TEMPORAL_NAMESPACE", "ai-assistant"),
        "temporal_ui_url": os.environ.get("TEMPORAL_UI_URL", "").rstrip("/"),
    }


@app.post("/ask")
async def ask(request: AskRequest) -> AskResponse:
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Please enter a question.")
    if not os.environ.get("TEMPORAL_HOST"):
        raise HTTPException(status_code=500, detail="TEMPORAL_HOST is not set")

    started = time.perf_counter()

    try:
        client = await _get_client()
        handle = await start_docs_qa(client, question)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not start workflow: %s", exc)
        detail = (
            f"Could not reach Temporal at {os.environ['TEMPORAL_HOST']}. "
            "Check that it is reachable and that TEMPORAL_HOST is correct."
        )
        raise HTTPException(status_code=503, detail=detail) from exc

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

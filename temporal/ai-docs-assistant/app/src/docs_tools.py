"""Documentation tools for the docs assistant. Each tool is a Temporal activity.

Sources:
- Temporal's documentation, through its public search endpoint and Markdown pages.
- Charmed Temporal's documentation on Charmhub, plus each charm's configuration and
  action reference. These pages are downloaded when the worker starts and kept fresh.
"""

import asyncio
import os
import re
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx
from temporalio import activity

TEMPORAL_DOCS_SEARCH_URL = "https://docs.temporal.io/api/retrieve"
TEMPORAL_DOCS_HOST = "docs.temporal.io"

# The Discourse topic behind https://charmhub.io/temporal-k8s/docs. Its "Navigation"
# table lists every page of the Charmed Temporal documentation.
CHARMED_TEMPORAL_DOCS_TOPIC = 8948
DISCOURSE_URL = "https://discourse.charmhub.io"

# Reference pages for every charm in a Charmed Temporal deployment.
CHARM_REFERENCE_PAGES = [
    ("charmhub/temporal-k8s/configure", "temporal-k8s configurations"),
    ("charmhub/temporal-k8s/actions", "temporal-k8s actions"),
    ("charmhub/temporal-worker-k8s/configure", "temporal-worker-k8s configurations"),
    ("charmhub/temporal-worker-k8s/actions", "temporal-worker-k8s actions"),
    ("charmhub/temporal-ui-k8s/configure", "temporal-ui-k8s configurations"),
    ("charmhub/temporal-admin-k8s/configure", "temporal-admin-k8s configurations"),
    ("charmhub/temporal-admin-k8s/actions", "temporal-admin-k8s actions"),
]

SEARCH_RESULTS = 6
PASSAGE_CHARS = 1500

# Shown to the docs sites so they can tell where requests come from.
USER_AGENT = "charmed-temporal-docs-assistant"


@dataclass
class DocsPage:
    page_id: str
    title: str
    url: str


# page_id -> page. Filled by refresh_charmed_temporal_docs().
_charm_pages: dict[str, DocsPage] = {}

# url -> (fetched_at, text). Kept in the worker process between activity runs.
_cache: dict[str, tuple[float, str]] = {}

# Held while the Charmed Temporal docs download, so a question asked during the
# startup download waits for it instead of starting a second one.
_refresh_lock = asyncio.Lock()


def _cache_ttl() -> float:
    return float(os.environ.get("DOCS_CACHE_TTL", "600"))


def _max_chars() -> int:
    return int(os.environ.get("DOCS_MAX_CHARS", "20000"))


def _http_client() -> httpx.AsyncClient:
    timeout = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
    return httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers={"User-Agent": USER_AGENT})


async def _get_text(client: httpx.AsyncClient, url: str, ttl: float) -> str:
    """Return the body of url, from the cache when it is younger than ttl seconds."""
    cached = _cache.get(url)
    if cached and time.monotonic() - cached[0] < ttl:
        return cached[1]

    response = await client.get(url)
    response.raise_for_status()
    _cache[url] = (time.monotonic(), response.text)
    return response.text


def _first_post(raw: str) -> str:
    """Keep the documentation post of a raw Discourse topic and drop the replies."""
    return raw.split("\n-------------------------\n", 1)[0]


def _parse_navigation(raw: str) -> list[DocsPage]:
    """Read the rows of the docs topic's Navigation table, such as

    | 2 | h-scaling | [Scaling](https://discourse.charmhub.io/t/10840) |
    """
    pages = []
    for title, url in re.findall(r"^\|\s*\d+\s*\|[^|]*\|\s*\[([^\]]+)\]\((https://[^)\s]+)\)", raw, re.MULTILINE):
        title = title.strip()
        parts = urlsplit(url)
        if parts.netloc == "charmhub.io":
            page_id = "charmhub" + parts.path
            # Reference rows are titled just "Actions" or "Configurations".
            title = f"{parts.path.split('/')[1]} {title.lower()}"
        else:
            topic = re.search(r"/(\d+)/?$", parts.path)
            if not topic:
                continue
            page_id = f"forum/{topic.group(1)}"
        pages.append(DocsPage(page_id=page_id, title=title, url=url))
    return pages


def _source_url(page_id: str) -> str:
    kind, _, rest = page_id.partition("/")
    if kind == "forum":
        return f"{DISCOURSE_URL}/raw/{rest}"
    return f"https://charmhub.io/{rest}.md"


async def _ensure_charm_pages() -> None:
    async with _refresh_lock:
        if not _charm_pages:
            await _refresh_charmed_temporal_docs()


async def refresh_charmed_temporal_docs() -> None:
    """Download the Charmed Temporal docs index and every page it lists."""
    async with _refresh_lock:
        await _refresh_charmed_temporal_docs()


async def _refresh_charmed_temporal_docs() -> None:
    async with _http_client() as client:
        raw = await _get_text(client, f"{DISCOURSE_URL}/raw/{CHARMED_TEMPORAL_DOCS_TOPIC}", ttl=0)
        pages = {page.page_id: page for page in _parse_navigation(_first_post(raw))}
        for page_id, title in CHARM_REFERENCE_PAGES:
            pages.setdefault(page_id, DocsPage(page_id, title, "https://charmhub.io/" + page_id.split("/", 1)[1]))

        results = await asyncio.gather(
            *(_get_text(client, _source_url(page_id), ttl=0) for page_id in pages),
            return_exceptions=True,
        )

    _charm_pages.clear()
    _charm_pages.update(pages)
    fetched = sum(not isinstance(result, Exception) for result in results)
    print(f"Docs cache: {fetched} of {len(pages)} Charmed Temporal pages cached.", flush=True)


async def keep_docs_cached() -> None:
    """Fetch the Charmed Temporal docs when the worker starts and refresh them before they expire.

    Questions then find these pages in the cache instead of waiting for a download.
    A page that fails to refresh keeps its previous copy.
    """
    while True:
        try:
            await refresh_charmed_temporal_docs()
        except Exception as exc:  # noqa: BLE001
            print(f"Docs cache: could not refresh the Charmed Temporal docs: {exc}", flush=True)
        await asyncio.sleep(_cache_ttl() * 0.9)


@activity.defn
async def charmed_temporal_docs_index() -> list[DocsPage]:
    """List the Charmed Temporal documentation pages that read_charmed_temporal_docs can open."""
    await _ensure_charm_pages()
    return list(_charm_pages.values())


@activity.defn
async def search_temporal_docs(question: str) -> str:
    """Search Temporal's documentation (docs.temporal.io, learn.temporal.io and the Temporal blog).

    Use it for questions about Temporal itself: Workflows, Activities, Workers, SDKs, the CLI,
    self-hosting. Ask a specific, complete question, naming the SDK language when relevant.
    Returns the most relevant passages, each with the URL of its page.
    """
    params = {"q": question, "src": "llms", "agent": USER_AGENT}
    try:
        async with _http_client() as client:
            response = await client.get(TEMPORAL_DOCS_SEARCH_URL, params=params)
            response.raise_for_status()
            results = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return f"The Temporal docs search failed: {exc}"

    if not results:
        return "No results. Try rephrasing the question."

    passages = [
        f"Source: {result.get('page_url', '')}\n{result.get('content', '')[:PASSAGE_CHARS]}"
        for result in results[:SEARCH_RESULTS]
    ]
    return "\n\n---\n\n".join(passages)


@activity.defn
async def read_temporal_docs_page(url: str) -> str:
    """Read a whole page from docs.temporal.io, for example one returned by search_temporal_docs.

    Use it when a search passage looks relevant but is incomplete.
    """
    parts = urlsplit(url)
    if parts.netloc != TEMPORAL_DOCS_HOST:
        return f"Only pages on https://{TEMPORAL_DOCS_HOST} can be read. Use the passages from the search instead."

    path = parts.path.rstrip("/") or "/index"
    if not path.endswith(".md"):
        path += ".md"
    try:
        async with _http_client() as client:
            text = await _get_text(client, f"https://{TEMPORAL_DOCS_HOST}{path}", ttl=_cache_ttl())
    except httpx.HTTPError as exc:
        return f"Could not read {url}: {exc}"
    return text[: _max_chars()]


@activity.defn
async def read_charmed_temporal_docs(page_id: str) -> str:
    """Read a page of the Charmed Temporal documentation: Canonical's Juju charms that deploy
    and operate Temporal on Kubernetes (temporal-k8s, temporal-worker-k8s, temporal-ui-k8s,
    temporal-admin-k8s).

    page_id must be one of the IDs in the list of Charmed Temporal pages, such as "forum/20314".
    """
    await _ensure_charm_pages()
    page = _charm_pages.get(page_id)
    if page is None:
        return f"Unknown page_id {page_id!r}. Use one of: {', '.join(_charm_pages)}"

    try:
        async with _http_client() as client:
            text = await _get_text(client, _source_url(page_id), ttl=_cache_ttl())
    except httpx.HTTPError as exc:
        return f"Could not read {page.url}: {exc}"
    return f"Source: {page.url}\n\n{_first_post(text)[: _max_chars()]}"


ACTIVITIES = [
    charmed_temporal_docs_index,
    search_temporal_docs,
    read_temporal_docs_page,
    read_charmed_temporal_docs,
]

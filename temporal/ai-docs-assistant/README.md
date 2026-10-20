# Charmed Temporal AI Docs Assistant Workshop

This workshop deploys Charmed Temporal and runs a documentation assistant on top
of it: a chat page where every question is answered by a Temporal workflow. The
workflow runs an activity that fetches documentation pages and asks an LLM
(through the OpenAI Agents SDK) to answer from them.

It follows the patterns in the [Temporal AI Cookbook](https://docs.temporal.io/ai-cookbook):
LLM calls run in activities, so Temporal retries them and records every step.

Estimated duration: about 45 minutes (15 minutes of deployment, then the exercises).

## Components

| Component | Purpose |
|-----------|---------|
| Temporal frontend, history, matching and worker services | The Temporal server, deployed as four `temporal-k8s` applications |
| Temporal UI | Web UI for inspecting workflow runs |
| Temporal admin | Admin tools; used here to create the Temporal namespace |
| PostgreSQL + PgBouncer | Temporal persistence and visibility databases |
| Docs assistant worker (`app/src/agent_worker.py`) | Runs `DocsQaWorkflow` and its activity, on your machine |
| Chat page (`app/src/chat_ui.py`, `app/src/chat.html`) | Starts one workflow per question and shows the answer |

## Prerequisites

- Canonical Kubernetes with a bootstrapped Juju controller
- `just` (`snap install --classic just`)
- `terraform` CLI (`snap install --classic terraform`)
- `kubectl` and `jq`
- `uv` (`snap install --classic astral-uv`)
- An OpenAI API key with a little credit. Each question costs roughly 0.002 USD
  with the default model, so the minimum top-up is plenty.

### Set your OpenAI API key

Save the key once, outside the repository, and load it in new terminals:

```bash
mkdir -p ~/.config/secrets
read -s OPENAI_API_KEY        # paste the key, press Enter
(umask 077; printf 'export OPENAI_API_KEY=%q\n' "$OPENAI_API_KEY" > ~/.config/secrets/openai.env)
echo '[ -f ~/.config/secrets/openai.env ] && . ~/.config/secrets/openai.env' >> ~/.bashrc
```

Only the worker needs the key. Never commit it.

## Quick Start

```bash
# Deploy Charmed Temporal and create the ai-assistant namespace (~15 min)
just deploy

# Terminal 1: forward the Temporal frontend to localhost:7233 (keep it running)
just forward

# Terminal 2: run the worker
just worker

# Terminal 3: run the chat page, then open http://localhost:8080
just chat

# Optional, terminal 4: Temporal UI on http://localhost:8088
just ui

# Tear down
just teardown
```

## Commands

| Command | Description |
|---------|-------------|
| `just deploy` | Create the Juju model, deploy Charmed Temporal, wait for it, create the namespace |
| `just forward` | Forward the Temporal frontend to `localhost:7233` |
| `just worker` | Run the docs assistant worker (needs `OPENAI_API_KEY`) |
| `just chat` | Run the chat page on `http://localhost:8080` |
| `just ui` | Forward the Temporal UI to `http://localhost:8088` |
| `just status` | Show `juju status` for the model |
| `just teardown` | Destroy the deployment, the model and the Terraform state |

## Exercises

### 1. Ask questions

Start with the suggested questions on the chat page, then try these:

| Question | What to expect |
|----------|----------------|
| What is the difference between a Workflow and an Activity? | A good answer: this is on the pages the assistant reads |
| How do I pass secrets to my worker with the `environment` config option? | A vague or "not enough context" answer: the details live deeper in the docs than the assistant reads |
| What is the capital of France? | A refusal: the prompt restricts it to the documentation |

### 2. Follow a question through Temporal

1. Run `just ui` and open `http://localhost:8088/namespaces/ai-assistant/workflows`.
2. Each question is one `DocsQaWorkflow` execution with its own Workflow ID.
3. Open one and read its **Event History**: the question as input, the
   `answer_docs_question` activity being scheduled, started and completed, and
   the answer as the result.

### 3. See durable execution

1. Ask a question and, while it is running, stop the worker with `Ctrl+C`.
2. In the Temporal UI the execution stays **Running** with the activity pending.
3. Start the worker again with `just worker`. The activity is retried and the
   answer appears in the chat page.

The workflow has a 120-second limit, so restart the worker promptly.

### 4. See a failing activity

Stop the worker, start it again with an invalid key
(`OPENAI_API_KEY=invalid just worker`) and ask a question. Watch the activity
fail and retry in the Temporal UI, then restart the worker with your real key.

## How It Works

1. **Terraform** deploys the charmed-temporal-solutions module into the
   `temporal-workshop` model, sized for a single node.
2. The **justfile** creates the `ai-assistant` Temporal namespace through the
   `temporal-admin` charm's `cli` action.
3. The **chat page** starts a `DocsQaWorkflow` execution for each question
   (Workflow ID `docs-qa-<uuid>`) and waits for its result.
4. The **worker** runs the workflow, which runs one activity,
   `answer_docs_question`:
   - it fetches the documentation pages in parallel and keeps them in memory
     for 10 minutes, so repeat questions skip the download;
   - it sends the question and the page text to the model with instructions to
     answer only from that text.
5. The answer is the workflow's result, which the chat page displays.

The LLM call happens only inside the activity, never in workflow code, because
workflow code must be deterministic. Each question is a separate workflow, so
the assistant does not remember earlier questions.

## Configuration

The worker and chat page read these environment variables. The justfile sets
the Temporal ones for you.

| Variable | Default | Used by | Purpose |
|----------|---------|---------|---------|
| `TEMPORAL_HOST` | `localhost:7233` | worker, chat | Temporal frontend address |
| `TEMPORAL_NAMESPACE` | `ai-assistant` | worker, chat | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `ai-docs-queue` | worker, chat | Task queue the worker polls |
| `OPENAI_API_KEY` | none | worker | OpenAI API key |
| `OPENAI_MODEL` | `gpt-4.1-mini` | worker | Model used for answers |
| `DOC_URLS` | three Temporal and Charmhub pages | chat | Comma-separated pages to answer from |
| `DOCS_CACHE_TTL` | `600` | worker | Seconds to keep fetched pages in memory |
| `DOCS_MAX_CHARS` | `12000` | worker | Maximum characters of page text sent to the model |
| `TEMPORAL_UI_URL` | `http://localhost:8088` (via `just chat`) | chat | Adds an "Open Temporal UI" link to the page |

## Limitations

- The assistant reads only the first 3,000 characters of each page, so it
  cannot answer detailed questions. Indexing the documentation into a vector
  store (for example PostgreSQL with the pgvector plugin, which
  `postgresql-k8s` supports) and retrieving the relevant passages per question
  is the natural next step.
- Answers do not cite the specific pages they came from.
- Questions are independent; there is no conversation memory.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| Temporal units show `error` during `just deploy` | A known race while the database schema is created (canonical/temporal-k8s-operator#133). The model retries failed hooks, so they recover on their own. |
| Chat page says it could not reach Temporal | `just forward` is not running, or `TEMPORAL_HOST` is wrong. |
| An answer never arrives and the page suggests checking the worker | The worker is not running, or is polling a different namespace or task queue. |
| Worker fails with `RestrictedWorkflowAccessError` | A library used only by activities was imported without `workflow.unsafe.imports_passed_through()`. Keep such imports inside that block in `agent_worker.py`. |

## File Structure

```
ai-docs-assistant/
├── justfile          # Deployment and demo automation
├── README.md
├── app/
│   ├── pyproject.toml
│   ├── uv.lock
│   └── src/
│       ├── agent_worker.py   # DocsQaWorkflow, its activity, and the worker
│       ├── chat_ui.py        # FastAPI server for the chat page
│       └── chat.html         # Chat page
└── terraform/
    ├── main.tf       # charmed-temporal-solutions module, sized for one node
    ├── variables.tf
    └── terraform.tf
```

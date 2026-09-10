# Charmed Airflow + Spark Workshop

This workshop allows you to deploy Charmed Apache Airflow 3.x and integrate it
with Charmed Apache Spark to be able to run Apache Spark jobs from a DAG.

## Components

| Component | Purpose |
|-----------|---------|
| Charmed Apache Airflow Coordinator | Distributes configurations to all components |
| KubernetesExecutor | Configures the Apache Airflow deployment to spawn worker Pods into a Kubernetes cluster |
| Charmed Apache Spark Integration Hub | Enables the integration of the Charmed Apache Spark and properly configures the corresponding options in the Apache Spark ecosystem |
| s3-integrator | Provides the S3 (MicroCeph) credentials used to store Spark event logs |
| Spark History Server | Web UI for inspecting completed and running Spark applications |
| git-integrator | Syncs DAGs from this Git repo into Airflow |
| PostgreSQL | Airflow metadata database |

## Prerequisites

- Canonical Kubernetes with a bootstrapped Juju controller
- `just` (`snap install --classic just`)
- `terraform` CLI (`snap install --classic terraform`)
- Python 3 with `cryptography` (`pip install cryptography`)
- MicroCeph with the RGW (S3) gateway enabled, plus `s3cmd` and `jq`

## Quick Start

```bash
# Deploy the full stack (~15 min)
just deploy

# Trigger a DAG (default: tiny_spark_demo)
just trigger
just trigger spark_session_demo
just trigger spark_fail_demo

# Watch Airflow worker pods spawn
just watch

# Watch Spark driver/executor pods
just watch-spark

# Print the Airflow UI address (pod IP:8080)
just get-ui-ip

# Print the Spark History Server address (pod IP:18080)
just get-spark-ui-ip

# Print the Airflow admin credentials
just get-api-server-creds

# Tear down (also removes the MicroCeph S3 bucket)
just teardown
```

## Commands

| Command | Description |
|---------|-------------|
| `just deploy` | Full deployment from scratch |
| `just trigger [DAG]` | Trigger a DAG (default `tiny_spark_demo`) |
| `just watch` | Live-watch Airflow worker pods |
| `just watch-spark` | Live-watch Spark driver/executor pods |
| `just status [DAG]` | Show latest run task states for a DAG |
| `just get-ui-ip` | Print the Airflow UI address (pod IP:8080) |
| `just get-spark-ui-ip` | Print the Spark History Server address (pod IP:18080) |
| `just get-api-server-creds` | Print Airflow API server admin credentials |
| `just teardown` | Destroy the model, namespaces, and the S3 bucket |

## The demo DAGs

The workshop ships three DAGs under [`dags/`](dags/):

| DAG | File | What it shows |
|-----|------|---------------|
| `tiny_spark_demo` | [dag_tiny_demo.py](dags/dag_tiny_demo.py) | Minimal `spark-submit` smoke test (prints the injected SA/namespace). |
| `spark_session_demo` | [dag_spark_session.py](dags/dag_spark_session.py) | Builds a full `SparkSession` / `SparkContext`, aggregates a range, and writes an event log to S3 (visible in the History Server). |
| `spark_fail_demo` | [dag_spark_fail.py](dags/dag_spark_fail.py) | Raises in the Spark driver so the task and DAG run end **failed** — useful to show how failures surface. |

Trigger any of them with `just trigger <dag_id>`.

What to expect when a Spark DAG runs:

- A short-lived Airflow worker Pod appears in `airflow-worker-namespace`
  (`just watch`). It submits the job in **cluster mode**, so Spark creates a
  driver Pod (`*-driver`) and executor Pods (`*-exec-*`) in the `airflow-spark`
  namespace (`just watch-spark`).
- Tasks turn green (or red for `spark_fail_demo`) in the Airflow UI.
- For `spark_session_demo`, a new application appears in the Spark History
  Server (`just get-spark-ui-ip`).

### Triggering and following a run from the UI

1. Run `just get-ui-ip` and open the printed `http://<pod-ip>:8080`
   (credentials from `just get-api-server-creds`).
2. Un-pause the DAG with the toggle on the left.
3. Press the **Trigger** (▶) button on the top right.
4. Open the run in the **Grid** view and click the Spark task, then open the
   Spark History Server (`just get-spark-ui-ip`) to inspect the application.

> The Airflow **task log** in the UI is often unavailable here: the worker Pod
> that ran the task is ephemeral and lives in another namespace, and no remote
> log store is configured. For the real job output, read the Spark driver Pod:
> `kubectl logs <*-driver> -n airflow-spark`.

> `get-ui-ip` / `get-spark-ui-ip` print the Pod IP, reachable from the host on a
> single-node cluster. For a shared or production deployment, expose the UIs
> through an ingress (e.g. Traefik) instead.

## Namespaces

The deployment uses three separate namespaces:

| Namespace | Contents |
|-----------|----------|
| `demo` | Charm Pods (coordinator, api-server, PostgreSQL, Spark Hub, s3-integrator, History Server) — this is the Juju model name |
| `airflow-worker-namespace` | Airflow worker Pods scheduled by the KubernetesExecutor |
| `airflow-spark` | Spark service account and Spark driver/executor Pods (fixed in the coordinator charm) |

## How It Works

1. **Terraform** deploys the charmed-airflow-solutions module with
   KubernetesExecutor, git-integrator, the Spark Integration Hub, the
   s3-integrator, and the Spark History Server — and wires all the relations
   between them (including `coordinator ↔ Spark Hub`).
2. The **justfile** creates the worker + Spark namespaces, provisions the
   MicroCeph S3 user/bucket, stages the PySpark job files (`spark_jobs/`) into
   S3, and creates/grants the Juju secrets (fernet key + S3 credentials). The
   cross-namespace RBAC the Spark submitter needs is provided by the executor
   charm.
3. The Spark Hub creates a ServiceAccount with RBAC and, from the s3-integrator
   relation, injects the S3 event-log settings and the Spark container image
   (`charmed-spark`) into the Spark configuration.
4. The executor injects `SPARK_NAMESPACE` and `SPARK_USERNAME` as env vars into
   worker Pods.
5. A Spark DAG's worker Pod runs `spark8t` in **cluster mode**: Spark creates a
   driver Pod (charmed-spark) in the Spark namespace, which spawns executor Pods
   and writes an event log to S3. The job `.py` is read from S3 (the
   airflow-spark worker image can't run Spark against S3 itself, so the job runs
   entirely inside the charmed-spark image).

## S3 layout (MicroCeph)

Spark artifacts live in one bucket, `spark-history`:

| Prefix | Contents | Written by |
|--------|----------|------------|
| `jobs/` | PySpark job scripts (`spark_jobs/*.py`) | justfile (`configure-microceph`) |
| `spark-events/` | Spark event logs, one dir per application | Spark drivers |

The bucket lives at the host level (outside Juju), so `just teardown` explicitly
empties and removes it.

## File Structure

```
airflow-spark/
├── justfile          # Deployment and demo automation
├── README.md
├── dags/
│   ├── dag_tiny_demo.py       # Minimal spark-submit smoke test
│   ├── dag_spark_session.py   # Full SparkSession job (event log → History Server)
│   └── dag_spark_fail.py      # Job that fails on purpose
├── spark_jobs/        # PySpark jobs staged into S3 and run in cluster mode
│   ├── session_job.py
│   └── fail_job.py
└── terraform/
    ├── main.tf       # Airflow + git-integrator + Spark Hub + s3 + History Server
    ├── variables.tf
    └── terraform.tf
```

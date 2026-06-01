# Charmed Airflow + Spark on Kubernetes

A production-style data analytics platform built entirely from Canonical's
charmed operators. This demo deploys Apache Airflow 3.x orchestrating parallel
Apache Spark jobs, with MicroCeph providing S3-compatible object storage
via RADOS Gateway (RGW).

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Host Machine                                │
│                                                                    │
│  ┌────────────┐   ┌──────────────────────────────────────────────┐ │
│  │  MicroCeph │   │             Canonical Kubernetes             │ │
│  │  (RGW/S3)  │   │                                              │ │
│  │  port 80   │   │  ┌─────────────────────────────────────────┐ │ │
│  │            │◄──┼──┤  Juju Model: airflow-spark              │ │ │
│  │  Buckets:  │   │  │                                         │ │ │
│  │  - airflow │   │  │  ┌─────────────┐   ┌──────────────┐    │ │ │
│  │    -spark  │   │  │  │ Coordinator │◄──┤ PostgreSQL   │    │ │ │
│  │            │   │  │  │   (hub)     │   │   (state)    │    │ │ │
│  │  Objects:  │   │  │  └──────┬──────┘   └──────────────┘    │ │ │
│  │  - DAG     │   │  │         │                               │ │ │
│  │    files   │   │  │    ┌────┴────┬──────────┬───────────┐   │ │ │
│  │  - Spark   │   │  │    ▼         ▼          ▼           ▼   │ │ │
│  │    results │   │  │  ┌────┐  ┌───────┐  ┌────────┐ ┌─────┐ │ │ │
│  │            │   │  │  │API │  │Schedu-│  │DAG     │ │Trig-│ │ │ │
│  │            │   │  │  │Srv │  │ler    │  │Process.│ │gerer│ │ │ │
│  │            │   │  │  └────┘  └───┬───┘  └────────┘ └─────┘ │ │ │
│  │            │   │  │              │ KubernetesExecutor        │ │ │
│  │            │   │  │         ┌────┴──────┐                   │ │ │
│  │            │   │  │         │  Executor │                   │ │ │
│  │            │   │  │         │  (config) │                   │ │ │
│  │            │   │  │         └─────┬─────┘                   │ │ │
│  │            │   │  │               │ spawns worker pods       │ │ │
│  │            │   │  │    ┌──────────┼──────────┐              │ │ │
│  │            │   │  │    ▼          ▼          ▼              │ │ │
│  │            │   │  │  ┌─────┐  ┌─────┐  ┌─────┐  (ephemeral│ │ │
│  │            │◄──┼──┤  │ W-1 │  │ W-2 │  │ W-3 │   workers) │ │ │
│  │            │   │  │  └──┬──┘  └──┬──┘  └──┬──┘             │ │ │
│  │            │   │  └─────┼────────┼────────┼─────────────────┘ │ │
│  │            │   │        │ KubernetesPodOperator                │ │
│  │            │   │  ┌─────┼────────┼────────┼─────────────────┐ │ │
│  │            │   │  │     ▼        ▼        ▼                 │ │ │
│  │            │◄──┼──┤  ┌─────┐ ┌─────┐ ┌─────┐  Namespace:   │ │ │
│  │            │   │  │  │Spark│ │Spark│ │Spark│  spark-       │ │ │
│  │            │   │  │  │ Job │ │ Job │ │ Job │  workloads    │ │ │
│  │            │   │  │  └─────┘ └─────┘ └─────┘               │ │ │
│  │            │   │  └─────────────────────────────────────────┘ │ │
│  └────────────┘   └──────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestrator** | Apache Airflow 3.x (Charmed) | DAG scheduling, task orchestration, UI |
| **Compute Engine** | Apache Spark 3.5 (Charmed) | Distributed data processing |
| **Object Storage** | MicroCeph (RADOS Gateway) | S3-compatible storage for DAGs, data, results |
| **Database** | PostgreSQL 14 (Charmed) | Airflow metadata, state, connections |
| **S3 Integration** | s3-integrator charm | Juju-native S3 credential management |
| **Spark Hub** | spark-integration-hub-k8s | Spark service account & configuration |
| **Container Runtime** | Canonical Kubernetes | Pod scheduling, networking |
| **Deployment** | Juju 3.6 | Charm lifecycle, relations, config |

## How It Works

### 1. Object Storage Layer (MicroCeph)

MicroCeph runs on the host machine as a snap, providing:
- **RADOS Gateway (RGW)** on port 80 — an S3-compatible HTTP API
- **Loop-backed OSDs** for storage (4GB × 3 in this demo)
- Accessible from K8s pods via the host's node IP (`10.0.0.78:80`)

The S3 bucket `airflow-spark` stores:
- `spark-events/dag_parallel_demo.py` — the DAG file (synced to Airflow via S3DagBundle)
- `demo/sales_50k.csv` — generated sales data (written by the ingest task)
- `demo/results/*` — Spark analytics output (Parquet files)

### 2. Coordination Layer (Juju + Charms)

The **Coordinator charm** is the central hub that:
- Collects cluster-wide config (fernet key, executor settings, S3 connection)
- Generates a unified `airflow.cfg` and distributes it to all components
- Manages the PostgreSQL connection for metadata
- Configures S3DagBundle so all components know where DAGs live

The **s3-integrator charm** provides S3 credentials via Juju relations —
when related to the coordinator, it automatically creates an Airflow
Connection object in the database with the MicroCeph endpoint, access key,
and secret key.

### 3. Execution Model

Airflow uses **KubernetesExecutor**, meaning:
- No long-running Celery workers — pods are created on-demand per task
- Each task runs in an isolated container with its own resources
- The **executor charm** provides a pod template that defines:
  - Init container: syncs DAG files from S3 before the task starts
  - Main container: runs the actual Airflow task
  - Environment variables: S3 connections, Airflow config

When a task uses **KubernetesPodOperator** (like the Spark jobs), it creates
a *second* pod in the `spark-workloads` namespace using the Charmed Spark
image, which has PySpark pre-installed and configured with S3A Hadoop
connectors.

### 4. The Demo Pipeline

```
ingest_data ──► analyze_categories ──┐
               analyze_regions    ───┤──► executive_summary
               analyze_trends    ───┘
```

| Task | What It Does | Pod Count |
|------|-------------|-----------|
| `ingest_data` | Generates 50K sales records, uploads CSV to MicroCeph S3 | 1 worker + 1 Spark |
| `analyze_categories` | PySpark: revenue by product category | 1 worker + 1 Spark |
| `analyze_regions` | PySpark: geographic sales distribution | 1 worker + 1 Spark |
| `analyze_trends` | PySpark: monthly time-series analysis | 1 worker + 1 Spark |
| `executive_summary` | PySpark: aggregates all results into final report | 1 worker + 1 Spark |

During the parallel phase (tasks 2a–2c), **6 pods** run simultaneously:
3 worker pods in `airflow-spark` + 3 Spark pods in `spark-workloads`.

## Prerequisites

- **Canonical Kubernetes** with a Juju controller bootstrapped
- **MicroCeph** snap installed with RGW enabled
- **just** command runner (`snap install --classic just` or `brew install just`)
- **s3cmd** for S3 operations (`pip install s3cmd`)
- **Python 3** with `cryptography` package (for Fernet key generation)

### MicroCeph Setup (one-time)

```bash
sudo snap install microceph
sudo snap refresh --hold microceph
sudo microceph cluster bootstrap
sudo microceph disk add loop,4G,3
sudo ceph config set global osd_pool_default_size 1
sudo ceph config set global osd_pool_default_min_size 1
sudo microceph enable rgw
sudo radosgw-admin user create \
  --uid=airflow \
  --display-name="Airflow S3 User" \
  --access-key=airflow-access-key \
  --secret-key=airflow-secret-key
```

## Quick Start

```bash
cd ~/Desktop/airflow-spark-demo

# Deploy everything (~10 minutes)
just deploy

# Trigger the analytics pipeline
just trigger

# Watch pods spawning in real-time (the demo money shot)
just watch          # worker pods
just watch-spark    # Spark pods

# Check task results
just status

# View analytics output
just logs-all

# Cleanup
just teardown
```

## Available Commands

| Command | Description |
|---------|-------------|
| `just deploy` | Full deployment from scratch |
| `just trigger` | Unpause and trigger the DAG |
| `just status` | Show task states for latest run |
| `just watch` | Live-watch worker pods |
| `just watch-spark` | Live-watch Spark pods |
| `just watch-all` | Snapshot of both namespaces |
| `just get-ui-ip` | Get Airflow UI address (admin/admin) |
| `just logs task=<name>` | View specific task output |
| `just logs-all` | View all task outputs |
| `just check-dag` | Verify DAG is detected |
| `just clean-pods` | Remove completed pods between runs |
| `just upload-dag` | Re-upload DAG to S3 |
| `just teardown` | Destroy Juju model and clean up |

## Known Workarounds

Three patches are applied automatically during `just deploy` to work around
bugs in the current charm revisions (3.1/edge):

1. **`patch_coordinator.py`** — Fixes `KeyError: tls_ca_chain` in
   `connection_manager.py` when S3 relation data doesn't include TLS fields
   (common with non-TLS S3 backends like MicroCeph RGW).

2. **`patch_executor.py`** — Adds `extra_env` (secret-backed environment
   variables) and `AIRFLOW_CONN_GIT_DEFAULT` to the init container in the
   pod template. Without this, the DAG bundle init container can't access
   S3 to sync DAG files.

3. **`patch_s3_worker.py`** — Injects the S3 connection URI as an
   environment variable into both init and main containers of the rendered
   worker pod template on the scheduler. The coordinator sets S3 connections
   in the Airflow database, but ephemeral worker pods need them as env vars.

## File Structure

```
airflow-spark-demo/
├── justfile                    # All deployment & demo automation
├── dag_parallel_demo.py        # The 5-task Spark analytics DAG
├── spark-rbac-manifest.yaml    # RBAC for Spark pods (ServiceAccount, Roles)
├── patches/
│   ├── patch_coordinator.py    # Fix tls_ca_chain KeyError
│   ├── patch_executor.py       # Add env vars to init container template
│   ├── patch_s3_worker.py      # Inject S3 conn into rendered worker template
│   └── get_latest_run_id.py    # Helper to query latest DAG run ID
└── README.md                   # This document
```

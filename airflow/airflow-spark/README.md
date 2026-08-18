
Deploy Airflow 3.x with Spark integration using Canonical's charmed operators.
Worker pods get Spark credentials injected automatically via the
Spark Integration Hub relation — no manual RBAC setup needed.

## Components

| Component | Purpose |
|-----------|---------|
| Airflow Coordinator | Generates and distributes `airflow.cfg` to all components |
| KubernetesExecutor | Spawns one worker pod per task (no long-running workers) |
| Spark Integration Hub | Creates a Spark ServiceAccount with RBAC, injects creds |
| git-integrator | Syncs DAGs from this Git repo into Airflow |
| PostgreSQL | Airflow metadata database |

## Prerequisites

- Canonical Kubernetes with a bootstrapped Juju controller
- `just` (`snap install --classic just`)
- `terraform` CLI
- Python 3 with `cryptography` (`pip install cryptography`)

## Quick Start

```bash
# Deploy the full stack (~15 min)
just deploy

# Trigger the demo DAG
just trigger

# Watch worker pods spawn
just watch

# Get the Airflow UI address
just get-ui-ip

# Tear down
just teardown
```

## Commands

| Command | Description |
|---------|-------------|
| `just deploy` | Full deployment from scratch |
| `just trigger` | Trigger the `tiny_spark_demo` DAG |
| `just watch` | Live-watch worker/spark pods |
| `just status` | Show latest DAG run task states |
| `just get-ui-ip` | Print Airflow UI address (admin/admin) |
| `just teardown` | Destroy model and clean up |

## How It Works

1. **Terraform** deploys the charmed-airflow-solutions module with KubernetesExecutor,
   plus git-integrator and Spark Integration Hub.
2. Local feature branches of the **coordinator** and **executor** charms are packed
   and refreshed (they add the `spark-service-account` relation endpoint).
3. The coordinator relates to the Spark Hub, which creates a ServiceAccount with
   RBAC permissions. The coordinator passes `{spark_namespace, spark_username}` via
   `extra_data` to the executor.
4. The executor injects `SPARK_NAMESPACE` and `SPARK_USERNAME` as env vars into
   worker pods. DAG code reads these with `os.environ`.

## File Structure

```
airflow-spark/
├── justfile          # Deployment and demo automation
├── README.md
├── dags/
│   └── dag_tiny_demo.py   # Demo DAG using Spark env vars
└── terraform/
    ├── main.tf       # Charmed Airflow + git-integrator + Spark Hub
    ├── variables.tf
    └── terraform.tf
```

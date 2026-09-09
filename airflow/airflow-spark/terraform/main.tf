# Charmed Airflow + Spark workshop deployment
#
# Stack:
#   - charmed-airflow-solutions (Airflow 3.x + PostgreSQL + pgbouncer)
#     with KubernetesExecutor
#   - git-integrator (DAG bundle source: workflows-team-workshops repo)
#   - spark-integration-hub-k8s (Spark SA + RBAC + config Secret)
#   - s3-integrator (MicroCeph S3 credentials for Spark event logs)
#   - spark-history-server-k8s (UI for completed/running Spark apps)
#
# DAG files live at:
#   https://github.com/canonical/workflows-team-workshops
#   → airflow/airflow-spark/dags/

module "charmed_airflow" {
  source     = "git::https://github.com/canonical/charmed-airflow-solutions//modules/charmed-airflow?ref=track/3.1"
  model_uuid = var.model_uuid

  executor = "kubernetes"

  airflow_coordinator = {
    config = {
      fernet_key_secret = var.fernet_key_secret
    }
  }

  airflow_kubernetes_executor = {
    config = {
      namespace  = "airflow-worker-namespace" # worker Pods; kept separate from the Spark namespace
      base_image = "ghcr.io/canonical/airflow-spark:3.1.8"
    }
  }
}

# ---------------------------------------------------------------------------
# git-integrator — distributes the workshop repo as an Airflow DAG bundle.
# Airflow picks up any *.py files under the configured path as DAGs.
# ---------------------------------------------------------------------------
module "git_integrator" {
  source     = "git::https://github.com/canonical/git-integrator//terraform?ref=git-integrator-rev5"
  model_uuid = var.model_uuid
  channel    = "1.0/edge"
  config = {
    repository_url = "https://github.com/canonical/workflows-team-workshops"
    tracking_ref   = "feature/airflow-spark"
    path           = "./airflow/airflow-spark/dags/"
  }
}

# Relate git-integrator to the Airflow coordinator so the coordinator creates
# a GitDagBundle pointing at the workshop repo.
resource "juju_integration" "coordinator_git" {
  model_uuid = var.model_uuid
  application {
    name     = module.charmed_airflow.applications.airflow.coordinator.application.name
    endpoint = "git"
  }
  application {
    name     = module.git_integrator.application.name
    endpoint = module.git_integrator.provides.git
  }
}

# ---------------------------------------------------------------------------
# Spark Integration Hub — creates the Spark ServiceAccount + RBAC + config
# Secret and wires the S3 event-log settings into it.
# ---------------------------------------------------------------------------
resource "juju_application" "spark_hub" {
  name       = "spark-integration-hub-k8s"
  model_uuid = var.model_uuid
  trust      = true
  charm {
    name    = "spark-integration-hub-k8s"
    channel = "3/stable"
  }
  config = {
    # Image used for Spark driver + executor Pods. The airflow-spark worker
    # image can't run Spark on K8s, so jobs run in this charmed-spark image
    # (cluster mode). Keep it version-consistent for driver and executors.
    spark-image = var.spark_image
  }
}

# The Hub shares the Spark SA identity with the coordinator, which injects
# SPARK_USERNAME / SPARK_NAMESPACE into every worker Pod.
resource "juju_integration" "coordinator_spark_hub" {
  model_uuid = var.model_uuid
  application {
    name     = module.charmed_airflow.applications.airflow.coordinator.application.name
    endpoint = "spark-service-account"
  }
  application {
    name     = juju_application.spark_hub.name
    endpoint = "spark-service-account"
  }
}

# ---------------------------------------------------------------------------
# s3-integrator — provides the MicroCeph S3 credentials that back Spark event
# logging. It feeds the Hub (so jobs write logs) and the History Server (so it
# reads them). Credentials are supplied through a Juju user secret whose URI is
# passed in via var.s3_credentials_secret and granted by the justfile.
# ---------------------------------------------------------------------------
resource "juju_application" "s3_integrator" {
  name       = "s3-integrator"
  model_uuid = var.model_uuid
  charm {
    name    = "s3-integrator"
    channel = "2/stable"
  }
  config = {
    bucket      = var.s3_bucket
    path        = var.s3_path
    endpoint    = var.s3_endpoint
    region      = "us-east-1"
    credentials = var.s3_credentials_secret
  }
}

# ---------------------------------------------------------------------------
# Spark History Server — web UI (port 18080) that renders completed and running
# Spark applications by reading their event logs from the same S3 bucket.
# ---------------------------------------------------------------------------
resource "juju_application" "spark_history" {
  name       = "spark-history-server-k8s"
  model_uuid = var.model_uuid
  charm {
    name    = "spark-history-server-k8s"
    channel = "4/edge"
  }
}

resource "juju_integration" "s3_to_hub" {
  model_uuid = var.model_uuid
  application {
    name     = juju_application.s3_integrator.name
    endpoint = "s3-credentials"
  }
  application {
    name     = juju_application.spark_hub.name
    endpoint = "s3-credentials"
  }
}

resource "juju_integration" "s3_to_history" {
  model_uuid = var.model_uuid
  application {
    name     = juju_application.s3_integrator.name
    endpoint = "s3-credentials"
  }
  application {
    name     = juju_application.spark_history.name
    endpoint = "s3-credentials"
  }
}

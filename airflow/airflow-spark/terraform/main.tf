# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Charmed Airflow + Spark workshop deployment
#
# Stack:
#   - charmed-airflow-solutions (Airflow 3.x + PostgreSQL + pgbouncer)
#     with KubernetesExecutor
#   - git-integrator (DAG bundle source: workflows-team-workshops repo)
#   - spark-integration-hub-k8s (Spark SA + RBAC + config Secret)
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
      namespace  = "airflow-spark"
      base_image = "apache/airflow:3.1.8"
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
  config = {
    repository_url = "https://github.com/canonical/workflows-team-workshops"
    tracking_ref   = "main"
    path           = "airflow/airflow-spark/dags"
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
# Spark Integration Hub — auto-creates the Spark SA, RBAC, and config Secret.
# The coordinator reads namespace + username from the relation and distributes
# them as SPARK_NAMESPACE / SPARK_USERNAME env vars in worker pods.
# ---------------------------------------------------------------------------
resource "juju_application" "spark_hub" {
  name       = "spark-integration-hub-k8s"
  model_uuid = var.model_uuid
  trust      = true
  charm {
    name    = "spark-integration-hub-k8s"
    channel = "3/stable"
  }
}

resource "juju_integration" "coordinator_spark" {
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

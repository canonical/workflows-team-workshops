locals {
  worker_configs = [
    {
      "name" : "worker-python",
      "image" : "localhost:5000/worker-python:dev",
      "queue" : "worker-python-queue",
      "namespace" : "worker-python-namespace"
    },
    {
      "name" : "worker-go",
      "image" : "localhost:5000/worker-go:dev",
      "queue" : "worker-go-queue",
      "namespace" : "worker-go-namespace"
    }
  ]
}

resource "juju_model" "temporal_server_model" {
  name = var.server_model

  cloud {
    name   = "k8s"
    region = "localhost"
  }

  credential = ""
  config = {
    logging-config              = "<root>=INFO"
    development                 = true
    no-proxy                    = "jujucharms.com"
    update-status-hook-interval = "5m"
  }
}

resource "juju_model" "temporal_worker_model" {
  name = var.worker_model

  cloud {
    name   = "k8s"
    region = "localhost"
  }

  credential = ""
  config = {
    logging-config              = "<root>=INFO"
    development                 = true
    no-proxy                    = "jujucharms.com"
    update-status-hook-interval = "5m"
  }
}

resource "juju_model" "cos_model" {
  name = var.cos_model

  cloud {
    name   = "k8s"
    region = "localhost"
  }

  credential = ""
  config = {
    logging-config              = "<root>=INFO"
    development                 = true
    no-proxy                    = "jujucharms.com"
    update-status-hook-interval = "5m"
  }
}

module "charmed-temporal" {
  # tflint-ignore: terraform_module_pinned_source
  source            = "git::https://github.com/canonical/charmed-temporal-solutions//modules/charmed-temporal?ref=track/1.23"
  model             = juju_model.temporal_server_model.name
  cos_configuration = true
}

module "temporal-worker" {
  for_each = {
    for worker_config in local.worker_configs : worker_config.name => worker_config
  }

  # tflint-ignore: terraform_module_pinned_source
  source = "git::https://github.com/canonical/temporal-worker-k8s-operator//terraform?ref=track/1.0"
  model  = juju_model.temporal_worker_model.name
  image = {
    "image" : each.value.image
  }
  channel = "1.0/edge"
  config = {
    "host" : module.charmed-temporal.applications.temporal.frontend.app_name,
    "queue" : each.value.queue,
    "namespace" : each.value.namespace
  }
}

module "cos-lite" {
  # tflint-ignore: terraform_module_pinned_source
  source       = "git::https://github.com/canonical/observability-stack//terraform/cos-lite?ref=tf-provider-v0"
  model        = juju_model.cos_model.name
  channel      = "1/stable"
  internal_tls = false
}

resource "juju_integration" "grafana-agent-to-cos-grafana" {
  model = juju_model.temporal_server_model.name

  application {
    name     = module.charmed-temporal.grafana_agent_k8s.app_name
    endpoint = "grafana-dashboards-provider"
  }
  application {
    name     = "grafana"
    endpoint = one(module.cos-lite.offers.grafana_dashboards.endpoints)
  }
}

resource "juju_integration" "grafana-agent-to-cos-loki" {
  model = juju_model.temporal_server_model.name

  application {
    name     = module.charmed-temporal.grafana_agent_k8s.app_name
    endpoint = "logging-provider"
  }
  application {
    name     = "grafana"
    endpoint = one(module.cos-lite.offers.loki_logging.endpoints)
  }
}

resource "juju_integration" "grafana-agent-to-cos-prometheus" {
  model = juju_model.temporal_server_model.name

  application {
    name     = "prometheus"
    endpoint = one(module.cos-lite.offers.prometheus_receive_remote_write.endpoints)
  }
  application {
    name     = module.charmed-temporal.grafana_agent_k8s.app_name
    endpoint = "metrics-endpoint"
  }
}

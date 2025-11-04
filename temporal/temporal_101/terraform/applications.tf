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

  worker_grafana_agent_k8s_name = "grafana-agent-k8s"
}

provider "juju" {
  controller_addresses = var.controller_addresses
  username = "admin"
  password = var.password
  ca_certificate = file("./ca-cert.pem")
}

resource "juju_model" "temporal_server_model" {
  name = var.server_model

  cloud {
    name = "k8s"
  }

  config = {
    automatically-retry-hooks = true
  }
}

resource "juju_model" "temporal_worker_model" {
  name = var.worker_model

  cloud {
    name = "k8s"
  }

  config = {
    automatically-retry-hooks = true
  }
}

resource "juju_model" "cos_model" {
  name = var.cos_model

  cloud {
    name = "k8s"
  }

  config = {
    automatically-retry-hooks = true
  }
}

module "charmed-temporal" {
  # tflint-ignore: terraform_module_pinned_source
  source            = "git::https://github.com/canonical/charmed-temporal-solutions//modules/charmed-temporal?ref=fix/expose_grafana_agent"
  model_uuid             = juju_model.temporal_server_model.id
  cos_configuration = true
}

module "temporal-worker" {
  for_each = {
    for worker_config in local.worker_configs : worker_config.name => worker_config
  }

  # tflint-ignore: terraform_module_pinned_source
  source = "git::https://github.com/canonical/temporal-worker-k8s-operator//terraform?ref=fix/switch_model"
  model_uuid  = juju_model.temporal_worker_model.id
  app_name = each.value.name
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

resource "juju_application" "worker_grafana_agent_k8s" {
  name  = local.worker_grafana_agent_k8s_name
  model_uuid = juju_model.temporal_worker_model.id
  charm {
    name    = "grafana-agent-k8s"
    channel = "1/stable"
  }
  trust  = true
  units  = 1
}

module "cos-lite" {
  # tflint-ignore: terraform_module_pinned_source
  source       = "git::https://github.com/canonical/observability-stack//terraform/cos-lite?ref=main"
  model_uuid        = juju_model.cos_model.id
  channel      = "1/stable"
  internal_tls = false
}

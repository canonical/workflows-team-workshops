
# Server COS Integrations

resource "juju_integration" "grafana-agent-to-cos-grafana" {
  model_uuid = juju_model.temporal_server_model.id

  application {
    name     = module.charmed-temporal.grafana_agent_k8s.app_name
    endpoint = "grafana-dashboards-provider"
  }
  application {
    offer_url = module.cos-lite.offers.grafana_dashboards.url
  }
}

resource "juju_integration" "grafana-agent-to-cos-loki" {
  model_uuid = juju_model.temporal_server_model.id

  application {
    name     = module.charmed-temporal.grafana_agent_k8s.app_name
    endpoint = "logging-consumer"
  }
  application {
    offer_url = module.cos-lite.offers.loki_logging.url
  }
}

resource "juju_integration" "grafana-agent-to-cos-prometheus" {
  model_uuid = juju_model.temporal_server_model.id

  application {
    offer_url = module.cos-lite.offers.prometheus_receive_remote_write.url
  }
  application {
    name     = module.charmed-temporal.grafana_agent_k8s.app_name
    endpoint = "send-remote-write"
  }
}

# # Worker to Grafana Agent

# resource "juju_integration" "worker_to_grafana_agent_grafana" {
#   model_uuid = juju_model.temporal_worker_model.id
#   application {
#     name     = local.worker_grafana_agent_k8s_name
#     endpoint = "metrics-endpoint"
#   }
#   application {
#     name     = "worker-python"
#     endpoint = "metrics-endpoint"
#   }
# }

# resource "juju_integration" "worker_to_grafana_agent_loki" {
#   model_uuid = juju_model.temporal_worker_model.id
#   application {
#     name     = local.worker_grafana_agent_k8s_name
#     endpoint = "logging-provider"
#   }
#   application {
#     name     = "worker-python"
#     endpoint = "logging"
#   }
# }

# resource "juju_integration" "worker_to_grafana_agent_prometheus" {
#   model_uuid = juju_model.temporal_worker_model.id
#   application {
#     name     = local.worker_grafana_agent_k8s_name
#     endpoint = "grafana-dashboards-consumer"
#   }
#   application {
#     name     = "worker-python"
#     endpoint = "grafana-dashboard"
#   }
# }

# # Worker COS Integrations

# resource "juju_integration" "worker-grafana-agent-to-cos-grafana" {
#   model_uuid = juju_model.temporal_worker_model.id

#   application {
#     name     = juju_application.worker_grafana_agent_k8s.name
#     endpoint = "grafana-dashboards-provider"
#   }
#   application {
#     offer_url = module.cos-lite.offers.grafana_dashboards.url
#   }
# }

# resource "juju_integration" "worker-grafana-agent-to-cos-loki" {
#   model_uuid = juju_model.temporal_worker_model.id

#   application {
#     name     = juju_application.worker_grafana_agent_k8s.name
#     endpoint = "logging-consumer"
#   }
#   application {
#     offer_url = module.cos-lite.offers.loki_logging.url
#   }
# }

# resource "juju_integration" "worker-grafana-agent-to-cos-prometheus" {
#   model_uuid = juju_model.temporal_worker_model.id

#   application {
#     offer_url = module.cos-lite.offers.prometheus_receive_remote_write.url
#   }
#   application {
#     name     = juju_application.worker_grafana_agent_k8s.name
#     endpoint = "send-remote-write"
#   }
# }

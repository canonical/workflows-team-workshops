# Charmed Temporal AI docs assistant workshop deployment
#
# Stack:
#   - charmed-temporal-solutions (Temporal frontend, history, matching and
#     worker services, UI, admin tools, PostgreSQL and PgBouncer)
#
# The assistant itself (worker + chat page) runs on the host from ../app and
# connects to the Temporal frontend through a port-forward.

module "charmed_temporal" {
  source     = "git::https://github.com/canonical/charmed-temporal-solutions//modules/charmed-temporal?ref=track/1.23"
  model_uuid = var.model_uuid

  # Sized for a single-node workshop cluster.
  # num-history-shards cannot be changed after the first deployment.
  temporal_server = {
    config = {
      num-history-shards         = "1"
      persistence-max-conns      = "6"
      persistence-max-idle-conns = "4"
      visibility-max-conns       = "3"
      visibility-max-idle-conns  = "2"
    }
  }

  pgbouncer = {
    config = {
      max_db_connections = "25"
    }
  }

  postgresql = {
    config = {
      profile = "testing"
    }
  }
}

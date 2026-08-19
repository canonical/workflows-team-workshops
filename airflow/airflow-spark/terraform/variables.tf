variable "model_uuid" {
  description = "UUID of the Juju model to deploy into."
  type        = string
}

variable "fernet_key_secret" {
  description = "URI of the Juju user secret containing the Airflow fernet key."
  type        = string
}

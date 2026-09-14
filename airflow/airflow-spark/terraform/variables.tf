variable "model_uuid" {
  description = "UUID of the Juju model to deploy into."
  type        = string
}

variable "fernet_key_secret" {
  description = "URI of the Juju user secret containing the Airflow fernet key."
  type        = string
}

variable "s3_credentials_secret" {
  description = "URI of the Juju user secret holding the S3 access-key/secret-key."
  type        = string
}

variable "s3_endpoint" {
  description = "S3 endpoint URL of the MicroCeph RGW gateway (e.g. http://10.0.0.79:80)."
  type        = string
}

variable "s3_bucket" {
  description = "S3 bucket that stores Spark event logs."
  type        = string
  default     = "spark-history"
}

variable "s3_path" {
  description = "Path/prefix inside the bucket for Spark event logs."
  type        = string
  default     = "spark-events"
}

variable "spark_image" {
  description = "Container image for Spark driver/executor Pods (must ship Spark on K8s)."
  type        = string
  default     = "ghcr.io/canonical/charmed-spark:4.0-22.04_edge"
}

variable "server_model" {
  description = "Name of model to deploy Temporal server applications in"
  type        = string
  default     = "temporal-server"
}

variable "worker_model" {
  description = "Name of model to deploy Temporal worker applications in"
  type        = string
  default     = "temporal-worker"
}

variable "cos_model" {
  description = "Name of model to deploy COS lite in"
  type        = string
  default     = "cos-lite"
}

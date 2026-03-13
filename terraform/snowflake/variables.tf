variable "snowflake_account" {
  description = "QEZTSCG-WG63257"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  default     = "svc_terraform"
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}
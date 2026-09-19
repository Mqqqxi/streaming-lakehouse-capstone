variable "environment" {
  type = string
}

variable "shard_count" {
  type    = number
  default = 2
}

variable "datalake_bucket_arn" {
  type = string
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "region" {
  type    = string
  default = "us-east-2"  # region confirmada y probada en la Clase 5
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "shard_count" {
  type    = number
  default = 2
}

variable "redshift_admin_password" {
  type      = string
  sensitive = true
  # No poner un valor por defecto aca. Pasarlo con:
  # terraform apply -var="redshift_admin_password=TuPasswordSegura123!"
}

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Completar con los valores que devolvió bootstrap-backend.sh
  # NOTA: en Terraform >= 1.10 el locking usa use_lockfile (nativo de S3),
  # ya no dynamodb_table. Si tu versión de Terraform es anterior a 1.10,
  # volvé a agregar la línea dynamodb_table = "..." en su lugar.
  backend "s3" {
    bucket       = "maxi-terraform-state-dev-nueva123"
    key          = "dev/terraform.tfstate"
    region       = "us-east-2"   # ej: us-east-2 (sin comillas de sobra, minúsculas)
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region
}

module "network" {
  source      = "../../modules/network"
  environment = var.environment
  region      = var.region
  vpc_cidr    = var.vpc_cidr
}

module "ingestion" {
  source              = "../../modules/ingestion"
  environment         = var.environment
  shard_count         = var.shard_count
  datalake_bucket_arn = module.network.datalake_bucket_arn
}

module "identity" {
  source              = "../../modules/identity"
  environment         = var.environment
  datalake_bucket_arn = module.network.datalake_bucket_arn
  kinesis_stream_arn  = module.ingestion.kinesis_stream_arn
}

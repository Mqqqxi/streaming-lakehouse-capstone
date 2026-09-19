# ── Lakehouse: Iceberg + Glue (Semana 5 — lo nuevo de hoy) ────────

resource "aws_glue_catalog_database" "lakehouse_db" {
  name        = "lakehouse_db"
  description = "Metastore central para las tablas Iceberg del pipeline de streaming"
}

resource "aws_s3_bucket_versioning" "datalake_versioning" {
  bucket = module.network.datalake_bucket_name

  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "flink_glue_access" {
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse_db.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse_db.name}/*",
    ]
  }

  statement {
    sid    = "LakehouseS3Access"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      module.network.datalake_bucket_arn,
      "${module.network.datalake_bucket_arn}/lakehouse/*",
    ]
  }
}

resource "aws_iam_role_policy" "flink_glue_policy" {
  name   = "flink-glue-lakehouse-access"
  role   = module.identity.flink_execution_role_name
  policy = data.aws_iam_policy_document.flink_glue_access.json
}

data "aws_caller_identity" "current" {}

output "lakehouse_db_name" {
  value = aws_glue_catalog_database.lakehouse_db.name
}

output "lakehouse_table_path" {
  value = "s3://${module.network.datalake_bucket_name}/lakehouse/events/"
}

output "datalake_bucket_name" {
  value = module.network.datalake_bucket_name
}

output "kinesis_stream_name" {
  value = module.ingestion.kinesis_stream_name
}

output "flink_app_name" {
  value = aws_kinesisanalyticsv2_application.flink_job.name
}

# ── Módulo de ingesta (Semana 2) ──────────────────────────────────

resource "aws_kinesis_stream" "main" {
  name             = "${var.environment}-events-stream"
  shard_count      = var.shard_count
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = { Name = "${var.environment}-events-stream" }
}

# ── Rol de Firehose (distinto del rol de Flink) ──
data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose_role" {
  name               = "${var.environment}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
}

data "aws_iam_policy_document" "firehose_permissions" {
  statement {
    sid    = "ReadKinesis"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.main.arn]
  }

  statement {
    sid    = "WriteS3"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.datalake_bucket_arn,
      "${var.datalake_bucket_arn}/*",
    ]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose_policy" {
  name   = "${var.environment}-firehose-permissions"
  role   = aws_iam_role.firehose_role.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}

resource "aws_kinesis_firehose_delivery_stream" "main" {
  name        = "${var.environment}-firehose-bronze"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = var.datalake_bucket_arn
    prefix             = "bronze/ingesta/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "bronze/errors/"
    buffering_size     = 5
    buffering_interval = 60
  }

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.main.arn
    role_arn            = aws_iam_role.firehose_role.arn
  }
}

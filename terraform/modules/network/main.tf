# ── Módulo de red (Semana 1) ──────────────────────────────────────

resource "aws_vpc" "data_vpc" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "${var.environment}-data-vpc" }
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_subnet" "private" {
  # 3 subredes, no 2: Redshift Serverless (Sem 6) exige un minimo de
  # 3 subredes en 3 AZs distintas para crear su workgroup. Con 2
  # alcanzaba para Flink/Kinesis, pero no para Redshift.
  count             = 3
  vpc_id            = aws_vpc.data_vpc.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags = { Name = "${var.environment}-private-${count.index}" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.data_vpc.id
  tags   = { Name = "${var.environment}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# S3 Gateway Endpoint — evita NAT Gateway y costos de transferencia
# para que Kinesis Firehose / Flink escriban en S3 sin salir a internet
resource "aws_vpc_endpoint" "s3" {
  vpc_id          = aws_vpc.data_vpc.id
  service_name    = "com.amazonaws.${var.region}.s3"
  route_table_ids = [aws_route_table.private.id]
  tags            = { Name = "${var.environment}-s3-endpoint" }
}

# Bucket del data lake — usado por Firehose (Sem 2), Flink checkpoints
# (Sem 4) y como warehouse de Iceberg (Sem 5)
resource "aws_s3_bucket" "datalake" {
  bucket = "${var.environment}-datalake-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${var.environment}-datalake" }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

data "aws_caller_identity" "current" {}

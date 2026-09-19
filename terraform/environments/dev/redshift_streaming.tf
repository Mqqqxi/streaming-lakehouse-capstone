# ─────────────────────────────────────────────────────────────────
# CLASE 6 — Redshift Streaming Ingestion
# Segundo consumidor del MISMO Kinesis Stream de la Semana 2, en
# paralelo a Flink. No reemplaza a Flink/Iceberg (Sem 4-5): los
# complementa. Flink persiste el historico gobernado; Redshift
# expone los datos calientes (ultimos segundos) como SQL.
#
# NO PROBADO EN VIVO todavia (a diferencia del resto del stack) --
# correlo con anticipacion antes de la clase y ajusta lo que haga
# falta, especialmente la sintaxis SQL de streaming ingestion, que
# varia segun la version del motor de Redshift.
# ─────────────────────────────────────────────────────────────────

# ── Rol de IAM para Redshift (separado del rol de Flink) ──────────
data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      # CONFIRMADO en la documentacion de AWS: Redshift Serverless
      # exige AMBOS principals en la trust policy, no solo uno --
      # redshift-serverless.amazonaws.com asocia el rol al namespace,
      # pero es redshift.amazonaws.com quien realmente lo asume en
      # tiempo de ejecucion al correr streaming ingestion.
      identifiers = [
        "redshift-serverless.amazonaws.com",
        "redshift.amazonaws.com",
      ]
    }
  }
}

resource "aws_iam_role" "redshift_streaming_role" {
  name               = "${var.environment}-redshift-streaming-ingest-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json
}

data "aws_kms_alias" "kinesis" {
  name = "alias/aws/kinesis"
}

data "aws_iam_policy_document" "redshift_streaming_permissions" {
  statement {
    sid    = "KinesisStreamingRead"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [module.ingestion.kinesis_stream_arn] # el MISMO stream de la Sem 2
  }

  # El stream de Kinesis esta cifrado con KMS (encryption_type = "KMS"
  # en modules/ingestion). Sin este permiso, Redshift puede describir
  # y listar el stream, pero no logra descifrar el contenido real de
  # los registros -- el sintoma es "Stream returned no new data" en
  # el refresh de la Materialized View, sin ningun error explicito.
  statement {
    sid       = "KinesisKmsDecrypt"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [data.aws_kms_alias.kinesis.target_key_arn]
  }

  statement {
    sid    = "GlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase", # necesario para "CREATE EXTERNAL DATABASE IF NOT EXISTS"
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
    ]
    # Acotar a los ARN especificos de lakehouse_db en un entorno real;
    # se deja abierto aca para simplificar la practica de clase.
    resources = [ "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog", "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/lakehouse_db", "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/lakehouse_db/*", ]
  }

  statement {
    sid = "S3IcebergDataRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      module.network.datalake_bucket_arn,
      "${module.network.datalake_bucket_arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "redshift_streaming_policy" {
  name   = "redshift-streaming-minimal-access"
  role   = aws_iam_role.redshift_streaming_role.id
  policy = data.aws_iam_policy_document.redshift_streaming_permissions.json
}

# ── Redshift Serverless: namespace (agrupa recursos) ───────────────
resource "aws_redshiftserverless_namespace" "lakehouse" {
  namespace_name       = "${var.environment}-lakehouse-ns"
  admin_username        = "admin_lakehouse"
  admin_user_password   = var.redshift_admin_password
  iam_roles             = [aws_iam_role.redshift_streaming_role.arn]
  default_iam_role_arn  = aws_iam_role.redshift_streaming_role.arn
}

# ── Redshift Serverless: workgroup (computo) ───────────────────────
resource "aws_redshiftserverless_workgroup" "lakehouse" {
  namespace_name = aws_redshiftserverless_namespace.lakehouse.namespace_name
  workgroup_name = "${var.environment}-lakehouse-wg"
  base_capacity   = 8 # RPUs minimos -- ajustar segun necesidad real

  # Mismo patron de seguridad de red que vienen usando desde la Sem 1:
  # todo el trafico pasa por la VPC, no por la red publica de AWS.
  # IMPORTANTE: con enhanced_vpc_routing = true, Redshift necesita UN
  # VPC Endpoint POR CADA SERVICIO al que se conecta (Kinesis, Glue,
  # etc). Sin el endpoint correspondiente, esa conexion especifica se
  # cuelga con "Connection timeout after 30000 ms" -- no es un error
  # de permisos, es que el trafico no tiene por donde salir.
  enhanced_vpc_routing = true
  subnet_ids            = module.network.private_subnet_ids
}

# ── VPC Endpoint de Glue: sin esto, con enhanced_vpc_routing activado,
# CREATE EXTERNAL SCHEMA ... FROM DATA CATALOG se cuelga con timeout
# (mismo problema que tuvimos con Kinesis, pero para el catalogo). ──
resource "aws_security_group" "glue_endpoint" {
  name        = "${var.environment}-glue-endpoint-sg"
  description = "Permite trafico HTTPS desde la VPC hacia el endpoint de Glue"
  vpc_id      = module.network.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "glue" {
  vpc_id              = module.network.vpc_id
  service_name         = "com.amazonaws.${var.region}.glue"
  vpc_endpoint_type    = "Interface"
  subnet_ids            = module.network.private_subnet_ids
  security_group_ids    = [aws_security_group.glue_endpoint.id]
  private_dns_enabled   = true
}

# ── VPC Endpoint de Kinesis: sin esto, con enhanced_vpc_routing
# activado, Redshift no tiene forma de llegar a Kinesis (no hay NAT
# Gateway en las subredes privadas) y las consultas de streaming
# ingestion se cuelgan con timeout de conexion. ─────────────────────
resource "aws_security_group" "kinesis_endpoint" {
  name        = "${var.environment}-kinesis-endpoint-sg"
  description = "Permite trafico HTTPS desde la VPC hacia el endpoint de Kinesis"
  vpc_id      = module.network.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = module.network.vpc_id
  service_name         = "com.amazonaws.${var.region}.kinesis-streams"
  vpc_endpoint_type    = "Interface"
  subnet_ids            = module.network.private_subnet_ids
  security_group_ids    = [aws_security_group.kinesis_endpoint.id]
  private_dns_enabled   = true
}

# ── Outputs para usar en los scripts SQL y en la verificacion ─────
output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.lakehouse.endpoint
}

output "redshift_namespace_name" {
  value = aws_redshiftserverless_namespace.lakehouse.namespace_name
}

output "redshift_streaming_role_arn" {
  value = aws_iam_role.redshift_streaming_role.arn
}

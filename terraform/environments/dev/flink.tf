# ── Aplicación de Managed Flink (Semana 4) ────────────────────────
# El jar se sube a S3 ANTES de correr terraform apply (ver README,
# paso "Compilar y subir el jar"). Terraform solo declara la app y
# le dice dónde está el código.

resource "aws_kinesisanalyticsv2_application" "flink_job" {
  name                   = "${var.environment}-lakehouse-flink-job"
  runtime_environment    = "FLINK-1_18"
  service_execution_role = module.identity.flink_execution_role_arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = module.network.datalake_bucket_arn
          file_key   = "flink-artifacts/lakehouse-streaming-job.jar"
        }
      }
      code_content_type = "ZIPFILE"
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkAppProperties"
        property_map = {
          "KINESIS_STREAM_ARN" = module.ingestion.kinesis_stream_arn
          "LAKEHOUSE_BUCKET"   = module.network.datalake_bucket_name
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 5000
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = 1
        parallelism_per_kpu  = 1
        auto_scaling_enabled = false
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level           = "INFO"
        metrics_level       = "APPLICATION"
      }
    }
  }

  # El logging de CloudWatch va DENTRO de este mismo recurso, no
  # como un recurso Terraform aparte.
  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink_log_stream.arn
  }

  tags = { Name = "${var.environment}-lakehouse-flink-job" }
}

resource "aws_cloudwatch_log_group" "flink_log_group" {
  name              = "/aws/kinesis-analytics/${var.environment}-lakehouse-flink-job"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink_log_stream" {
  name           = "flink-log-stream"
  log_group_name = aws_cloudwatch_log_group.flink_log_group.name
}

# Clase 6 — Que se agrego al stack

Este stack ahora incluye, ademas de todo lo de la Clase 5 (Terraform de infra + Flink funcionando, probado en vivo), los archivos nuevos de la Clase 6: Redshift Streaming Ingestion.

## Archivos nuevos

- `environments/dev/redshift_streaming.tf` — rol de IAM para Redshift, namespace y workgroup de Redshift Serverless. Se conecta al MISMO Kinesis Stream y al MISMO Glue Data Catalog que ya tenian de las Semanas 2 y 5.
- `sql/` — los 5 scripts SQL en orden: esquema externo hacia Glue, streaming ingestion + Materialized View, el JOIN entre datos calientes y frios, seguridad RBAC, y verificacion de refresh.

## Diferencia importante respecto al resto del stack

Todo lo de `modules/`, el `flink.tf` y el `glue_lakehouse.tf` de las Semanas 1-5 ya fue probado en vivo, con varias rondas de debugging real hasta que funciono de punta a punta.

**`redshift_streaming.tf` y los scripts SQL son nuevos y todavia NO pasaron por ese mismo proceso.** Es muy probable que necesiten ajustes al correrlos por primera vez contra una cuenta real — sobre todo la sintaxis exacta de `CREATE EXTERNAL SCHEMA ... FROM KINESIS`, que varia segun la version del motor de Redshift Serverless.

## Como usarlo

1. Aplicar el Terraform nuevo (necesita el password de Redshift como variable):
```bash
cd environments/dev
terraform apply -var="redshift_admin_password=TuPasswordSegura123!"
```

2. Sacar el endpoint de Redshift:
```bash
terraform output redshift_workgroup_endpoint
```

3. Conectarse con ese endpoint desde el Editor de consultas de Redshift en la consola de AWS, y correr los scripts de `sql/` en orden, del 01 al 05, reemplazando `<cuenta>` por tu Account ID real.

4. Antes de correr `03_join_frio_caliente.sql`, generar trafico nuevo con `python test/prueba_en_vivo.py` para tener datos frescos tanto en el stream (que Redshift lee directo) como en Iceberg (via Flink).

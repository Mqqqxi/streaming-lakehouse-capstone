# Stack completo desde cero — Semanas 1, 2, 4 y 5

Para cuando no tenés nada previo desplegado. Levanta VPC+IAM (Sem 1),
Kinesis+Firehose (Sem 2), la app de Flink (Sem 4) y el lakehouse
Iceberg+Glue (Sem 5) en una sola pasada.

## Requisitos previos

```bash
aws configure          # credenciales con permisos de admin en la cuenta
terraform -version     # >= 1.5
mvn -version            # >= 3.8, con Java 11
```

## 1. Bootstrap del backend remoto (una sola vez)

```bash
cd bootstrap
chmod +x bootstrap-backend.sh
export AWS_REGION=us-east-1
./bootstrap-backend.sh mi-tfstate-bucket-unico
```

Verificá tu versión de Terraform primero (`terraform -version`):

- **Terraform >= 1.10** (el `main.tf` ya viene configurado así): solo pasás el nombre del bucket, como arriba. El locking usa `use_lockfile = true`, nativo de S3, sin tabla DynamoDB.
- **Terraform < 1.10**: corré `./bootstrap-backend.sh mi-tfstate-bucket-unico --legacy-dynamodb mi-tabla-locks` y en `main.tf` reemplazá la línea `use_lockfile = true` por `dynamodb_table = "mi-tabla-locks"`.

Copiá los valores que te imprime al final y pegalos en
`environments/dev/main.tf`, dentro del bloque `backend "s3"`
(reemplazando los `CAMBIAR-...`). **Ojo con la región:** tiene que ser
un código real de AWS (ej. `us-east-1`, `sa-east-1`), no el placeholder.

## 2. Levantar la infraestructura base (Sem 1, 2, 4 — sin el jar todavía)

```bash
cd environments/dev
terraform init
terraform plan
terraform apply
```

Esto crea: VPC, subredes, S3 Gateway Endpoint, bucket del data lake,
rol IAM de Flink, Kinesis Stream, Firehose, y la *declaración* de la
app de Flink (todavía sin código real cargado — falla al iniciar
hasta el paso 4, es esperable).

Guardá los outputs, los vas a necesitar:

```bash
terraform output
```

## 3. Compilar el job de Flink (Sem 4 + Sem 5 ya integradas)

```bash
cd ../../flink-app
mvn clean package -DskipTests
```

Esto genera `target/lakehouse-streaming-job.jar` con el shade plugin
(incluye todas las dependencias, necesario para Managed Flink).

## 4. Subir el jar y aplicar Terraform de nuevo

```bash
export LAKEHOUSE_BUCKET=$(cd ../environments/dev && terraform output -raw datalake_bucket_name)

aws s3 cp target/lakehouse-streaming-job.jar \
  s3://$LAKEHOUSE_BUCKET/flink-artifacts/lakehouse-streaming-job.jar

cd ../environments/dev
terraform apply    # ahora la app de Flink encuentra el jar y puede iniciar
```

## 5. Iniciar la aplicación de Flink

Managed Flink no arranca solo al crearse — hay que iniciarla:

```bash
FLINK_APP_NAME=$(terraform output -raw flink_app_name)

aws kinesisanalyticsv2 start-application \
  --application-name $FLINK_APP_NAME \
  --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'
```

Verificá el estado (puede tardar 1-2 min en pasar a `RUNNING`):

```bash
aws kinesisanalyticsv2 describe-application --application-name $FLINK_APP_NAME \
  --query 'ApplicationDetail.ApplicationStatus'
```

## 6. Prueba end-to-end

```bash
cd ../../test
pip install boto3 --break-system-packages

export KINESIS_STREAM_NAME=$(cd ../environments/dev && terraform output -raw kinesis_stream_name)
export AWS_REGION=us-east-1

python3 prueba_en_vivo.py
```

## 7. Verificación manual

```bash
aws glue get-table --database-name lakehouse_db --name sensor_events

aws s3 ls s3://$LAKEHOUSE_BUCKET/lakehouse/sensor_events/metadata/
```

## Errores esperables

| Síntoma | Causa probable |
|---|---|
| `terraform apply` falla en el primer `apply` por la app de Flink | Esperable — todavía no subiste el jar (paso 3-4 van después del primer apply) |
| App de Flink en estado `RUNNING` pero sin datos | No la iniciaste con `start-application` (paso 5) |
| `AccessDeniedException` en Glue | Revisar que `flink_glue_policy` se haya aplicado — `terraform state show aws_iam_role_policy.flink_glue_policy` |
| Tabla no aparece en Glue después de varios minutos | Confirmar que el checkpointing quedó habilitado en `flink.tf` (`checkpointing_enabled = true`) |
| Bucket ya existe / nombre duplicado | El nombre del bucket incluye el account ID para ser único, pero si corriste el bootstrap dos veces con el mismo nombre de state bucket, va a fallar — usá un nombre distinto |

## Orden de destrucción (si necesitás limpiar todo)

```bash
cd environments/dev
aws kinesisanalyticsv2 stop-application --application-name $FLINK_APP_NAME --force true
terraform destroy
```

El bucket de state y la tabla DynamoDB del bootstrap NO se borran con
`terraform destroy` (se crearon a mano) — si querés borrarlos, es
manual con `aws s3 rb` y `aws dynamodb delete-table`.

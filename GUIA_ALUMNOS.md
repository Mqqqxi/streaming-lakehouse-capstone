# Guia para alumnos: levantar el pipeline completo desde cero
## Terraform + Kinesis + Flink + Iceberg/Glue en AWS (Windows)

Esta guia levanta, paso a paso, un pipeline de streaming completo: infraestructura como codigo (Semana 1), ingesta en tiempo real (Semana 2), procesamiento con estado (Semana 4), y almacenamiento transaccional gobernado (Semana 5). Al final vas a tener una tabla Iceberg real, consultable con SQL desde Athena, con datos que recorrieron el pipeline entero.

**Regla de oro de esta guia: usa la MISMA region de AWS en TODOS los pasos, sin excepcion.** La mayoria de los problemas al armar este tipo de pipelines vienen de mezclar regiones sin darse cuenta. Elegi una region ahora (por ejemplo `us-east-2`) y anotala en un papel: la vas a necesitar en cada comando.

---

## Parte 0 — Instalar lo necesario

Necesitas 5 herramientas. Instalalas en este orden, en Windows.

### 0.1 Git Bash
Descargar de https://git-scm.com/download/win, instalar con opciones por defecto. Se usa para correr scripts `.sh`. **PowerShell no puede correr `chmod` ni scripts bash** — para esos pasos especificos, usa Git Bash; para el resto de los comandos (Terraform, AWS CLI), PowerShell funciona bien.

### 0.2 AWS CLI
Descargar de https://aws.amazon.com/cli/ (instalador `.msi`). Verificar con `aws --version`.

### 0.3 Java 11+ y Maven
- Java: https://adoptium.net/, version JDK 11 o superior (JDK 17 tambien funciona).
- Maven: https://maven.apache.org/download.cgi, descargar el zip binario, descomprimir en `C:\maven`, agregar `C:\maven\bin` al PATH del sistema (Panel de Control → Variables de entorno).
- Verificar con `java -version` y `mvn -version`.

### 0.4 Python
Descargar de https://www.python.org/downloads/, **marcar "Add python.exe to PATH"** durante la instalacion. Verificar con `python --version`.

### 0.5 Terraform
Descargar de https://developer.hashicorp.com/terraform/install. Verificar con `terraform -version`.

---

## Parte 1 — Cuenta de AWS y credenciales

### 1.1 Confirmar el plan de la cuenta

Entrar a la consola de AWS → "Billing and Cost Management" → "Costo y uso". Si dice **"Plan Gratuito"**, hay que actualizar a **Plan de pago** antes de seguir — Kinesis no funciona en el plan gratuito reducido, aunque tengas credito disponible. Boton "Plan de actualizacion" → cargar tarjeta.

### 1.2 Usuario de IAM

Consola de AWS → IAM → Users → Create user. Adjuntar la policy `AdministratorAccess` (para este ejercicio de practica). Crear un Access Key desde "Security credentials".

### 1.3 Configurar el CLI

```powershell
aws configure
```

Ingresar el Access Key ID, el Secret Access Key, y como region default poner la region elegida (ej. `us-east-2`). Verificar:

```powershell
aws sts get-caller-identity
```

---

## Parte 2 — Descomprimir el proyecto

Descomprimir `stack-completo.zip` en una carpeta como `C:\clase5\stack-completo`. Estructura:

```
stack-completo/
  bootstrap/          <- script para crear el backend de Terraform
  modules/            <- red, identidad, ingesta (Semanas 1-2)
  environments/dev/   <- junta todo + Flink (Sem 4) + Iceberg/Glue (Sem 5)
  flink-app/          <- codigo Java del job de Flink
  test/               <- script de prueba en vivo
```

---

## Parte 3 — Bootstrap del backend de Terraform

Abrir **Git Bash** (no PowerShell) para este paso puntual:

```bash
cd bootstrap
chmod +x bootstrap-backend.sh
export AWS_REGION=us-east-2
./bootstrap-backend.sh <tu-nombre-unico-de-bucket>
```

El nombre del bucket debe ser unico en TODO AWS, no solo en tu cuenta — agregale tu nombre y un numero. Anotar el bucket y la region que imprime al final.

---

## Parte 4 — Configurar main.tf

Abrir `environments/dev/main.tf` con un editor de texto. Buscar el bloque `backend "s3"` y reemplazar:

```hcl
backend "s3" {
  bucket       = "<tu-bucket-del-bootstrap>"
  key          = "dev/terraform.tfstate"
  region       = "us-east-2"      # LA MISMA region de siempre
  use_lockfile = true
  encrypt      = true
}
```

Tambien revisar `environments/dev/variables.tf` y confirmar que `region` tenga el mismo valor por defecto (`us-east-2`, o la que hayas elegido).

---

## Parte 5 — Primer apply (infraestructura base)

Desde PowerShell, parado en `environments/dev`:

```powershell
terraform init
terraform apply
```

Confirmar con `yes`. Esto crea VPC, IAM, Kinesis, Firehose, Glue database, y declara la app de Flink (todavia sin codigo).

**Verificacion critica de region:**

```powershell
terraform state show aws_kinesisanalyticsv2_application.flink_job
```

Mirar el campo `arn` — debe empezar con `arn:aws:kinesisanalytics:us-east-2:...` (o la region que elegiste). Si dice una region distinta a la que pusiste en `main.tf`, hay una variable de entorno `AWS_REGION` residual pisando la configuracion — revisar con `echo $env:AWS_REGION` en PowerShell y corregir antes de seguir.

---

## Parte 6 — Compilar el codigo de Flink

El codigo en `flink-app/src/main/java/.../LakehouseStreamingJob.java` ya incluye las correcciones necesarias para que Managed Flink funcione:

- Lee la configuracion via `KinesisAnalyticsRuntime.getApplicationProperties()` (no `System.getenv()`).
- El `sourceConfig` del builder de Kinesis incluye la region de AWS explicita (extraida del ARN del stream).
- Usa `TypeInformation.of(String.class)` explicito para evitar problemas de type erasure de Java.
- La ventana de agregacion es por **tiempo de procesamiento** (`TumblingProcessingTimeWindows`), no por tiempo de evento — esto es importante: con tiempo de evento, si el trafico llega en una rafaga y se corta (como en esta practica), la ventana nunca cierra. Con tiempo de procesamiento, cierra sola cada 1 minuto de reloj real.

```powershell
cd ../../flink-app
mvn clean package -DskipTests
```

Confirmar `BUILD SUCCESS`. Se genera `target/lakehouse-streaming-job-1.0.0.jar`.

---

## Parte 7 — Subir el jar y segundo apply

```powershell
$LAKEHOUSE_BUCKET = (terraform -chdir=../environments/dev output -raw datalake_bucket_name)
aws s3 cp target/lakehouse-streaming-job-1.0.0.jar s3://$LAKEHOUSE_BUCKET/flink-artifacts/lakehouse-streaming-job.jar

cd ../environments/dev
terraform apply
```

---

## Parte 8 — Iniciar la aplicacion de Flink

Managed Flink no arranca solo al crearse. Generar el archivo de configuracion de arranque (usar `-Encoding ascii`, **nunca `utf8`** — utf8 agrega un caracter invisible al principio del archivo que rompe el parser del AWS CLI):

```powershell
'{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}' | Out-File -FilePath restore-config.json -Encoding ascii -NoNewline

aws kinesisanalyticsv2 start-application --application-name dev-lakehouse-flink-job --run-configuration file://restore-config.json --region us-east-2
```

Verificar (esperar 1-2 min):

```powershell
aws kinesisanalyticsv2 describe-application --application-name dev-lakehouse-flink-job --region us-east-2 --query 'ApplicationDetail.ApplicationStatus'
```

Debe decir `"RUNNING"`.

**Verificacion mas profunda (recomendada):** entrar a la consola → Managed Apache Flink → tu app → "Abrir el panel de Apache Flink" → Running Jobs → tu job. El `Job State` debe decir `RUNNING` en verde, sin numero de reintentos creciendo, y las tres tareas del grafo deben decir `RUNNING`, no `CANCELED`. Si dice `RESTARTING`, hay un error real ocurriendo — revisar la pestana `Exceptions` de ese mismo panel.

---

## Parte 9 — Prueba en vivo

```powershell
cd ../../test
pip install boto3

$env:KINESIS_STREAM_NAME = (terraform -chdir=../environments/dev output -raw kinesis_stream_name)
$env:AWS_REGION = "us-east-2"

python prueba_en_vivo.py
```

El script manda 50 eventos y espera 90 segundos. Con la ventana de tiempo de procesamiento, puede necesitar hasta 1 minuto adicional para que la ventana cierre — si el resultado no muestra datos todavia, esperar un minuto mas y volver a consultar.

**Salida esperada:**
```
Tabla encontrada: lakehouse_db.sensor_events
table_type: ICEBERG
metadata_location: s3://.../metadata/....metadata.json
```

---

## Parte 10 — Verificacion visual en consola de AWS

Importante: la consola web tambien tiene selector de region (arriba a la derecha) — confirmar que este en la region correcta antes de buscar cualquier recurso.

1. **Kinesis → Data streams → tu stream → Monitoring**: grafico de datos entrantes.
2. **Managed Apache Flink → tu app → Abrir el panel de Apache Flink → Running Jobs**: grafo con contadores de Records Sent/Received en vivo.
3. **Glue → Databases → lakehouse_db → Tables → sensor_events**: la tabla registrada.
4. **S3 → tu bucket → lakehouse/lakehouse_db.db/sensor_events/**: deben existir DOS carpetas, `metadata/` y `data/`. Si falta `data/`, todavia no hubo un commit real con filas.
5. **Athena → Editor**: `SELECT * FROM lakehouse_db.sensor_events` — **desactivar el toggle "Volver a utilizar los resultados de la consulta"** antes de ejecutar, sino puede mostrar un resultado en cache viejo.

---

## Parte 11 — Si necesitas cambiar el codigo (ciclo de actualizacion)

Managed Flink "fija" la version del jar que usa al momento de crear o actualizar la aplicacion — **sobrescribir el archivo en S3 no alcanza**, hay que decirle explicitamente que lo vuelva a leer. Si haces cualquier cambio en el `.java`, seguir este ciclo completo:

```powershell
# 1. Si la app esta corriendo, pararla primero
aws kinesisanalyticsv2 stop-application --application-name dev-lakehouse-flink-job --force --region us-east-2
# esperar a que pase a READY

# 2. Recompilar
cd flink-app
mvn clean package -DskipTests

# 3. Subir el jar nuevo
aws s3 cp target/lakehouse-streaming-job-1.0.0.jar s3://$LAKEHOUSE_BUCKET/flink-artifacts/lakehouse-streaming-job.jar

# 4. Consultar la version REAL actual (no asumir un numero)
cd ../environments/dev
aws kinesisanalyticsv2 describe-application --application-name dev-lakehouse-flink-job --region us-east-2 --query 'ApplicationDetail.ApplicationVersionId'

# 5. Generar el update con ESE numero exacto (reemplazar el 1 por el numero real)
'{"ApplicationName":"dev-lakehouse-flink-job","CurrentApplicationVersionId":1,"ApplicationConfigurationUpdate":{"ApplicationCodeConfigurationUpdate":{"CodeContentUpdate":{"S3ContentLocationUpdate":{"BucketARNUpdate":"arn:aws:s3:::TU-BUCKET","FileKeyUpdate":"flink-artifacts/lakehouse-streaming-job.jar"}}}}}' | Out-File -FilePath update-code.json -Encoding ascii -NoNewline

aws kinesisanalyticsv2 update-application --cli-input-json file://update-code.json --region us-east-2

# 6. Arrancar de nuevo
aws kinesisanalyticsv2 start-application --application-name dev-lakehouse-flink-job --run-configuration file://restore-config.json --region us-east-2
```

**Nota:** si el `update-application` falla con `InvalidApplicationConfigurationException` sobre snapshots, es porque la app todavia esta `RUNNING` — hacer el `stop-application` del paso 1 primero.

---

## Parte 12 — Errores comunes y su solucion

| Error | Causa | Solucion |
|---|---|---|
| `Invalid AWS Region: CAMBIAR-region` | `main.tf` sin editar | Completar bucket/region reales del bootstrap |
| `chmod: no reconocido` | Comando bash en PowerShell | Usar Git Bash para scripts `.sh` |
| `SubscriptionRequiredException` en Kinesis | Cuenta en Plan Gratuito | Actualizar a Plan de pago |
| `InvalidArgumentException` fileKey no encontrado | Jar no subido antes del 2do apply | Compilar y subir el jar primero |
| `NullPointerException` en `KinesisStreamsSource` | Falta `sourceConfig` o falta la region dentro de el | Ya resuelto en el codigo de esta guia |
| `InvalidTypesException: type erasure` | Falta tipo explicito en `fromSource` | Ya resuelto (`TypeInformation.of(String.class)`) |
| App vuelve a `READY` despues de subir jar nuevo | Version de S3 "pineada" por Managed Flink | Forzar con `update-application` (Parte 11) |
| `ConcurrentModificationException` en update | `CurrentApplicationVersionId` desactualizado | Consultar la version real antes de cada update |
| `ResourceInUseException: cannot force stop` | Ya habia un stop en curso | Esperar a que termine, no reintentar |
| Job en estado `RESTARTING` en el dashboard nativo | Excepcion real durante ejecucion | Ver pestana "Exceptions" del Flink Dashboard, no solo `describe-application` |
| `region must not be null` | Falta region dentro del `sourceConfig` | Ya resuelto en el codigo de esta guia |
| Tabla creada pero `data/` vacia, `total-records: 0` en el snapshot | Ventana de tiempo de evento nunca cierra con trafico en rafaga | Ya resuelto (ventana de tiempo de procesamiento) |
| Athena muestra 0 filas con la tabla ya poblada | Cache de resultados de Athena | Desactivar "Volver a utilizar los resultados de la consulta" |
| Error de parseo JSON con caracter raro al inicio | `Out-File -Encoding utf8` agrega BOM | Usar `-Encoding ascii -NoNewline` |

---

## Parte 13 — Destruir todo al terminar

Kinesis y Managed Flink facturan por hora, esten o no procesando datos.

```powershell
aws kinesisanalyticsv2 stop-application --application-name dev-lakehouse-flink-job --force --region us-east-2
cd environments/dev
terraform destroy
```

El bucket del state (creado en la Parte 3) no se borra solo:

```powershell
aws s3 rb s3://<tu-bucket-del-bootstrap> --force --region us-east-2
```

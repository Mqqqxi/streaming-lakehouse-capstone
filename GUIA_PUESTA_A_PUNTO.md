# Guia completa: pipeline de punta a punta (Semanas 1-6)
## Terraform + Kinesis + Flink + Iceberg/Glue + Redshift, desde cero

**Regla de oro: usa la MISMA region de AWS en TODOS los pasos** (`us-east-2` en esta guia).

**Regla de oro 2: todos los comandos de PowerShell se corren desde la carpeta que indica cada paso.** Si un comando falla con "no existe" o "no se encuentra", lo primero a revisar es `pwd` — la causa mas comun de errores en esta guia es estar parado en la carpeta equivocada.

---

## PARTE A — Preparacion (una sola vez)

- [ ] **1. Instalar Git Bash** → https://git-scm.com/download/win
- [ ] **2. Instalar AWS CLI** → https://aws.amazon.com/cli/
- [ ] **3. Instalar Java 11+** → https://adoptium.net/
- [ ] **4. Instalar Maven** → https://maven.apache.org/download.cgi (descomprimir, agregar `bin` al PATH)
- [ ] **5. Instalar Python** → https://www.python.org/downloads/ (marcar "Add to PATH")
- [ ] **6. Instalar Terraform** → https://developer.hashicorp.com/terraform/install
- [ ] **7. Confirmar Plan de pago en AWS** (no Plan Gratuito) — Billing → Costo y uso
- [ ] **8. Configurar credenciales:**
```powershell
aws configure
```
- [ ] **9. Descomprimir `stack-completo.zip`**, por ejemplo en `C:\clase6`

---

## PARTE B — Bootstrap del backend de Terraform (una sola vez)

- [ ] **10. Abrir Git Bash** (no PowerShell) y correr, DENTRO de la carpeta `bootstrap`. Reemplazar el nombre del bucket por uno propio y unico, **sin los signos `<` `>`**:
```bash
cd bootstrap
chmod +x bootstrap-backend.sh
export AWS_REGION=us-east-2
./bootstrap-backend.sh luciano-clase6-tfstate-2026
```
Anotar el bucket que devuelve al final.

- [ ] **11. Editar `environments/dev/main.tf`**, bloque `backend "s3"`:
```hcl
bucket = "<tu-bucket-del-paso-10>"
region = "us-east-2"
```

---

## PARTE C — Configurar la contraseña de Redshift (una sola vez)

Como el archivo de Redshift vive en la misma carpeta que el resto de la infraestructura, Terraform pide esta contraseña desde el PRIMER `apply`. Para no escribirla en cada comando, la guardamos en un archivo:

- [ ] **12. Crear `environments/dev/terraform.tfvars`:**
```powershell
cd environments\dev
'redshift_admin_password = "Semana6Pass123!"' | Out-File -FilePath terraform.tfvars -Encoding ascii -NoNewline
```
Cambiar `Semana6Pass123!` por una contraseña propia (8+ caracteres, mayuscula, minuscula, numero) y **anotarla** — la vas a necesitar para conectarte a Redshift mas adelante. Con este archivo, Terraform la toma sola de ahora en mas, sin flags.

---

## PARTE D — Infraestructura base (Semanas 1, 2, 4, 5, y la red de Redshift)

Parado en `environments\dev`:

- [ ] **13. Inicializar:**
```powershell
terraform init
```

- [ ] **14. Crear la infraestructura:**
```powershell
terraform apply
```
Confirmar `yes`. Crea VPC (con 3 subredes — Redshift Serverless exige minimo 3 en 3 AZs distintas), IAM, Kinesis, Firehose, Glue database, Redshift namespace/workgroup, y declara la app de Flink (todavia sin codigo).

**Es normal y esperado** que este primer `apply` termine con UN error: la app de Flink va a fallar porque el jar todavia no existe en S3. Todo lo demas deberia quedar creado sin problema. Seguir con la Parte E.

---

## PARTE E — Compilar y desplegar el codigo de Flink

- [ ] **15. Compilar** (tiene que ser desde la carpeta `flink-app`, NO desde la carpeta profunda donde esta el `.java`):
```powershell
cd ..\..\flink-app
mvn clean package -DskipTests
```
Confirmar `BUILD SUCCESS`. El jar queda en `flink-app\target\lakehouse-streaming-job-1.0.0.jar`.

- [ ] **16. Subir el jar:**
```powershell
cd ..\environments\dev
$LAKEHOUSE_BUCKET = (terraform output -raw datalake_bucket_name)
cd ..\flink-app
aws s3 cp target\lakehouse-streaming-job-1.0.0.jar s3://$LAKEHOUSE_BUCKET/flink-artifacts/lakehouse-streaming-job.jar
```

- [ ] **17. Volver a aplicar Terraform** (ahora si deberia crear la app de Flink sin errores):
```powershell
cd ..\environments\dev
terraform apply
```

- [ ] **18. Preparar el archivo de arranque** (encoding ascii, NUNCA utf8):
```powershell
'{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}' | Out-File -FilePath restore-config.json -Encoding ascii -NoNewline
```

- [ ] **19. Iniciar la aplicacion de Flink:**
```powershell
aws kinesisanalyticsv2 start-application --application-name dev-lakehouse-flink-job --run-configuration file://restore-config.json --region us-east-2
```

- [ ] **20. Esperar 1-2 min y verificar:**
```powershell
aws kinesisanalyticsv2 describe-application --application-name dev-lakehouse-flink-job --region us-east-2 --query 'ApplicationDetail.ApplicationStatus'
```
Debe decir `"RUNNING"`.

---

## PARTE F — Probar Kinesis + Flink + Iceberg

- [ ] **21. Configurar variables y correr la prueba** (sin `-chdir`, que da problemas de sintaxis en PowerShell — navegar con `cd` en su lugar):
```powershell
cd ..\environments\dev
$streamName = (terraform output -raw kinesis_stream_name)
cd ..\..\test
pip install boto3
$env:KINESIS_STREAM_NAME = $streamName
$env:AWS_REGION = "us-east-2"
python prueba_en_vivo.py
```

- [ ] **22. Si no aparecen datos**, esperar 1 minuto mas y volver a correr `python prueba_en_vivo.py`.

- [ ] **23. Verificar que hay datos reales** (no solo metadata vacia):
```powershell
aws s3 ls s3://$LAKEHOUSE_BUCKET/lakehouse/lakehouse_db.db/sensor_events/data/ --region us-east-2 --recursive
```
Debe devolver al menos un archivo `.parquet`.

---

## PARTE G — Redshift Streaming Ingestion (lo nuevo de la Semana 6)

### G.1 — Obtener los datos de conexion

- [ ] **24.**
```powershell
cd ..\environments\dev
terraform output redshift_workgroup_endpoint
terraform output redshift_namespace_name
aws sts get-caller-identity --query Account --output text
```
Anotar el Account ID (12 digitos) — lo vas a necesitar para reemplazar `<cuenta>` en los scripts SQL.

### G.2 — Entrar al Query Editor v2

- [ ] **25.** Consola de AWS → confirmar region **Ohio (us-east-2)** arriba a la derecha → buscar "Redshift" → menu izquierdo → "Query Editor v2".

- [ ] **26. Si aparece una pantalla "Configure account"** (primera vez que se usa en la cuenta): dejar todo por defecto (no tildar "Customize encryption settings", dejar "S3 URI" vacio) y click en **"Configure account"**. Es un paso de configuracion unica, no hace falta tocar nada mas.

### G.3 — Conectarse con el usuario correcto (el paso que mas cuesta la primera vez)

Por defecto, el editor intenta conectarse con autenticacion automatica ("Federated user"), que va a fallar con un error rojo "Error accessing the database... not authenticated with IAM credentials". Hay que cambiar el metodo:

- [ ] **27.** En el panel izquierdo, en la linea que dice **"Serverless: dev-lakehouse-..."** (arriba del todo del arbol, NO en "awsdatacatalog"), buscar el icono de tres puntos verticales (⋮) a la derecha del icono "i".

- [ ] **28.** Click ahi → **"Edit connection"**.

- [ ] **29.** Cambiar el metodo de autenticacion de "Federated user" a **"Database user name and password"**.
  - Database user name: `admin_lakehouse`
  - Password: la contraseña del Paso 12

- [ ] **30.** Guardar/reconectar. El error rojo deberia desaparecer.

**Importante: ignorar por completo el nodo "awsdatacatalog"** que aparece en "external databases" — es una integracion automatica de Redshift con Glue que siempre usa autenticacion IAM propia, no la vamos a usar. No hace falta clickearlo ni expandirlo.

### G.4 — Correr los scripts, en la base de datos correcta

- [ ] **31.** Abrir una pestaña de consulta (la que ya esta abierta, o el boton "+"). Arriba, cerca del boton "Run", hay un selector de base de datos — probablemente diga "awsdatacatalog". **Cambiarlo a `dev`.**

- [ ] **32.** Abrir `sql/01_external_schema_glue.sql`, copiar **el archivo COMPLETO** (no solo la ultima linea), reemplazar `<cuenta>` por el Account ID del Paso 24, pegar en el editor (con `dev` seleccionado), y Run. Debe devolver una fila con `sensor_events`.

- [ ] **33.** Abrir `sql/02_kinesis_streaming_ingestion.sql`, reemplazar `<cuenta>`, revisar que el nombre del stream coincida, copiar completo, pegar, Run.

- [ ] **34.** Abrir `sql/04_seguridad_rbac.sql`, copiar completo, pegar, Run (no necesita reemplazos).

- [ ] **35.** Abrir `sql/05_verificar_refresh.sql`, copiar completo, pegar, Run. Si esta vacio, esperar 1-2 min y reintentar.

### G.5 — Generar trafico y correr el JOIN final

- [ ] **36.** Volver a PowerShell:
```powershell
cd ..\..\test
python prueba_en_vivo.py
```

- [ ] **37.** Volver al Query Editor v2 (con `dev` seleccionado), abrir `sql/03_join_frio_caliente.sql`, copiar completo, pegar, Run. Si devuelve filas con `temperatura_actual` y `promedio_historico` juntos, **el pipeline de las 6 semanas esta funcionando de punta a punta.**

---

## PARTE H — Verificacion completa en la consola de AWS

Para cada pantalla: **confirmar primero que el selector de region diga "Ohio" / us-east-2**.

| Servicio | Donde entrar | Que deberias ver |
|---|---|---|
| **Kinesis** | Kinesis → Secuencias de datos → tu stream → Monitoring | Grafico de "Incoming data" con actividad |
| **Managed Apache Flink** | Managed Apache Flink → tu app → "Abrir el panel de Apache Flink" → Running Jobs | `Job State: RUNNING` en verde; las 3 tareas en `RUNNING`, no `CANCELED` |
| **Glue** | Glue → Databases → `lakehouse_db` → Tables | Tabla `sensor_events`, `table_type: ICEBERG` |
| **S3** | S3 → tu bucket → `lakehouse/lakehouse_db.db/sensor_events/` | Carpetas `data/` Y `metadata/` (si falta `data/`, no hubo commit real todavia) |
| **Redshift** | Redshift → Query Editor v2 (con `dev` seleccionado) | Resultado del JOIN con filas de temperatura actual e historica juntas |
| **Athena** | Athena → Editor (con cache DESACTIVADO) | `SELECT * FROM lakehouse_db.sensor_events` devuelve filas |

---

## PARTE I — Terminar y limpiar (IMPORTANTE — evita costos)

- [ ] **38. Detener Flink primero:**
```powershell
cd ..\environments\dev
aws kinesisanalyticsv2 stop-application --application-name dev-lakehouse-flink-job --force --region us-east-2
```
Esperar a `READY`.

- [ ] **39. Vaciar el bucket S3 manualmente** (tiene versionado). Consola AWS → S3 → Buckets → tildar el checkbox de tu bucket (SIN entrar) → boton **"Vaciar"** → confirmar.

- [ ] **40. Destruir toda la infraestructura:**
```powershell
terraform destroy
```
(No hace falta `-var`, ya lo toma de `terraform.tfvars`). Confirmar `yes`.

- [ ] **41. Borrar el bucket del state:**
```powershell
aws s3 rb s3://<tu-bucket-del-paso-10> --force --region us-east-2
```

---

## Errores mas comunes (todo lo encontrado hasta ahora)

| Pasa esto | Causa | Solucion |
|---|---|---|
| `bash: syntax error near unexpected token` en el bootstrap | Se copio el comando con `<` `>` literales | Reemplazar por un nombre real, sin esos simbolos |
| `var.redshift_admin_password` pide valor en cada apply | El archivo de Redshift esta en la misma carpeta que el resto | Crear `terraform.tfvars` (Parte C) |
| `ValidationException: not enough free IP addresses` | Redshift Serverless exige minimo 3 subredes en 3 AZs | Ya corregido en el codigo del stack (3 subredes) |
| `MissingProjectException` al correr `mvn` | Se corrio desde una carpeta muy profunda, no desde `flink-app` | `cd` hasta `flink-app` (donde esta `pom.xml`) |
| `The user-provided path ... does not exist` al subir el jar | Ruta relativa incorrecta al jar | El jar esta en `flink-app\target\` |
| `Invalid -chdir option` | Sintaxis fragil en PowerShell | Evitar `-chdir`, navegar con `cd` |
| `InvalidArgumentException` fileKey no encontrado (Flink) | Es el PRIMER apply, antes de subir el jar | Esperado — seguir con la Parte E |
| `chmod: no reconocido` | Comando bash en PowerShell | Usar Git Bash para el Paso 10 |
| `BucketNotEmpty` al hacer destroy | Flink seguia corriendo, volvia a llenar el bucket | Parar Flink (Paso 38) ANTES de vaciar (Paso 39) |
| Athena muestra 0 filas con datos ya cargados | Cache de resultados | Desactivar "Volver a utilizar resultados" |
| "Error accessing the database awsdatacatalog", "not authenticated with IAM credentials" | El editor conecto con "Federated user" en vez de usuario/contraseña | Editar la conexion (⋮ junto a "Serverless: dev-lakehouse-...") y cambiar a "Database user name and password" |
| Click repetido en "awsdatacatalog" sigue dando error | Ese nodo siempre usa IAM, no lo vamos a usar | Ignorarlo por completo, trabajar en `dev` |
| Una consulta corre bien pero "Returned rows: 0" en `svv_external_tables` | Se pego solo la ultima linea del script, no el `CREATE EXTERNAL SCHEMA` completo | Copiar y pegar el archivo `.sql` COMPLETO, no un fragmento |
| `CREATE EXTERNAL SCHEMA ... FROM KINESIS` con error de sintaxis | Varia segun version del motor Redshift Serverless | Revisar documentacion AWS actual |

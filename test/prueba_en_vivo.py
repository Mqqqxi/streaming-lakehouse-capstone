"""
CLASE 5 — Prueba en vivo del pipeline completo (1 -> 2 -> 4 -> 5)

Uso durante la práctica:
  1. Correr generar_eventos() para mandar tráfico al MISMO stream
     de Kinesis que se creó en la Pre-entrega 2.
  2. Esperar ~60-90s (el checkpoint de Flink, ya configurado en la
     Pre-entrega 4, es el que dispara el commit de Iceberg).
  3. Correr verificar_tabla_glue() para confirmar que la tabla
     "lakehouse_db.sensor_events" (nueva de hoy) tiene metadata
     actualizada.

Requiere: pip install boto3 --break-system-packages
Variables de entorno esperadas (ya deberían existir del setup previo):
  KINESIS_STREAM_NAME  -> nombre del stream de la Pre-entrega 2
  AWS_REGION
"""

import boto3
import json
import os
import random
import time
import uuid

STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]
REGION = os.environ.get("AWS_REGION", "us-east-1")
GLUE_DATABASE = "lakehouse_db"       # nuevo en clase 5
GLUE_TABLE = "sensor_events"          # nuevo en clase 5

kinesis = boto3.client("kinesis", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)


def generar_eventos(cantidad: int = 50):
    """
    YA EXISTÍA EN ESPÍRITU (Sem 2): mismo patrón de PartitionKey
    variable que se usó para evitar Hot Shards en la pre-entrega 2.
    Se reusa acá para no repetir el error ya corregido en su momento.
    """
    sensores = [f"sensor-{i}" for i in range(1, 6)]

    for _ in range(cantidad):
        sensor_id = random.choice(sensores)
        evento = f"{sensor_id},{round(random.uniform(18, 35), 2)}"

        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=evento.encode("utf-8"),
            PartitionKey=str(uuid.uuid4()),  # variable -> distribuye entre shards
        )

    print(f"Enviados {cantidad} eventos al stream '{STREAM_NAME}'.")
    print("Esperar el próximo checkpoint de Flink (configurado a 60s) antes de verificar.")


def verificar_tabla_glue():
    """
    NUEVO EN CLASE 5: confirma que Flink efectivamente comiteó datos
    a la tabla Iceberg registrada en Glue Data Catalog.
    """
    try:
        respuesta = glue.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)
    except glue.exceptions.EntityNotFoundException:
        print("La tabla todavía no existe en Glue. Esperá al primer checkpoint de Flink.")
        return

    tabla = respuesta["Table"]
    print(f"Tabla encontrada: {GLUE_DATABASE}.{GLUE_TABLE}")
    print(f"Última actualización: {tabla.get('UpdateTime')}")
    print(f"Ubicación S3: {tabla['StorageDescriptor']['Location']}")

    # Parámetros que agrega Iceberg específicamente (evidencia de que
    # es una tabla Iceberg real y no un archivo plano registrado a mano)
    params = tabla.get("Parameters", {})
    print(f"table_type: {params.get('table_type', 'NO DETECTADO')}")
    print(f"metadata_location: {params.get('metadata_location', 'NO DETECTADO')}")


def listar_snapshots_s3(bucket: str):
    """
    NUEVO EN CLASE 5: mirar directamente en S3 la carpeta metadata/
    de Iceberg -- es la evidencia visual más clara para mostrar en
    vivo que existen snapshots versionados, no solo archivos sueltos.
    """
    s3 = boto3.client("s3", region_name=REGION)
    prefix = "lakehouse/sensor_events/metadata/"

    respuesta = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    archivos = [obj["Key"] for obj in respuesta.get("Contents", [])]

    print(f"Archivos de metadata encontrados ({len(archivos)}):")
    for archivo in sorted(archivos):
        print(f"  - {archivo}")


if __name__ == "__main__":
    print("=== Paso 1: generar tráfico ===")
    generar_eventos(50)

    print("\nEsperando 90s para el checkpoint de Flink...")
    time.sleep(90)

    print("\n=== Paso 2: verificar tabla en Glue ===")
    verificar_tabla_glue()

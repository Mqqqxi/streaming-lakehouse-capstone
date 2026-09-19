-- CLASE 6 - Paso 2
-- Mapea el Kinesis Stream de la Sem 2 como objeto consultable, y
-- crea la Materialized View donde aterrizan los datos calientes.
-- Reemplazar <cuenta>, el nombre del rol, y el nombre del stream si
-- lo cambiaste.
--
-- IMPORTANTE: prueba_en_vivo.py manda los eventos como texto plano
-- CSV ("sensor-4,30.79"), NO como JSON. Por eso el parseo usa
-- SPLIT_PART, no JSON_PARSE -- una version anterior de este script
-- usaba JSON_PARSE + WHERE CAN_JSON_PARSE(...), lo cual filtraba
-- el 100% de las filas sin dar ningun error (el filtro simplemente
-- nunca encontraba JSON valido, porque los datos nunca fueron JSON).
-- Si tu productor de datos SI manda JSON real, volver a JSON_PARSE
-- tiene sentido -- pero confirma el formato real del payload antes
-- de asumirlo.

CREATE EXTERNAL SCHEMA kinesis_raw
FROM KINESIS
IAM_ROLE 'arn:aws:iam::<cuenta>:role/dev-redshift-streaming-ingest-role';

CREATE MATERIALIZED VIEW sensor_events_live AUTO REFRESH YES AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    SPLIT_PART(from_varbyte(kinesis_data, 'utf-8'), ',', 1) AS sensor_id,
    SPLIT_PART(from_varbyte(kinesis_data, 'utf-8'), ',', 2)::float AS avg_temperature
FROM kinesis_raw."dev-events-stream";  -- el MISMO stream de la Sem 2

-- Vista de consumo: ya parseada, lista para el equipo de BI.
-- Este es el "firewall de datos" del que habla la teoria: nadie
-- consulta kinesis_raw directamente, solo esta vista transformada.
CREATE SCHEMA IF NOT EXISTS analytics_consumption;

CREATE VIEW analytics_consumption.sensor_events_parsed AS
SELECT
    approximate_arrival_timestamp AS event_time,
    sensor_id,
    avg_temperature
FROM sensor_events_live;

-- CLASE 6 - Paso 5
-- Diagnostico: por que fallo (o no) el ultimo refresh de la MV.

SELECT *
FROM SVL_MV_REFRESH_STATUS
WHERE mv_name = 'sensor_events_live'
ORDER BY start_time DESC
LIMIT 5;

-- Si el lag crece de forma sostenida (MillisBehindLatest en
-- CloudWatch), es Consumer Lag: escalar shards de Kinesis o el
-- base_capacity del workgroup de Redshift.

package com.coderhouse.lakehouse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
 * CLASE 5 - Job de Flink con sink a Iceberg/Glue
 *
 * Este job retoma el consumo de Kinesis, watermarks y ventana de
 * agregacion que ya existian desde la Semana 4, y agrega el sink
 * nuevo que escribe en una tabla Iceberg registrada en Glue.
 *
 * Bloques marcados "YA EXISTIA (Sem 4)" son lo que el grupo ya
 * tenia funcionando. Los marcados "NUEVO EN CLASE 5" son lo que
 * se agrega hoy.
 */
public class LakehouseStreamingJob {

    public static void main(String[] args) throws Exception {

        // ---- YA EXISTIA (Sem 4): entorno y checkpoints ----
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000); // 1 min, necesario para que Iceberg comitee

        // Managed Flink expone las environment_properties definidas en
        // Terraform (flink.tf) a traves de esta API del runtime, NO
        // como variables de entorno del sistema operativo.
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        if (applicationProperties == null) {
            throw new RuntimeException("KinesisAnalyticsRuntime.getApplicationProperties() devolvio null");
        }

        Properties flinkAppProps = applicationProperties.get("FlinkAppProperties");

        if (flinkAppProps == null) {
            throw new RuntimeException(
                    "No se encontro el grupo 'FlinkAppProperties'. Grupos disponibles: "
                            + applicationProperties.keySet());
        }

        String streamArn = flinkAppProps.getProperty("KINESIS_STREAM_ARN");
        String lakehouseBucket = flinkAppProps.getProperty("LAKEHOUSE_BUCKET");

        if (streamArn == null || lakehouseBucket == null) {
            throw new RuntimeException(
                    "Faltan propiedades. Claves encontradas en FlinkAppProperties: "
                            + flinkAppProps.stringPropertyNames()
                            + " -- streamArn=" + streamArn
                            + " lakehouseBucket=" + lakehouseBucket);
        }

        // ---- YA EXISTIA (Sem 4) / CORREGIDO: consumo del stream de la Sem 2 ----
        // El sourceConfig necesita la region de AWS explicita -- el
        // conector de Kinesis NO la deduce del entorno ni de ningun
        // otro lado. La extraemos del mismo ARN del stream, que tiene
        // el formato arn:aws:kinesis:REGION:ACCOUNT:stream/NOMBRE.
        String awsRegion = streamArn.split(":")[3];

        org.apache.flink.configuration.Configuration sourceConfig =
                new org.apache.flink.configuration.Configuration();
        sourceConfig.setString("aws.region", awsRegion);

        KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
                .setStreamArn(streamArn)
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(new SimpleStringSchema())
                .build();

        // ---- YA EXISTIA (Sem 4) / CORREGIDO: consumo del stream de la Sem 2 ----
        // El watermark necesita un timestamp assigner explicito. Sin
        // esto, todos los eventos quedan con el mismo tiempo de evento
        // interno y la ventana de agregacion NUNCA se cierra -- por
        // eso la tabla se creaba bien pero nunca tenia datos reales
        // (metadata/ existia, data/ nunca se llegaba a crear).
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, recordTimestamp) -> System.currentTimeMillis());

        DataStream<String> rawEvents = env.fromSource(
                source,
                watermarkStrategy,
                "kinesis-source",
                TypeInformation.of(String.class)
        );

        // ---- YA EXISTIA (Sem 4): ventana de agregacion stateful ----
        KeyedStream<String, String> keyed = rawEvents.keyBy(LakehouseStreamingJob::extractSensorId);

        // ---- CORREGIDO: ventana por TIEMPO DE PROCESAMIENTO, no por
        // tiempo de evento. Con event time, el watermark solo avanza
        // cuando llegan eventos nuevos -- si el trafico llega en una
        // sola rafaga y se corta (como en esta demo), el watermark se
        // congela y la ventana NUNCA cierra. Processing time se basa
        // en el reloj de pared del operador, asi que cierra siempre,
        // haya o no trafico nuevo en ese momento.
        DataStream<String> aggregated = keyed
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new AverageAggregateFunction());

        // ---- NUEVO EN CLASE 5: catalogo Glue para Iceberg ----
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type", "iceberg");
        catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProps.put("warehouse", "s3://" + lakehouseBucket + "/lakehouse/");
        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        // La firma real de CatalogLoader.custom exige un Configuration
        // de Hadoop, aunque Glue no use HDFS -- es un requisito de la
        // API, no de la logica de negocio.
        CatalogLoader catalogLoader = CatalogLoader.custom(
                "glue_catalog",
                catalogProps,
                new Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
        );

        TableIdentifier tableId = TableIdentifier.of("lakehouse_db", "sensor_events");

        // ---- NUEVO EN CLASE 5: crear la tabla si todavia no existe ----
        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableId)) {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "sensor_id", Types.StringType.get()),
                    Types.NestedField.required(2, "avg_temperature", Types.DoubleType.get()),
                    Types.NestedField.required(3, "event_time_millis", Types.LongType.get())
            );
            catalog.createTable(tableId, schema);
        }

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // ---- NUEVO EN CLASE 5: convertir el stream a RowData y escribir ----
        DataStream<RowData> rowDataStream = aggregated.map(new ToRowDataMapper());

        FlinkSink.forRowData(rowDataStream)
                .tableLoader(tableLoader)
                .append();

        env.execute("clase5-lakehouse-streaming");
    }

    // ---- YA EXISTIA (Sem 4): parseo del sensor_id ----
    private static String extractSensorId(String event) {
        return event.split(",")[0];
    }

    // ---- YA EXISTIA en espiritu (Sem 4): funcion de agregacion ----
    // Calcula el promedio de temperatura por sensor dentro de la ventana.
    public static class AverageAggregateFunction
            implements AggregateFunction<String, Accumulator, String> {

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(String value, Accumulator acc) {
            String[] parts = value.split(",");
            acc.sensorId = parts[0];
            acc.sum += Double.parseDouble(parts[1]);
            acc.count += 1;
            return acc;
        }

        @Override
        public String getResult(Accumulator acc) {
            double avg = acc.count == 0 ? 0.0 : acc.sum / acc.count;
            return acc.sensorId + "," + avg;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            Accumulator merged = new Accumulator();
            merged.sensorId = (a.sensorId != null) ? a.sensorId : b.sensorId;
            merged.sum = a.sum + b.sum;
            merged.count = a.count + b.count;
            return merged;
        }
    }

    public static class Accumulator implements Serializable {
        String sensorId;
        double sum = 0.0;
        long count = 0;
    }

    // ---- NUEVO EN CLASE 5: pasar el resultado agregado a RowData ----
    public static class ToRowDataMapper implements MapFunction<String, RowData> {
        @Override
        public RowData map(String value) {
            String[] parts = value.split(",");
            GenericRowData row = new GenericRowData(3);
            row.setField(0, StringData.fromString(parts[0]));
            row.setField(1, Double.parseDouble(parts[1]));
            row.setField(2, System.currentTimeMillis());
            return row;
        }
    }
}

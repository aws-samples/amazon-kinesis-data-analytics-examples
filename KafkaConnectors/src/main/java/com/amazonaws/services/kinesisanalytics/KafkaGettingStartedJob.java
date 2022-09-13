package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;


public class KafkaGettingStartedJob {

    private static DataStream<String> createKafkaSourceFromApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        Properties sourceProperties = KinesisAnalyticsRuntime.getApplicationProperties().get("KafkaSource");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers((String) sourceProperties.get("bootstrap.servers"))
                .setTopics((String) sourceProperties.get("topic"))
                .setGroupId("kafka-replication")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    private static KafkaSink<String> createKafkaSinkFromApplicationProperties() throws IOException {
        Properties sinkProperties = KinesisAnalyticsRuntime.getApplicationProperties().get("KafkaSink");

        return KafkaSink.<String>builder()
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic((String) sinkProperties.get("topic"))
                        .setKeySerializationSchema(new SimpleStringSchema())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createKafkaSourceFromApplicationProperties(env);

        // Add sink
        input.sinkTo(createKafkaSinkFromApplicationProperties());

        env.execute("Flink Streaming Java API Skeleton");
    }
}
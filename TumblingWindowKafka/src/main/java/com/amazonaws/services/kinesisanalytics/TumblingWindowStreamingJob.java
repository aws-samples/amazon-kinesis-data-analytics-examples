package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class TumblingWindowStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentInformation.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String version = EnvironmentInformation.getVersion();
        LOG.error("Flink Version is {}", version);

        DataStreamSource<String> inputDataStream = createKafkaSourceFromApplicationProperties(env);
        KafkaSink<String> sink = createKafkaSinkFromApplicationProperties();

        ObjectMapper jsonParser = new ObjectMapper();
        DataStream<Tuple2<String, Integer>> aggregateCount = inputDataStream.map(value -> {
                    JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                    return new Tuple2<>(jsonNode.get("ticker").toString(), 1);
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0) // Logically partition the stream for each word
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1); // Sum the number of words per partition

        aggregateCount.map(Tuple2::toString).sinkTo(sink);

        env.execute("Tumbling Window Word Count");
    }

    private static DataStreamSource<String> createKafkaSourceFromApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        Properties sourceProperties = KinesisAnalyticsRuntime.getApplicationProperties().get("KafkaSource");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers((String) sourceProperties.get("bootstrap.servers"))
                .setTopics((String) sourceProperties.get("topic"))
                .setGroupId("kafka-replication")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(sourceProperties)
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    private static KafkaSink<String> createKafkaSinkFromApplicationProperties() throws IOException {
        Properties sinkProperties = KinesisAnalyticsRuntime.getApplicationProperties().get("KafkaSink");

        return KafkaSink.<String>builder()
                .setBootstrapServers(sinkProperties.getProperty("bootstrap.servers"))
                .setKafkaProducerConfig(sinkProperties)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic((String) sinkProperties.get("topic"))
                        .setKeySerializationSchema(new SimpleStringSchema())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }
}

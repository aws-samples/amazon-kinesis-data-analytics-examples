package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TumblingWindowStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentInformation.class);

    public static void main(String[] args) throws Exception
    {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        String version = EnvironmentInformation.getVersion();
        LOG.error("Flink Version is...", version);

        String inputTopic = (String) applicationProperties.get("KafkaSource").getOrDefault("topic", "input");
        String outputTopic = (String) applicationProperties.get("KafkaSink").getOrDefault("topic", "output");

        DataStreamSource<String> inputDataStream =
                env.addSource(new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                applicationProperties.get("KafkaSource")));

        FlinkKafkaProducer sink = setupKafkaSink(applicationProperties, outputTopic);

        ObjectMapper jsonParser = new ObjectMapper();
        DataStream<Tuple2<String, Integer>> aggregateCount = inputDataStream.map(value ->
        {
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            return new Tuple2<>(jsonNode.get("TICKER").toString(), 1);

        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0) // Logically partition the stream for each word
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1); // Sum the number of words per partition

        aggregateCount.map(x -> x.toString()).addSink(sink);




        env.execute("Tumbling Window Word Count");

    }

    private static FlinkKafkaProducer<String> setupKafkaSink(Map<String, Properties> applicationProperties, String outputTopic) {
        KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        outputTopic, // target topic
                        element.getBytes(StandardCharsets.UTF_8)); // record contents
            }
        };

         FlinkKafkaProducer<String> sink =
                new FlinkKafkaProducer<>(
                        outputTopic,
                serializationSchema,
                applicationProperties.get("KafkaSink"),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

         return sink;
    }

}

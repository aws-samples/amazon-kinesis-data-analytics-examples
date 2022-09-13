package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.time.Duration;
import java.util.Properties;

public class S3StreamingSinkJob {
    private static final String region = "us-west-2";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String s3SinkPath = "s3a://ka-app-<username>/data";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");
        inputProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");
        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new SimpleStringSchema(),
                inputProperties));
    }

    private static StreamingFileSink<String> createS3SinkFromStaticConfig() {
        return StreamingFileSink
                .forRowFormat(new Path(s3SinkPath), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(2))
                                .withInactivityInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024 * 10))
                                .build())
                .build();
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createSourceFromStaticConfig(env);

        ObjectMapper jsonParser = new ObjectMapper();

        input.map(value -> { // Parse the JSON
                    JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                    return new Tuple2<>(jsonNode.get("ticker").toString(), 1);
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(v -> v.f0) // Logically partition the stream for each word
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .sum(1) // Count the appearances by ticker per partition
                .map(value -> value.f0 + " count: " + value.f1.toString() + "\n")
                .addSink(createS3SinkFromStaticConfig());

        env.execute("Flink S3 Streaming Sink Job");
    }
}

package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class LambdaSinkJob {
    private static final String region = "eu-west-1";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String lambdaFunctionName = "my-lambda-function";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");
        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new SimpleStringSchema(),
                inputProperties));
    }

    private static AwsLambdaSink<TickCount> createS3SinkFromStaticConfig() {
        return new AwsLambdaSink<>(lambdaFunctionName, TickCount.class);
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createSourceFromStaticConfig(env);

        ObjectMapper jsonParser = new ObjectMapper();

        input.map(value -> { // Parse the JSON
                    JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                    return new Tuple2<>(jsonNode.get("ticker").toString(), 1);
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0) // Logically partition the stream for each word
                // .timeWindow(Time.minutes(1)) // Tumbling window definition // Flink 1.11
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) // Flink 1.13
                .sum(1) // Count the appearances by ticker per partition
                .map(t -> new TickCount(t.f0, t.f1))
                .addSink(createS3SinkFromStaticConfig());

        env.execute("Flink Lambda Sink Job");
    }
}

package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

/**
 * A Kinesis Data Analytics for Java application that calculates minimum stock
 * price for all stock symbols in a given Kinesis stream over a sliding window
 * and overrides the operator level parallelism in the flink application.
 * <p>
 * Note that the maximum parallelism in the Flink code cannot be greater than
 * provisioned parallelism (default is 1). To get this application to work,
 * use following AWS CLI commands to set the parallelism configuration of the
 * Kinesis Data Analytics for Java application.
 * <p>
 * 1. Fetch the current application version Id using following command:
 * aws kinesisanalyticsv2 describe-application --application-name <Application Name>
 * 2. Update the parallelism configuration of the application using version Id:
 * aws kinesisanalyticsv2 update-application
 * --application-name <Application Name>
 * --current-application-version-id <VersionId>
 * --application-configuration-update "{\"FlinkApplicationConfigurationUpdate\": { \"ParallelismConfigurationUpdate\": {\"ParallelismUpdate\": 5, \"ConfigurationTypeUpdate\": \"CUSTOM\" }}}"
 */
public class SlidingWindowStreamingJobWithParallelism {
    private static final String region = "us-west-2";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String outputStreamName = "ExampleOutputStream";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static KinesisStreamsSink<String> createSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(AWSConfigConstants.AWS_REGION, region);

        return KinesisStreamsSink.<String>builder()
                .setKinesisClientProperties(outputProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setStreamName(outputStreamName)
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createSourceFromStaticConfig(env);
        ObjectMapper jsonParser = new ObjectMapper();
        input.map(value -> { // Parse the JSON
                    JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                    return new Tuple2<>(jsonNode.get("ticker").toString(), jsonNode.get("price").asDouble());
                }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(v -> v.f0) // Logically partition the stream per stock symbol
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .min(1) // Calculate minimum price per stock over the window
                .setParallelism(3) // Set parallelism for the min operator
                .map(value -> value.f0 + String.format(",%.2f", value.f1) + "\n")
                .sinkTo(createSinkFromStaticConfig());

        env.execute("Min Stock Price");
    }
}

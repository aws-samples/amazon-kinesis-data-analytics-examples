package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.time.Instant;
import java.util.Properties;

/**
 * A basic Kinesis Data Analytics for Java application with Kinesis data
 * streams as source and sink.
 */
public class BasicStreamingJob {
    private static final String region = "us-east-1";
    private static final String inputStreamName = "window-data-analysis-cli";
    private static final String aggregateOutputStreamName = "aggregate-output-data-analysis-cli";
    private static final String normalOutputStreamName = "output-data-analysis-cli";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }


    private static KinesisStreamsSink<String> createSinkFromStaticConfig(String outputStreamName) {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(AWSConfigConstants.AWS_REGION, region);

        return KinesisStreamsSink.<String>builder()
                .setKinesisClientProperties(outputProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setStreamName(outputProperties.getProperty("OUTPUT_STREAM", outputStreamName))
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromStaticConfig(env);
        ObjectMapper objectMapper = new ObjectMapper();
        DataStream<String> inputToStore =input.map(payload -> objectMapper.readValue(payload, Event.class)).map(event -> event.toString() + "\n");
        DataStream<String> aggregates = input
                .map(payload -> objectMapper.readValue(payload, Event.class))
                .keyBy(Event::getGroupId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce((a, b) -> new Event(a.getGroupId(), String.valueOf(Instant.now().getEpochSecond()), a.getAmount() + b.getAmount()))
                .map(event -> event.toString() + "\n");

        inputToStore.sinkTo(createSinkFromStaticConfig(normalOutputStreamName));
        aggregates.sinkTo(createSinkFromStaticConfig(aggregateOutputStreamName));
        env.execute("Flink Streaming Java API Skeleton");
    }
}

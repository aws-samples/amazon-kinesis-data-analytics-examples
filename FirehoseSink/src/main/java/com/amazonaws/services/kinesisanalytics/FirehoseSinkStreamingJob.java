package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;

/**
 * A Kinesis Data Analytics for Java application that calculates minimum stock price for all stock symbols in a given Kinesis stream over a sliding window and
 * writes output to a Firehose Delivery Stream.
 * <p>
 * To run this application, update the mainClass definition in pom.xml.
 * <p>
 * Note that additional permissions are needed for the IAM role to use Firehose sink:
 * {
 * "Sid": "WriteDeliveryStream",
 * "Effect": "Allow",
 * "Action": "firehose:*",
 * "Resource": "arn:aws:firehose:us-west-2:012345678901:deliverystream/ExampleDeliveryStream"
 * }
 */
public class FirehoseSinkStreamingJob {
    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final String region = "us-west-2";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String outputDeliveryStreamName = "ExampleDeliveryStream";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new SimpleStringSchema(), inputProperties));
    }


    private static KinesisFirehoseSink<String> createFirehoseSinkFromStaticConfig() {
        Properties sinkProperties = new Properties();
        sinkProperties.setProperty(AWS_REGION, region);

        return KinesisFirehoseSink.<String>builder()
                .setFirehoseClientProperties(sinkProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setDeliveryStreamName(outputDeliveryStreamName)
                .build();
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createSourceFromStaticConfig(env);
        input.map(value -> { // Parse the JSON
                    JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                    return new Tuple2<>(jsonNode.get("ticker").asText(),
                            jsonNode.get("price").asDouble());
                }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(v -> v.f0) // Logically partition the stream per stock symbol
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .min(1) // Calculate the minimum price over the window
                .map(value -> value.f0 + ": min - " + value.f1.toString() + "\n")
                .sinkTo(createFirehoseSinkFromStaticConfig()); // Write to Firehose Delivery Stream

        env.execute("Minimum Stock Price");
    }
}

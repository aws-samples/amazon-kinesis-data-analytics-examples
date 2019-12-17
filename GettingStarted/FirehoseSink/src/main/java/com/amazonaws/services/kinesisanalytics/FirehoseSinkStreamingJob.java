package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

/**
 * A Kinesis Data Analytics for Java application that calculates minimum stock price for all stock symbols in a given Kinesis stream over a sliding window and
 * writes output to a Firehose Delivery Stream.
 * <p>
 * To run this application, update the mainClass definition in pom.xml.
 * <p>
 * Note that additional permissions are needed for the IAM role to use Firehose sink:
 *         {
 *             "Sid": "WriteDeliveryStream",
 *             "Effect": "Allow",
 *             "Action": "firehose:*",
 *             "Resource": "arn:aws:firehose:us-west-2:012345678901:deliverystream/ExampleDeliveryStream"
 *         }
 */
public class FirehoseSinkStreamingJob {
    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final String region = "us-west-2";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String outputDeliveryStreamName = "ExampleDeliveryStream";

    private static DataStream<String> createSourceFromStaticConfig(
            StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new SimpleStringSchema(), inputProperties));
    }


    private static FlinkKinesisFirehoseProducer<String> createFirehoseSinkFromStaticConfig() {
        /*
         * com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants
         * lists of all of the properties that firehose sink can be configured with.
         */

        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);

        FlinkKinesisFirehoseProducer<String> sink = new FlinkKinesisFirehoseProducer<>(outputDeliveryStreamName, new SimpleStringSchema(), outputProperties);
        return sink;
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createSourceFromStaticConfig(env);
        input.map(value -> { // Parse the JSON
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            return new Tuple2<>(jsonNode.get("TICKER").asText(),
                    jsonNode.get("PRICE").asDouble());
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(0) // Logically partition the stream per stock symbol
                .timeWindow(Time.seconds(10), Time.seconds(5)) // Sliding window definition
                .min(1) // Calculate the minimum price over the window
                .map(value -> value.f0 + ": min - " + value.f1.toString() + "\n")
                .addSink(createFirehoseSinkFromStaticConfig()); // write to Firehose Delivery Stream

        env.execute("Mininum Stock Price");
    }
}

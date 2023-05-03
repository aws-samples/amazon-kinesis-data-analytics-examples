package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesisanalytics.sink.CloudWatchLogSink;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogGroup;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutRetentionPolicyRequest;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.List;
import java.util.Properties;

/**
 * A Kinesis Data Analytics for Java application that calculates maximum stock
 * price for all stock symbols in a given Kinesis stream over a sliding window
 * and uses Cloud Watch Logs as the sink.
 * <p>
 * Additional permissions are required for the IAM role to run this application
 * for interaction with CloudWatchLogs (apart from the ones mentioned in link):
 *         {
 *             "Sid": "CloudWatchLogGroupPermissions",
 *             "Effect": "Allow",
 *             "Action": [
 *                 "logs:DescribeLogGroups",
 *                 "logs:DescribeLogStreams",
 *                 "logs:CreateLogGroup",
 *                 "logs:PutRetentionPolicy"
 *             ],
 *             "Resource": [
 *                 "arn:aws:logs:us-west-2:<ACCOUNT_ID>:log-group:/aws/kinesis-analytics-java/test:log-stream:*"
 *             ]
 *         },
 *         {
 *             "Sid": "CloudwatchLogStreamsPermissions",
 *             "Effect": "Allow",
 *             "Action": [
 *                 "logs:CreateLogStream",
 *                 "logs:PutLogEvents"
 *             ],
 *             "Resource": [
 *                 "arn:aws:logs:us-west-2:<ACCOUNT_ID>:log-group:/aws/kinesis-analytics-java/test:log-stream:StockPriceStatistics"
 *             ]
 *         }
 */
public class CustomSinkStreamingJob {

    private static final String region = "us-west-2";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String CLOUD_WATCH_LOG_GROUP = "/aws/kinesis-analytics-java/test";
    private static final String CLOUD_WATCH_LOG_STREAM = "StockPriceStatistics";
    private static final int LOG_RETENTION_IN_DAYS = 3;

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupLogGroupAndStream(region, CLOUD_WATCH_LOG_GROUP, CLOUD_WATCH_LOG_STREAM);

        DataStream<String> input = createSourceFromStaticConfig(env);
        ObjectMapper jsonParser = new ObjectMapper();
        input.map(value -> {
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            return new Tuple2<>(jsonNode.get("ticker").asText(), jsonNode.get("price").asDouble());
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(v -> v.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .max(1)
                .map(value -> value.f0 + ":  max - " + value.f1.toString() + "\n")
                .addSink(new CloudWatchLogSink(region, CLOUD_WATCH_LOG_GROUP, CLOUD_WATCH_LOG_STREAM));

        env.execute("Max Stock Price");
    }

    private static void setupLogGroupAndStream(String awsRegion, String logGroupName, String logStreamName) {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AWSLogs awsLogsClient = AWSLogsClientBuilder.standard().withCredentials(credentialsProvider).withRegion(awsRegion).build();

        // Create log group if it does not exist
        DescribeLogGroupsResult describeLogGroupsResult = awsLogsClient.describeLogGroups();
        List<LogGroup> logGroups = describeLogGroupsResult.getLogGroups();
        boolean logGroupExists = false;
        for (LogGroup g : logGroups) {
            if (g.getLogGroupName().equals(logGroupName)) {
                logGroupExists = true;
            }
        }
        if (!logGroupExists) {
            awsLogsClient.createLogGroup(new CreateLogGroupRequest(logGroupName));
            awsLogsClient.putRetentionPolicy(new PutRetentionPolicyRequest()
                    .withLogGroupName(logGroupName)
                    .withRetentionInDays(LOG_RETENTION_IN_DAYS));
        }

        // Create log stream if it does not exist
        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest()
                .withLogGroupName(logGroupName).withLogStreamNamePrefix(logStreamName);
        DescribeLogStreamsResult describeLogStreamsResult = awsLogsClient.describeLogStreams(describeLogStreamsRequest);
        List<LogStream> logStreams = describeLogStreamsResult.getLogStreams();
        if (logStreams.isEmpty()) {
            awsLogsClient.createLogStream(new CreateLogStreamRequest(logGroupName, logStreamName));
        }
    }
}
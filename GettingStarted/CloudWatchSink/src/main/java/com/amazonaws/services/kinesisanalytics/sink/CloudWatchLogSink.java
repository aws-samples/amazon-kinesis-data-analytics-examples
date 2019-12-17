package com.amazonaws.services.kinesisanalytics.sink;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * A sink to write output as log messages to AWS Cloud Watch Logs.
 * This class is not thread-safe or optimized for performance.
 * This class does not implement any checkpoint semantics nor
 * extend the CheckpointedFunction interface.
 */
public class CloudWatchLogSink extends RichSinkFunction<String> {
    private static final int MAX_BATCH_SIZE = 10000;
    private static final long MAX_BUFFER_TIME_MILLIS = 60 * 1000;

    private transient AWSLogs awsLogsClient;
    private String awsRegion;
    private String logGroupName;
    private List<InputLogEvent> logEvents;
    private String logStreamName;
    private long lastFlushTimeMillis;

    public CloudWatchLogSink(String awsRegion, String logGroupName, String logStreamName) {
        this.awsRegion = awsRegion;
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
        this.logEvents = new ArrayList<>();
        this.lastFlushTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) {
        final AWSCredentialsProvider credentialsProvider  = new DefaultAWSCredentialsProviderChain();
        awsLogsClient = AWSLogsClientBuilder.standard().withCredentials(credentialsProvider).withRegion(awsRegion).build();
    }

    @Override
    public void invoke(String message, Context context) {
        logEvents.add(new InputLogEvent().withMessage(message).withTimestamp(System.currentTimeMillis()));
        if (logEvents.size() >= MAX_BATCH_SIZE || lastFlushTimeMillis + MAX_BUFFER_TIME_MILLIS <= System.currentTimeMillis()) {
            // flush the messages
            PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest()
                    .withLogEvents(logEvents)
                    .withLogGroupName(logGroupName)
                    .withLogStreamName(logStreamName)
                    .withSequenceToken(getUploadSequenceToken());
            awsLogsClient.putLogEvents(putLogEventsRequest);
            lastFlushTimeMillis = System.currentTimeMillis();
            logEvents.clear();
        }
    }

    private String getUploadSequenceToken() {
        DescribeLogStreamsRequest req = new DescribeLogStreamsRequest().withLogGroupName(logGroupName).withLogStreamNamePrefix(logStreamName);
        DescribeLogStreamsResult res = awsLogsClient.describeLogStreams(req);
        return res.getLogStreams().get(0).getUploadSequenceToken();
    }
}

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Tests for {@link AwsLambdaSink}.
 */
public class AwsLambdaSinkTests {

    /**
     * Create Lambda sink with attributes to test all scenarios.
     */
    private static final long MAX_PAYLOAD_BYTES = 256 * 1000; // 256 KB for async lambda invocation
    private static final long MAX_BUFFER_TIME_MILLIS = 3000L;
    private static final int MAX_CONCURRENCY = 2;
    private static final int MAX_RECORDS_PER_CALL = 2;
    private static final String FUNCTION_NAME = "test";
    private final ObjectMapper jsonParser = new ObjectMapper();

    public AwsLambdaSink<TestRecord> initSink(AWSLambdaAsync mockLambda) {
        return new AwsLambdaSink<>(FUNCTION_NAME, TestRecord.class)
                .withAwsLambdaClient(mockLambda)
                .withMaxBufferTimeInMillis(24 * 60 * 1000) // 24 hours
                .withMaxConcurrency(10000)
                .withMaxRecordsPerFunctionCall(10000)
                .withSkipBadRecords(true);
    }

    @Test
    public void shouldPublishOnTimeout() throws Exception {
        AWSLambdaAsync mockLambda = Mockito.mock(AWSLambdaAsync.class);
        SinkFunction.Context mockContext = Mockito.mock(SinkFunction.Context.class);
        List<TestRecord> list = new ArrayList<>();
        TestRecord TestRecord = new TestRecord(1);
        list.add(TestRecord);
        list.add(TestRecord);

        AwsLambdaSink<TestRecord> awsLambdaSink = initSink(mockLambda).withMaxBufferTimeInMillis(MAX_BUFFER_TIME_MILLIS);
        awsLambdaSink.open(new Configuration());
        awsLambdaSink.invoke(TestRecord, mockContext);
        Thread.sleep(MAX_BUFFER_TIME_MILLIS + 1);
        awsLambdaSink.invoke(TestRecord, mockContext);
        Mockito.verify(mockLambda).invoke(new InvokeRequest()
                .withFunctionName(FUNCTION_NAME)
                .withInvocationType(InvocationType.Event)
                .withPayload(jsonParser.writeValueAsString(list)));

    }

    @Test
    public void shouldPublishOnMaxRecordsPerInvoke() throws Exception {
        AWSLambdaAsync mockLambda = Mockito.mock(AWSLambdaAsync.class);
        SinkFunction.Context mockContext = Mockito.mock(SinkFunction.Context.class);
        List<TestRecord> list = new ArrayList<>();
        TestRecord TestRecord = new TestRecord(1);
        AwsLambdaSink<TestRecord> awsLambdaSink = initSink(mockLambda)
                .withMaxRecordsPerFunctionCall(MAX_RECORDS_PER_CALL)
                .withMaxConcurrency(MAX_CONCURRENCY);
        awsLambdaSink.open(new Configuration());
        for (int i = 0; i < (MAX_RECORDS_PER_CALL * MAX_CONCURRENCY) + 1; i++) {
            list.add(TestRecord);
            awsLambdaSink.invoke(TestRecord, mockContext);
        }

        Mockito.verify(mockLambda, Mockito.times(MAX_CONCURRENCY)).invoke(Mockito.any(InvokeRequest.class));
        int j = 0;
        for (int i = 0; i < MAX_CONCURRENCY + 1; i++) {
            j += i == MAX_CONCURRENCY ? 1 : MAX_RECORDS_PER_CALL;
            Mockito.verify(mockLambda, Mockito.atLeastOnce()).invoke(new InvokeRequest()
                    .withFunctionName(FUNCTION_NAME)
                    .withInvocationType(InvocationType.Event)
                    .withPayload(jsonParser.writeValueAsString(list.subList(j - MAX_RECORDS_PER_CALL, j)))
            );
        }
    }

    @Test
    public void shouldPublishOnBufferedBytesReached() throws Exception {
        AWSLambdaAsync mockLambda = Mockito.mock(AWSLambdaAsync.class);
        SinkFunction.Context mockContext = Mockito.mock(SinkFunction.Context.class);
        List<TestRecord> list = new ArrayList<>();
        TestRecord testRecord = new TestRecord();
        testRecord.setRandomBytes(new byte[20000]);
        AwsLambdaSink<TestRecord> awsLambdaSink = initSink(mockLambda).withMaxConcurrency(1);
        awsLambdaSink.open(new Configuration());
        long bytesSentSoFar = 0;
        while (bytesSentSoFar < MAX_PAYLOAD_BYTES) {
            list.add(testRecord);
            awsLambdaSink.invoke(testRecord, mockContext);
            bytesSentSoFar += jsonParser.writeValueAsBytes(testRecord).length;
        }

        Mockito.verify(mockLambda, Mockito.times(2)).invoke(Mockito.any(InvokeRequest.class));
        Mockito.verify(mockLambda, Mockito.atLeastOnce()).invoke(new InvokeRequest()
                .withFunctionName(FUNCTION_NAME)
                .withInvocationType(InvocationType.Event)
                .withPayload(jsonParser.writeValueAsString(list.subList(0, list.size() - 1)))
        );
        Mockito.verify(mockLambda, Mockito.atLeastOnce()).invoke(new InvokeRequest()
                .withFunctionName(FUNCTION_NAME)
                .withInvocationType(InvocationType.Event)
                .withPayload(jsonParser.writeValueAsString(list.subList(list.size() - 1, list.size())))
        );
    }

    @Test
    public void shouldSkipBadRecord() throws Exception {
        AWSLambdaAsync mockLambda = Mockito.mock(AWSLambdaAsync.class);
        SinkFunction.Context mockContext = Mockito.mock(SinkFunction.Context.class);
        List<TestRecord> list = new ArrayList<>();
        TestRecord goodRecord = new TestRecord(1);
        TestRecord badRecord = new TestRecord();
        list.add(goodRecord);
        badRecord.setRandomBytes(new byte[257 * 1000]); // 257 KB record
        AwsLambdaSink<TestRecord> awsLambdaSink = initSink(mockLambda).withMaxConcurrency(1)
                .withMaxRecordsPerFunctionCall(1);
        awsLambdaSink.open(new Configuration());
        awsLambdaSink.invoke(goodRecord, mockContext);
        awsLambdaSink.invoke(badRecord, mockContext); // Should skip this call

        Mockito.verify(mockLambda, Mockito.only()).invoke(new InvokeRequest()
                .withFunctionName(FUNCTION_NAME)
                .withInvocationType(InvocationType.Event)
                .withPayload(jsonParser.writeValueAsString(list))
        );
    }

    @Test(expected = RuntimeException.class)
    public void shouldRaiseExceptionOnBadRecord() throws Exception {
        AWSLambdaAsync mockLambda = Mockito.mock(AWSLambdaAsync.class);
        SinkFunction.Context mockContext = Mockito.mock(SinkFunction.Context.class);
        TestRecord badRecord = new TestRecord();
        badRecord.setRandomBytes(new byte[257 * 1000]); // 257 KB record
        AwsLambdaSink<TestRecord> awsLambdaSink = initSink(mockLambda).withMaxConcurrency(1)
                .withMaxRecordsPerFunctionCall(1)
                .withSkipBadRecords(false);
        awsLambdaSink.open(new Configuration());
        awsLambdaSink.invoke(badRecord, mockContext); // Should Raise error this call
    }

    @Test(expected = InvalidDefinitionException.class)
    public void shouldRaiseJsonParsingException() throws Exception {
        SinkFunction.Context mockContext = Mockito.mock(SinkFunction.Context.class);
        AwsLambdaSink<AwsLambdaSinkTests> awsLambdaSink = new AwsLambdaSink<>(FUNCTION_NAME, AwsLambdaSinkTests.class);
        awsLambdaSink.open(new Configuration());
        awsLambdaSink.invoke(new AwsLambdaSinkTests(), mockContext); // Should Raise error this call
    }
}

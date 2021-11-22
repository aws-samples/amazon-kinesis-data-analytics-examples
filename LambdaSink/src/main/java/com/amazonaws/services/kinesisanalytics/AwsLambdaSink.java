package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Aws lambda sink takes json as string & invokes lambda function as needed. Use with methods to tune behaviour.
 * <p>
 * {@link AwsLambdaSink#withMaxBufferTimeInMillis(long)}
 * {@link AwsLambdaSink#withMaxConcurrency(int)}
 * {@link AwsLambdaSink#withMaxConcurrency(int)}
 */
public class AwsLambdaSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AwsLambdaSink.class);

    private final String functionName;
    private final Class<T> recordType;
    private boolean skipBadRecords = true;
    private int maxRecordsPerFunctionCall = 100;
    private int maxConcurrency = 100; // Maximum concurrent lambda invocation
    private long maxBufferTimeInMillis = 60 * 1000; // 1 minute
    private static final long MAX_PAYLOAD_BYTES = 256 * 1000; // 256 KB for async lambda invocation
    private List<T> bufferedRecords;
    private long buffedBytes;
    private long lastPublishTime;
    private transient ListState<T> checkPointedState;
    private AWSLambdaAsync awsLambdaAsync;
    private final ObjectMapper jsonParser = new ObjectMapper();


    /**
     * Creates new AWSLambdaSink
     *
     * @param functionName the lambda function to invoke, Name, Alias or ARN is supported
     * @param recordType   the type of the records this sink will process
     */
    public AwsLambdaSink(String functionName, Class<T> recordType) {
        this.functionName = functionName;
        this.recordType = recordType;
    }

    /**
     * Sets maxRecordsPerFunctionCall
     *
     * @param maxRecordsPerFunctionCall the max records to pass per lambda function invocation. Default: 100
     * @return this object for chaining
     */
    public AwsLambdaSink<T> withMaxRecordsPerFunctionCall(int maxRecordsPerFunctionCall) {
        this.maxRecordsPerFunctionCall = maxRecordsPerFunctionCall;
        return this;
    }

    /**
     * Sets maxBufferTimeInMillis
     *
     * @param maxBufferTimeInMillis the max buffer time to keep records in memory. Set this value according to the lateness allowed. Default: 1 minute
     * @return this object for chaining
     */
    public AwsLambdaSink<T> withMaxBufferTimeInMillis(long maxBufferTimeInMillis) {
        this.maxBufferTimeInMillis = maxBufferTimeInMillis;
        return this;
    }

    /**
     * Sets maxConcurrency
     *
     * @param maxConcurrency Maximum number of lambda function calls to be done concurrently. Default: 100
     * @return this object for chaining
     */
    public AwsLambdaSink<T> withMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        return this;
    }

    /**
     * Sets skipBadRecords
     *
     * @param skipBadRecords Continue processing by skipping bad records. Set this value to FALSE for failing on bad record. Default: true
     * @return this object for chaining
     */
    public AwsLambdaSink<T> withSkipBadRecords(boolean skipBadRecords) {
        this.skipBadRecords = skipBadRecords;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.awsLambdaAsync = AWSLambdaAsyncClientBuilder.defaultClient();
        this.bufferedRecords = new ArrayList<>();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        // Ensure all records are under max lambda payload size
        byte[] valueAsBytes = jsonParser.writeValueAsBytes(value);
        if (valueAsBytes.length > MAX_PAYLOAD_BYTES) {
            if (skipBadRecords) {
                LOG.warn("Skipping record with MD5 hash " + DigestUtils.md5Hex(valueAsBytes) + " as it exceeds max allowed lambda function payload size.");
                return;
            } else
                throw new RuntimeException("Record with MD5 hash " + DigestUtils.md5Hex(valueAsBytes) + " exceeds max allowed lambda function payload size.");
        }

        // Add new received record to the buffer
        bufferedRecords.add(value);
        this.buffedBytes += valueAsBytes.length;

        if (shouldPublish()) {
            List<List<T>> batches = new ArrayList<>();
            int currentBatchIndex = 0;
            int recordsInCurrentBatch = 0;
            long bytesInCurrentBatch = 0;
            batches.add(new ArrayList<>());
            for (T bufferedRecord : bufferedRecords) {
                if (recordsInCurrentBatch < maxRecordsPerFunctionCall
                        || bytesInCurrentBatch < (MAX_PAYLOAD_BYTES - (bufferedRecords.size() * 2L) - 4)
                    // current batch will be converted as array which adds 4 bytes for bracket & 2 bytes for each comma
                    // {rec1} = 20 bytes, {rec2} = 40 bytes will be converted to
                    // [{rec1},{rec2}] here array square bracket adds 2 character & one comma per record which all occupies 2 bytes each
                ) {
                    String record = jsonParser.writeValueAsString(bufferedRecord);
                    batches.get(currentBatchIndex).add(bufferedRecord);
                    recordsInCurrentBatch++;
                    bytesInCurrentBatch += record.getBytes().length;
                } else {
                    batches.add(++currentBatchIndex, new ArrayList<>());
                    recordsInCurrentBatch = 0;
                }
            }

            batches.parallelStream().forEach(batch -> {
                try {
                    awsLambdaAsync.invoke(new InvokeRequest()
                            .withFunctionName(functionName)
                            .withInvocationType(InvocationType.Event)
                            .withPayload(jsonParser.writeValueAsString(batch)));
                } catch (JsonProcessingException e) {
                    LOG.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage(), e);
                }
            });

            // Reset all once published
            bufferedRecords.clear();
            this.buffedBytes = 0;
            this.lastPublishTime = System.currentTimeMillis();
        }
    }

    /**
     * Publish records from buffer when
     * <li>Buffered records are more than max records per function call * maximum concurrent calls we can make</li>
     * <li>Total bytes accumulated are more than max payload (256 KB) times concurrency</li>
     * <li>Maximum lateness has reached (i.e. max buffer time in milliseconds)</li>
     *
     * @return true if any of above defined condition is met, false otherwise
     */
    private boolean shouldPublish() {
        return bufferedRecords.size() > maxRecordsPerFunctionCall * maxConcurrency
                || (buffedBytes / MAX_PAYLOAD_BYTES) > maxConcurrency
                || (lastPublishTime + maxBufferTimeInMillis) > System.currentTimeMillis();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointedState.clear();
        for (T element : bufferedRecords) {
            checkPointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>("recordList", recordType);

        checkPointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (T element : checkPointedState.get()) {
                bufferedRecords.add(element);
            }
        }
    }
}

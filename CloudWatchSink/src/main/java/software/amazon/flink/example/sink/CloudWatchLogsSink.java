/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.flink.example.sink;

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import lombok.Builder;
import static java.util.Collections.emptyList;

public class CloudWatchLogsSink<T> extends AsyncSinkBase<T, LogEvent> {
    private static final long serialVersionUID = 0L;

    // https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
    // The maximum number of log events in a batch is 10,000.
    private static final int MAX_BATCH_SIZE = 10_000;
    // The maximum batch size is 1,048,576 bytes
    private static final int MAX_BATCH_SIZE_IN_BYTES = 1_048_576;
    private static final int MAX_IN_FLIGHT_REQUESTS = 8;
    private static final int MAX_BUFFERED_LOGS = MAX_BATCH_SIZE * MAX_IN_FLIGHT_REQUESTS;
    private static final int MAX_TIME_IN_BUFFER_MS = 10_000;
    // Each log event can be no larger than 256 KB.
    private static final int MAX_LOG_SIZE = 262_144;

    private final String logGroupName;
    private final String logStreamName;
    private final Properties clientProperties;

    @Builder
    public CloudWatchLogsSink(
            final String logGroupName,
            final String logStreamName,
            final Properties clientProperties,
            final ElementConverter<T, LogEvent> elementConverter) {
        super(elementConverter,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_LOGS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_LOG_SIZE);
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
        this.clientProperties = clientProperties;
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<LogEvent>> createWriter(InitContext initContext) throws IOException {
        return restoreWriter(initContext, emptyList());
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<LogEvent>> restoreWriter(InitContext initContext, Collection<BufferedRequestState<LogEvent>> collection) throws IOException {
        return new CloudWatchLogsSinkWriter<>(
                logGroupName,
                logStreamName,
                clientProperties,
                getElementConverter(),
                initContext,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                collection,
                CloudWatchLogsSinkWriter::createSdkAsyncHttpClient,
                CloudWatchLogsSinkWriter::createCloudWatchLogsAsyncClient);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<LogEvent>> getWriterStateSerializer() {
        return new CloudWatchLogsStateSerializer();
    }
}

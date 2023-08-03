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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.util.AWSAsyncSinkUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;
import static java.util.Optional.ofNullable;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkFatalExceptionClassifiers.getInterruptedExceptionClassifier;

@Slf4j
public class CloudWatchLogsSinkWriter<T> extends AsyncSinkWriter<T, LogEvent> {

    private static final FatalExceptionClassifier FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(getInterruptedExceptionClassifier());

    private final String logGroupName;
    private final String logStreamName;
    private final SdkAsyncHttpClient asyncHttpClient;
    private final CloudWatchLogsAsyncClient cloudWatchLogsAsyncClient;

    public CloudWatchLogsSinkWriter(
            String logGroupName,
            String logStreamName,
            Properties clientProperties,
            ElementConverter<T, LogEvent> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            Collection<BufferedRequestState<LogEvent>> states,
            Function<Properties, SdkAsyncHttpClient> asyncHttpClientSupplier,
            BiFunction<Properties, SdkAsyncHttpClient, CloudWatchLogsAsyncClient> cloudWatchLogsClientSupplier) {
        super(elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                states);
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;

        clientProperties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

        asyncHttpClient = asyncHttpClientSupplier.apply(clientProperties);
        cloudWatchLogsAsyncClient = cloudWatchLogsClientSupplier.apply(clientProperties, asyncHttpClient);
    }

    @Override
    protected void submitRequestEntries(
            List<LogEvent> requestEntries,
            Consumer<List<LogEvent>> failedLogs) {
        List<LogEvent> logsToSend = requestEntries.stream()
                // The log events in the batch must be in chronological order by their timestamp.
                .sorted(Comparator.comparing(LogEvent::getTimestamp))
                .collect(Collectors.toList());

        PutLogEventsRequest request = PutLogEventsRequest.builder()
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .logEvents(logsToSend.stream()
                        .map(logEvent -> InputLogEvent.builder()
                                .message(logEvent.getLog())
                                .timestamp(logEvent.getTimestamp().toEpochMilli())
                                .build())
                        .collect(Collectors.toList()))
                .build();

        CompletableFuture<PutLogEventsResponse> future = cloudWatchLogsAsyncClient.putLogEvents(request);

        log.debug("Sending batch of {} logs", logsToSend.size());

        future.whenComplete((response, error) -> {
            if (error == null) {
                int successStartIndex = ofNullable(response.rejectedLogEventsInfo())
                        .map(rejected -> Math.max(
                                ofNullable(rejected.expiredLogEventEndIndex())
                                        .orElse(0),
                                ofNullable(rejected.tooOldLogEventEndIndex())
                                        .orElse(0)))
                        .orElse(0);

                int successEndIndex = ofNullable(response.rejectedLogEventsInfo())
                        .map(RejectedLogEventsInfo::tooNewLogEventStartIndex)
                        .orElseGet(logsToSend::size);

                // Re-enqueue the "too new" logs
                failedLogs.accept(logsToSend.subList(successEndIndex, logsToSend.size()));

                if (successStartIndex > 0) {
                    log.warn("Failed to deliver {} too old/expired logs. {}",
                            successEndIndex, logsToSend.subList(0, successEndIndex));
                }

                log.debug("Sent batch of {} logs", successEndIndex - successStartIndex);
            } else {
                log.warn("Failed to send batch of {} logs", logsToSend.size(), error);

                if (isRetryable(error)) {
                    failedLogs.accept(logsToSend);
                }
            }
        });
    }

    @Override
    protected long getSizeInBytes(LogEvent logEvent) {
        // https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
        // The maximum batch size is 1,048,576 bytes.
        // This size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
        // Assuming worst case 4 bytes per character

        return logEvent.getLog().length() * 4L + 26;
    }

    @Override
    public void close() {
        asyncHttpClient.close();
        cloudWatchLogsAsyncClient.close();
    }

    private boolean isRetryable(Throwable err) {
        return FATAL_EXCEPTION_CLASSIFIER.isFatal(err, getFatalExceptionCons());
    }

    public static SdkAsyncHttpClient createSdkAsyncHttpClient(final Properties clientProperties) {
        return AWSGeneralUtil.createAsyncHttpClient(clientProperties);
    }

    public static CloudWatchLogsAsyncClient createCloudWatchLogsAsyncClient(
            final Properties clientProperties,
            final SdkAsyncHttpClient asyncHttpClient) {
        return AWSAsyncSinkUtil.createAwsAsyncClient(
                clientProperties,
                asyncHttpClient,
                CloudWatchLogsAsyncClient.builder(),
                "Apache Fink Amazon CloudWatch Logs Sink",
                "sink.cloudwatch.user-agent-prefix");
    }
}

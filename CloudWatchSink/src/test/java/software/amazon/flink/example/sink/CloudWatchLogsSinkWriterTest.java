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

import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;
import static java.time.Duration.ofDays;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloudWatchLogsSinkWriterTest {

    private static final String LOG_GROUP = "log-group";
    private static final String LOG_STREAM = "log-stream";

    private CloudWatchLogsSinkWriter<String> sinkWriter;

    @Mock
    private SdkAsyncHttpClient httpClient;

    @Mock
    private CloudWatchLogsAsyncClient cloudWatchLogsClient;

    @BeforeEach
    void setUp() {
        sinkWriter = new CloudWatchLogsSinkWriter<>(
                LOG_GROUP,
                LOG_STREAM,
                new Properties(),
                (a, b) -> null,
                new TestSinkInitContext(),
                10,
                10,
                100,
                10,
                10,
                10,
                emptyList(),
                props -> httpClient,
                (properties, asyncHttpClient) -> cloudWatchLogsClient);
    }

    @Test
    void submitValidRequestEntries() throws Exception {
        ArgumentCaptor<PutLogEventsRequest> captor = ArgumentCaptor.forClass(PutLogEventsRequest.class);
        when(cloudWatchLogsClient.putLogEvents(captor.capture()))
                .thenReturn(completedFuture(PutLogEventsResponse.builder().build()));

        LogEvent expected1 = logEvent();
        LogEvent expected2 = logEvent();

        assertThat(submitLogsAndGetFailures(List.of(expected1, expected2)))
                .isEmpty();

        PutLogEventsRequest request = captor.getValue();

        assertThat(request.logGroupName()).isEqualTo(LOG_GROUP);
        assertThat(request.logStreamName()).isEqualTo(LOG_STREAM);
        assertThat(request.logEvents()).containsExactly(
                InputLogEvent.builder()
                        .message(expected1.getLog())
                        .timestamp(expected1.getTimestamp().toEpochMilli())
                        .build(),
                InputLogEvent.builder()
                        .message(expected2.getLog())
                        .timestamp(expected2.getTimestamp().toEpochMilli())
                        .build());
    }

    @Test
    void submitRequestEntriesWithLogsInFuture() throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(completedFuture(PutLogEventsResponse.builder()
                        .rejectedLogEventsInfo(RejectedLogEventsInfo.builder()
                                .tooNewLogEventStartIndex(2)
                                .build())
                        .build()));

        LogEvent future1 = logEvent(ofDays(1));
        LogEvent future2 = logEvent(ofDays(2));

        assertThat(submitLogsAndGetFailures(List.of(
                logEvent(),
                logEvent(),
                future1,
                future2)))
                // future1 and future2 should be re-queued to send
                .containsExactly(future1, future2);
    }

    @Test
    void logsOrderedChronologically() throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(completedFuture(PutLogEventsResponse.builder()
                        .rejectedLogEventsInfo(RejectedLogEventsInfo.builder()
                                .tooNewLogEventStartIndex(1)
                                .build())
                        .build()));

        LogEvent future = LogEvent.builder()
                .log("future")
                .timestamp(Instant.now().plus(ofDays(10)))
                .build();

        LogEvent now = LogEvent.builder()
                .log("now")
                .timestamp(Instant.now())
                .build();

        assertThat(submitLogsAndGetFailures(List.of(future, now)))
                // future should be re-queued to send
                .containsExactly(future);
    }

    @Test
    void submitRequestEntriesWithLogsInPastAndInFuture() throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(completedFuture(PutLogEventsResponse.builder()
                        .rejectedLogEventsInfo(RejectedLogEventsInfo.builder()
                                .expiredLogEventEndIndex(1)
                                .tooNewLogEventStartIndex(4)
                                .build())
                        .build()));

        LogEvent future1 = logEvent(ofDays(1));
        LogEvent future2 = logEvent(ofDays(2));

        assertThat(submitLogsAndGetFailures(List.of(
                logEvent(),
                logEvent(),
                logEvent(),
                logEvent(),
                future1,
                future2)))
                // future1 and future2 should be re-queued to send
                .containsExactly(future1, future2);
    }

    @Test
    void retryOnError() throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(failedFuture(new RuntimeException()));

        LogEvent log1 = logEvent();
        LogEvent log2 = logEvent();

        assertThat(submitLogsAndGetFailures(List.of(log1, log2)))
                // log1, log2 should be re-queued to send
                .containsExactly(log1, log2);
    }

    @Test
    void fatalError() {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(failedFuture(new InterruptedException()));

        // We time out since the future does not complete
        assertThatExceptionOfType(TimeoutException.class)
                .isThrownBy(() -> submitLogsAndGetFailures(List.of(logEvent(), logEvent())));
    }

    @Test
    void getSizeInBytes() {
        long actual = sinkWriter.getSizeInBytes(LogEvent.builder()
                .log("a dummy log message")
                .timestamp(Instant.now())
                .build());

        assertThat(actual).isEqualTo(102L);
    }

    @Test
    void close() {
        sinkWriter.close();

        verify(httpClient).close();
        verify(cloudWatchLogsClient).close();
    }

    private List<LogEvent> submitLogsAndGetFailures(List<LogEvent> logEvents) throws Exception {
        CompletableFuture<List<LogEvent>> future = new CompletableFuture<>();
        sinkWriter.submitRequestEntries(logEvents, future::complete);

        return future.get(1, SECONDS);
    }

    private LogEvent logEvent() {
        return logEvent(Duration.ofMillis(0));
    }

    private LogEvent logEvent(Duration duration) {
        return LogEvent.builder()
                .log(UUID.randomUUID().toString())
                .timestamp(Instant.now().plus(duration))
                .build();
    }
}

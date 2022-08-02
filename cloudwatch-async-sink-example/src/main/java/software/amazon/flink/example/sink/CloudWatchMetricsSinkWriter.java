/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSAsyncSinkUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Slf4j
public class CloudWatchMetricsSinkWriter<T> extends AsyncSinkWriter<T, MetricDatum> {

    private final String namespace;
    private final SdkAsyncHttpClient asyncHttpClient;
    private final CloudWatchAsyncClient cloudWatchAsyncClient;

    public CloudWatchMetricsSinkWriter(
            String namespace,
            Properties clientProperties,
            ElementConverter<T, MetricDatum> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            Collection<BufferedRequestState<MetricDatum>> states) {
        super(elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                states);
        this.namespace = namespace;

        clientProperties.setProperty(AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP1_1");

        asyncHttpClient = AWSGeneralUtil.createAsyncHttpClient(clientProperties);
        cloudWatchAsyncClient = AWSAsyncSinkUtil.createAwsAsyncClient(
                clientProperties,
                asyncHttpClient,
                CloudWatchAsyncClient.builder(),
                "Apache Fink Amazon CloudWatch Metrics Sink",
                "sink.cloudwatch.user-agent-prefix");
    }

    @Override
    protected void submitRequestEntries(
            List<MetricDatum> metricsToSend,
            Consumer<List<MetricDatum>> failedMetrics) {

        PutMetricDataRequest request = PutMetricDataRequest.builder()
                .namespace(namespace)
                .metricData(metricsToSend)
                .build();

        CompletableFuture<PutMetricDataResponse> future = cloudWatchAsyncClient.putMetricData(request);

        log.debug("Sending batch of {} metrics", metricsToSend.size());

        future.whenComplete((response, error) -> {
            if (error == null) {
                failedMetrics.accept(Collections.emptyList());
                log.debug("Sent batch of {} metrics", metricsToSend.size());
            } else {
                failedMetrics.accept(metricsToSend);
                log.warn("Failed to send batch of {} metrics", metricsToSend.size(), error);
            }
        });

    }

    @Override
    protected long getSizeInBytes(MetricDatum metricDatum) {
        return metricDatum.metricName().length() * 2L +
                8L + // Double value, 8 bytes
                8L + // Long timestamp, 8 bytes
                metricDatum.dimensions().stream()
                        .mapToLong(dim -> dim.name().length() * 2L + dim.value().length() * 2L)
                        .sum();
    }

    @Override
    public void close() {
        asyncHttpClient.close();
        cloudWatchAsyncClient.close();
    }

}

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

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import lombok.Builder;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class CloudWatchMetricsSink<T> extends AsyncSinkBase<T, MetricDatum> {

    private final String namespace;
    private final Properties clientProperties;

    @Builder
    public CloudWatchMetricsSink(
            String namespace,
            Properties clientProperties,
            String metricName,
            ValueExtractor<T> valueExtractor,
            TimestampExtractor<T> timestampExtractor,
            DimensionsExtractor<T> dimensionsExtractor) {
        super(
                CloudWatchMetricsElementConverter.<T>builder()
                        .metricName(metricName)
                        .valueExtractor(valueExtractor)
                        .timestampExtractor(timestampExtractor)
                        .dimensionExtractor(dimensionsExtractor)
                        .build(),
                1_000, // maxBatchSize
                1, // maxInFlightRequests
                10_000, // maxBufferedRequestEntries
                40 * 1_024, // maxBatchSizeInBytes
                10_000, // maxTimeInBufferMS
                1_000); // maxRecordSizeInBytes
        this.namespace = namespace;
        this.clientProperties = clientProperties;
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<MetricDatum>> createWriter(InitContext initContext) throws IOException {
        return restoreWriter(initContext, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<MetricDatum>> restoreWriter(InitContext initContext, Collection<BufferedRequestState<MetricDatum>> collection) throws IOException {
        return new CloudWatchMetricsSinkWriter<>(
                namespace,
                clientProperties,
                getElementConverter(),
                initContext,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                collection);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<MetricDatum>> getWriterStateSerializer() {
        return new CloudWatchMetricsStateSerializer();
    }

    public interface ValueExtractor<T> extends Function<T, Double>, Serializable {
    }

    public interface TimestampExtractor<T> extends Function<T, Instant>, Serializable {
    }

    public interface DimensionsExtractor<T> extends Function<T, List<Dimension>>, Serializable {
    }

}

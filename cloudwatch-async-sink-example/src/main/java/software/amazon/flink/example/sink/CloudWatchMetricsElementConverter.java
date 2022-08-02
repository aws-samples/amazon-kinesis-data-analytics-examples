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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import lombok.Builder;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;

@Builder
public class CloudWatchMetricsElementConverter<T> implements ElementConverter<T, MetricDatum> {

    private final String metricName;
    private final Function<T, Double> valueExtractor;
    private final Function<T, Instant> timestampExtractor;
    private final Function<T, List<Dimension>> dimensionExtractor;

    @Override
    public MetricDatum apply(T record, SinkWriter.Context context) {
        return MetricDatum.builder()
                .metricName(metricName)
                .value(valueExtractor.apply(record))
                .timestamp(timestampExtractor.apply(record))
                .dimensions(dimensionExtractor.apply(record))
                .build();
    }
}

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

package software.amazon.flink.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import software.amazon.flink.example.model.OrderStats;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.temporal.ChronoUnit.SECONDS;

@AllArgsConstructor
public abstract class DataGenSourceFunction implements SourceFunction<OrderStats> {

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final String category;

    abstract long getValue(final Instant timestamp);

    @Override
    @SneakyThrows
    public void run(SourceContext<OrderStats> sourceContext) throws Exception {
        Instant timestamp = Instant.now().minus(Duration.ofHours(2)).truncatedTo(SECONDS);

        while (running.get()) {
            while (Instant.now().isBefore(timestamp)) {
                Thread.sleep(1000);
            }

            sourceContext.collect(OrderStats.builder()
                    .count(getValue(timestamp))
                    .timestamp(timestamp)
                    .category(category)
                    .build());

            timestamp = timestamp.plus(Duration.ofSeconds(1));
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}

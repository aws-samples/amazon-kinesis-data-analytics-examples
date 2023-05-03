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

package software.amazon.flink.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.flink.example.model.OrderStats;
import software.amazon.flink.example.sink.CloudWatchMetricsSink;
import software.amazon.flink.example.source.SawToothSourceFunction;
import software.amazon.flink.example.source.SineSourceFunction;

import java.util.List;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class CloudwatchAsyncSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SineSourceFunction sports = new SineSourceFunction("sports");
        SawToothSourceFunction clothing = new SawToothSourceFunction("clothing");

        DataStreamSource<OrderStats> sportsStream = env.addSource(sports);
        DataStreamSource<OrderStats> clothingStream = env.addSource(clothing);

        Properties clientProperties = new Properties();
        clientProperties.setProperty(AWS_REGION, US_EAST_1.id());

        CloudWatchMetricsSink<OrderStats> cloudWatchMetricsSink = CloudWatchMetricsSink.<OrderStats>builder()
                .clientProperties(clientProperties)
                .namespace("cloudwatch-metrics-example")
                .metricName("count")
                .valueExtractor(OrderStats::getCount)
                .timestampExtractor(OrderStats::getTimestamp)
                .dimensionsExtractor(orderStats -> List.of(
                        Dimension.builder()
                                .name("category")
                                .value(orderStats.getCategory())
                                .build()))
                .build();

        sportsStream.union(clothingStream).sinkTo(cloudWatchMetricsSink);

        env.execute("Cloudwatch Metrics Async Sink Example");
    }

}
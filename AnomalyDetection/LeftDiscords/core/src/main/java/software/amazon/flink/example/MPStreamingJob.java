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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class MPStreamingJob {

    private static final String region = "us-east-1";
    private static final String inputStreamName = "InputStream";
    private static final String outputStreamName = "OutputStream";

    private static DataStream<String> setSource(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static KinesisStreamsSink<OutputWithLabel> getSink() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(AWSConfigConstants.AWS_REGION, region);

        return KinesisStreamsSink.<OutputWithLabel>builder()
                .setKinesisClientProperties(outputProperties)
                .setSerializationSchema(new JsonSerializationSchema<>())
                .setStreamName(outputStreamName)
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();
    }

    public void execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputDataStream = setSource(env);

        SingleOutputStreamOperator<OutputWithLabel> DAMPDataStream = inputDataStream
                .process(MPProcessFunction.withTimeSeriesPeriod(3));

        DAMPDataStream.sinkTo(getSink());

        env.execute("Matrix Profile streaming");
    }

    public static void main(String[] args) throws Exception {
        MPStreamingJob flinkJob = new MPStreamingJob();
        flinkJob.execute();
    }
}

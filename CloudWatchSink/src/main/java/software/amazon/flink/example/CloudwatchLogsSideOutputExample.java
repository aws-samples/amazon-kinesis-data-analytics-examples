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

package software.amazon.flink.example;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.Properties;

import software.amazon.flink.example.sink.CloudWatchLogsSink;
import software.amazon.flink.example.sink.LogEvent;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator.stringGenerator;

public class CloudwatchLogsSideOutputExample {
    private static final long LOGS_PER_SECOND = 10;
    private static final long NUMBER_OF_LOGS_TO_GENERATE = 1_000_000;
    private static final String REGION = "us-east-1";
    private static final String LOG_GROUP_NAME = "flink-logs-sample-log-group";
    private static final String LOG_STREAM_NAME = "flink-logs-sample-log-stream";

    public static void main(final String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Disable chaining to show full job graph in UI
        env.disableOperatorChaining();

        OutputTag<String> logsSideOutput = new OutputTag<>("logs-side-output", STRING);

        SingleOutputStreamOperator<String> stream = env
                .addSource(new DataGeneratorSource<>(stringGenerator(32), LOGS_PER_SECOND, NUMBER_OF_LOGS_TO_GENERATE))
                .returns(String.class)
                .name("Random Data Generator")
                .uid("rdg")
                .process(new ProcessFunction<String, String>() {

                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        // emit data to regular output
                        out.collect(value);

                        // emit log to side output
                        ctx.output(logsSideOutput, "Received value of: " + value);
                    }
                })
                .name("Pass Through Process function")
                .uid("process");

        // Discard the output of the process function
        stream.addSink(new DiscardingSink<String>());

        // Direct logs from the side output to the CloudWatch Logs sink
        stream.getSideOutput(logsSideOutput)
                .sinkTo(buildCloudWatchLogsSink())
                .name("CloudWatch logs sink")
                .uid("sink");

        env.execute("Cloudwatch Logs Sink Example");
    }

    private static Sink<String> buildCloudWatchLogsSink() {
        Properties properties = new Properties();
        properties.put(AWSConfigConstants.AWS_REGION, REGION);

        return CloudWatchLogsSink.<String>builder()
                .logGroupName(LOG_GROUP_NAME)
                .logStreamName(LOG_STREAM_NAME)
                .elementConverter((log, ctx) -> LogEvent.builder()
                        .log(log)
                        .timestamp(Instant.now())
                        .build())
                .clientProperties(properties)
                .build();
    }
}

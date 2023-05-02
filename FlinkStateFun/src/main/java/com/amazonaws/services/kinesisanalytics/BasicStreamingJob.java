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
 *
 *  This file has been extended from the Apache Flink StateFun Playground project skeleton.
 */

package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJob;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.spi.Modules;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A basic Kinesis Data Analytics for Java application with Kinesis data
 * streams as source and sink.
 */
public class BasicStreamingJob {

    public static void main(String[] args) throws Exception {
        final Configuration flinkConfiguration = new Configuration();
        /**
         Only set below Flink Configurations, when you run outside of Kinesis Analytics

         flinkConfiguration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
         flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
         flinkConfiguration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://bucket/flink-checkpoints/");
         flinkConfiguration.set(SavepointConfigOptions.SAVEPOINT_PATH, "s3://bucket/flink-savepoint/");
         env.setParallelism(8);
        flinkConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
        */

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfiguration);
        StatefulFunctionsConfig stateFunConfig = StatefulFunctionsConfig.fromEnvironment(env);

        stateFunConfig.setProvider((StatefulFunctionsUniverseProvider) (classLoader, statefulFunctionsConfig) -> {
                Modules modules = Modules.loadFromClassPath();
                return modules.createStatefulFunctionsUniverse(statefulFunctionsConfig);
        });

        StatefulFunctionsJob.main(env, stateFunConfig);

    }
}

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

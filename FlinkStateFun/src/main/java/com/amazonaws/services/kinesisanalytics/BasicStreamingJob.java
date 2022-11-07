package com.amazonaws.services.kinesisanalytics;

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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StatefulFunctionsConfig stateFunConfig = StatefulFunctionsConfig.fromEnvironment(env);

        stateFunConfig.setProvider((StatefulFunctionsUniverseProvider) (classLoader, statefulFunctionsConfig) -> {
            Modules modules = Modules.loadFromClassPath();
            return modules.createStatefulFunctionsUniverse(stateFunConfig);
        });

        StatefulFunctionsJob.main(env, stateFunConfig);

    }
}

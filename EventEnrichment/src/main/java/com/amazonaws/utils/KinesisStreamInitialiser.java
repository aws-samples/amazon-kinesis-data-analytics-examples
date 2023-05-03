package com.amazonaws.utils;

import com.amazonaws.regions.Regions;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.pojo.Customer;

import java.io.IOException;
import java.util.Properties;

public class KinesisStreamInitialiser {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamInitialiser.class);

    public static Properties getKinesisConsumerConfig(ParameterTool parameter) throws IOException {
        //Read region info from config
        AppProperties properties = new AppProperties();
        final String region = properties.getProperty("region");

        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, parameter.get("Region", region));
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        return kinesisConsumerConfig;
    }

    public static DataStreamSource<Customer> getKinesisStream(StreamExecutionEnvironment env, Properties kinesisConsumerConfig, String streamName) {
        return env.addSource(new FlinkKinesisConsumer<>(
                streamName,
                new JsonDeserializationSchema<>(Customer.class),
                kinesisConsumerConfig
        ));
    }
}

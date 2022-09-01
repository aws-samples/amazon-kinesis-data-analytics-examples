package com.amazonaws.services.kinesisanalytics.overrides;

import com.amazonaws.services.kinesisanalytics.CustomFlinkKafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class CustomFlinkKafkaConsumer<T> extends FlinkKafkaConsumer {
    public CustomFlinkKafkaConsumer(List<String> topics,
                                    KafkaDeserializationSchema<T> deserializer,
                                    Properties props) {
        super(topics, deserializer, props);
    }

    public CustomFlinkKafkaConsumer(Pattern subscriptionPattern,
                                    KafkaDeserializationSchema<T> deserializer,
                                    Properties props) {
        super(subscriptionPattern, deserializer, props);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        CustomFlinkKafkaUtil.dropCerts();
        super.open(configuration);
    }
}
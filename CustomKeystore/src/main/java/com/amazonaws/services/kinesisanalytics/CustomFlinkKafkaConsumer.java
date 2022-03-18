package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class CustomFlinkKafkaConsumer<T> extends FlinkKafkaConsumer<T> {
    public CustomFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
    }

    /**
     * Override the 'open' method of the FlinkKafkaConsumer to drop our custom
     * keystore. This is necessary so that certs are available to be picked up in spite of
     * runner restarts and replacements.
     */
    @Override
    public void open(Configuration configuration) throws Exception {
        // write truststore to /tmp
        // NOTE: make sure that truststore is in JKS format for KDA/Flink. See README for details
        CustomFlinkKafkaUtil.initializeKafkaTruststore();

        super.open(configuration);
    }
}

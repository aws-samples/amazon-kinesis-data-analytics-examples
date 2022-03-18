package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class CustomFlinkKafkaProducer<T> extends FlinkKafkaProducer<T> {

    public CustomFlinkKafkaProducer(String topicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig, Semantic semantic) {
        super(topicId, serializationSchema, producerConfig, semantic);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // write keystore to /tmp
        // NOTE: make sure that keystore is in JKS format for KDA/Flink. See README for details
        CustomFlinkKafkaUtil.initializeKafkaTruststore();

        super.initializeState(context);
    }

}


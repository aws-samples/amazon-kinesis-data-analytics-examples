package com.amazonaws.services.kinesisanalytics.overrides;

import com.amazonaws.services.kinesisanalytics.CustomFlinkKafkaUtil;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

public class CustomFlinkKafkaProducer<T> extends FlinkKafkaProducer<T> {

    public CustomFlinkKafkaProducer(String topicId,
                                    KafkaSerializationSchema<T> serializationSchema,
                                    Properties producerConfig,
                                    Semantic semantic,
                                    int kafkaProducersPoolSize) {
        super(topicId, serializationSchema, producerConfig, semantic, kafkaProducersPoolSize);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        CustomFlinkKafkaUtil.dropCerts();

        super.initializeState(context);
    }
}
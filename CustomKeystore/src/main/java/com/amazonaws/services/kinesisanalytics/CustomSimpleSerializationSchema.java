package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class CustomSimpleStringSerializationSchema extends SimpleStringSchema {

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        // write keystore to /tmp
        // NOTE: make sure that keystore is in JKS format for KDA/Flink. See README for details
        CustomFlinkKafkaUtil.initializeKafkaTruststore();

        super.open(context);
    }
}

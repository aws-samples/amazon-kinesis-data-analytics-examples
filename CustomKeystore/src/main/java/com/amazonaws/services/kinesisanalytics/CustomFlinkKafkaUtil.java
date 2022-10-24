package com.amazonaws.services.kinesisanalytics;

import org.apache.commons.io.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.net.URL;

public class CustomFlinkKafkaUtil {

    public static void initializeKafkaTruststore() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL inputUrl = classLoader.getResource("kafka.client.truststore.jks");
        File dest = new File("/tmp/kafka.client.truststore.jks");

        try {
            FileUtils.copyURLToFile(inputUrl, dest);
        } catch (Exception ex) {
            throw new FlinkRuntimeException("Failed to initialize Kakfa truststore", ex);
        }
    }
}
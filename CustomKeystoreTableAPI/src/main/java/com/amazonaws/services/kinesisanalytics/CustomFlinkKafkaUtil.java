package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class CustomFlinkKafkaUtil {
    public static void initializeKafkaCertstore(String resource, String destinationFolder) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File destDir = new File(destinationFolder);
        File targetFile = new File(destDir, resource);

        try (InputStream input = classLoader.getResourceAsStream(resource);
             OutputStream outStream = new FileOutputStream(targetFile)) {
            input.transferTo(outStream);
        } catch (Exception ex) {
            throw new FlinkRuntimeException("Failed to initialize Kafka certstore", ex);
        }
    }

    public static void dropCerts() {
        /// Truststore on KDA cluster should be sufficient for most use cases and will "automatically" work.
        /// If your scenario requires a custom truststore, then make sure you include it in resources
        /// and uncomment the below line.
        // CustomFlinkKafkaUtil.initializeKafkaCertstore("client.truststore.jks", "/tmp");

        /// Drop Keystore. For this to work, this keystore file should be included in src/main/resources.
        /// Keystore is not included in this repo because you have to create a keystore for your use case.
        /// For MSK, please see: https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html
        CustomFlinkKafkaUtil.initializeKafkaCertstore("kafka.client.keystore.jks", "/tmp");
    }
}
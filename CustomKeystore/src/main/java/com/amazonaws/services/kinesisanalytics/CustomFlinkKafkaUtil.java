package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class CustomFlinkKafkaUtil {

    public static void initializeKafkaTruststore() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File destDir = new File("/tmp");
        File targetFile = new File(destDir, "kafka.client.truststore.jks");

        try (InputStream input = classLoader.getResourceAsStream("kafka.client.truststore.jks");
             OutputStream outStream = new FileOutputStream(targetFile)) {

            int available = input.available();
            byte[] buffer = new byte[available];
            input.read(buffer);

            outStream.write(buffer);
            outStream.flush();
        } catch (Exception ex) {
            throw new FlinkRuntimeException("Failed to initialize Kakfa truststore", ex);
        }
    }
}


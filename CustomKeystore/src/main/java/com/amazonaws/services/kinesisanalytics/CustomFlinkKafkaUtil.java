package com.amazonaws.services.kinesisanalytics;

import org.apache.commons.io.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.net.URL;
import java.util.UUID;

public class CustomFlinkKafkaUtil {

    public static void initializeKafkaTruststore() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL inputUrl = classLoader.getResource("kafka.client.truststore.jks");
        String destFileName = "/tmp/kafka.client.truststore.jks";
        File dest = new File(destFileName);

        String tmpFileName = destFileName.concat(UUID.randomUUID().toString().concat(".tmp"));
        File tmpDest = new File(tmpFileName);

        try {
            if(!dest.exists()) {
                // Copy to temporary file and rename to avoid race conditions at high parallelism
                FileUtils.copyURLToFile(inputUrl, tmpDest);
                tmpDest.renameTo(dest);
            }
        } catch (Exception ex) {
            throw new FlinkRuntimeException("Failed to initialize Kakfa truststore", ex);
        }
    }
}


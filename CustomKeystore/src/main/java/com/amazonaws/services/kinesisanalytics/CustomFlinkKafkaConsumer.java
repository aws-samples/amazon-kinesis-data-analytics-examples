package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class CustomFlinkKafkaConsumer<T> extends FlinkKafkaConsumer<T> {
    public CustomFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
    }

    /*
     Override the 'open' method of the FlinkKafkaConsumer to drop our custom
     keystore. This is necessary so that certs are available to be picked up in spite of
     runner restarts and replacements.
     */
    @Override
    public void open(Configuration configuration) throws Exception {
        // write truststore to /tmp
        // NOTE: make sure that truststore is in JKS format for KDA/Flink. See README for details
        dropFile("/tmp");

        super.open(configuration);
    }

    private void dropFile(String destFolder) throws Exception
    {
        InputStream input = null;
        OutputStream outStream = null;

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            input = classLoader.getResourceAsStream("kafka.client.truststore.jks");
            byte[] buffer = new byte[input.available()];
            input.read(buffer);

            File destDir = new File(destFolder);
            File targetFile = new File(destDir, "kafka.client.truststore.jks");
            outStream = new FileOutputStream(targetFile);
            outStream.write(buffer);
            outStream.flush();
        }
        catch (Exception ex)
        {
            System.out.println(ex.getMessage());

            if(input != null) {
                input.close();
            }

            if(outStream != null) {
                outStream.close();
            }
        }
    }
}

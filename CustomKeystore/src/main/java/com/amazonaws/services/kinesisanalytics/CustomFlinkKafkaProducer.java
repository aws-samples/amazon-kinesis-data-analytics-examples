package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.configuration.Configuration;
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

    Logger LOG = LoggerFactory.getLogger(CustomFlinkKafkaProducer.class);


    public CustomFlinkKafkaProducer(String topicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig, Semantic semantic) throws Exception {
        super(topicId, serializationSchema, producerConfig, semantic);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // write keystore to /tmp
        // NOTE: make sure that keystore is in JKS format for KDA/Flink. See README for details
        dropFile("/tmp");
        super.initializeState(context);
    }


    private void dropFile(String destFolder) throws Exception
    {
        InputStream input = null;
        OutputStream outStream = null;

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            input = classLoader.getResourceAsStream("kafka.client.truststore.jks");
            int available = input.available();
            byte[] buffer = new byte[available];
            int bytesRead = input.read(buffer);


            File destDir = new File(destFolder);
            File targetFile = new File(destDir, "kafka.client.truststore.jks");
            outStream = new FileOutputStream(targetFile);
            outStream.write(buffer);
            outStream.flush();

        }
        catch (Exception ex)
        {
            LOG.info(ex.getMessage());

            if(input != null) {
                input.close();
            }

            if(outStream != null) {
                outStream.close();
            }
        }
    }
}


package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Helpers {
    private static final Logger LOG = LoggerFactory.getLogger(Helpers.class);

    public static final String MSKBOOTSTRAP_SERVERS = "MSKBootstrapServers";
    public static final String KAFKA_SOURCE_TOPIC_KEY = "KafkaSourceTopic";
    public static final String KAFKA_CONSUMER_GROUP_ID_KEY = "KafkaConsumerGroupId";
    public static final String S3_BUCKET_REGION_KEY = "S3BucketRegion";
    public static final String KEYSTORE_S3_BUCKET_KEY = "KeystoreS3Bucket";
    public static final String KEYSTORE_S3_PATH_KEY = "KeystoreS3Path";
    public static final String TRUSTSTORE_S3_BUCKET_KEY = "TruststoreS3Bucket";
    public static final String TRUSTSTORE_S3_PATH_KEY = "TruststoreS3Path";
    public static final String KEYSTORE_PASS_SECRET_KEY = "KeystorePassSecret";
    public static final String KEYSTORE_PASS_SECRET_FIELD_KEY = "KeystorePassSecretField";

    public static Properties getAppProperties() throws IOException {
        // note: this won't work when running locally
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

        if(flinkProperties == null) {
            LOG.error("Unable to retrieve FlinkApplicationProperties; please ensure that you've " +
                    "supplied them via application properties.");
            return null;
        }

        if(!flinkProperties.containsKey(MSKBOOTSTRAP_SERVERS)) {
            LOG.error("Unable to retrieve property: " + MSKBOOTSTRAP_SERVERS);
            return null;
        }

        if(!flinkProperties.containsKey(KAFKA_SOURCE_TOPIC_KEY)) {
            LOG.error("Unable to retrieve property: " + KAFKA_SOURCE_TOPIC_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(KAFKA_CONSUMER_GROUP_ID_KEY)) {
            LOG.error("Unable to retrieve property: " + KAFKA_CONSUMER_GROUP_ID_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(S3_BUCKET_REGION_KEY)) {
            LOG.error("Unable to retrieve property: " + S3_BUCKET_REGION_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(KEYSTORE_S3_BUCKET_KEY)) {
            LOG.error("Unable to retrieve property: " + KEYSTORE_S3_BUCKET_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(KEYSTORE_S3_PATH_KEY)) {
            LOG.error("Unable to retrieve property: " + KEYSTORE_S3_PATH_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(TRUSTSTORE_S3_BUCKET_KEY)) {
            LOG.error("Unable to retrieve property: " + TRUSTSTORE_S3_BUCKET_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(TRUSTSTORE_S3_PATH_KEY)) {
            LOG.error("Unable to retrieve property: " + TRUSTSTORE_S3_PATH_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(KEYSTORE_PASS_SECRET_KEY)) {
            LOG.error("Unable to retrieve property: " + KEYSTORE_PASS_SECRET_KEY);
            return null;
        }

        if(!flinkProperties.containsKey(KEYSTORE_PASS_SECRET_FIELD_KEY)) {
            LOG.error("Unable to retrieve property: " + KEYSTORE_PASS_SECRET_FIELD_KEY);
            return null;
        }

        return flinkProperties;
    }
}

package com.amazonaws.kinesisanalytics.beam;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface BasicBeamStreamingJobOptions extends FlinkPipelineOptions, AwsOptions {
    Logger LOG = LoggerFactory.getLogger(BasicBeamStreamingJobOptions.class);

    @Description("Name of the Kinesis Data Stream to read from")
    String getInputStreamName();

    void setInputStreamName(String value);

    @Description("Name of the Kinesis Data Stream to write to")
    String getOutputStreamName();

    void setOutputStreamName(String value);

    static String[] argsFromKinesisApplicationProperties(String[] args, String applicationPropertiesName) {
        Properties beamProperties = null;

        try {
            //Load configuration from package resources
            //Map<String, Properties> applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties(FlinkPipelineOptions.class.getClassLoader().getResource("application-properties.json").getPath());
            //Alternatively, get from KDA environment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            if (applicationProperties == null) {
                LOG.warn("Unable to load application properties from the Kinesis Analytics Runtime");

                return new String[0];
            }

            beamProperties = applicationProperties.get(applicationPropertiesName);

            if (beamProperties == null) {
                LOG.warn("Unable to load {} properties from the Kinesis Analytics Runtime", applicationPropertiesName);

                return new String[0];
            }

            LOG.info("Parsing application properties: {}", applicationPropertiesName);
        } catch (IOException e) {
            LOG.warn("Failed to retrieve application properties", e);

            return new String[0];
        }

        String[] kinesisOptions = beamProperties
                .entrySet()
                .stream()
                .map(property -> String.format("--%s%s=%s",
                        Character.toLowerCase(((String) property.getKey()).charAt(0)),
                        ((String) property.getKey()).substring(1),
                        property.getValue()
                ))
                .toArray(String[]::new);

        return kinesisOptions;
    }
}
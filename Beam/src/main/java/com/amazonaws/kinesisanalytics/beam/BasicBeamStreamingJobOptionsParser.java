package com.amazonaws.kinesisanalytics.beam;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class BasicBeamStreamingJobOptionsParser {
    private static final Logger LOG = LoggerFactory.getLogger(BasicBeamStreamingJobOptions.class);

    static String[] argsFromKinesisApplicationProperties(String[] args, String applicationPropertiesName) {
        Properties beamProperties;

        try {
            //Load configuration from KDA environment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            if (applicationProperties == null) {
                throw new IllegalStateException("Unable to load application properties from the Kinesis Analytics Runtime");
            }

            beamProperties = applicationProperties.get(applicationPropertiesName);

            if (beamProperties == null) {
                throw new IllegalStateException(String.format("Unable to load %s properties from the Kinesis Analytics Runtime", applicationPropertiesName));
            }

            LOG.info("Parsing application properties: {}", applicationPropertiesName);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to retrieve application properties", e);
        }

        return beamProperties
                .entrySet()
                .stream()
                .map(property -> String.format("--%s%s=%s",
                        Character.toLowerCase(((String) property.getKey()).charAt(0)),
                        ((String) property.getKey()).substring(1),
                        property.getValue()
                ))
                .toArray(String[]::new);
    }
}

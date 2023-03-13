package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.gsr.avro.BuySell;
import com.amazonaws.services.kinesisanalytics.gsr.avro.Trade;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KDAFlinkStreamingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(KDAFlinkStreamingJob.class);

    // Name of the local JSON resource with the application properties in the same format as they are received from the KDA runtime
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    // Names of the configuration group containing the application properties
    private static final String APPLICATION_CONFIG_GROUP = "FlinkApplicationProperties";

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    /**
     * Load application properties from KDA runtime or from a local resource, when the environment is local
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (isLocal(env)) {
            LOGGER.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            return KinesisAnalyticsRuntime.getApplicationProperties(
                    KDAFlinkStreamingJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOGGER.info("Loading application properties from KDA");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }

    /**
     * Amazon Kinesis Flink Consumer using AWS Glue Schema Registry
     *
     * @param <T>              record type
     * @param payloadAvroClass AVRO-generated class for the message body
     * @param streamName       Amazon Kinesis Data Stream name for Input Data
     * @param streamRegion     Amazon Kinesis Data Stream Region
     * @param gsrConfig        Configuration for GSR
     * @return a FlinkKinesisConsumer
     */
    private static <T extends SpecificRecord> FlinkKinesisConsumer<T> kinesisSource(
            Class<T> payloadAvroClass,
            String streamName,
            String streamRegion,
            Map<String, Object> gsrConfig) {

        // Deserialization Schema for the message body: AVRO specific record
        DeserializationSchema<T> kinesisDeserializationSchema = GlueSchemaRegistryAvroDeserializationSchema.forSpecific(payloadAvroClass, gsrConfig);

        // Properties for Amazon Kinesis Data Streams Source, we need to specify from where we want to consume the data.
        // STREAM_INITIAL_POSITION: LATEST: consume messages that have arrived from the moment application has been deployed
        // STREAM_INITIAL_POSITION: TRIM_HORIZON: consume messages starting from first available in the Kinesis Stream
        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.put(AWSConfigConstants.AWS_REGION, streamRegion);
        kinesisConsumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        // If EFO consumer is needed, uncomment the following block.
        /*
        kinesisConsumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.name());
        kinesisConsumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME,"my-efo-consumer");
         */

        return new FlinkKinesisConsumer<>(streamName, kinesisDeserializationSchema, kinesisConsumerConfig);
    }

    /**
     * Amazon Kinesis Flink Sink using AWS Glue Schema Registry
     *
     * @param <T>                   record type
     * @param payloadAvroClass      AVRO-generated class for the message body
     * @param streamName            Amazon Kinesis Data Stream name for Input Data
     * @param streamRegion          Amazon Kinesis Data Stream Region
     * @param gsrConfig             Configuration for GSR
     * @param partitionKeyExtractor Extracts from the record the key that will be used as partition key on Kinesis Data Stream
     * @return a FlinkKinesisConsumer
     */
    private static <T extends SpecificRecord> KinesisStreamsSink<T> kinesisSink(
            Class<T> payloadAvroClass,
            String streamName,
            String streamRegion,
            Map<String, Object> gsrConfig,
            PartitionKeyGenerator<T> partitionKeyExtractor) {

        // Serialization Schema for the message body: AVRO specific record
        SerializationSchema<T> kinesisDeserializationSchema = GlueSchemaRegistryAvroSerializationSchema.forSpecific(payloadAvroClass, streamName, gsrConfig);

        // Properties for Amazon Kinesis Data Streams Sink
        Properties kinesisSinkConfig = new Properties();
        kinesisSinkConfig.put(AWSConfigConstants.AWS_REGION, streamRegion);

        return KinesisStreamsSink.<T>builder()
                .setKinesisClientProperties(kinesisSinkConfig)
                .setSerializationSchema(kinesisDeserializationSchema)
                .setPartitionKeyGenerator(partitionKeyExtractor)
                .setStreamName(streamName)
                .build();
    }

    /**
     * Glue Schema Registry SerDe configuration
     *
     * @param schemaRegistryRegion Glue Schema Registry region
     * @param schemaRegistryName   Glue Schema Registry name
     * @return Map containing the Glue Schema Registry serializer/deserializer configuration
     */
    private static Map<String, Object> schemaRegistryConf(String schemaRegistryRegion, String schemaRegistryName) {
        Map<String, Object> serializerConfigs = new HashMap<>();
        serializerConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, schemaRegistryRegion);
        serializerConfigs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        serializerConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
        serializerConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, schemaRegistryName);
        // The name of the Schema in GSR is not specified explicitly. The SerDe automatically uses the name of the Stream
        return serializerConfigs;
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // Local dev specific settings
        if (isLocal(env)) {
            // Checkpointing and parallelism are set by KDA when running on AWS
            env.enableCheckpointing(60000);
            env.setParallelism(2);
        }

        // Application configuration
        Properties applicationProperties = loadApplicationProperties(env).get(APPLICATION_CONFIG_GROUP);
        String inputStream = Preconditions.checkNotNull(applicationProperties.getProperty("input.stream"), "Input Kinesis Stream not defined");
        String outputStream = Preconditions.checkNotNull(applicationProperties.getProperty("output.stream"), "Output Kinesis Stream not defined");
        String streamRegion = Preconditions.checkNotNull(applicationProperties.getProperty("stream.region"), "Region of Kinesis Streams not Defined");
        String schemaRegistryName = Preconditions.checkNotNull(applicationProperties.getProperty("schema.registry.name"), "Schema Registry Name not defined");
        String schemaRegistryRegion = Preconditions.checkNotNull(applicationProperties.getProperty("schema.registry.region"), "Schema Registry Region not defined");

        Map<String, Object> gsrConfig = schemaRegistryConf(schemaRegistryRegion, schemaRegistryName);

        FlinkKinesisConsumer<Trade> source = kinesisSource(
                Trade.class,
                inputStream,
                streamRegion,
                gsrConfig);

        KinesisStreamsSink<Trade> sink = kinesisSink(
                Trade.class,
                outputStream,
                streamRegion,
                gsrConfig,
                Trade::getAccountNr);

        DataStream<Trade> kinesis = env.addSource(source).uid("kinesis-trade-source");

        DataStream<Trade> filtered = kinesis.filter(trade -> trade.getBuySell() == BuySell.valueOf("SELL"));

        filtered.sinkTo(sink).uid("kinesis-trade-sink");

        env.execute("Flink App");
    }
}

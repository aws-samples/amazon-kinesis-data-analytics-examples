package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.avro.RoomTemperature;
import com.amazonaws.services.kinesisanalytics.avro.TemperatureSample;
import com.amazonaws.services.kinesisanalytics.domain.RoomAverageTemperatureCalculator;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * Sample Kinesis Data Analytics Flink application.
 *
 * It reads from a Kafka topic temperature samples serialized as AVRO using Glue Schema Registry.
 * It calculates average room temperatures every minute, and publish them to another Kafka topic, again serialized as
 * AVRO using Glue Schema Registry.
 */
public class KdaFlinkStreamingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(KdaFlinkStreamingJob.class);

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
                    KdaFlinkStreamingJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOGGER.info("Loading application properties from KDA");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }

    /**
     * KafkaSource for any AVRO-generated class (SpecificRecord) using AWS Glue Schema Registry.
     *
     * @param <T>                  record type
     * @param payloadAvroClass     AVRO-generated class for the message body
     * @param bootstrapServers     Kafka bootstrap server
     * @param topic                topic name
     * @param consumerGroupId      Kafka Consumer Group ID
     * @param schemaRegistryName   Glue Schema Registry name
     * @param schemaRegistryRegion Glue Schema Registry region
     * @param kafkaConsumerConfig  configuration passed to the Kafka Consumer
     * @return a KafkaSource instance
     */
    private static <T extends SpecificRecord> KafkaSource<T> kafkaSource(
            Class<T> payloadAvroClass,
            String bootstrapServers,
            String topic,
            String consumerGroupId,
            String schemaRegistryName,
            String schemaRegistryRegion,
            Properties kafkaConsumerConfig) {

        // DeserializationSchema for the message body: AVRO specific record, with Glue Schema Registry
        Map<String, Object> deserializerConfig = Map.of(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName(),
                AWSSchemaRegistryConstants.AWS_REGION, schemaRegistryRegion,
                AWSSchemaRegistryConstants.REGISTRY_NAME, schemaRegistryName);
        DeserializationSchema<T> legacyDeserializationSchema = GlueSchemaRegistryAvroDeserializationSchema.forSpecific(payloadAvroClass, deserializerConfig);
        KafkaRecordDeserializationSchema<T> kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.valueOnly(legacyDeserializationSchema);

        // ... more Kafka consumer configurations (e.g. MSK IAM auth) go here...

        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroupId)
                .setDeserializer(kafkaRecordDeserializationSchema)
                .setProperties(kafkaConsumerConfig)
                .build();
    }

    /**
     * KafkaSink for any AVRO-generated class (specific records) using AWS Glue Schema Registry
     * using a Kafka message key (a String) extracted from the record using a KeySelector.
     *
     * @param <T>                  record type
     * @param payloadAvroClass     AVRO-generated class for the message body
     * @param messageKeyExtractor  KeySelector to extract the message key from the record
     * @param bootstrapServers     Kafka bootstrap servers
     * @param topic                topic name
     * @param schemaRegistryName   Glue Schema Registry name
     * @param schemaRegistryRegion Glue Schema Registry region
     * @param kafkaProducerConfig  configuration passed to the Kafka Producer
     * @return a KafkaSink
     */
    private static <T extends SpecificRecord> KafkaSink<T> keyedKafkaSink(
            Class<T> payloadAvroClass,
            KeySelector<T, String> messageKeyExtractor,
            String bootstrapServers,
            String topic,
            String schemaRegistryName,
            String schemaRegistryRegion,
            Properties kafkaProducerConfig) {

        // SerializationSchema for the message body: AVRO with Glue Schema Registry
        // (GlueSchemaRegistryAvroSerializationSchema expects a Map<String,Object> as configuration)
        Map<String, Object> serializerConfig = Map.of(
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true, // Enable Schema Auto-registration (the name of the schema is based on the name of the topic)
                AWSSchemaRegistryConstants.AWS_REGION, schemaRegistryRegion,
                AWSSchemaRegistryConstants.REGISTRY_NAME, schemaRegistryName);
        SerializationSchema<T> valueSerializationSchema = GlueSchemaRegistryAvroSerializationSchema.forSpecific(payloadAvroClass, topic, serializerConfig);

        // SerializationSchema for the message key.
        // Extracts the key (a String) from the record using a KeySelector
        // and covert the String to bytes as UTF-8 (same default behaviour of org.apache.flink.api.common.serialization.SimpleStringSchema)
        SerializationSchema<T> keySerializationSchema = record -> {
            try {
                return messageKeyExtractor.getKey(record).getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // ... more Kafka consumer configurations (e.g. MSK IAM auth) go here...

        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(valueSerializationSchema)
                                .setKeySerializationSchema(keySerializationSchema)
                                .build())
                .setKafkaProducerConfig(kafkaProducerConfig)
                .build();
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
        String bootstrapServers = Preconditions.checkNotNull(applicationProperties.getProperty("bootstrap.servers"), "bootstrap.servers not defined");
        String sourceTopic = Preconditions.checkNotNull(applicationProperties.getProperty("source.topic"), "source.topic not defined");
        String sourceConsumerGroupId = applicationProperties.getProperty("source.consumer.group.id", "flink-avro-gsr-sample");
        String sinkTopic = Preconditions.checkNotNull(applicationProperties.getProperty("sink.topic"), "sink.topic not defined");
        String schemaRegistryName = Preconditions.checkNotNull(applicationProperties.getProperty("schema.registry.name"), "schema.registry.name not defined");
        String schemaRegistryRegion = Preconditions.checkNotNull(applicationProperties.getProperty("schema.registry.region"), "schema.registry.region not defined");


        KafkaSource<TemperatureSample> source = kafkaSource(
                TemperatureSample.class,
                bootstrapServers,
                sourceTopic,
                sourceConsumerGroupId,
                schemaRegistryName,
                schemaRegistryRegion,
                new Properties() // ...any other Kafka consumer property
        );

        KafkaSink<RoomTemperature> sink = keyedKafkaSink(
                RoomTemperature.class,
                RoomTemperature::getRoom,
                bootstrapServers,
                sinkTopic,
                schemaRegistryName,
                schemaRegistryRegion,
                new Properties() // ...any other Kafka producer property
        );


        DataStream<TemperatureSample> temperatureSamples = env.fromSource(
                        source,
                        WatermarkStrategy.<TemperatureSample>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withIdleness(Duration.ofSeconds(60))
                                .withTimestampAssigner((sample, ts) -> sample.getSampleTime().toEpochMilli()),
                        "Temperature Samples source")
                .uid("temperature-samples-source");

        DataStream<RoomTemperature> roomTemperatures = temperatureSamples
                .keyBy(TemperatureSample::getRoom)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(new RoomAverageTemperatureCalculator())
                .uid("room-temperatures");

        roomTemperatures.sinkTo(sink).uid("room-temperatures-sink");

        env.execute("Flink app");
    }
}

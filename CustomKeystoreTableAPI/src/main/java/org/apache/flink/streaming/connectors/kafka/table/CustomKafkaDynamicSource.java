package org.apache.flink.streaming.connectors.kafka.table;

import com.amazonaws.services.kinesisanalytics.overrides.CustomFlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CustomKafkaDynamicSource extends KafkaDynamicSource {
    public CustomKafkaDynamicSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable Pattern topicPattern,
            Properties properties,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis,
            boolean upsertMode) {
        super(physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                upsertMode);
    }

    @Override
    protected FlinkKafkaConsumer<RowData> createKafkaConsumer(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final DynamicKafkaDeserializationSchema.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(DynamicKafkaDeserializationSchema.MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                producedDataType.getChildren().size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();

        final KafkaDeserializationSchema<RowData> kafkaDeserializer =
                new DynamicKafkaDeserializationSchema(
                        adjustedPhysicalArity,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo,
                        upsertMode);

        final FlinkKafkaConsumer<RowData> kafkaConsumer;
        if (topics != null) {
            kafkaConsumer = new CustomFlinkKafkaConsumer<>(topics, kafkaDeserializer, properties);
        } else {
            kafkaConsumer = new CustomFlinkKafkaConsumer<>(topicPattern, kafkaDeserializer, properties);
        }

        switch (startupMode) {
            case EARLIEST:
                kafkaConsumer.setStartFromEarliest();
                break;
            case LATEST:
                kafkaConsumer.setStartFromLatest();
                break;
            case GROUP_OFFSETS:
                kafkaConsumer.setStartFromGroupOffsets();
                break;
            case SPECIFIC_OFFSETS:
                kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
                break;
            case TIMESTAMP:
                kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
                break;
        }

        kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);

        if (watermarkStrategy != null) {
            kafkaConsumer.assignTimestampsAndWatermarks(watermarkStrategy);
        }
        return kafkaConsumer;
    }
}

package com.amazonaws.services.kinesisanalytics.overrides;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.*;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class CustomKafkaDynamicTableFactory extends KafkaDynamicTableFactory {
    public static final String CUSTOM_IDENTIFIER = "customkafka";

    @Override
    public String factoryIdentifier() {
        return CUSTOM_IDENTIFIER;
    }

    @Override
    protected KafkaDynamicSource createKafkaTableSource(DataType physicalDataType,
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
                                                        long startupTimestampMillis) {
        return new CustomKafkaDynamicSource(
                physicalDataType,
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
                false);
    }

    @Override
    protected KafkaDynamicSink createKafkaTableSink(DataType physicalDataType,
                                                    @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
                                                    EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
                                                    int[] keyProjection,
                                                    int[] valueProjection,
                                                    @Nullable String keyPrefix,
                                                    String topic,
                                                    Properties properties,
                                                    FlinkKafkaPartitioner<RowData> partitioner,
                                                    KafkaSinkSemantic semantic,
                                                    Integer parallelism) {
        return new CustomKafkaDynamicSink(
                physicalDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                semantic,
                false,
                SinkBufferFlushMode.DISABLED,
                parallelism);
    }
}
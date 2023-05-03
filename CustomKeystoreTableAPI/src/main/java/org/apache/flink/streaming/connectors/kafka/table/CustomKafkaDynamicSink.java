package org.apache.flink.streaming.connectors.kafka.table;

import com.amazonaws.services.kinesisanalytics.overrides.CustomFlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class CustomKafkaDynamicSink extends KafkaDynamicSink {
    public CustomKafkaDynamicSink(
            DataType consumedDataType,
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            KafkaSinkSemantic semantic,
            boolean upsertMode,
            SinkBufferFlushMode flushMode,
            @Nullable Integer parallelism) {
        super(consumedDataType,
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
                upsertMode,
                flushMode,
                parallelism);
    }

    @Override
    protected FlinkKafkaProducer<RowData> createKafkaProducer(SerializationSchema<RowData> keySerialization,
                                                              SerializationSchema<RowData> valueSerialization) {
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RowData.FieldGetter[] keyFieldGetters =
                Arrays.stream(keyProjection)
                        .mapToObj(
                                targetField ->
                                        RowData.createFieldGetter(
                                                physicalChildren.get(targetField), targetField))
                        .toArray(RowData.FieldGetter[]::new);

        final RowData.FieldGetter[] valueFieldGetters =
                Arrays.stream(valueProjection)
                        .mapToObj(
                                targetField ->
                                        RowData.createFieldGetter(
                                                physicalChildren.get(targetField), targetField))
                        .toArray(RowData.FieldGetter[]::new);

        // determine the positions of metadata in the consumed row
        final int[] metadataPositions =
                Stream.of(WritableMetadata.values())
                        .mapToInt(
                                m -> {
                                    final int pos = metadataKeys.indexOf(m.key);
                                    if (pos < 0) {
                                        return -1;
                                    }
                                    return physicalChildren.size() + pos;
                                })
                        .toArray();

        // check if metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        final DynamicKafkaSerializationSchema kafkaSerializer =
                new DynamicKafkaSerializationSchema(
                        topic,
                        partitioner,
                        keySerialization,
                        valueSerialization,
                        keyFieldGetters,
                        valueFieldGetters,
                        hasMetadata,
                        metadataPositions,
                        upsertMode);

        return new CustomFlinkKafkaProducer<>(
                topic,
                kafkaSerializer,
                properties,
                FlinkKafkaProducer.Semantic.valueOf(semantic.toString()),
                FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
    }
}
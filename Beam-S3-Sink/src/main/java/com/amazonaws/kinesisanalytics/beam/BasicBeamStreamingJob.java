package com.amazonaws.kinesisanalytics.beam;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import samples.trading.avro.TradeEvent;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.util.Arrays;

public class BasicBeamStreamingJob {
    public static final String BEAM_APPLICATION_PROPERTIES = "BeamApplicationProperties";

    private static class DeserializeAvro extends DoFn<KinesisRecord, GenericRecord> {
        private static final ObjectMapper jsonParser = new ObjectMapper();

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {

            byte[] payload = c.element().getDataAsBytes();
            JsonNode jsonNode = jsonParser.readValue(payload, JsonNode.class);
            TradeEvent output = TradeEvent.newBuilder()
                    .setTicker(jsonNode.get("ticker").toString())
                    .setEventTime(jsonNode.get("event_time").toString())
                    .setPrice(jsonNode.get("price").doubleValue())
                    .build();
            c.output(output);
        }
    }

    private static final class SimpleHashPartitioner implements KinesisPartitioner<byte[]> {
        @Override
        @NonNull public String getPartitionKey(byte[] value) {
            return String.valueOf(Arrays.hashCode(value));
        }
    }

    public static void main(String[] args) {

        String[] kinesisArgs = BasicBeamStreamingJobOptionsParser.argsFromKinesisApplicationProperties(args, BEAM_APPLICATION_PROPERTIES);
        BasicBeamStreamingJobOptions options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs)).as(BasicBeamStreamingJobOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);

        PipelineOptionsValidator.validate(BasicBeamStreamingJobOptions.class, options);
        Pipeline p = Pipeline.create(options);

        p
        .apply("KDS source",
                KinesisIO
                        .read()
                        .withStreamName(options.getInputStreamName())
                        .withInitialPositionInStream(InitialPositionInStream.LATEST)
        )
        .apply("Deserialize",
                ParDo.of(new DeserializeAvro())
        )
        .setCoder(
                AvroCoder.of(GenericRecord.class, TradeEvent.SCHEMA$)
        )
        .apply("Map elements into",
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                        .via((GenericRecord r) -> r)
        )
        .setCoder(
                        AvroCoder.of(GenericRecord.class, TradeEvent.SCHEMA$)
        )
        .apply(
                Window.into(FixedWindows.of(Duration.standardSeconds(60)))
        )
        .apply("Sink to S3",
                FileIO.
                        <GenericRecord>write()
                        .via(ParquetIO
                                .sink(TradeEvent.SCHEMA$)
                                .withCompressionCodec(CompressionCodecName.SNAPPY)
                        )
                        .to(options.getSinkLocation())
                        .withSuffix(".parquet")
        );

        p.run().waitUntilFinish();
    }
}

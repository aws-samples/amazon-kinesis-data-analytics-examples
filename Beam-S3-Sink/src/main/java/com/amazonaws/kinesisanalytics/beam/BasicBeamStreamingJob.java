package com.amazonaws.kinesisanalytics.beam;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisPartitioner;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samples.trading.avro.TradeEvent;

import java.util.Arrays;
import java.util.Optional;

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

    private static final class SimpleHashPartitioner implements KinesisPartitioner {
        @Override
        public String getPartitionKey(byte[] value) {
            return String.valueOf(Arrays.hashCode(value));
        }

        @Override
        public String getExplicitHashKey(byte[] value) {
            return null;
        }
    }

    public static void main(String[] args) {

        String[] kinesisArgs = BasicBeamStreamingJobOptionsParser.argsFromKinesisApplicationProperties(args, BEAM_APPLICATION_PROPERTIES);
        BasicBeamStreamingJobOptions options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs)).as(BasicBeamStreamingJobOptions.class);
        options.setRunner(FlinkRunner.class);

        Regions region = Optional
                .ofNullable(Regions.fromName(options.getAwsRegion()))
                .orElse(Regions.fromName(Regions.getCurrentRegion().getName()));

        PipelineOptionsValidator.validate(BasicBeamStreamingJobOptions.class, options);
        Pipeline p = Pipeline.create(options);

        p
        .apply("KDS source",
                KinesisIO
                        .read()
                        .withStreamName(options.getInputStreamName())
                        .withAWSClientsProvider(new DefaultCredentialsProviderClientsProvider(region))
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
                        .to("s3://<YOUR S3 PREFIX>")
                        .withSuffix(".parquet")
        );

        p.run().waitUntilFinish();
    }
}

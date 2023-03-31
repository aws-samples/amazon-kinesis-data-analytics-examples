package com.amazonaws.kinesisanalytics.beam;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BasicBeamStreamingJob {
    public static final String BEAM_APPLICATION_PROPERTIES = "BeamApplicationProperties";

    private static class PingPongFn extends DoFn<KinesisRecord, byte[]> {
        private static final Logger LOG = LoggerFactory.getLogger(PingPongFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {
            String content = new String(c.element().getDataAsBytes(), StandardCharsets.UTF_8);
            if (content.trim().equalsIgnoreCase("ping")) {
                LOG.info("Ponged!");
                c.output("pong\n".getBytes(StandardCharsets.UTF_8));
            } else {
                LOG.info("No action for: " + content);
                c.output(c.element().getDataAsBytes());
            }
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

        p.apply("KDS source",
                        KinesisIO.read()
                                .withStreamName(options.getInputStreamName())
                                .withInitialPositionInStream(InitialPositionInStream.LATEST))
                .apply("Pong transform", ParDo.of(new PingPongFn()))
                .apply("KDS sink",
                        KinesisIO.<byte[]>write()
                                .withStreamName(options.getOutputStreamName())
                                .withSerializer(r -> r)
                                // For this to properly balance across shards, the keys would need to be supplied dynamically
                                .withPartitioner(new SimpleHashPartitioner())
        );

        p.run().waitUntilFinish();
    }
}

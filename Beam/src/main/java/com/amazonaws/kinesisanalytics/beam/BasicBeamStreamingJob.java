package com.amazonaws.kinesisanalytics.beam;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class BasicBeamStreamingJob {
    private static class PingPongFn extends DoFn<KinesisRecord, byte[]> {
        private static final Logger LOG = LoggerFactory.getLogger(PingPongFn.class);

        private final Counter pings = Metrics.counter(PingPongFn.class, "pings");

        @ProcessElement
        public void processElement(ProcessContext c) {
            String content = new String(c.element().getDataAsBytes(), StandardCharsets.UTF_8);
            if (content.trim().equalsIgnoreCase("ping")) {
                LOG.info("Ponged!");
                pings.inc();
                c.output("pong\n".getBytes(StandardCharsets.UTF_8));
            } else {
                LOG.info("No action for: " + content);
                c.output(c.element().getDataAsBytes());
            }
        }
    }

    public static void main(String[] args) {
        String[] kinesisArgs = BasicBeamStreamingJobOptions.argsFromKinesisApplicationProperties(args,"BeamApplicationProperties");
        BasicBeamStreamingJobOptions options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs)).as(BasicBeamStreamingJobOptions.class);
        options.setRunner(FlinkRunner.class);
        Regions region = Optional
                .ofNullable(Regions.getCurrentRegion())
                .map(r -> Regions.fromName(r.getName()))
                .orElse(Regions.fromName(options.getAwsRegion()));

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
        .apply("Pong transform",
            ParDo.of(new PingPongFn())
        )
        .apply("KDS sink",
            KinesisIO
                .write()
                .withStreamName(options.getOutputStreamName())
                .withAWSClientsProvider(new DefaultCredentialsProviderClientsProvider(region))
                // for this to properly balance across shards, the keys would need to be supplied dynamically
                .withPartitionKey("staticKey")
        );

        p.run().waitUntilFinish();
    }
}

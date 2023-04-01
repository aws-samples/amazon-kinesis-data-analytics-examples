package com.amazonaws.kinesisanalytics.beam;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Description;

public interface BasicBeamStreamingJobOptions extends FlinkPipelineOptions, AwsOptions {
    @Description("Name of the Kinesis Data Stream to read from")
    String getInputStreamName();

    void setInputStreamName(String value);

    @Description("S3 location to write output to")
    String getSinkLocation();

    void setSinkLocation(String value);
}

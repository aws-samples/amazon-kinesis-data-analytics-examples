package com.amazonaws.kinesisanalytics.beam;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Description;

public interface BasicBeamStreamingJobOptions extends FlinkPipelineOptions, AwsOptions {
    @Description("Name of the Kinesis Data Stream to read from")
    String getInputStreamName();

    void setInputStreamName(String value);

    @Description("Name of the Kinesis Data Stream to write to")
    String getOutputStreamName();

    void setOutputStreamName(String value);
}
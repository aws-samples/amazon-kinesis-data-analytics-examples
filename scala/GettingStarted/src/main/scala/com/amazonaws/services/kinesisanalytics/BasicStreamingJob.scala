package com.amazonaws.services.kinesisanalytics

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer

import java.util
import java.util.{Map, Properties}

private val streamNameKey = "stream.name"
private val defaultInputStreamName = "ExampleInputStream"
private val defaultOutputStreamName = "ExampleOutputStream"

private def createSource: FlinkKinesisConsumer[String] = {
  val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
  val inputProperties = applicationProperties.get("ConsumerConfigProperties")

  new FlinkKinesisConsumer[String](inputProperties.getProperty(streamNameKey, defaultInputStreamName),
    new SimpleStringSchema, inputProperties)
}

private def createSink: KinesisStreamsSink[String] = {
  val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
  val outputProperties = applicationProperties.get("ProducerConfigProperties")

  KinesisStreamsSink.builder[String]
    .setKinesisClientProperties(outputProperties)
    .setSerializationSchema(new SimpleStringSchema)
    .setStreamName(outputProperties.getProperty(streamNameKey, defaultOutputStreamName))
    .setPartitionKeyGenerator((element: String) => String.valueOf(element.hashCode))
    .build
}

@main def main(): Unit = {
  val environment = StreamExecutionEnvironment.getExecutionEnvironment
  environment
    .addSource(createSource)
    .sinkTo(createSink)
  environment.execute("Flink Streaming Scala Example")
}
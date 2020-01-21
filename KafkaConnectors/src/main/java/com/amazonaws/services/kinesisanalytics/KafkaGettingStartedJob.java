package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class KafkaGettingStartedJob {

	private static DataStream<String> createKafkaSourceFromApplicationProperties(StreamExecutionEnvironment env) throws IOException {
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		return env.addSource(new FlinkKafkaConsumer<>((String) applicationProperties.get("KafkaSource").get("topic"),
				new SimpleStringSchema(), applicationProperties.get("KafkaSource")));
	}

	private static FlinkKafkaProducer<String> createKafkaSinkFromApplicationProperties() throws IOException {
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		KeyedSerializationSchema<String> serializationSchema =
				(KeyedSerializationSchema<String>)(new SimpleStringSchema());

		// Configure FlinkProducer for exactly-once semantics
		FlinkKafkaProducer<String> sink = new FlinkKafkaProducer<>(
				(String) applicationProperties.get("KafkaSink").get("topic"),
				serializationSchema,
				applicationProperties.get("KafkaSink"),
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
		return sink;
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> input = createKafkaSourceFromApplicationProperties(env);

		// Add sink
		input.addSink(createKafkaSinkFromApplicationProperties());

		env.execute("Flink Streaming Java API Skeleton");
	}
}
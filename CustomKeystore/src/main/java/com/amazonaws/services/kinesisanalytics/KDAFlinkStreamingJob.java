package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KDAFlinkStreamingJob {

	public static void main(String[] args) throws Exception {
		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up the truststore in the JobManager
		CustomFlinkKafkaUtil.initializeKafkaTruststore();

		// Add source
		DataStream<String> stream = env.fromSource(createKafkaSourceFromApplicationProperties(), WatermarkStrategy.noWatermarks(), "Kafka Source");

		// Add sink
		stream.sinkTo(createKafkaSinkFromApplicationProperties());

		env.execute("Flink Streaming Java With Custom Keystore");
	}

	private static KafkaSource<String> createKafkaSourceFromApplicationProperties() throws IOException {
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		Properties sourceProps = applicationProperties.get("KafkaSource");

		// Configure location where runtime will look for custom keystore
		sourceProps.setProperty("ssl.truststore.location", "/tmp/kafka.client.truststore.jks");

		String brokers = String.valueOf(sourceProps.get("bootstrap.servers"));

		return KafkaSource.<String>builder()
				.setBootstrapServers(brokers)
				.setTopics((String) sourceProps.get("topic"))
				.setValueOnlyDeserializer(new CustomSimpleStringDeserializationSchema())
				.setProperties(sourceProps)
				.build();
	}

	private static KafkaSink<String> createKafkaSinkFromApplicationProperties() throws IOException {
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		Properties sinkProps = applicationProperties.get("KafkaSink");

		// Configure location where runtime will look for custom keystore
		sinkProps.setProperty("ssl.truststore.location", "/tmp/kafka.client.truststore.jks");

		String brokers = String.valueOf(sinkProps.get("bootstrap.servers"));

		return KafkaSink.<String>builder()
				.setBootstrapServers(brokers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic((String) sinkProps.get("topic"))
						.setValueSerializationSchema(new CustomSimpleStringSerializationSchema())
						.build()
				)
				.setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
				.setTransactionalIdPrefix("my-app-prefix")
				.setKafkaProducerConfig(sinkProps)
				.build();
	}
}
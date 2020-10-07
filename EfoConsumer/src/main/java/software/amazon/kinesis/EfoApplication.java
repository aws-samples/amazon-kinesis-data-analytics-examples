package software.amazon.kinesis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import software.amazon.kinesis.connectors.flink.FlinkKinesisConsumer;
import software.amazon.kinesis.connectors.flink.FlinkKinesisProducer;
import software.amazon.kinesis.connectors.flink.serialization.KinesisDeserializationSchemaWrapper;

import java.util.Map;
import java.util.Properties;

import static java.util.Optional.ofNullable;
import static software.amazon.kinesis.connectors.flink.config.AWSConfigConstants.AWS_REGION;
import static software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;

public class EfoApplication {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

		Properties consumerProperties = ofNullable(applicationProperties.get("ConsumerConfigProperties")).orElseGet(Properties::new);
		Properties producerProperties = ofNullable(applicationProperties.get("ProducerConfigProperties")).orElseGet(Properties::new);

		SingleOutputStreamOperator<String> op = env
				.addSource(createKdsSource(consumerProperties))
				.map(s -> s + ": processed by basic EFO app\n");

		op.addSink(createKdsSink(producerProperties));

		env.execute("EFO Application");
	}

	private static SourceFunction<String> createKdsSource(final Properties consumerConfig) {
		consumerConfig.putIfAbsent(RECORD_PUBLISHER_TYPE, "EFO");
		consumerConfig.putIfAbsent(EFO_CONSUMER_NAME, "basic-efo-flink-app");
		consumerConfig.putIfAbsent(AWS_REGION, "us-west-2");

		return new FlinkKinesisConsumer<String>(
				consumerConfig.getProperty("INPUT_STREAM", "ExampleInptStream"),
				new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()), consumerConfig);
	}

	private static SinkFunction<String> createKdsSink(final Properties producerConfig) {
		producerConfig.putIfAbsent(AWS_REGION, "us-west-2");

		FlinkKinesisProducer<String> producer = new FlinkKinesisProducer<>(
				new SimpleStringSchema(),
				producerConfig);

		producer.setDefaultStream(producerConfig.getProperty("OUTPUT_STREAM", "ExampleOutputStream"));
		producer.setDefaultPartition("partition");

		return producer;
	}

}

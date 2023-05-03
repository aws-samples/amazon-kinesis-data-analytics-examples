package software.amazon.kinesis;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;

import java.util.Map;
import java.util.Properties;

import static java.util.Optional.ofNullable;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;

public class EfoApplication {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        Properties consumerProperties = ofNullable(applicationProperties.get("ConsumerConfigProperties")).orElseGet(Properties::new);
        Properties producerProperties = ofNullable(applicationProperties.get("ProducerConfigProperties")).orElseGet(Properties::new);

        SingleOutputStreamOperator<String> op = env
                .addSource(createKdsSource(consumerProperties))
                .map(s -> s + ": processed by basic EFO app\n");

        op.sinkTo(createKdsSink(producerProperties));

        env.execute("EFO Application");
    }

    private static SourceFunction<String> createKdsSource(final Properties consumerConfig) {
        consumerConfig.putIfAbsent(RECORD_PUBLISHER_TYPE, "EFO");
        consumerConfig.putIfAbsent(EFO_CONSUMER_NAME, "basic-efo-flink-app");
        consumerConfig.putIfAbsent(AWS_REGION, "us-west-2");

        return new FlinkKinesisConsumer<>(
                consumerConfig.getProperty("INPUT_STREAM", "ExampleInputStream"),
                new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()), consumerConfig);
    }

    private static KinesisStreamsSink<String> createKdsSink(final Properties sinkProperties) {
        sinkProperties.putIfAbsent(AWS_REGION, "us-west-2");

        return KinesisStreamsSink.<String>builder()
                .setKinesisClientProperties(sinkProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setStreamName(sinkProperties.getProperty("OUTPUT_STREAM", "ExampleOutputStream"))
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();
    }
}

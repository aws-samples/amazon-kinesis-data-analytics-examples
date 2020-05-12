package com.amazonaws.services.kinesisanalytics;
 
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class RecordCountApplication {
    private static final String INPUT_PROPERTIES_KEY_PREFIX = "consumer.config.0";
    private static final String PRODUCER_PROPERTIES_KEY_PREFIX = "producer.config.0";
    private static final String INPUT_STREAM_KEY = "input.stream.name";
    private static final String INPUT_REGION_KEY = "aws.region";
    private static final String INPUT_STARTING_POSITION_KEY = "flink.stream.initpos";
    private static final String OUTPUT_STREAM_KEY = "output.stream.name";
    private static final String SHARD_COUNT_KEY = "shard.count";
    private static final String flinkJobName = "ApplicationLifeCycleTestCanaryWithRunconfiguration StreamingJobForStart";
    private static Logger LOG = LoggerFactory.getLogger(StreamingJobWithRunConfigurationStart.class);
 
    public RecordCountApplication() {
    }
 
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> runConfigurations = KinesisAnalyticsRuntime.getApplicationProperties();
        Iterator var3 = runConfigurations.entrySet().iterator();
 
        while(var3.hasNext()) {
            Entry<String, Properties> entry = (Entry)var3.next();
            LOG.info("Property Group ID: " + (String)entry.getKey() + " Property mapping: " + getPropertyAsString((Properties)entry.getValue()));
        }
 
        Properties inputConfig = (Properties)runConfigurations.get("consumer.config.0");
        String inputStreamName = inputConfig.getProperty("input.stream.name");
        String inputRegion = inputConfig.getProperty("aws.region");
        String inputStartingPosition = inputConfig.getProperty("flink.stream.initpos");
        Properties consumerProperty = new Properties();
        consumerProperty.put("aws.region", inputRegion);
        consumerProperty.put("flink.stream.initpos", inputStartingPosition);
        DataStream<String> kinesisInput = env.addSource(new FlinkKinesisConsumer(inputStreamName, new SimpleStringSchema(), consumerProperty));
        DataStream<String> noopMapperFunctionBeforeFilter = kinesisInput.map(new NoOpMapperFunction("ReceivedRecords"));
        LOG.info("Creating flink data stream with input stream name: {} in the region {} with initial position {}", new Object[]{inputStreamName, inputRegion, inputStartingPosition});
        DataStream<String> kinesisProcessed = noopMapperFunctionBeforeFilter.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                return RecordSchemaHelper.isGreaterThanCanaryMinSpeed(value);
            }
        });
        DataStream<String> noopMapperFunctionAfterFilter =
                kinesisProcessed.map(new NoOpMapperFunction("FilteredRecords"));
        Properties outputConfig = (Properties)runConfigurations.get("producer.config.0");
        String outputStreamName = outputConfig.getProperty("output.stream.name");
        String shardCount = outputConfig.getProperty("shard.count");
        String outputRegion = outputConfig.getProperty("aws.region");
        Properties producerConfig = new Properties();
        producerConfig.put("aws.region", outputRegion);
        producerConfig.put("AggregationEnabled", "false");
        FlinkKinesisProducer<String> kinesisOutput = new FlinkKinesisProducer(new SimpleStringSchema(), producerConfig);
        kinesisOutput.setDefaultStream(outputStreamName);
        kinesisOutput.setDefaultPartition(RandomStringUtils.random(Integer.valueOf(shardCount)));
        noopMapperFunctionAfterFilter.addSink(kinesisOutput);
        LOG.info("Starting flink job : {} with using input kinesis stream : {} with initial position {} and output kinesis stream {} with shard count {}", new Object[]{"ApplicationLifeCycleTestCanaryWithRunconfiguration StreamingJobForStart", inputStreamName, inputStartingPosition, outputStreamName, shardCount});
        env.execute("ApplicationLifeCycleTestCanaryWithRunconfiguration StreamingJobForStart");
    }
 
    private static String getPropertyAsString(Properties prop) {
        StringWriter writer = new StringWriter();
 
        try {
            prop.store(writer, "");
        } catch (IOException var3) {
            LOG.error("Failed to log properties content.");
            throw new RuntimeException(var3);
        }
 
        return writer.getBuffer().toString();
    }
 
    /**
     * NoOp mapper function which acts as a pass through for the records. It would also publish the custom metric for
     * the number of received records.
     */
    private static class NoOpMapperFunction extends RichMapFunction<String, String> {
        private transient int valueToExpose = 0;
        private final String customMetricName;
 
        public NoOpMapperFunction(final String customMetricName) {
            this.customMetricName = customMetricName;
        }
 
        @Override
        public void open(Configuration config) {
            getRuntimeContext().getMetricGroup()
                    .addGroup("kinesisanalytics")
                    .addGroup("Service", "FunctionalCanary")
                    .addGroup("StreamingJobWithRunConfigurationStart")
                    .gauge(customMetricName, (Gauge<Integer>) () -> valueToExpose);
        }
 
        @Override
        public String map(String value) throws Exception {
            valueToExpose++;
            return value;
        }
    }
}


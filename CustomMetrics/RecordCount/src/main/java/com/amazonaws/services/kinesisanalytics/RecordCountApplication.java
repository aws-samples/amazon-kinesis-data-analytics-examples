    package com.amazonaws.services.kinesisanalytics;
     
    import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
    import java.util.Map;
    import java.util.Properties;
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
     
    /**
     * A sample Kinesis Data Analytics for Java Application with Kinesis data streams as source and sink with simple filter
     * function.
     */
    public class RecordCountApplication {
     
        private static Logger LOG = LoggerFactory.getLogger(RecordCountApplication.class);
     
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Map<String, Properties> runConfigurations = KinesisAnalyticsRuntime.getApplicationProperties();
     
            // Get the Kinesis Consumer properties from runtime configuration
            Properties inputConfig = runConfigurations.get("consumer.config.0");
            String inputStreamName = inputConfig.getProperty("input.stream.name");
            String inputRegion = inputConfig.getProperty("aws.region");
            String inputStartingPosition = inputConfig.getProperty("flink.stream.initpos");
            Properties consumerProperty = new Properties();
            consumerProperty.put("aws.region", inputRegion);
            consumerProperty.put("flink.stream.initpos", inputStartingPosition);
     
            // Add the Kinesis Consumer as input
            DataStream<String> kinesisInput =
                    env.addSource(new FlinkKinesisConsumer(inputStreamName, new SimpleStringSchema(), consumerProperty));
     
            // Add the NoOpMapperFunction to publish custom 'ReceivedRecords' metric before filtering
            DataStream<String> noopMapperFunctionBeforeFilter = kinesisInput.map(new NoOpMapperFunction("ReceivedRecords"));
     
            // Add the FilterFunction to filter the records based on MinSpeed (i.e. 106)
            DataStream<String> kinesisProcessed = noopMapperFunctionBeforeFilter.filter(new FilterFunction<String>() {
                public boolean filter(String value) throws Exception {
                    return RecordSchemaHelper.isGreaterThanMinSpeed(value);
                }
            });
     
            // Add the NoOpMapperFunction to publish custom 'FilteredRecords' metric after filtering
            DataStream<String> noopMapperFunctionAfterFilter =
                    kinesisProcessed.map(new NoOpMapperFunction("FilteredRecords"));
     
            // Get the Kinesis Producer properties from runtime configuration
            Properties outputConfig = runConfigurations.get("producer.config.0");
            String outputStreamName = outputConfig.getProperty("output.stream.name");
            String shardCount = outputConfig.getProperty("shard.count");
            String outputRegion = outputConfig.getProperty("aws.region");
            Properties producerConfig = new Properties();
            producerConfig.put("aws.region", outputRegion);
            producerConfig.put("AggregationEnabled", "false");
     
            // Add the Kinesis Producer as output
            FlinkKinesisProducer<String> kinesisOutput = new FlinkKinesisProducer(new SimpleStringSchema(), producerConfig);
            kinesisOutput.setDefaultStream(outputStreamName);
            kinesisOutput.setDefaultPartition(RandomStringUtils.random(Integer.valueOf(shardCount)));
            noopMapperFunctionAfterFilter.addSink(kinesisOutput);
     
            LOG.info("Starting flink job: {} with using input kinesis stream: {} with initial position: {} and output"
                             + " kinesis stream: {} with shard count: {}", new Object[] { "RecordCountApplication",
                             inputStreamName, inputStartingPosition, outputStreamName, shardCount});
            env.execute("RecordCountApplication Job");
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
                        .addGroup("Program", "RecordCountApplication")
                        .addGroup("NoOpMapperFunction")
                        .gauge(customMetricName, (Gauge<Integer>) () -> valueToExpose);
            }
     
            @Override
            public String map(String value) throws Exception {
                valueToExpose++;
                return value;
            }
        }
    }
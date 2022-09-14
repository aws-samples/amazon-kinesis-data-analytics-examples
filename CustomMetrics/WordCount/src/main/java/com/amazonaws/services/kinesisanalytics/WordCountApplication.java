    package com.amazonaws.services.kinesisanalytics;
     
    import java.util.Properties;
    import org.apache.flink.api.common.functions.RichFlatMapFunction;
    import org.apache.flink.api.common.serialization.SimpleStringSchema;
    import org.apache.flink.api.java.tuple.Tuple2;
    import org.apache.flink.configuration.Configuration;
    import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
    import org.apache.flink.metrics.Counter;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
    import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
    import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
    import org.apache.flink.util.Collector;
     
    /**
     * A basic Kinesis Data Analytics for Java application with Kinesis data
     * streams as source and sink.
     */
    public class WordCountApplication {
     
        private static final String region = "us-east-1";
        private static final String inputStreamName = "ExampleInputStream";
        private static final String outputStreamName = "ExampleOutputStream";
     
        private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
            Properties inputProperties = new Properties();
            inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
            inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
     
            return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
        }
     
        private static KinesisStreamsSink<String> createSinkFromStaticConfig() {
            Properties producerConfig = new Properties();
            producerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, region);
            producerConfig.setProperty("AggregationEnabled", "false");

            return KinesisStreamsSink.<String>builder()
                    .setKinesisClientProperties(producerConfig)
                    .setSerializationSchema(new SimpleStringSchema())
                    .setStreamName(outputStreamName)
                    .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                    .build();
        }
     
        public static void main(String[] args) throws Exception {
            // Set up the streaming execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
     
            // Read the input from kinesis source
            DataStream<String> input = createSourceFromStaticConfig(env);
     
            // Split up the lines in pairs (2-tuples) containing: (word,1), and
            // group by the tuple field "0" and sum up tuple field "1"
            DataStream<Tuple2<String, Integer>> wordCountStream = input.flatMap(new Tokenizer()).keyBy(0).sum(1);
     
            // Serialize the tuple to string format, and publish the output to kinesis sink
            wordCountStream.map(Tuple2::toString).sinkTo(createSinkFromStaticConfig());
     
            // Execute the environment
            env.execute("Flink Word Count Application");
        }
     
        /**
         * Implements the string tokenizer that splits sentences into words as a
         * user-defined FlatMapFunction. The function takes a line (String) and
         * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
         * Integer>}).
         */
        private static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
     
            private transient Counter counter;
     
            @Override
            public void open(Configuration config) {
                this.counter = getRuntimeContext().getMetricGroup()
                        .addGroup("kinesisanalytics")
                        .addGroup("Service", "WordCountApplication")
                        .addGroup("Tokenizer")
                        .counter("TotalWords");
            }
     
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");
     
                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        counter.inc();
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }
     
    }
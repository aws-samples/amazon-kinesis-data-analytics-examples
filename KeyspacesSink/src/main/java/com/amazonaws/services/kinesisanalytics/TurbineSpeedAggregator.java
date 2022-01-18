package com.amazonaws.services.kinesisanalytics;

 import com.datastax.driver.core.Cluster;
 import com.datastax.driver.core.ConsistencyLevel;
 import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
 import com.datastax.driver.mapping.Mapper;
 import com.jayway.jsonpath.JsonPath;
 import org.apache.flink.api.common.functions.MapFunction;
 import org.apache.flink.api.common.functions.ReduceFunction;
 import org.apache.flink.api.common.serialization.SimpleStringSchema;
 import org.apache.flink.api.java.tuple.Tuple5;
 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
 import org.apache.flink.streaming.api.windowing.time.Time;
 import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
 import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
 import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
 import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
 import software.aws.mcs.auth.SigV4AuthProvider;

 import java.util.Properties;

public class TurbineSpeedAggregator {
    private static final String region = "us-east-1";
    private static final String inputStreamName = "ExampleInputStream";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = createSourceFromStaticConfig(env);

        DataStream<TurbineAggregatedRecord> result = input
                .map(new WindTurbineInputMap())
                .keyBy(v -> v.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce(new AggregateReducer())
                .map(new AggregateMap());

        QueryOptionsSerializable queryOptions = new QueryOptionsSerializable();
        queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        CassandraSink.addSink(result)
                .setClusterBuilder(
                        new ClusterBuilder() {

                            private static final long serialVersionUID = 2793938419775311824L;

                            @Override
                            public Cluster buildCluster(Cluster.Builder builder) {
                                return builder
                                        .addContactPoint("cassandra.us-east-1.amazonaws.com")
                                        .withPort(9142)
                                        .withSSL()
                                        .withAuthProvider(new SigV4AuthProvider(region))
                                        .withLoadBalancingPolicy(
                                                DCAwareRoundRobinPolicy
                                                        .builder()
                                                        .withLocalDc(region)
                                                        .build())
                                        .withQueryOptions(queryOptions)
                                        .build();
                            }
                        })
                .setMapperOptions(() -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)})
                .setDefaultKeyspace("sensor_data")
                .build();

        env.execute("Wind Turbine Data Aggregator");
    }

    public static class WindTurbineInputMap implements MapFunction<String, Tuple5<String, Long, Long, Long,Long>> {
        @Override
        public Tuple5<String, Long, Long, Long, Long> map(String value) throws Exception {
            String turbineID = JsonPath.read(value, "$.turbineId");
            Long speed = Long.parseLong(JsonPath.read(value, "$.speed").toString());
            return new Tuple5<>(turbineID, speed, 1L, speed, speed);
        }
    }

    public static class AggregateReducer implements ReduceFunction<Tuple5<String, Long, Long, Long, Long>> {
        @Override
        public Tuple5<String, Long, Long, Long, Long> reduce(Tuple5<String, Long, Long, Long, Long> value1, Tuple5<String, Long, Long, Long, Long> value2) {
            return new Tuple5<>(value1.f0, value1.f1 + value2.f1, value1.f2 + 1, value2.f3 < value1.f3 ? value2.f3 : value1.f3, value2.f4 > value1.f4 ? value2.f4 : value1.f4);
        }
    }

    public static class AggregateMap implements MapFunction<Tuple5<String, Long, Long, Long, Long> , TurbineAggregatedRecord> {
        @Override
        public TurbineAggregatedRecord map(Tuple5<String, Long, Long, Long, Long> value) throws Exception {
            return new TurbineAggregatedRecord(value.f0,System.currentTimeMillis()/1000, value.f4, value.f3, value.f1/value.f2);

        }
    }
}


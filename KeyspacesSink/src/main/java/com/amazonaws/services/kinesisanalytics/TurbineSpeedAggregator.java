package com.amazonaws.services.kinesisanalytics;

 import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
 import com.datastax.driver.core.Cluster;
 import com.datastax.driver.core.ConsistencyLevel;
 import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
 import com.datastax.driver.mapping.Mapper;
 import com.jayway.jsonpath.JsonPath;
 import org.apache.flink.api.common.functions.MapFunction;
 import org.apache.flink.api.common.functions.ReduceFunction;
 import org.apache.flink.api.common.serialization.SimpleStringSchema;
 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
 import org.apache.flink.streaming.api.windowing.time.Time;
 import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
 import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
 import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
 import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
 import software.aws.mcs.auth.SigV4AuthProvider;

 import java.util.Map;
 import java.util.Properties;

public class TurbineSpeedAggregator {

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env, String region, String inputStreamName) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        String region = (String) applicationProperties.get("WindTurbineEnvironment").get("region");
        String inputStreamName = (String) applicationProperties.get("WindTurbineEnvironment").get("inputStreamName");

        DataStream<String> input = createSourceFromStaticConfig(env, region, inputStreamName);

        DataStream<TurbineAggregatedRecord> result = input
                .map(new WindTurbineInputMap())
                .keyBy(t -> t.turbineId)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
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
                                        .addContactPoint("cassandra."+ region +".amazonaws.com")
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

    public static class WindTurbineInputMap implements MapFunction<String, TurbineSensorMessageModel> {
        @Override
        public TurbineSensorMessageModel map(String value) throws Exception {
            TurbineSensorMessageModel sensorMessageModel = new TurbineSensorMessageModel();
            sensorMessageModel.turbineId = JsonPath.read(value, "$.turbineId");
            Long speed = Long.parseLong(JsonPath.read(value, "$.speed").toString());;
            sensorMessageModel.speed = speed;
            sensorMessageModel.sumSpeed = speed;
            sensorMessageModel.minSpeed = speed;
            sensorMessageModel.maxSpeed = speed;
            return sensorMessageModel;
        }
    }

    public static class AggregateReducer implements ReduceFunction<TurbineSensorMessageModel> {
        @Override
        public TurbineSensorMessageModel reduce(TurbineSensorMessageModel value1, TurbineSensorMessageModel value2) {
            TurbineSensorMessageModel reducedSensorMessageModel = new TurbineSensorMessageModel();
            reducedSensorMessageModel.turbineId = value2.turbineId;
            reducedSensorMessageModel.speed = value2.speed;
            reducedSensorMessageModel.messageCount = value1.messageCount + value2.messageCount;
            reducedSensorMessageModel.sumSpeed = value1.sumSpeed + value2.sumSpeed;
            reducedSensorMessageModel.minSpeed = value1.minSpeed < value2.sumSpeed ? value1.minSpeed : value2.minSpeed;
            reducedSensorMessageModel.maxSpeed = value1.maxSpeed > value2.maxSpeed ? value1.maxSpeed : value2.maxSpeed;
            return reducedSensorMessageModel;
        }
    }

    public static class AggregateMap implements MapFunction<TurbineSensorMessageModel , TurbineAggregatedRecord> {
        @Override
        public TurbineAggregatedRecord map(TurbineSensorMessageModel value) throws Exception {
            return new TurbineAggregatedRecord(value.turbineId,System.currentTimeMillis()/1000, value.maxSpeed, value.minSpeed, value.sumSpeed/value.messageCount);

        }
    }
}


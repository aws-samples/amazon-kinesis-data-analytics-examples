
package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.dateFormat;


public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameter;
        parameter = ParameterTool.fromArgs(args);

        //read the parameters from the Kinesis Analytics environment
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties flinkProperties = null;

        String kafkaTopic = parameter.get("kafka-topic", "AWSKafkaTutorialTopic");
        String brokers = parameter.get("brokers", "");
        String s3Path = parameter.get("s3Path", "");


        if (applicationProperties != null) {
            flinkProperties = applicationProperties.get("FlinkApplicationProperties");
        }

        if (flinkProperties != null) {
            kafkaTopic = flinkProperties.get("kafka-topic").toString();
            brokers = flinkProperties.get("brokers").toString();
            s3Path = flinkProperties.get("s3Path").toString();
        }
        LOG.info("kafkaTopic is ", kafkaTopic);
        LOG.info("brokers is ", brokers);
        LOG.info("s3Path is ", s3Path);


        //Create Properties object for the Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);


        //Process stream using table API
        StreamingTableAPI.process(env, kafkaTopic, s3Path + "/tableapi", kafkaProps);

        //Process stream using sql API
        StreamingSQLAPI.process(env, kafkaTopic, s3Path + "/sqlapi", kafkaProps);
    }


    public static class StreamingTableAPI {

        public static void process(StreamExecutionEnvironment env, String kafkaTopic, String s3Path, Properties kafkaProperties)  {

            org.apache.flink.table.api.bridge.java.StreamTableEnvironment streamTableEnvironment = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().useBlinkPlanner().build());

            //create the table
            final FlinkKafkaConsumer<StockRecord> consumer = new FlinkKafkaConsumer<StockRecord>(kafkaTopic, new KafkaEventDeserializationSchema(), kafkaProperties);
            consumer.setStartFromEarliest();
            //Obtain stream
            DataStream<StockRecord> events = env.addSource(consumer);

            Table table = streamTableEnvironment.fromDataStream(events);


            final Table filteredTable = table.
                    select(
                            $("event_time"), $("ticker"), $("price"),
                            dateFormat($("event_time"), "yyyy-MM-dd").as("dt"),
                            dateFormat($("event_time"), "HH").as("hr")
                    ).
                    where($("price").isGreater(50));


            final String s3Sink = "CREATE TABLE sink_table (" +
                    "event_time TIMESTAMP," +
                    "ticker STRING," +
                    "price DOUBLE," +
                    "dt STRING," +
                    "hr STRING" +
                    ")" +
                    " PARTITIONED BY (ticker,dt,hr)" +
                    " WITH" +
                    "(" +
                    " 'connector' = 'filesystem'," +
                    " 'path' = '" + s3Path + "'," +
                    " 'format' = 'json'" +
                    ") ";

            //send to s3
            streamTableEnvironment.executeSql(s3Sink);
            filteredTable.executeInsert("sink_table");


        }

    }

    public static class StreamingSQLAPI {

        public static void process(StreamExecutionEnvironment env, String kafkaTopic, String s3Path, Properties kafkaProperties)  {

            org.apache.flink.table.api.bridge.java.StreamTableEnvironment streamTableEnvironment = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().useBlinkPlanner().build());

            final String createTableStmt = "CREATE TABLE StockRecord " +
                    "(" +
                    "event_time TIMESTAMP," +
                    "ticker STRING," +
                    "price DOUBLE" +
                    ")" +
                    " WITH (" +
                    " 'connector' = 'kafka'," +
                    " 'topic' = '" + kafkaTopic + "'," +
                    " 'properties.bootstrap.servers' = '" + kafkaProperties.get("bootstrap.servers")
                    + "'," +
                    " 'properties.group.id' = 'testGroup'," +
                    " 'format' = 'json'," +
                    " 'scan.startup.mode' = 'earliest-offset'" +
                    ")";


            final String s3Sink = "CREATE TABLE sink_table (" +
                    "event_time TIMESTAMP," +
                    "ticker STRING," +
                    "price DOUBLE," +
                    "dt STRING," +
                    "hr STRING" +
                    ")" +
                    " PARTITIONED BY (ticker,dt,hr)" +
                    " WITH" +
                    "(" +
                    " 'connector' = 'filesystem'," +
                    " 'path' = '" + s3Path + "'," +
                    " 'format' = 'json'" +
                    ") ";


            streamTableEnvironment.executeSql(createTableStmt);
            streamTableEnvironment.executeSql(s3Sink);

            final String insertSql = "INSERT INTO sink_table SELECT event_time,ticker,price,DATE_FORMAT(event_time, 'yyyy-MM-dd') as dt, " +
                    "DATE_FORMAT(event_time, 'HH') as hh FROM StockRecord WHERE price > 50";
            streamTableEnvironment.executeSql(insertSql);

        }

    }
    public static class KafkaEventDeserializationSchema extends AbstractDeserializationSchema<StockRecord> {

        @Override
        public StockRecord deserialize(byte[] bytes) {
            try {
                StockRecord event = (StockRecord) Event.parseEvent(bytes);
                return event;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(StockRecord event) {
            return false;
        }

        @Override
        public TypeInformation<StockRecord> getProducedType() {
            return TypeExtractor.getForClass(StockRecord.class);
        }
    }


    @Getter
    @Setter
    @ToString
    public static class StockRecord extends Event {
        private Timestamp event_time;
        private String ticker;
        private Double price;
    }

    public static class Event {

        private static Gson gson = new GsonBuilder()
                .setDateFormat("yyyy-MM-dd hh:mm:ss")
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.parse(json.getAsString()))
                .create();

        public static Event parseEvent(byte[] event) {

            JsonReader jsonReader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(event)));
            JsonElement jsonElement = Streams.parse(jsonReader);

            //convert json to POJO, based on the type attribute
            return gson.fromJson(jsonElement, StockRecord.class);
        }
    }
}
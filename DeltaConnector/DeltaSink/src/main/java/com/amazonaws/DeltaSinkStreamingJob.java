package com.amazonaws;

import java.util.Arrays;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.Date;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

public class DeltaSinkStreamingJob
{
    private static final String STREAM_REGION = "StreamRegion";
    private static final String SOURCE_STREAM_NAME = "SourceStreamName";
    private static final String DELTA_SINK_PATH = "DeltaSinkPath";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env, String sourceStreamName, String region) {

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(sourceStreamName,
                new SimpleStringSchema(),
                inputProperties));
    }
    private static DataStream<RowData> createDeltaSink(
        DataStream<RowData> stream,
        Path deltaPath,
        RowType rowType) {
        String[] partitionCols = { "surname" };


        Configuration configuration = new Configuration();
        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
            deltaPath,
            configuration,
            rowType)
            .build();
        stream.sinkTo(deltaSink);
        return stream;
    }

    
    public static void main( String[] args ) throws Exception
    {
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Reading runtime properties
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
        String sourceStreamName = flinkProperties.get(SOURCE_STREAM_NAME).toString();
        String streamRegion = flinkProperties.get(STREAM_REGION).toString();
        String deltaSinkPath = flinkProperties.get(DELTA_SINK_PATH).toString();

        

        //Create Kinesis Stream Source and parse incoming stream
        DataStream<String> input = createSourceFromStaticConfig(env, sourceStreamName, streamRegion);
        ObjectMapper jsonParser = new ObjectMapper();
        DataStream<RowData> rowStream = input.map(value -> { // Parse the JSON
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            //convert string to Timestamp
            String eventTime = jsonNode.get("EVENT_TIME").asText();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            Date parsedEventDate = dateFormat.parse(eventTime);
            Timestamp eventTimestamp = new Timestamp(parsedEventDate.getTime());
            return GenericRowData.of(
                    TimestampData.fromTimestamp(eventTimestamp),
                    StringData.fromString(jsonNode.get("TICKER").asText()),
                    jsonNode.get("PRICE").asInt()
            );
        });

        //Create Delta Sink
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("event_time", new TimestampType()),
                new RowType.RowField("ticker", new VarCharType(50)),
                new RowType.RowField("price", new IntType())
        ));
        Path deltaPath = new Path(deltaSinkPath);
        createDeltaSink(rowStream, deltaPath, rowType);
        env.execute("Flink Example Delta Sink");
    }
}

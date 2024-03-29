package com.amazonaws;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple3;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.sink.DeltaSink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

public class DeltaSourceStreamingJob
{
    private static final String DELTA_SOURCE_TABLE_PATH = "DeltaSourceTablePath";
    private static final String DELTA_SINK_TABLE_PATH = "DeltaSinkTablePath";

    private static DataStream<RowData> createContinuousDeltaSourceAllColumns(
            StreamExecutionEnvironment env,
            Path deltaTablePath) {

        DeltaSource<RowData> deltaSource = DeltaSource
                .forContinuousRowData(
                        deltaTablePath,
                        new Configuration())
                .build();

        return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
    }
    
    private static DataStream<RowData> createDeltaSink(
        DataStream<RowData> stream,
        Path deltaSinkTablePath,
        RowType rowType) {

        Configuration configuration = new Configuration();
        
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
            deltaSinkTablePath,
            configuration,
            rowType)
            .build();

        stream.sinkTo(deltaSink);
        return stream;
    }

    public static void main( String[] args ) throws Exception
    {
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Reading runtime properties
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
        Path deltaSourcePath = new Path(flinkProperties.get(DELTA_SOURCE_TABLE_PATH).toString());
        Path deltaSinkPath = new Path(flinkProperties.get(DELTA_SINK_TABLE_PATH).toString());

        // Stream data from source delta table
        DataStream<RowData> input = createContinuousDeltaSourceAllColumns(env, deltaSourcePath);


        //find max price per ticker in 1 minute Tumbling window
        WatermarkStrategy < Tuple3<Long, String, Integer>> ws =
                WatermarkStrategy
                        . < Tuple3<Long, String, Integer>> forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f0);
        DataStream<RowData> aggStream = input.map(value -> {
            return new Tuple3<Long, String, Integer>(
                    Long.valueOf(value.getTimestamp(0, 0).toTimestamp().getTime()),
                    value.getString(1).toString(),
                    value.getInt(2)
            );
        }).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(ws)
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .max(2)
                .map(value -> {
                    return GenericRowData.of(TimestampData.fromEpochMillis(value.f0), StringData.fromString(value.f1), value.f2);
                });


        //sink aggregated stream to another Delta table
        //For this example sink is chosen to be another Delta table...
        //but it could be any other target
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("event_time", new TimestampType()),
                new RowType.RowField("ticker", new VarCharType(50)),
                new RowType.RowField("max_price", new IntType())
        ));
        createDeltaSink(aggStream, deltaSinkPath, rowType);


        env.execute("Flink Example Delta Source");
    }
}

package com.amazonaws.operators;

import com.amazonaws.utils.S3LoadData;
import com.amazonaws.pojo.Customer;
import com.amazonaws.pojo.Location;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

public class PeriodicPerPartitionLoadEnrichmentData extends KeyedProcessFunction<String, Customer, Customer> {
    private transient ValueState<Location> locationState = null;
    private S3LoadData s3Data = null;
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicPerPartitionLoadEnrichmentData.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Location> stateDesc = new ValueStateDescriptor<>(
                "locationRefData",
                TypeInformation.of(Location.class)
        );

        locationState = getRuntimeContext().getState(stateDesc);
        s3Data = new S3LoadData();
    }

    @Override
    public void processElement(Customer customer, Context context, Collector<Customer> collector) throws Exception {
        if (locationState.value() == null) {
            locationState.update(s3Data.loadReferenceLocationData(customer.getRole()));
        }

        customer.setBuildingNo(locationState.value().getBuildingNo());
        collector.collect(customer);

        //Invoke reference data load every 60 seconds.
        //Flink uses timer coalescing to register one timer per key per timestamp.
        //Here the timestamp is rounded to every minute.
        context.timerService().registerProcessingTimeTimer(everyNthSeconds(context.timerService().currentProcessingTime(), 60));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Customer> out) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        LOG.info(String.format("Timer function triggered at %s to reload reference data again", sdf.format(timestamp)));

        locationState.update(s3Data.loadReferenceLocationData(ctx.getCurrentKey()));
    }

    private long everyNthSeconds(long currentProcessingTime, int seconds) {
        long millis = seconds * 1000;

        return (currentProcessingTime / millis) * millis + millis;
    }
}

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
import java.util.ArrayList;
import java.util.List;

public class PeriodicPerPartitionLoadEnrichmentData extends KeyedProcessFunction<String, Customer, Customer> {
    private transient ValueState<Location> locationState = null;
    private S3LoadData s3Data = null;
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicPerPartitionLoadEnrichmentData.class);
    private List<String> timerConfiguredKeys = new ArrayList();

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

        //Invalidate reference data load every 60 seconds.
        //Register timer once per key and not for every element.
        if (!timerConfiguredKeys.contains(customer.getRole())) {
            context.timerService().registerProcessingTimeTimer(everyNthSeconds(context.timerService().currentProcessingTime(), 60));

            timerConfiguredKeys.add(customer.getRole());
            LOG.info("Added key to list of timers: " + customer.getRole());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Customer> out) throws Exception {
        timerConfiguredKeys.remove(ctx.getCurrentKey());
        LOG.info("Removed key from the list of timers: " + ctx.getCurrentKey());

        //Invalidate reference data
        locationState.update(null);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        LOG.info(String.format("Timer function triggered at %s to invalidate state to force reload next time", sdf.format(timestamp)));
    }

    private long everyNthSeconds(long currentProcessingTime, int seconds) {
        long millis = seconds * 1000;

        return (currentProcessingTime / millis) * millis + millis;
    }
}

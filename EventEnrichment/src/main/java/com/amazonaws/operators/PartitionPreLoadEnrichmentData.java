package com.amazonaws.operators;

import com.amazonaws.utils.S3LoadData;
import com.amazonaws.pojo.Customer;
import com.amazonaws.pojo.Location;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PartitionPreLoadEnrichmentData extends RichFlatMapFunction<Customer, Customer> {

    private transient ValueState<Location> locationState = null;
    S3LoadData s3Data = null;

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
    public void flatMap(Customer customer, Collector<Customer> collector) throws Exception {
        if (locationState.value() == null) {
            locationState.update(s3Data.loadReferenceLocationData(customer.getRole()));
        }

        customer.setBuildingNo(locationState.value().getBuildingNo());
        collector.collect(customer);
    }
}

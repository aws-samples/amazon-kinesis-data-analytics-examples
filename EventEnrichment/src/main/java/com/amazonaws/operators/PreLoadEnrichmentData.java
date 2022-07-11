package com.amazonaws.operators;

import com.amazonaws.utils.S3LoadData;
import com.amazonaws.pojo.Customer;
import com.amazonaws.pojo.Location;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PreLoadEnrichmentData extends RichFlatMapFunction<Customer, Customer> {

    private MapState<String, Location> locationMapState = null;
    private S3LoadData s3Data = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Location> mapStateDesc = new MapStateDescriptor<>(
                "locationRefData",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(Location.class)
        );

        locationMapState = getRuntimeContext().getMapState(mapStateDesc);
        s3Data = new S3LoadData();
    }

    @Override
    public void flatMap(Customer customer, Collector<Customer> collector) throws Exception {
        if (!locationMapState.contains(customer.getRole())) {
            locationMapState.putAll(s3Data.loadReferenceLocationData());
        }

        customer.setBuildingNo(locationMapState.get(customer.getRole()).getBuildingNo());
        collector.collect(customer);
    }
}

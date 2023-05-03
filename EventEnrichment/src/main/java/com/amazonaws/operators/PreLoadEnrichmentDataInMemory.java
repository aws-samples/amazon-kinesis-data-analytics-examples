package com.amazonaws.operators;

import com.amazonaws.utils.S3LoadData;
import com.amazonaws.pojo.Customer;
import com.amazonaws.pojo.Location;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class PreLoadEnrichmentDataInMemory extends RichFlatMapFunction<Customer, Customer> {

    private HashMap<String, Location> locationMap = new HashMap();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        S3LoadData s3Data = new S3LoadData();
        locationMap.putAll(s3Data.loadReferenceLocationData());
    }

    @Override
    public void flatMap(Customer customer, Collector<Customer> collector) throws Exception {
        customer.setBuildingNo(locationMap.get(customer.getRole()).getBuildingNo());
        collector.collect(customer);
    }
}

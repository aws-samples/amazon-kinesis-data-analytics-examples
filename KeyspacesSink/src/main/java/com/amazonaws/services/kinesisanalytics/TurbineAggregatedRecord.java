package com.amazonaws.services.kinesisanalytics;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "sensor_data", name = "turbine_aggregated_sensor_data", readConsistency = "LOCAL_QUORUM", writeConsistency = "LOCAL_QUORUM")
public class TurbineAggregatedRecord {

    @Column(name = "turbineid")
    @PartitionKey(0)
    private String turbineid = "";

    @Column(name = "reported_time")
    private long reported_time = 0;

    @Column(name = "max_speed")
    private long max_speed = 0;

    @Column(name = "min_speed")
    private long min_speed = 0;

    @Column(name = "avg_speed")
    private long avg_speed = 0;

    public TurbineAggregatedRecord() {}

    public TurbineAggregatedRecord(String turbineid, long reported_time, long max_speed, long min_speed, long avg_speed) {
        this.setTurbineid(turbineid);
        this.setReported_time(reported_time);
        this.setMax_speed(max_speed);
        this.setMin_speed(min_speed);
        this.setAvg_speed(avg_speed);
    }

    public String getTurbineid() {
        return turbineid;
    }

    public long getReported_time() {
        return reported_time;
    }

    public long getMax_speed() {
        return max_speed;
    }

    public long getMin_speed() {
        return min_speed;
    }

    public long getAvg_speed() {
        return avg_speed;
    }

    public void setTurbineid(String turbineid) {
        this.turbineid = turbineid;
    }

    public void setReported_time(long reported_time) {
        this.reported_time = reported_time;
    }

    public void setMax_speed(long max_speed) {
        this.max_speed = max_speed;
    }

    public void setMin_speed(long min_speed) {
        this.min_speed = min_speed;
    }

    public void setAvg_speed(long avg_speed) {
        this.avg_speed = avg_speed;
    }

    @Override
    public String toString() {
        return getTurbineid() + " : " + getReported_time() + " : " +getMax_speed() +  " : " + getMin_speed() +  " : " + getAvg_speed();
    }
}


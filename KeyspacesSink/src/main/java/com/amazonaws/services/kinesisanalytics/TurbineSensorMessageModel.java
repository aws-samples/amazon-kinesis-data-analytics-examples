package com.amazonaws.services.kinesisanalytics;

public class TurbineSensorMessageModel {
    public String turbineId;
    public Long speed;

    public Long messageCount = 1L;
    public Long maxSpeed = 0L;
    public Long minSpeed = 0L;
    public Long sumSpeed = 0L;
}
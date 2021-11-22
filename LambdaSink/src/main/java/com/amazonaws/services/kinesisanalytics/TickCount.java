package com.amazonaws.services.kinesisanalytics;

public class TickCount {
    private String tick;
    private int count;

    public TickCount(String tick, int count) {
        this.tick = tick;
        this.count = count;
    }

    public String getTick() {
        return tick;
    }

    public void setTick(String tick) {
        this.tick = tick;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}

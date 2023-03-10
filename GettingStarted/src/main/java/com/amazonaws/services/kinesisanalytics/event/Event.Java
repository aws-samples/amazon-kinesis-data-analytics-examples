package com.amazonaws.services.kinesisanalytics.event;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Event {
    private String groupId;
    private String rowId;
    private int amount;

    public Event() {
    }

    public Event(String groupId, String rowId, int amount) {
        this.groupId = groupId;
        this.rowId = rowId;
        this.amount = amount;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getRowId() {
        return rowId;
    }

    public int getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

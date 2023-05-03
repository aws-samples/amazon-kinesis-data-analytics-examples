package com.amazonaws.pojo;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.dynamodbv2.model.Record;

public class Location extends Record {
    private String role;
    private String buildingNo;

    public Location(String role, String buildingNo) {
        this.role = role;
        this.buildingNo = buildingNo;
    }

    @JsonGetter("role")
    public String getRole() {
        return role;
    }

    @JsonSetter("role")
    public void setRole(String role) {
        this.role = role;
    }

    @JsonGetter("building_no")
    public String getBuildingNo() {
        return buildingNo;
    }

    @JsonSetter("building_no")
    public void setBuildingNo(String buildingNo) {
        this.buildingNo = buildingNo;
    }

    @Override
    public String toString() {
        return "Location{" +
                "role='" + role + '\'' +
                ", buildingNo='" + buildingNo + '\'' +
                '}';
    }
}

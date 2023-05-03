package com.amazonaws.pojo;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Customer {
    private String empId;
    private String empFirstName;
    private String empLastName;
    private String role;
    private String buildingNo;

    @JsonGetter("id")
    public String getEmpId() {
        return empId;
    }

    @JsonSetter("id")
    public void setEmpId(String empId) {
        this.empId = empId;
    }

    @JsonGetter("fname")
    public String getEmpFirstName() {
        return empFirstName;
    }

    @JsonSetter("fname")
    public void setEmpFirstName(String empFirstName) {
        this.empFirstName = empFirstName;
    }

    @JsonGetter("lname")
    public String getEmpLastName() {
        return empLastName;
    }

    @JsonSetter("lname")
    public void setEmpLastName(String empLastName) {
        this.empLastName = empLastName;
    }

    @JsonGetter("role")
    public String getRole() {
        return role;
    }

    @JsonSetter("role")
    public void setRole(String role) {
        this.role = role;
    }

    @JsonGetter("building")
    public String getBuildingNo() {
        return buildingNo;
    }

    @JsonSetter("building")
    public void setBuildingNo(String buildingNo) {
        this.buildingNo = buildingNo;
    }

    @Override
    public String toString() {
        return "CustomerEvent{" +
                "empId='" + empId + '\'' +
                ", empFirstName='" + empFirstName + '\'' +
                ", empLastName='" + empLastName + '\'' +
                ", role='" + role + '\'' +
                ", buildingNo='" + buildingNo + '\'' +
                '}';
    }
}

package com.yth.realtime.dto;


public class RegionAverageTemperature {
    private String id; // MongoDB의 _id 필드와 매핑
    private double avgTemperature;

    public RegionAverageTemperature() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }
}
package com.yth.realtime.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class InfluxDBMeasurement {
    private String measurement;
    private Map<String, String> tags = new HashMap<>();
    private Map<String, Object> fields = new HashMap<>();
    private Instant timestamp;

    public InfluxDBMeasurement() {
        this.timestamp = Instant.now();
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void addTag(String key, String value) {
        this.tags.put(key, value);
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void addField(String key, Object value) {
        this.fields.put(key, value);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
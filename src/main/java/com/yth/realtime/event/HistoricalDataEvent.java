package com.yth.realtime.event;

import java.util.Map;

import org.springframework.context.ApplicationEvent;

public class HistoricalDataEvent extends ApplicationEvent {
    private static final long serialVersionUID = 1L;
    private final String deviceId;
    private final Map<String, Object> data;

    public HistoricalDataEvent(Object source, String deviceId, Map<String, Object> data) {
        super(source);
        this.deviceId = deviceId;
        this.data = data;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
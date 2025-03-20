package com.yth.realtime.event;

import java.util.Map;

import org.springframework.context.ApplicationEvent;

public class OpcuaDataEvent extends ApplicationEvent {
    private final Map<String, Object> data;

    public OpcuaDataEvent(Object source, Map<String, Object> data) {
        super(source);
        this.data = data;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
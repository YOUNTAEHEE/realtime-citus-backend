package com.yth.realtime.client;
@FunctionalInterface
public interface ValueUpdateHandler {
    void handleValueUpdate(String group, String varName, Object value);
}
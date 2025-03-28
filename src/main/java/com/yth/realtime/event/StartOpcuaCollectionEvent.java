package com.yth.realtime.event;

public class StartOpcuaCollectionEvent {
    private final Object source;

    public StartOpcuaCollectionEvent(Object source) {
        this.source = source;
    }

    public Object getSource() {
        return source;
    }
}

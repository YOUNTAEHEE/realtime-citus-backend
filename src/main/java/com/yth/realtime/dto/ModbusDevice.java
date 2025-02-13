package com.yth.realtime.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ModbusDevice {
    private String deviceId;
    private String name;
    private String host;
    private int port;
    private int startAddress;
    private int length;
    private int slaveId;


    public ModbusDevice(String deviceId, String name, String host, int port, int startAddress, int length, int slaveId) {
        this.deviceId = deviceId;
        this.name = name;
        this.host = host;
        this.port = port;
        this.startAddress = startAddress;
        this.length = length;
        this.slaveId = slaveId;
    }
}
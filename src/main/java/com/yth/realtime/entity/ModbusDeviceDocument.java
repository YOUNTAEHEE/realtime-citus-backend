package com.yth.realtime.entity;

import java.time.LocalDateTime;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.yth.realtime.dto.ModbusDevice;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "modbus_devices")
public class ModbusDeviceDocument {
    @Id
    private String id;
    
    @Indexed(unique = true)  // deviceId를 유니크 인덱스로 설정
    private String deviceId;
    
    private String name;
    private String host;
    private int port;
    private int startAddress;
    private int length;
    private int slaveId;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private boolean active;  // 장치 활성화 상태

    // ModbusDevice -> ModbusDeviceDocument 변환
    public static ModbusDeviceDocument from(ModbusDevice device) {
        return ModbusDeviceDocument.builder()
                .deviceId(device.getDeviceId())
                .name(device.getName())
                .host(device.getHost())
                .port(device.getPort())
                .startAddress(device.getStartAddress())
                .length(device.getLength())
                .slaveId(device.getSlaveId())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .active(true)
                .build();   
    }

    // ModbusDeviceDocument -> ModbusDevice 변환
    public ModbusDevice toModbusDevice() {
        return new ModbusDevice(
                this.deviceId,
                this.name,
                this.host,
                this.port,
                this.startAddress,
                this.length,
                this.slaveId
        );
    }

    // 업데이트 시간 자동 설정
    public void updateTimestamp() {
        this.updatedAt = LocalDateTime.now();
    }
} 
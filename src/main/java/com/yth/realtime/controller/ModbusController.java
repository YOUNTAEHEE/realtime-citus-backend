package com.yth.realtime.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.service.ModbusService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/modbus")
@RequiredArgsConstructor
@CrossOrigin(origins = {"http://localhost:3000", "http://localhost:3001"})

public class ModbusController {
    private final ModbusService modbusService;
    private final WebSocketHandler webSocketHandler;
    
    @PostMapping("/device")
    public ResponseEntity<?> addDevice(@RequestBody ModbusDevice device) {
        try {
            if (modbusService.addDevice(device)) {
                // 장치 추가 성공하면 WebSocket 핸들러에 알림
                webSocketHandler.addDeviceToSession(device);
                return ResponseEntity.ok().body(Map.of(
                    "status", "success",
                    "deviceId", device.getDeviceId()
                ));
            }
            return ResponseEntity.badRequest().body(Map.of("error", "장치 연결 실패"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/device/{deviceId}")
    public ResponseEntity<?> deleteDevice(@PathVariable String deviceId) {
        try {
            if(modbusService.deleteDevice(deviceId)) {
                return ResponseEntity.ok().body(Map.of("status", "success"));
            } else {
                return ResponseEntity.badRequest().body(Map.of("error", "장치 삭제 실패"));
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/device/list")
    public ResponseEntity<?> deviceList() {
        try{
            List<ModbusDevice> deviceList = modbusService.deviceList();
            return ResponseEntity.ok().body(Map.of("devices", deviceList));
        }catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/device/check/{deviceId}")
    public ResponseEntity<?> checkDevice(@PathVariable String deviceId) {
        boolean exists = modbusService.deviceExists(deviceId);
        return exists ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }
}
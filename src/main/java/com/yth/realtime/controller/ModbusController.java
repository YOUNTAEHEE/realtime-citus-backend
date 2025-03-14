package com.yth.realtime.controller;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.dto.SettingsDTO;
import com.yth.realtime.service.ModbusService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/modbus")
@RequiredArgsConstructor
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001"})
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001", "https://realtime-citus-nagp.vercel.app" })
// @CrossOrigin(origins = "*", allowCredentials = "false")
public class ModbusController {
    private final ModbusService modbusService;
    private final WebSocketHandler webSocketHandler;
    private static final Logger log = LoggerFactory.getLogger(ModbusController.class);

    @PostMapping("/device")
    public ResponseEntity<?> addDevice(@RequestBody ModbusDevice device) {
        try {
            if (modbusService.addDevice(device)) {
                // 장치 추가 성공하면 WebSocket 핸들러에 알림
                webSocketHandler.addDeviceToSession(device);
                return ResponseEntity.ok().body(Map.of(
                        "status", "success",
                        "deviceId", device.getDeviceId()));
            }
            return ResponseEntity.badRequest().body(Map.of("message", "장치 연결 실패"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("message", e.getMessage()));
        }
    }

    // 장치 수정 API
    @PutMapping("/device/edit/{deviceId}")
    public ResponseEntity<?> editDevice(@PathVariable String deviceId, @RequestBody ModbusDevice device) {
        try {
            log.info("장치 업데이트 요청: {}", device);

            // 장치 존재 여부 확인
            if (!modbusService.deviceExists(device.getDeviceId())) {
                return ResponseEntity.notFound().build();
            }

            // 장치 업데이트
            modbusService.updateDevice(device);

            // 웹소켓 핸들러에게 장치 업데이트 알림
            webSocketHandler.addDeviceToSession(device);

            return ResponseEntity.ok().body(Map.of(
                    "status", "success",
                    "deviceId", device.getDeviceId()));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(
                    Map.of("message", "장치 업데이트 중 오류가 발생했습니다: " + e.getMessage()));
        }
    }

    @DeleteMapping("/device/{deviceId}")
    public ResponseEntity<?> deleteDevice(@PathVariable String deviceId) {
        try {
            if (modbusService.deleteDevice(deviceId)) {
                return ResponseEntity.ok().body(Map.of("status", "success"));
            } else {
                return ResponseEntity.badRequest().body(Map.of("message", "장치 삭제 실패"));
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @GetMapping("/device/list")
    public ResponseEntity<?> deviceList() {
        try {
            List<ModbusDevice> deviceList = modbusService.deviceList();
            return ResponseEntity.ok().body(Map.of("devices", deviceList));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @GetMapping("/device/check/{deviceId}")
    public ResponseEntity<?> checkDevice(@PathVariable String deviceId) {
        boolean exists = modbusService.deviceExists(deviceId);
        return exists ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @GetMapping("/device/{deviceId}")
    public ResponseEntity<?> getDevice(@PathVariable String deviceId) {
        ModbusDevice device = modbusService.getDevice(deviceId);
        return device != null ? ResponseEntity.ok(device) : ResponseEntity.notFound().build();
    }

    @PostMapping("/settings")
    public ResponseEntity<?> saveSettings(@RequestBody SettingsDTO settings) {
        try {

            modbusService.saveSettings(settings);
            return ResponseEntity.ok().body(Map.of("status", "success"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/get/settings")
    public ResponseEntity<?> getSettings() {
        System.out.println("getSettings 엔드포인트 호출됨");
        SettingsDTO settings = modbusService.getSettings();
        System.out.println("반환 데이터: " + settings);
        return ResponseEntity.ok().body(settings);
    }
}

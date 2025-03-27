package com.yth.realtime.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.dto.HistoricalDataRequest;
import com.yth.realtime.dto.HistoricalDataResponse;
import com.yth.realtime.service.OpcuaHistoricalService;
import com.yth.realtime.service.OpcuaService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/opcua")
@Slf4j
@RequiredArgsConstructor
public class OpcuaController {

    private final OpcuaService opcuaService;
    private final OpcuaWebSocketHandler webSocketHandler;
    private final OpcuaHistoricalService opcuaHistoricalService;

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("connected", opcuaService.isConnected());
        status.put("autoReconnect", opcuaService.isAutoReconnect());
        return ResponseEntity.ok(status);
    }

    @PostMapping("/connect")
    public ResponseEntity<Map<String, Object>> connect() {
        boolean success = opcuaService.connect();
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        response.put("message", success ? "OPC UA 서버에 연결되었습니다" : "OPC UA 서버 연결 실패");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/disconnect")
    public ResponseEntity<Map<String, Object>> disconnect() {
        opcuaService.disconnect();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "OPC UA 서버 연결이 해제되었습니다");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startDataCollection() {
        opcuaService.startDataCollection();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "OPC UA 데이터 수집이 시작되었습니다");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stopOpcuaService() {
        log.info("OPC UA 서비스 중지 요청 수신 - 웹소켓 연결만 해제");
        try {
            // 웹소켓 세션만 정리하고 데이터 수집은 계속 유지
            webSocketHandler.clearAllSessions();
            return ResponseEntity.ok("OPC UA 웹소켓 연결이 해제되었지만 데이터 수집은 계속됩니다.");
        } catch (Exception e) {
            log.error("OPC UA 웹소켓 연결 해제 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("웹소켓 연결 해제 실패: " + e.getMessage());
        }
    }

    @PostMapping("/stop-service")
    public ResponseEntity<Map<String, Object>> stopDataCollection() {
        opcuaService.stopDataCollection();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "OPC UA 데이터 수집이 중지되었습니다");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/data")
    public ResponseEntity<Map<String, Map<String, Object>>> getAllData() {
        Map<String, Map<String, Object>> data = opcuaService.getAllData();
        return ResponseEntity.ok(data);
    }

    @GetMapping("/data/{groupName}")
    public ResponseEntity<Map<String, Object>> getGroupData(@PathVariable String groupName) {
        Map<String, Object> data = opcuaService.getGroupData(groupName);
        return ResponseEntity.ok(data);
    }

    @PostMapping("/auto-reconnect/{enabled}")
    public ResponseEntity<Map<String, Object>> setAutoReconnect(@PathVariable boolean enabled) {
        opcuaService.setAutoReconnect(enabled);
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("autoReconnect", enabled);
        response.put("message", enabled ? "자동 재연결이 활성화되었습니다" : "자동 재연결이 비활성화되었습니다");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/historical")
    public ResponseEntity<HistoricalDataResponse> getHistoricalData(@RequestBody HistoricalDataRequest request) {
        log.warn("===== 과거 데이터 조회 요청 수신: {} =====", request);

        try {
            // 서비스 호출 직전 로그
            log.warn("서비스 호출 직전: {}", request.getDeviceGroup());
            HistoricalDataResponse response = opcuaHistoricalService.getHistoricalData(
                    request.getStartTime(),
                    request.getEndTime(),
                    request.getDeviceGroup());
            log.warn("서비스 호출 완료: 성공={}", response.isSuccess());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("과거 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().body(
                    HistoricalDataResponse.builder()
                            .success(false)
                            .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
                            .build());
        }
    }
}
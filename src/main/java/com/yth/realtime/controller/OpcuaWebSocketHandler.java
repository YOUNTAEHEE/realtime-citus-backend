package com.yth.realtime.controller;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yth.realtime.event.OpcuaDataEvent;
import com.yth.realtime.service.OpcuaInfluxDBService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OpcuaWebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(OpcuaWebSocketHandler.class);

    // 연결된 세션 목록 (스레드 안전한 리스트 사용)
    private final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();

    // 서비스 주입
    private final OpcuaInfluxDBService opcuaInfluxDBService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private ApplicationEventPublisher eventPublisher; // 이 부분이 제대로 주입되었는지 확인

    /**
     * 웹소켓 연결 수립 시 호출되는 메서드
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("OPC UA 웹소켓 연결 수립: {}", session.getId());
        sessions.add(session);

        try {
            // 연결 확인 메시지 전송
            Map<String, Object> response = new HashMap<>();
            response.put("type", "connection");
            response.put("status", "connected");
            response.put("sessionId", session.getId());
            response.put("timestamp", LocalDateTime.now().toString());
            response.put("message", "OPC UA 웹소켓 서버에 연결되었습니다");

            session.sendMessage(new TextMessage(objectMapper.writeValueAsString(response)));
        } catch (Exception e) {
            log.error("OPC UA 웹소켓 연결 응답 전송 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * 웹소켓 연결 종료 시 호출되는 메서드
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.info("OPC UA 웹소켓 연결 종료: {}, 상태: {}", session.getId(), status);
        sessions.remove(session);
    }

    /**
     * 클라이언트로부터 메시지 수신 시 호출되는 메서드
     */
    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            String payload = message.getPayload();
            log.debug("OPC UA 클라이언트로부터 메시지 수신: {}", payload);

            Map<String, Object> request = objectMapper.readValue(payload, Map.class);
            String type = (String) request.get("type");

            switch (type) {
                case "getHistoricalData":
                    handleHistoricalDataRequest(session, request);
                    break;

                case "getLiveData":
                    handleLiveDataRequest(session, request);
                    break;

                case "ping":
                    // 핑-퐁 메시지 처리
                    session.sendMessage(new TextMessage("{\"type\":\"pong\",\"timestamp\":\"" +
                            LocalDateTime.now().toString() + "\"}"));
                    break;

                default:
                    log.warn("알 수 없는 메시지 타입: {}", type);
                    session.sendMessage(new TextMessage("{\"type\":\"error\",\"message\":\"알 수 없는 메시지 타입입니다\"}"));
            }

        } catch (Exception e) {
            log.error("OPC UA 웹소켓 메시지 처리 중 오류: {}", e.getMessage(), e);
            try {
                session.sendMessage(new TextMessage("{\"type\":\"error\",\"message\":\"메시지 처리 중 오류가 발생했습니다\"}"));
            } catch (IOException ex) {
                log.error("오류 메시지 전송 실패: {}", ex.getMessage());
            }
        }
    }

    /**
     * 과거 데이터 요청 처리
     */
    private void handleHistoricalDataRequest(WebSocketSession session, Map<String, Object> request) throws IOException {
        String deviceGroup = (String) request.getOrDefault("deviceGroup", "all");
        Integer hours = (Integer) request.getOrDefault("hours", 1);

        log.info("과거 데이터 요청 처리: 장치그룹={}, 시간범위={}시간", deviceGroup, hours);

        // InfluxDB에서 과거 데이터 조회
        List<Map<String, Object>> historicalData = opcuaInfluxDBService.getRecentOpcuaData(deviceGroup, hours * 60);

        Map<String, Object> response = new HashMap<>();
        response.put("type", "historicalData");

        // 중요 변경: 클라이언트가 기대하는 구조로 변경
        Map<String, Object> dataContainer = new HashMap<>();
        dataContainer.put("timeSeries", historicalData); // 데이터를 timeSeries 키로 감싸기
        response.put("data", dataContainer);

        response.put("deviceGroup", deviceGroup);
        response.put("period", hours + "h");
        response.put("count", historicalData.size());

        session.sendMessage(new TextMessage(objectMapper.writeValueAsString(response)));
        log.info("과거 데이터 전송 완료: 장치={}, 기간={}시간, 데이터 수={}",
                deviceGroup, hours, historicalData.size());
    }

    /**
     * 실시간 데이터 요청 처리
     */
    private void handleLiveDataRequest(WebSocketSession session, Map<String, Object> request) throws IOException {
        String deviceGroup = (String) request.getOrDefault("deviceGroup", "all");

        log.info("실시간 데이터 요청 처리: 장치그룹={}", deviceGroup);

        // 최신 데이터 조회
        Map<String, Object> latestData = opcuaInfluxDBService.getLatestOpcuaData(deviceGroup);

        Map<String, Object> response = new HashMap<>();
        response.put("type", "liveData");
        response.put("deviceGroup", deviceGroup);
        response.put("timestamp", LocalDateTime.now().toString());
        response.put("data", latestData);

        session.sendMessage(new TextMessage(objectMapper.writeValueAsString(response)));
        log.debug("실시간 데이터 전송 완료: 장치={}", deviceGroup);
    }

    /**
     * 모든 연결된 클라이언트에 OPC UA 데이터 전송
     */
    public void sendOpcuaData(Map<String, Object> data) {
        if (sessions.isEmpty()) {
            return; // 연결된 세션이 없으면 전송하지 않음
        }

        try {
            // 데이터 JSON 직렬화
            String jsonData = objectMapper.writeValueAsString(data);
            TextMessage message = new TextMessage(jsonData);

            // 열린 세션만 필터링하여 메시지 전송
            List<WebSocketSession> invalidSessions = new ArrayList<>();
            int successCount = 0;

            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    try {
                        session.sendMessage(message);
                        successCount++;
                    } catch (Exception e) {
                        log.error("OPC UA 데이터 전송 실패 (세션 ID: {}): {}", session.getId(), e.getMessage());
                        invalidSessions.add(session);
                    }
                } else {
                    invalidSessions.add(session);
                }
            }

            // 닫힌 세션 제거
            sessions.removeAll(invalidSessions);

            if (successCount > 0 && log.isDebugEnabled()) {
                log.debug("OPC UA 데이터 전송 성공: {}개 세션", successCount);
            }

        } catch (Exception e) {
            log.error("OPC UA 데이터 전송 중 오류: {}", e.getMessage(), e);
        }
    }

    /**
     * 모든 연결된 클라이언트에 OPC UA 과거 데이터 전송
     */
    public void sendOpcuaHistoricalData(Map<String, Object> data) {
        if (sessions.isEmpty()) {
            return;
        }

        try {
            String jsonData = objectMapper.writeValueAsString(data);
            TextMessage message = new TextMessage(jsonData);

            List<WebSocketSession> invalidSessions = new ArrayList<>();
            int successCount = 0;

            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    try {
                        session.sendMessage(message);
                        successCount++;
                    } catch (Exception e) {
                        log.error("OPC UA 과거 데이터 전송 실패 (세션 ID: {}): {}", session.getId(), e.getMessage());
                        invalidSessions.add(session);
                    }
                } else {
                    invalidSessions.add(session);
                }
            }

            // 닫힌 세션 제거
            sessions.removeAll(invalidSessions);

            if (successCount > 0) {
                log.info("OPC UA 과거 데이터 전송 성공: {}개 세션", successCount);
            }

        } catch (Exception e) {
            log.error("OPC UA 과거 데이터 전송 중 오류: {}", e.getMessage(), e);
        }
    }

    /**
     * 웹소켓 에러 처리
     */
    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.error("OPC UA 웹소켓 전송 오류 (세션 ID: {}): {}", session.getId(), exception.getMessage());
    }

    /**
     * 현재 연결된 세션 수 반환
     */
    public int getSessionCount() {
        return sessions.size();
    }

    @EventListener
    public void handleOpcuaDataEvent(OpcuaDataEvent event) {
        // 이 메서드가 호출되는지 로그로 확인
        System.out.println("이벤트 수신: " + event.getData());
        Map<String, Object> data = event.getData();
        sendOpcuaData(data);
    }

    // 이벤트 발행 부분
    private void processOpcuaData(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
        // ...처리 로직...

        Map<String, Object> message = new HashMap<>();
        message.put("type", "opcua");
        message.put("timestamp", timestamp.toString());
        message.put("data", allData);

        // 이 부분이 제대로 호출되는지 로그로 확인
        System.out.println("이벤트 발행: " + message);
        eventPublisher.publishEvent(new OpcuaDataEvent(this, message));
    }

    /**
     * 모든 OPC UA 웹소켓 세션을 정리하는 메서드
     * OPC UA 연결이나 데이터 수집은 유지하고 웹소켓 연결만 해제합니다.
     */
    public void clearAllSessions() {
        log.info("모든 OPC UA 웹소켓 세션 정리 - 연결된 세션 수: {}", sessions.size());

        for (WebSocketSession session : new ArrayList<>(sessions)) {
            try {
                if (session.isOpen()) {
                    session.close();
                    log.info("OPC UA 웹소켓 세션 정상 종료: {}", session.getId());
                }
            } catch (Exception e) {
                log.error("OPC UA 웹소켓 세션 종료 실패: {}", e.getMessage());
            }
        }

        sessions.clear();
        log.info("모든 OPC UA 웹소켓 세션 정리 완료");
    }
}
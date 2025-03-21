//실시간 데이터 조회(디비는 저장만)
// package com.yth.realtime.controller;

// import java.util.ArrayList;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.CopyOnWriteArrayList;
// import java.util.concurrent.Executors;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.ScheduledFuture;
// import java.util.concurrent.TimeUnit;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.stereotype.Component;
// import org.springframework.web.socket.TextMessage;
// import org.springframework.web.socket.WebSocketSession;
// import org.springframework.web.socket.handler.TextWebSocketHandler;

// import com.yth.realtime.dto.ModbusDevice;
// import com.yth.realtime.service.ModbusService;

// import lombok.RequiredArgsConstructor;

// @Component
// @RequiredArgsConstructor
// public class WebSocketHandler extends TextWebSocketHandler {
//     private static final Logger log = LoggerFactory.getLogger(WebSocketHandler.class);
//     private static final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
//     private final ModbusService modbusService;
//     private final Map<String, ModbusDevice> sessionDevices = new ConcurrentHashMap<>();
//     private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//     private final Map<String, ScheduledFuture<?>> deviceSchedulers = new ConcurrentHashMap<>();
//     private final Map<String, List<WebSocketSession>> deviceSessions = new ConcurrentHashMap<>();

//     @Override
//     public void afterConnectionEstablished(WebSocketSession session) {
//         try {
//             log.info("새로운 WebSocket 연결 수립: {}", session.getId());
//             sessions.add(session);
//             session.sendMessage(new TextMessage("{\"status\": \"connected\", \"sessionId\": \"" +
//                     session.getId() + "\"}"));
//         } catch (Exception e) {
//             log.error("WebSocket 연결 중 오류 발생: {}", e.getMessage(), e);
//         }
//     }

//     // 컨트롤러에서 호출할 메서드
//     public void addDeviceToSession(ModbusDevice device) {
//         sessionDevices.put(device.getDeviceId(), device);

//         // 해당 장치에 대한 세션 목록이 없으면 생성
//         deviceSessions.computeIfAbsent(device.getDeviceId(), k -> new CopyOnWriteArrayList<>());

//         // 첫 번째 세션인 경우에만 스케줄러 시작
//         boolean isFirstSession = deviceSessions.get(device.getDeviceId()).isEmpty();

//         // 모든 세션에 장치 등록
//         sessions.forEach(session -> {
//             deviceSessions.get(device.getDeviceId()).add(session);
//         });

//         // 첫 세션인 경우에만 스케줄러 시작
//         if (isFirstSession) {
//             startDeviceDataCollection(device);
//         }
//     }

//     // 장치별 스케줄러 중지 메서드 추가
//     public void stopDeviceScheduler(String deviceId) {
//         ScheduledFuture<?> future = deviceSchedulers.remove(deviceId);
//         if (future != null && !future.isDone()) {
//             future.cancel(false);
//             log.info("장치 스케줄러 중지: {}", deviceId);
//         }
//     }

//     // 장치별 데이터 수집 스케줄러 (세션과 분리)
//     private void startDeviceDataCollection(ModbusDevice device) {
//         // 기존 스케줄러 중지
//         stopDeviceScheduler(device.getDeviceId());

//         ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
//             try {
//                 // 데이터 읽기
//                 int[] data = modbusService.readModbusData(device);

//                 // 모든 연결된 세션에 데이터 전송
//                 String jsonData = String.format(
//                         "{\"deviceId\": \"%s\", \"temperature\": %.1f, \"humidity\": %.1f}",
//                         device.getDeviceId(), data[0] / 10.0, data[1] / 10.0);

//                 List<WebSocketSession> sessions = deviceSessions.get(device.getDeviceId());
//                 if (sessions != null) {
//                     List<WebSocketSession> invalidSessions = new ArrayList<>();

//                     for (WebSocketSession session : sessions) {
//                         if (session.isOpen()) {
//                             session.sendMessage(new TextMessage(jsonData));
//                         } else {
//                             invalidSessions.add(session);
//                         }
//                     }

//                     // 닫힌 세션 제거
//                     sessions.removeAll(invalidSessions);
//                 }
//             } catch (Exception e) {
//                 log.error("데이터 전송 실패: {}", e.getMessage());
//             }
//         }, 0, 1, TimeUnit.SECONDS); // 1초마다 데이터 전송

//         // 스케줄러 저장
//         deviceSchedulers.put(device.getDeviceId(), future);
//     }
// }

//실시간 데이터 조회(디비에서 조회)
package com.yth.realtime.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.event.HistoricalDataEvent;
import com.yth.realtime.service.InfluxDBService;
import com.yth.realtime.service.ModbusService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class WebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(WebSocketHandler.class);
    private static final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ModbusService modbusService;
    private final InfluxDBService influxDBService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, ModbusDevice> sessionDevices = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Map<String, ScheduledFuture<?>> deviceSchedulers = new ConcurrentHashMap<>();
    private final Map<String, List<WebSocketSession>> deviceSessions = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        try {
            log.info("새로운 WebSocket 연결 수립: {}", session.getId());
            sessions.add(session);
            session.sendMessage(new TextMessage("{\"status\": \"connected\", \"sessionId\": \"" +
                    session.getId() + "\"}"));
        } catch (Exception e) {
            log.error("WebSocket 연결 중 오류 발생: {}", e.getMessage(), e);
        }
    }

    // 컨트롤러에서 호출할 메서드
    public void addDeviceToSession(ModbusDevice device) {
        sessionDevices.put(device.getDeviceId(), device);

        // 해당 장치에 대한 세션 목록이 없으면 생성
        deviceSessions.computeIfAbsent(device.getDeviceId(), k -> new CopyOnWriteArrayList<>());

        // 첫 번째 세션인 경우에만 스케줄러 시작
        boolean isFirstSession = deviceSessions.get(device.getDeviceId()).isEmpty();

        // 모든 세션에 장치 등록
        sessions.forEach(session -> {
            deviceSessions.get(device.getDeviceId()).add(session);
        });

        // 첫 세션인 경우에만 스케줄러 시작
        if (isFirstSession) {
            startDeviceDataCollection(device);
        }
    }

    // 장치별 스케줄러 중지 메서드 추가
    public void stopDeviceScheduler(String deviceId) {
        ScheduledFuture<?> future = deviceSchedulers.remove(deviceId);
        if (future != null && !future.isDone()) {
            future.cancel(false);
            log.info("장치 스케줄러 중지: {}", deviceId);
        }
    }

    /**
     * 특정 장치의 24시간 이전 데이터를 연결된 클라이언트들에게 전송
     */
    @EventListener
    public void handleHistoricalDataEvent(HistoricalDataEvent event) {
        try {
            String deviceId = event.getDeviceId();
            Map<String, Object> message = event.getData();

            log.info("24시간 이전 데이터 이벤트 수신: 장치ID={}, 데이터 크기={}",
                    deviceId, message.get("data") instanceof List ? ((List) message.get("data")).size() : "N/A");

            // 현재 열린 세션 수 확인
            log.info("현재 열린 WebSocket 세션 수: {}", sessions.size());

            // 메시지를 JSON으로 변환
            String messageJson;
            try {
                messageJson = objectMapper.writeValueAsString(message);
                log.info("JSON 변환 성공, 메시지 길이: {} 바이트", messageJson.getBytes().length);
            } catch (Exception e) {
                log.error("JSON 변환 실패: {}", e.getMessage(), e);
                return;
            }

            TextMessage textMessage = new TextMessage(messageJson);

            int successCount = 0;
            // 모든 세션에 데이터 전송
            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    try {
                        session.sendMessage(textMessage);
                        successCount++;
                    } catch (Exception e) {
                        log.error("세션({})에 메시지 전송 실패: {}", session.getId(), e.getMessage());
                    }
                } else {
                    log.warn("세션({})이 닫혀 있습니다. 메시지를 전송하지 않습니다.", session.getId());
                }
            }

            if (successCount > 0) {
                log.info("24시간 이전 데이터 전송 성공: {} (전송된 세션: {}개)", deviceId, successCount);
            } else {
                log.warn("24시간 이전 데이터 전송 실패: {}. 열린 세션이 없습니다.", deviceId);
            }
        } catch (Exception e) {
            log.error("24시간 이전 데이터 이벤트 처리 중 오류: {}", e.getMessage(), e);
        }
    }

    // 장치별 데이터 수집 스케줄러 (InfluxDB에서 데이터 조회)
    private void startDeviceDataCollection(ModbusDevice device) {
        // 기존 스케줄러 중지
        stopDeviceScheduler(device.getDeviceId());

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                // 모드버스에서 데이터 읽기 (InfluxDB에 저장됨)
                modbusService.readModbusData(device);

                // InfluxDB에서 최신 데이터 조회
                Map<String, Object> latestData = influxDBService.getLatestSensorData(device.getDeviceId());
                log.info("장치 {}: 최신 데이터 조회 결과 - {}", device.getDeviceId(), latestData);

                // 데이터 포맷팅
                String jsonData = objectMapper.writeValueAsString(Map.of(
                        "deviceId", device.getDeviceId(),
                        "temperature", latestData.get("temperature"),
                        "humidity", latestData.get("humidity"),
                        "timestamp", latestData.get("timestamp")));
                log.info("웹소켓으로 전송할 데이터: {}", jsonData);

                // 모든 연결된 세션에 데이터 전송
                List<WebSocketSession> sessions = deviceSessions.get(device.getDeviceId());
                if (sessions != null) {
                    List<WebSocketSession> invalidSessions = new ArrayList<>();

                    for (WebSocketSession session : sessions) {
                        if (session.isOpen()) {
                            session.sendMessage(new TextMessage(jsonData));
                        } else {
                            invalidSessions.add(session);
                        }
                    }

                    // 닫힌 세션 제거
                    sessions.removeAll(invalidSessions);
                }
            } catch (Exception e) {
                log.error("데이터 전송 실패: {}", e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS); // 1초마다 데이터 전송

        // 스케줄러 저장
        deviceSchedulers.put(device.getDeviceId(), future);
    }

    // /**
    // * OPC UA 데이터를 모든 웹소켓 세션에 전송
    // */
    // public void sendOpcuaData(Map<String, Object> data) {
    // if (sessions.isEmpty()) {
    // log.warn("활성화된 WebSocket 세션이 없습니다");
    // return;
    // }

    // try {
    // String jsonData = objectMapper.writeValueAsString(data);
    // TextMessage message = new TextMessage(jsonData);

    // int successCount = 0;
    // for (WebSocketSession session : sessions) {
    // if (session.isOpen()) {
    // try {
    // session.sendMessage(message);
    // successCount++;
    // } catch (Exception e) {
    // log.error("세션({})에 OPC UA 데이터 전송 실패: {}", session.getId(), e.getMessage());
    // }
    // }
    // }

    // if (successCount > 0) {
    // log.debug("OPC UA 데이터 전송 성공: {}개 세션", successCount);
    // }
    // } catch (Exception e) {
    // log.error("OPC UA 데이터 JSON 변환 실패: {}", e.getMessage(), e);
    // }
    // }
}
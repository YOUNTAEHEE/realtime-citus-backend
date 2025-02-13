package com.yth.realtime.controller;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.yth.realtime.dto.ModbusDevice;
import com.yth.realtime.service.ModbusService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class WebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(WebSocketHandler.class);
    private static final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ModbusService modbusService;
    private final Map<String, ModbusDevice> sessionDevices = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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
        sessions.forEach(session -> {
            try {
                startSendingData(session, device);
            } catch (Exception e) {
                log.error("데이터 전송 시작 실패: {}", e.getMessage(), e);
            }
        });
    }

    private void startSendingData(WebSocketSession session, ModbusDevice device) {
        scheduler.scheduleAtFixedRate(() -> {
            if (!session.isOpen()) {
                return;
            }

            try {
                int[] data = modbusService.readModbusData(device);
                
                String jsonData = String.format(
                    "{\"deviceId\": \"%s\", \"temperature\": %.1f, \"humidity\": %.1f}",
                    device.getDeviceId(), data[0] / 10.0, data[1] / 10.0
                );
                session.sendMessage(new TextMessage(jsonData));
                
            } catch (Exception e) {
                log.error("데이터 전송 실패: {}", e.getMessage());
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
}
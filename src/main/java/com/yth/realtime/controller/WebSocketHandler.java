package com.yth.realtime.controller;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.yth.realtime.service.ModbusService;

@Component
public class WebSocketHandler extends TextWebSocketHandler {
    private static final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ModbusService modbusService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static final Logger log = LoggerFactory.getLogger(WebSocketHandler.class);

    public WebSocketHandler(ModbusService modbusService) {
        this.modbusService = modbusService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        startSendingData(session);
    }

    private void startSendingData(WebSocketSession session) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (session.isOpen()) {
                    int[] data = modbusService.readModbusData();
                    String jsonData = String.format("{\"temperature\": %d, \"humidity\": %d}", 
                        data[0], data[1]);
                    session.sendMessage(new TextMessage(jsonData));
                }
            } catch (Exception e) {
                log.error("데이터 전송 실패: {}", e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
    }

    public void broadcastData() {
        int[] data = modbusService.readModbusData();
        String response = "{\"temperature\": " + data[0] + ", \"humidity\": " + data[1] + "}";

        for (WebSocketSession session : sessions) {
            try {
                session.sendMessage(new TextMessage(response));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

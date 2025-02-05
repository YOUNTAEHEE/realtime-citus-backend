// 이 서비스는 제거해야 합니다
package com.yth.realtime.service;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.yth.realtime.controller.WebSocketHandler;

@Service
@EnableScheduling
public class ModbusPollingService {
    private final WebSocketHandler webSocketHandler;

    public ModbusPollingService(WebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }

    @Scheduled(fixedRate = 1000) // 중복 실행의 원인
    public void pollAndBroadcast() {
        webSocketHandler.broadcastData();
    }
}

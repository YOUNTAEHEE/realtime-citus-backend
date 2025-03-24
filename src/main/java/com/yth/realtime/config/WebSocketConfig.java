package com.yth.realtime.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import com.yth.realtime.controller.OpcuaWebSocketHandler;
import com.yth.realtime.controller.WebSocketHandler;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final WebSocketHandler webSocketHandler;
    private final OpcuaWebSocketHandler opcuaWebSocketHandler;
    private static final Logger log = LoggerFactory.getLogger(WebSocketConfig.class);

    public WebSocketConfig(WebSocketHandler webSocketHandler, OpcuaWebSocketHandler opcuaWebSocketHandler) {
        this.webSocketHandler = webSocketHandler;
        this.opcuaWebSocketHandler = opcuaWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        log.info("웹소켓 핸들러 등록 시작");

        registry.addHandler(webSocketHandler, "/ws/modbus")
                .setAllowedOrigins("*");
        log.info("Modbus 웹소켓 핸들러 등록 완료: /ws/modbus");

        registry.addHandler(opcuaWebSocketHandler, "/ws/opcua")
                .setAllowedOrigins("*");
        log.info("OPC UA 웹소켓 핸들러 등록 완료: /ws/opcua");
    }
}

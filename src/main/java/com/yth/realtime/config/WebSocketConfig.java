package com.yth.realtime.config;

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

    public WebSocketConfig(WebSocketHandler webSocketHandler, OpcuaWebSocketHandler opcuaWebSocketHandler) {
        this.webSocketHandler = webSocketHandler;
        this.opcuaWebSocketHandler = opcuaWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(webSocketHandler, "/ws/modbus")
                .setAllowedOrigins("*");

        registry.addHandler(opcuaWebSocketHandler, "/ws/opcua")
                .setAllowedOrigins("*");
    }
}

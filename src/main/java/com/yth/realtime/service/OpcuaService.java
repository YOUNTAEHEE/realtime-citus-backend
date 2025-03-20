package com.yth.realtime.service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.yth.realtime.controller.WebSocketHandler;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class OpcuaService {
    private static final Logger log = LoggerFactory.getLogger(OpcuaService.class);

    private final OpcuaClient opcuaClient;
    private final WebSocketHandler webSocketHandler;
    private final InfluxDBService influxDBService;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> dataCollectionTask;
    private boolean autoReconnect = true;

    @Autowired
    public OpcuaService(OpcuaClient opcuaClient, WebSocketHandler webSocketHandler, InfluxDBService influxDBService) {
        this.opcuaClient = opcuaClient;
        this.webSocketHandler = webSocketHandler;
        this.influxDBService = influxDBService;
    }

    /**
     * 서비스 시작 시 OPC UA 서버에 연결 시도
     */
    @PostConstruct
    public void init() {
        connect();
        startDataCollection();
    }

    /**
     * 서비스 종료 시 OPC UA 연결 해제
     */
    @PreDestroy
    public void cleanup() {
        stopDataCollection();
        disconnect();
    }

    /**
     * OPC UA 서버 연결
     */
    public boolean connect() {
        return opcuaClient.connect();
    }

    /**
     * OPC UA 서버 연결 해제
     */
    public void disconnect() {
        opcuaClient.disconnect();
    }

    /**
     * 데이터 수집 시작
     */
    public void startDataCollection() {
        // 이미 실행 중인 작업이 있다면 중지
        stopDataCollection();

        // 500ms 간격으로 데이터 수집 스케줄링
        dataCollectionTask = scheduler.scheduleAtFixedRate(() -> {
            try {
                // OPC UA 서버 연결 상태 확인
                if (!opcuaClient.isConnected()) {
                    log.warn("OPC UA 서버 연결이 끊겼습니다. 재연결 시도...");
                    if (autoReconnect) {
                        opcuaClient.connect();
                    }
                    return;
                }

                // 모든 그룹의 데이터 수집
                Map<String, Map<String, Object>> allData = opcuaClient.readAllValues();
                LocalDateTime timestamp = LocalDateTime.now();

                // 수집한 데이터 처리
                processOpcuaData(allData, timestamp);

            } catch (Exception e) {
                log.error("OPC UA 데이터 수집 중 오류: {}", e.getMessage(), e);
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        log.info("OPC UA 데이터 수집 시작됨 (500ms 간격)");
    }

    /**
     * 데이터 수집 중지
     */
    public void stopDataCollection() {
        if (dataCollectionTask != null && !dataCollectionTask.isDone()) {
            dataCollectionTask.cancel(false);
            log.info("OPC UA 데이터 수집 중지됨");
        }
    }

    /**
     * 수집된 OPC UA 데이터 처리
     */
    private void processOpcuaData(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
        // 웹소켓으로 데이터 전송
        try {
            Map<String, Object> wsMessage = new HashMap<>();
            wsMessage.put("type", "opcua");
            wsMessage.put("timestamp", timestamp.toString());
            wsMessage.put("data", allData);

            webSocketHandler.sendOpcuaData(wsMessage);
        } catch (Exception e) {
            log.error("웹소켓 데이터 전송 실패: {}", e.getMessage(), e);
        }

        // InfluxDB에 데이터 저장 (필요한 경우)
        try {
            // 여기에 InfluxDB 저장 로직 추가
            // 예: influxDBService.saveOpcuaData(allData, timestamp);
        } catch (Exception e) {
            log.error("InfluxDB 데이터 저장 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * 특정 그룹의 데이터 조회
     */
    public Map<String, Object> getGroupData(String groupName) {
        return opcuaClient.readGroupValues(groupName);
    }

    /**
     * 모든 그룹의 데이터 조회
     */
    public Map<String, Map<String, Object>> getAllData() {
        return opcuaClient.readAllValues();
    }

    /**
     * 연결 상태 확인
     */
    public boolean isConnected() {
        return opcuaClient.isConnected();
    }

    /**
     * 자동 재연결 설정
     */
    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    /**
     * 자동 재연결 상태 확인
     */
    public boolean isAutoReconnect() {
        return autoReconnect;
    }
}
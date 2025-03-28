package com.yth.realtime.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.yth.realtime.controller.OpcuaWebSocketHandler;
import com.yth.realtime.event.OpcuaDataEvent;
import com.yth.realtime.event.StartOpcuaCollectionEvent;

import jakarta.annotation.PreDestroy;

@Service
public class OpcuaService {
    private static final Logger log = LoggerFactory.getLogger(OpcuaService.class);

    private final OpcuaClient opcuaClient;
    private final OpcuaWebSocketHandler webSocketHandler;
    private final OpcuaInfluxDBService influxDBService;
    private final ApplicationEventPublisher eventPublisher;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> dataCollectionTask;
    private boolean autoReconnect = true;

    // private final ExecutorService dbSaveExecutor =
    // Executors.newFixedThreadPool(5);
    // private final ExecutorService dbQueryExecutor =
    // Executors.newFixedThreadPool(5);
    // 현재 CPU 코어 수 기반으로 워크 스틸링 풀 생성
    ExecutorService dbSaveExecutor = Executors.newWorkStealingPool();
    ExecutorService dbQueryExecutor = Executors.newWorkStealingPool();

    @Autowired
    public OpcuaService(OpcuaClient opcuaClient, OpcuaWebSocketHandler opcuaWebSocketHandler,
            OpcuaInfluxDBService opcuaInfluxDBService, ApplicationEventPublisher eventPublisher) {
        this.opcuaClient = opcuaClient;
        this.webSocketHandler = opcuaWebSocketHandler;
        this.influxDBService = opcuaInfluxDBService;
        this.eventPublisher = eventPublisher;
    }

    /**
     * 서비스 종료 시 OPC UA 연결 해제
     */
    @PreDestroy
    public void cleanup() {
        stopDataCollection();
        disconnect();
        // 3. WebSocket 세션 정리 (추가)
        webSocketHandler.clearAllSessions();
        // 스레드풀 정리
        dbSaveExecutor.shutdown();
        dbQueryExecutor.shutdown();

        try {
            if (!dbSaveExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                dbSaveExecutor.shutdownNow();
            }
            if (!dbQueryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                dbQueryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            dbSaveExecutor.shutdownNow();
            dbQueryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
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

        // 5ms 간격으로 데이터 수집 스케줄링
        dataCollectionTask = scheduler.scheduleAtFixedRate(() -> {
            long startTime = System.currentTimeMillis();
            try {
                // OPC UA 서버 연결 상태 확인
                if (!opcuaClient.isConnected()) {
                    log.warn("OPC UA 서버 연결이 끊겼습니다. 재연결 시도...");
                    if (autoReconnect) {
                        opcuaClient.connect();
                    }
                    return;
                }

                // 데이터 수집 시간 측정
                long collectionStart = System.currentTimeMillis();
                Map<String, Map<String, Object>> allData = opcuaClient.readAllValues();
                long collectionTime = System.currentTimeMillis() - collectionStart;

                LocalDateTime timestamp = LocalDateTime.now();

                // 1. DB 저장 스레드
                final Map<String, Map<String, Object>> dataCopy = new HashMap<>(allData);
                dbSaveExecutor.submit(() -> {
                    try {
                        long saveStart = System.currentTimeMillis();
                        saveToInfluxDB(dataCopy, timestamp);
                        long saveTime = System.currentTimeMillis() - saveStart;
                        log.debug("DB 저장 완료: 시간={}, 소요시간={}ms", timestamp, saveTime);
                    } catch (Exception e) {
                        log.error("DB 저장 오류: {}", e.getMessage());
                    }
                });

                // 2. 별도로 DB 조회 스레드
                dbQueryExecutor.submit(() -> {
                    try {
                        // DB 조회 시간 측정
                        long queryStart = System.currentTimeMillis();
                        Map<String, Object> latestData = influxDBService.getLatestOpcuaData("all");
                        long queryTime = System.currentTimeMillis() - queryStart;

                        // 조회한 데이터를 프론트엔드로 전송
                        sendDataToFrontend(latestData);

                        log.debug("DB 조회 및 전송 완료: 조회시간={}ms", queryTime);
                    } catch (Exception e) {
                        log.error("DB 조회 오류: {}", e.getMessage());
                    }
                });

                // 전체 작업 소요 시간 측정
                long totalTime = System.currentTimeMillis() - startTime;
                if (totalTime > 10) { // 10ms 이상 걸리면 로깅
                    log.warn("데이터 수집 작업 지연: {}ms (설정: 5ms) - 수집:{}ms",
                            totalTime, collectionTime);
                }

            } catch (Exception e) {
                log.error("OPC UA 데이터 수집 중 오류: {}", e.getMessage());
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        log.info("OPC UA 데이터 수집 시작됨 (5ms 간격)");
    }
    // 디비저장 조회 아님//구독
    // public void startSubscriptionBasedCollection() {
    // Map<String, Map<String, Object>> currentData = new HashMap<>();

    // opcuaClient.startSubscription((group, varName, value) -> {
    // synchronized (currentData) {
    // // 1. 기존 Map 업데이트
    // currentData.computeIfAbsent(group, g -> new HashMap<>()).put(varName, value);

    // // 2. 타임스탬프 생성
    // LocalDateTime now = LocalDateTime.now();

    // // 3. InfluxDB 저장 (복사본 사용)
    // Map<String, Map<String, Object>> dataForSave = new HashMap<>(currentData);
    // dbSaveExecutor.submit(() -> saveToInfluxDB(dataForSave, now));

    // // 4. WebSocket 전송 (복사본 사용)
    // Map<String, Object> flatData = flattenData(dataForSave);
    // dbQueryExecutor.submit(() -> sendDataToFrontend(flatData));

    // log.debug("구독 수신 → 저장 및 전송: {}.{}", group, varName);
    // }
    // });

    // log.info("Subscription 기반 데이터 수집 시작됨");
    // }

    // 디비저장조회//구독
    // public void startSubscriptionBasedCollection() {
    // opcuaClient.startSubscription((group, varName, value) -> {
    // LocalDateTime now = LocalDateTime.now();

    // // 수신된 값 하나로 Map 구성
    // Map<String, Map<String, Object>> groupWrapper = new HashMap<>();
    // Map<String, Object> variableMap = new HashMap<>();
    // variableMap.put(varName, value);
    // groupWrapper.put(group, variableMap);

    // // 1. InfluxDB 저장
    // dbSaveExecutor.submit(() -> {
    // try {
    // saveToInfluxDB(groupWrapper, now);
    // log.debug("변수 저장 완료 → {}", varName);

    // // 2. 최신 데이터 InfluxDB에서 조회
    // Map<String, Object> latestData = influxDBService.getLatestOpcuaData("all");

    // // 3. WebSocket 전송
    // sendDataToFrontend(latestData);
    // log.debug("프론트에 최신 데이터 전송 완료");

    // } catch (Exception e) {
    // log.error("구독 데이터 저장/전송 중 오류: {}", e.getMessage(), e);
    // }
    // });
    // });

    // log.info("Subscription + 조회 기반 데이터 전송 시작됨");
    // }

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
     * InfluxDB에 데이터 저장
     */
    private void saveToInfluxDB(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
        try {
            log.info("OPC UA 데이터 저장 시작: 시간={}, 그룹 수={}", timestamp, allData.size());

            // 데이터를 평탄화
            Map<String, Object> flattenedData = flattenData(allData);

            if (flattenedData.isEmpty()) {
                log.warn("저장할 데이터가 없습니다");
                return;
            }

            // InfluxDBMeasurement 사용 대신 Point 직접 사용
            Point dataPoint = Point.measurement("opcua_data")
                    .addTag("system", "PCS_System");

            // 필드 추가
            for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();

                // 필드명 정리
                fieldName = fieldName.replaceAll("[^a-zA-Z0-9_]", "_");

                if (value == null)
                    continue;

                if (value instanceof Number) {
                    if (value instanceof Double) {
                        Double doubleValue = (Double) value;
                        if (!Double.isNaN(doubleValue) && !Double.isInfinite(doubleValue)) {
                            dataPoint.addField(fieldName, doubleValue);
                        }
                    } else if (value instanceof Integer) {
                        dataPoint.addField(fieldName, (Integer) value);
                    } else if (value instanceof Long) {
                        dataPoint.addField(fieldName, (Long) value);
                    } else if (value instanceof Float) {
                        dataPoint.addField(fieldName, (Float) value);
                    }
                } else if (value instanceof String) {
                    String strValue = (String) value;
                    if (!strValue.isEmpty()) {
                        try {
                            double numValue = Double.parseDouble(strValue);
                            dataPoint.addField(fieldName, numValue);
                        } catch (NumberFormatException e) {
                            dataPoint.addField(fieldName, strValue);
                        }
                    }
                }
            }

            // 타임스탬프 설정
            // Instant saveTime = Instant.now(); // 현재 시간 사용
            // LocalDateTime → Instant로 변환해서 저장
            Instant saveTime = timestamp.atZone(ZoneId.systemDefault()).toInstant();
            dataPoint.time(saveTime, WritePrecision.NS);

            // 저장
            WriteApiBlocking writeApi = influxDBService.getWriteApi();
            writeApi.writePoint(influxDBService.getBucket(), influxDBService.getOrg(), dataPoint);

            log.info("OPC UA 데이터 직접 저장 완료: 필드 수={}", flattenedData.size());

        } catch (Exception e) {
            log.error("InfluxDB 데이터 저장 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * 중첩된 맵 구조 평탄화
     */
    private Map<String, Object> flattenData(Map<String, Map<String, Object>> nestedData) {
        Map<String, Object> flattenedData = new HashMap<>();

        for (Map.Entry<String, Map<String, Object>> groupEntry : nestedData.entrySet()) {
            String groupName = groupEntry.getKey();
            Map<String, Object> groupData = groupEntry.getValue();

            for (Map.Entry<String, Object> fieldEntry : groupData.entrySet()) {
                String fieldName = fieldEntry.getKey();
                Object fieldValue = fieldEntry.getValue();

                // 그룹_필드명 형식으로 키 생성 (예: PCS1_SOC)
                String flatKey = fieldName;
                flattenedData.put(flatKey, fieldValue);
            }
        }

        return flattenedData;
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

    @EventListener
    public void onStartCollection(StartOpcuaCollectionEvent event) {
        // log.info("StartOpcuaCollectionEvent 수신 → connect + startDataCollection 실행");
        connect();
        startDataCollection();
        // startSubscriptionBasedCollection(); // ✅ 구독 방식 사용
    }

    /**
     * 프론트엔드로 데이터 전송 메서드
     * 
     * @param data 전송할 OPC UA 데이터
     */
    private void sendDataToFrontend(Map<String, Object> data) {
        try {
            if (data == null || data.isEmpty()) {
                log.warn("전송할 데이터가 비어 있습니다");
                return;
            }

            // 웹소켓 메시지 구성 (sendLatestDataToClient와 동일한 형식으로)
            Map<String, Object> wsMessage = new HashMap<>();
            wsMessage.put("type", "opcua");
            wsMessage.put("timestamp", LocalDateTime.now().toString());

            // 데이터 구조 통일 (sendLatestDataToClient와 동일한 방식으로)
            Map<String, Object> cleanedData = new HashMap<>(data);

            // 메타데이터 필드 제거 (필요시)
            cleanedData.remove("time");
            cleanedData.remove("_time");
            cleanedData.remove("table");
            cleanedData.remove("result");
            cleanedData.remove("_start");
            cleanedData.remove("_stop");
            cleanedData.remove("_measurement");

            // OPC_UA 키 아래에 데이터 넣기 (sendLatestDataToClient와 동일)
            Map<String, Object> opcuaData = new HashMap<>();
            opcuaData.put("OPC_UA", cleanedData);
            wsMessage.put("data", opcuaData);

            // 이벤트 발행 (동일하게 유지)
            eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));
            log.info("프론트엔드로 데이터 전송 완료: 필드 수={}", cleanedData.size());

            // 디버깅용: 일부 필드 출력
            if (log.isInfoEnabled() && !cleanedData.isEmpty()) {
                int count = 0;
                StringBuilder fields = new StringBuilder();
                for (String key : cleanedData.keySet()) {
                    if (count++ < 3) {
                        fields.append(key).append(", ");
                    } else {
                        break;
                    }
                }
                log.info("전송된 데이터 샘플 필드: {}", fields);
            }
        } catch (Exception e) {
            log.error("데이터 전송 오류: {}", e.getMessage(), e);
        }
    }

}
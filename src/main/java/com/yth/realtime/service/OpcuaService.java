package com.yth.realtime.service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import com.yth.realtime.controller.OpcuaWebSocketHandler;
import com.yth.realtime.event.OpcuaDataEvent;
import com.yth.realtime.model.InfluxDBMeasurement;

import jakarta.annotation.PostConstruct;
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

    @Autowired
    public OpcuaService(OpcuaClient opcuaClient, OpcuaWebSocketHandler opcuaWebSocketHandler,
            OpcuaInfluxDBService opcuaInfluxDBService, ApplicationEventPublisher eventPublisher) {
        this.opcuaClient = opcuaClient;
        this.webSocketHandler = opcuaWebSocketHandler;
        this.influxDBService = opcuaInfluxDBService;
        this.eventPublisher = eventPublisher;
    }

    /**
     * 서비스 시작 시 OPC UA 서버에 연결 시도
     */
    @PostConstruct
    public void init() {
        try {
            connect();
            startDataCollection();
        } catch (Exception e) {
            log.error("OPC UA 서비스 초기화 실패: {}", e.getMessage(), e);
            // 실패해도 애플리케이션은 계속 실행
        }
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
        try {
            // 1. InfluxDB에 먼저 데이터 저장
            saveToInfluxDB(allData, timestamp);

            // 2. 저장 후 최신 데이터 조회하여 클라이언트에 전송
            sendLatestDataToClient();

        } catch (Exception e) {
            log.error("OPC UA 데이터 처리 중 오류: {}", e.getMessage(), e);
        }
    }

    /**
     * InfluxDB에 데이터 저장
     */
    private void saveToInfluxDB(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
        try {
            // 데이터를 평탄화하여 저장 준비
            Map<String, Object> flattenedData = flattenData(allData);

            InfluxDBMeasurement measurement = new InfluxDBMeasurement();
            measurement.setMeasurement("opcua_data");

            // 시스템 태그 추가
            measurement.addTag("system", "PCS_System");

            // 모든 필드 저장
            for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Number) {
                    measurement.addField(fieldName, (Number) value);
                } else if (value instanceof String) {
                    try {
                        // 문자열을 숫자로 변환 시도
                        double numValue = Double.parseDouble((String) value);
                        measurement.addField(fieldName, numValue);
                    } catch (NumberFormatException e) {
                        // 숫자로 변환할 수 없으면 문자열 필드로 저장
                        measurement.addField(fieldName, (String) value);
                    }
                }
            }

            measurement.setTimestamp(timestamp.toInstant(ZoneOffset.UTC));
            influxDBService.saveData(measurement);
            log.debug("OPC UA 데이터 InfluxDB 저장 완료: {}", timestamp);
        } catch (Exception e) {
            log.error("InfluxDB 데이터 저장 실패: {}", e.getMessage(), e);
            throw e; // 상위 메서드에서 처리할 수 있도록 예외 전파
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
                String flatKey = groupName + "_" + fieldName;
                flattenedData.put(flatKey, fieldValue);
            }
        }

        return flattenedData;
    }

    /**
     * 최신 저장된 데이터를 조회하여 클라이언트에 전송
     */
    private void sendLatestDataToClient() {
        try {
            // 최근 데이터를 조회하는 쿼리
            String query = "SELECT * FROM opcua_data WHERE time > now() - 1s ORDER BY time DESC LIMIT 1";
            List<Map<String, Object>> latestResults = influxDBService.queryData(query);

            if (latestResults.isEmpty()) {
                log.warn("최신 OPC UA 데이터가 조회되지 않았습니다");
                return;
            }

            // 조회된 데이터를 그룹화
            Map<String, Object> latestResult = latestResults.get(0);
            Map<String, Map<String, Object>> groupedData = groupInfluxData(latestResult);

            // 타임스탬프 추출
            LocalDateTime timestamp = (LocalDateTime) latestResult.get("time");

            // 이벤트 발행
            Map<String, Object> wsMessage = new HashMap<>();
            wsMessage.put("type", "opcua");
            wsMessage.put("timestamp", timestamp.toString());
            wsMessage.put("data", groupedData);

            eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));
            log.debug("OPC UA 최신 데이터 전송 완료: {}", timestamp);
        } catch (Exception e) {
            log.error("OPC UA 최신 데이터 조회 및 전송 중 오류: {}", e.getMessage(), e);
        }
    }

    /**
     * InfluxDB에서 조회한 평탄화된 데이터를 그룹화
     */
    private Map<String, Map<String, Object>> groupInfluxData(Map<String, Object> influxData) {
        Map<String, Map<String, Object>> groupedData = new HashMap<>();

        // 시간 필드는 별도 처리
        influxData.remove("time");

        // 그룹화 작업
        for (Map.Entry<String, Object> entry : influxData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // 그룹_필드명 패턴 확인 (예: PCS1_SOC)
            int underscoreIdx = key.indexOf('_');

            if (underscoreIdx > 0) {
                String groupName = key.substring(0, underscoreIdx);
                String fieldName = key.substring(underscoreIdx + 1);

                groupedData.computeIfAbsent(groupName, k -> new HashMap<>()).put(fieldName, value);
            } else {
                // 언더스코어가 없는 경우 Common 그룹에 저장
                groupedData.computeIfAbsent("Common", k -> new HashMap<>()).put(key, value);
            }
        }

        return groupedData;
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

    /**
     * 지정된 기간 동안의 OPC UA 데이터 조회 및 클라이언트에 전송
     */
    public void sendHistoricalData(String deviceGroup, LocalDateTime start, LocalDateTime end) {
        try {
            // InfluxDB에서 과거 데이터 조회
            String query = buildHistoricalDataQuery(deviceGroup, start, end);
            List<Map<String, Object>> results = influxDBService.queryData(query);

            if (results.isEmpty()) {
                log.warn("조회된 OPC UA 과거 데이터가 없습니다. 그룹: {}, 기간: {} - {}",
                        deviceGroup, formatDateTime(start), formatDateTime(end));
                return;
            }

            // 결과 데이터 그룹화 및 가공
            Map<String, List<Map<String, Object>>> processedResults = processHistoricalData(results, deviceGroup);

            // 웹소켓 메시지 구성
            Map<String, Object> message = new HashMap<>();
            message.put("type", "history");
            message.put("deviceGroup", deviceGroup);
            message.put("source", "opcua");
            message.put("start", formatDateTime(start));
            message.put("end", formatDateTime(end));
            message.put("data", processedResults);

            // 웹소켓으로 데이터 전송
            webSocketHandler.sendOpcuaHistoricalData(message);
            log.info("OPC UA 과거 데이터 전송 완료: {}, 데이터 수: {}", deviceGroup, results.size());

        } catch (Exception e) {
            log.error("OPC UA 과거 데이터 조회 및 전송 중 오류: {}", e.getMessage(), e);
        }
    }

    /**
     * 과거 데이터 처리 및 그룹화
     */
    private Map<String, List<Map<String, Object>>> processHistoricalData(
            List<Map<String, Object>> results, String deviceGroup) {

        Map<String, List<Map<String, Object>>> processedResults = new HashMap<>();

        // deviceGroup으로 필터링할 필드 접두사 결정
        String prefix = "";
        if (!deviceGroup.equals("system")) {
            prefix = deviceGroup + "_";
        }

        // 타임스탬프별로 데이터 정리
        for (Map<String, Object> result : results) {
            LocalDateTime timestamp = (LocalDateTime) result.get("time");

            // 해당 타임스탬프의 관련 데이터만 추출
            Map<String, Object> dataPoint = new HashMap<>();
            dataPoint.put("timestamp", formatDateTime(timestamp));

            for (Map.Entry<String, Object> entry : result.entrySet()) {
                String key = entry.getKey();

                // 시간 필드는 이미 처리함
                if (key.equals("time"))
                    continue;

                // 요청한 디바이스 그룹에 해당하는 필드만 추출
                if (deviceGroup.equals("system") || key.startsWith(prefix)) {
                    // 접두사 제거
                    String cleanKey = deviceGroup.equals("system") ? key : key.substring(prefix.length());
                    dataPoint.put(cleanKey, entry.getValue());
                }
            }

            // 비어있지 않은 데이터만 추가
            if (dataPoint.size() > 1) { // timestamp 외에 다른 데이터가 있는 경우
                processedResults.computeIfAbsent("timeSeries", k -> new ArrayList<>()).add(dataPoint);
            }
        }

        return processedResults;
    }

    /**
     * InfluxDB 쿼리 생성
     */
    private String buildHistoricalDataQuery(String deviceGroup, LocalDateTime start, LocalDateTime end) {
        String fields;

        if (deviceGroup.equals("system")) {
            // 모든 필드 조회
            fields = "*";
        } else {
            // 특정 그룹의 필드만 조회 (예: PCS1_*)
            fields = deviceGroup + "_*";
        }

        return String.format(
                "SELECT %s, time FROM opcua_data " +
                        "WHERE time >= '%s' AND time <= '%s' " +
                        "ORDER BY time ASC",
                fields,
                formatInfluxTime(start),
                formatInfluxTime(end));
    }

    /**
     * 24시간 이전 데이터 조회 및 클라이언트에 전송
     */
    public void send24HourHistoricalData(String deviceGroup) {
        LocalDateTime end = LocalDateTime.now();
        LocalDateTime start = end.minusHours(24);
        sendHistoricalData(deviceGroup, start, end);
    }

    /**
     * LocalDateTime을 InfluxDB 쿼리용 시간 문자열로 변환
     */
    private String formatInfluxTime(LocalDateTime time) {
        return time.format(DateTimeFormatter.ISO_DATE_TIME) + "Z";
    }

    /**
     * LocalDateTime을 표시용 문자열로 변환
     */
    private String formatDateTime(LocalDateTime time) {
        return time.format(DateTimeFormatter.ISO_DATE_TIME);
    }
}
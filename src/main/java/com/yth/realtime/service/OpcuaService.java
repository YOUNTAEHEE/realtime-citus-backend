package com.yth.realtime.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    private final ExecutorService dbSaveExecutor = Executors.newFixedThreadPool(5);
    private final ExecutorService dbQueryExecutor = Executors.newFixedThreadPool(5);

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
    // @PostConstruct
    // public void init() {
    // try {
    // connect();
    // startDataCollection();
    // } catch (Exception e) {
    // log.error("OPC UA 서비스 초기화 실패: {}", e.getMessage(), e);
    // // 실패해도 애플리케이션은 계속 실행
    // }
    // }

    // 대신 아래와 같이 명시적 시작 메서드 추가
    // public void startService() {
    // try {
    // connect();
    // startDataCollection();
    // log.info("OPC UA 서비스 수동으로 시작됨");
    // } catch (Exception e) {
    // log.error("OPC UA 서비스 시작 실패: {}", e.getMessage(), e);
    // }
    // }

    // // 중지 메서드 추가
    // public void stopService() {
    // try {
    // if (scheduler != null) {
    // scheduler.shutdown();
    // log.info("OPC UA 데이터 수집 스케줄러 중지됨");
    // }
    // if (opcuaClient != null) {
    // opcuaClient.disconnect();
    // log.info("OPC UA 클라이언트 연결 해제됨");
    // }
    // } catch (Exception e) {
    // log.error("OPC UA 서비스 중지 중 오류: {}", e.getMessage(), e);
    // }
    // }

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
        }, 0, 5, TimeUnit.MILLISECONDS);

        log.info("OPC UA 데이터 수집 시작됨 (5ms 간격)");
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
    // private void processOpcuaData(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
    //     try {
    //         // 해결 방안 1: 저장 작업 완료 확인 후 조회
    //         saveToInfluxDB(allData, timestamp);

    //         // // 약간의 지연 추가 (데이터가 확실히 저장되도록)
    //         // try {
    //         // Thread.sleep(100); // 100ms 지연
    //         // } catch (InterruptedException e) {
    //         // Thread.currentThread().interrupt();
    //         // }

    //         sendLatestDataToClient();

    //         // 해결 방안 2: 저장된 데이터를 직접 전송 (조회 단계 생략)
    //         // saveToInfluxDB(allData, timestamp);
    //         // sendDataDirectly(allData, timestamp);

    //     } catch (Exception e) {
    //         log.error("OPC UA 데이터 처리 중 오류: {}", e.getMessage(), e);
    //     }
    // }

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
     * 최신 저장된 데이터를 조회하여 클라이언트에 전송
     */
    // private void sendLatestDataToClient() {
    //     try {
    //         // 기존 쿼리 및 데이터 조회 부분은 유지
    //         String query = String.format(
    //                 "from(bucket: \"%s\") " +
    //                         "|> range(start: -1h) " +
    //                         "|> filter(fn: (r) => r._measurement == \"opcua_data\") " +
    //                         "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
    //                         "|> sort(columns: [\"_time\"], desc: true) " +
    //                         "|> limit(n: 1)",
    //                 influxDBService.getBucket());

    //         log.info("실행할 쿼리: {}", query);
    //         List<Map<String, Object>> latestResults = influxDBService.queryData(query);
    //         log.info("조회 결과 레코드 수: {}", latestResults.size());

    //         // 웹소켓 메시지 준비
    //         Map<String, Object> wsMessage = new HashMap<>();
    //         wsMessage.put("type", "opcua");
    //         wsMessage.put("timestamp", LocalDateTime.now().toString());

    //         if (!latestResults.isEmpty()) {
    //             // 첫 번째 결과 가져오기
    //             Map<String, Object> latestResult = latestResults.get(0);
    //             log.info("조회 결과의 키 목록: {}", latestResult.keySet());

    //             // 필요없는 메타데이터 필드 제거
    //             Map<String, Object> cleanResult = new HashMap<>(latestResult);
    //             cleanResult.remove("time");
    //             cleanResult.remove("_time");
    //             cleanResult.remove("table");
    //             cleanResult.remove("result");
    //             cleanResult.remove("_start");
    //             cleanResult.remove("_stop");
    //             cleanResult.remove("_measurement");

    //             // OPC_UA 키 아래에 평탄화된 데이터 넣기
    //             Map<String, Object> opcuaData = new HashMap<>();
    //             opcuaData.put("OPC_UA", cleanResult);
    //             wsMessage.put("data", opcuaData);

    //             log.info("프론트엔드로 전송할 OPC_UA 데이터: 필드 수={}", cleanResult.size());
    //         } else {
    //             // 데이터가 없는 경우 OPC UA 클라이언트에서 직접 데이터 가져오기
    //             log.warn("InfluxDB에서 데이터를 찾을 수 없습니다. OPC UA 클라이언트에서 직접 데이터를 가져옵니다.");
    //             Map<String, Map<String, Object>> currentData = opcuaClient.readAllValues();

    //             // allData를 평탄화하여 OPC_UA 키 아래에 넣기
    //             Map<String, Object> flattenedData = flattenData(currentData);
    //             Map<String, Object> opcuaData = new HashMap<>();
    //             opcuaData.put("OPC_UA", flattenedData);
    //             wsMessage.put("data", opcuaData);

    //             log.info("OPC UA 클라이언트에서 직접 가져온 데이터: 필드 수={}", flattenedData.size());
    //         }

    //         // 이벤트 발행
    //         eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));
    //         log.info("OPC UA 데이터 전송 완료");

    //     } catch (Exception e) {
    //         log.error("OPC UA 최신 데이터 조회 및 전송 중 오류: {}", e.getMessage(), e);

    //         // 오류 발생 시 직접 데이터 전송
    //         try {
    //             Map<String, Map<String, Object>> currentData = opcuaClient.readAllValues();
    //             sendDataDirectlyWithOpcUAFormat(currentData, LocalDateTime.now());
    //         } catch (Exception ex) {
    //             log.error("대체 데이터 전송도 실패: {}", ex.getMessage());
    //         }
    //     }
    // }

    // OPC_UA 형식으로 데이터 전송
    // private void sendDataDirectlyWithOpcUAFormat(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
    //     try {
    //         // 데이터 평탄화
    //         Map<String, Object> flattenedData = flattenData(allData);

    //         // 로그 추가
    //         log.info("전송할 데이터 필드 수: {}", flattenedData.size());
    //         log.info("샘플 필드: {}",
    //                 flattenedData.keySet().stream().limit(5).collect(Collectors.joining(", ")));

    //         // 필수 필드 확인
    //         String[] requiredFields = { "Filtered_Grid_Freq", "PCS1_TPWR_P_REAL" };
    //         for (String field : requiredFields) {
    //             log.info("필드 {} 값: {}", field, flattenedData.get(field));
    //         }

    //         // OPC_UA 형식으로 변환
    //         Map<String, Object> opcuaData = new HashMap<>();
    //         opcuaData.put("OPC_UA", flattenedData);

    //         Map<String, Object> wsMessage = new HashMap<>();
    //         wsMessage.put("type", "opcua");
    //         wsMessage.put("timestamp", timestamp.toString());
    //         wsMessage.put("data", opcuaData);

    //         // 메시지 발행
    //         eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));
    //         log.info("OPC UA 데이터 전송 완료");
    //     } catch (Exception e) {
    //         log.error("OPC UA 데이터 전송 중 오류: {}", e.getMessage(), e);
    //     }
    // }

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
    // public void sendHistoricalData(String deviceGroup, LocalDateTime start,
    // LocalDateTime end) {
    // try {
    // // InfluxDB에서 과거 데이터 조회
    // String query = buildHistoricalDataQuery(deviceGroup, start, end);
    // List<Map<String, Object>> results = influxDBService.queryData(query);

    // if (results.isEmpty()) {
    // log.warn("조회된 OPC UA 과거 데이터가 없습니다. 그룹: {}, 기간: {} - {}",
    // deviceGroup, formatDateTime(start), formatDateTime(end));
    // return;
    // }

    // // 결과 데이터 그룹화 및 가공
    // Map<String, List<Map<String, Object>>> processedResults =
    // processHistoricalData(results, deviceGroup);

    // // 웹소켓 메시지 구성
    // Map<String, Object> message = new HashMap<>();
    // message.put("type", "history");
    // message.put("deviceGroup", deviceGroup);
    // message.put("source", "opcua");
    // message.put("start", formatDateTime(start));
    // message.put("end", formatDateTime(end));
    // message.put("data", processedResults);

    // // 웹소켓으로 데이터 전송
    // webSocketHandler.sendOpcuaHistoricalData(message);
    // log.info("OPC UA 과거 데이터 전송 완료: {}, 데이터 수: {}", deviceGroup, results.size());

    // } catch (Exception e) {
    // log.error("OPC UA 과거 데이터 조회 및 전송 중 오류: {}", e.getMessage(), e);
    // }
    // }

    /**
     * 과거 데이터 처리 및 그룹화
     */
    // private Map<String, List<Map<String, Object>>> processHistoricalData(
    // List<Map<String, Object>> results, String deviceGroup) {

    // Map<String, List<Map<String, Object>>> processedResults = new HashMap<>();

    // // deviceGroup으로 필터링할 필드 접두사 결정
    // String prefix = "";
    // if (!deviceGroup.equals("system")) {
    // prefix = deviceGroup + "_";
    // }

    // // 타임스탬프별로 데이터 정리
    // for (Map<String, Object> result : results) {
    // LocalDateTime timestamp = (LocalDateTime) result.get("time");

    // // 해당 타임스탬프의 관련 데이터만 추출
    // Map<String, Object> dataPoint = new HashMap<>();
    // dataPoint.put("timestamp", formatDateTime(timestamp));

    // for (Map.Entry<String, Object> entry : result.entrySet()) {
    // String key = entry.getKey();

    // // 시간 필드는 이미 처리함
    // if (key.equals("time"))
    // continue;

    // // 요청한 디바이스 그룹에 해당하는 필드만 추출
    // if (deviceGroup.equals("system") || key.startsWith(prefix)) {
    // // 접두사 제거
    // String cleanKey = deviceGroup.equals("system") ? key :
    // key.substring(prefix.length());
    // dataPoint.put(cleanKey, entry.getValue());
    // }
    // }

    // // 비어있지 않은 데이터만 추가
    // if (dataPoint.size() > 1) { // timestamp 외에 다른 데이터가 있는 경우
    // processedResults.computeIfAbsent("timeSeries", k -> new
    // ArrayList<>()).add(dataPoint);
    // }
    // }

    // return processedResults;
    // }

    /**
     * InfluxDB 쿼리 생성
     */
    // private String buildHistoricalDataQuery(String deviceGroup, LocalDateTime
    // start, LocalDateTime end) {
    // String fieldFilter;

    // if (deviceGroup.equals("system")) {
    // // 모든 필드 조회
    // fieldFilter = ""; // 필터 없음
    // } else {
    // // 특정 그룹의 필드만 조회 (예: PCS1_*)
    // fieldFilter = String.format("and (r._field =~ /^%s_.*/)", deviceGroup);
    // }

    // // Flux 쿼리 포맷
    // return String.format(
    // "from(bucket: \"%s\") " +
    // "|> range(start: %s, stop: %s) " +
    // "|> filter(fn: (r) => r._measurement == \"opcua_data\" %s) " +
    // "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn:
    // \"_value\") " +
    // "|> sort(columns: [\"_time\"], desc: false)",
    // influxDBService.getBucket(),
    // formatInfluxTime(start),
    // formatInfluxTime(end),
    // fieldFilter);
    // }

    /**
     * 24시간 이전 데이터 조회 및 클라이언트에 전송
     */
    // public void send24HourHistoricalData(String deviceGroup) {
    // LocalDateTime end = LocalDateTime.now();
    // LocalDateTime start = end.minusHours(24);
    // sendHistoricalData(deviceGroup, start, end);
    // }

    /**
     * 1시간 이전 데이터 조회 및 클라이언트에 전송
     */
    // public void send1HourHistoricalData(String deviceGroup) {
    // LocalDateTime end = LocalDateTime.now();
    // LocalDateTime start = end.minusHours(1);
    // sendHistoricalData(deviceGroup, start, end);
    // }

    /**
     * LocalDateTime을 InfluxDB 쿼리용 시간 문자열로 변환
     */
    // private String formatInfluxTime(LocalDateTime time) {
    //     return time.format(DateTimeFormatter.ISO_DATE_TIME);
    // }

    /**
     * LocalDateTime을 표시용 문자열로 변환
     */
    // private String formatDateTime(LocalDateTime time) {
    //     return time.format(DateTimeFormatter.ISO_DATE_TIME);
    // }

    @EventListener
    public void onStartCollection(StartOpcuaCollectionEvent event) {
        log.info("StartOpcuaCollectionEvent 수신 → connect + startDataCollection 실행");
        connect();
        startDataCollection();
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

    /**
     * 타임스탬프가 있는 버전의 프론트엔드 데이터 전송 메서드
     * 
     * @param data      전송할 OPC UA 데이터
     * @param timestamp 데이터의 타임스탬프
     */
    // private void sendDataToFrontend(Map<String, Object> data, LocalDateTime timestamp) {
    //     try {
    //         if (data == null || data.isEmpty()) {
    //             log.warn("전송할 데이터가 비어 있습니다");
    //             return;
    //         }

    //         // 웹소켓 메시지 구성
    //         Map<String, Object> wsMessage = new HashMap<>();
    //         wsMessage.put("type", "opcua");
    //         wsMessage.put("timestamp", timestamp.toString());

    //         // OPC_UA 형식으로 변환
    //         Map<String, Object> opcuaData = new HashMap<>();
    //         opcuaData.put("OPC_UA", data);
    //         wsMessage.put("data", opcuaData);

    //         // 이벤트 발행을 통한 데이터 전송
    //         eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));

    //         // 로그 추가 (일부 키값 출력)
    //         if (log.isDebugEnabled()) {
    //             String sampleKeys = data.keySet().stream()
    //                     .limit(3)
    //                     .collect(Collectors.joining(", "));
    //             log.debug("데이터 전송 완료: 필드 수={}, 샘플 필드=[{}]",
    //                     data.size(), sampleKeys);
    //         }
    //     } catch (Exception e) {
    //         log.error("데이터 전송 오류: {}", e.getMessage());
    //     }
    // }
}
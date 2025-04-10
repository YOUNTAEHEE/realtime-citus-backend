package com.yth.realtime.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

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

    // private final ScheduledExecutorService scheduler =
    // Executors.newScheduledThreadPool(1);
    // private ScheduledFuture<?> dataCollectionTask;
    // private boolean autoReconnect = true;

    // private final ExecutorService dbSaveExecutor =
    // Executors.newFixedThreadPool(5);
    // private final ExecutorService dbQueryExecutor =
    // Executors.newFixedThreadPool(5);
    // 현재 CPU 코어 수 기반으로 워크 스틸링 풀 생성
    // ExecutorService dbSaveExecutor = Executors.newWorkStealingPool();
    // ExecutorService dbQueryExecutor = Executors.newWorkStealingPool();
    ExecutorService collectorPool = Executors.newFixedThreadPool(4); // 수집 병렬 스레드 4개
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService saveExecutor = Executors.newFixedThreadPool(8); // 저장 스레드
    ExecutorService storageExecutor = Executors.newFixedThreadPool(16);
    // private final ExecutorService sendExecutor =
    // Executors.newSingleThreadExecutor(); // 전송 스레드
    ExecutorService sendExecutor = Executors.newFixedThreadPool(1);
    private final BlockingQueue<TimestampedData> saveQueue = new LinkedBlockingQueue<>(1000);
    // private final BlockingQueue<LocalDateTime> sendQueue = new
    // LinkedBlockingQueue<>(1000);

    private ScheduledFuture<?> dataCollectionTask;
    private boolean autoReconnect = true;
    // 중복 방지용 Set
    private final Set<LocalDateTime> seenTimestamps = ConcurrentHashMap.newKeySet();
    // 중복 제거가 가능한 큐
    private final LinkedBlockingDeque<LocalDateTime> sendQueue = new LinkedBlockingDeque<>(1000);

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
    // @PreDestroy
    // public void cleanup() {
    // stopDataCollection();
    // disconnect();
    // // 3. WebSocket 세션 정리 (추가)
    // webSocketHandler.clearAllSessions();
    // // 스레드풀 정리
    // dbSaveExecutor.shutdown();
    // dbQueryExecutor.shutdown();

    // try {
    // if (!dbSaveExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
    // dbSaveExecutor.shutdownNow();
    // }
    // if (!dbQueryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
    // dbQueryExecutor.shutdownNow();
    // }
    // } catch (InterruptedException e) {
    // dbSaveExecutor.shutdownNow();
    // dbQueryExecutor.shutdownNow();
    // Thread.currentThread().interrupt();
    // }
    // }

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
    // public void startDataCollection() {
    // // 이미 실행 중인 작업이 있다면 중지
    // stopDataCollection();

    // // 5ms 간격으로 데이터 수집 스케줄링
    // dataCollectionTask = scheduler.scheduleAtFixedRate(() -> {
    // long startTime = System.currentTimeMillis();
    // try {
    // // OPC UA 서버 연결 상태 확인
    // if (!opcuaClient.isConnected()) {
    // log.warn("OPC UA 서버 연결이 끊겼습니다. 재연결 시도...");
    // if (autoReconnect) {
    // opcuaClient.connect();
    // }
    // return;
    // }

    // // 데이터 수집 시간 측정
    // long collectionStart = System.currentTimeMillis();
    // Map<String, Map<String, Object>> allData = opcuaClient.readAllValues();
    // long collectionTime = System.currentTimeMillis() - collectionStart;

    // LocalDateTime timestamp = LocalDateTime.now();

    // // 1. DB 저장 스레드
    // final Map<String, Map<String, Object>> dataCopy = new HashMap<>(allData);
    // dbSaveExecutor.submit(() -> {
    // try {
    // long saveStart = System.currentTimeMillis();
    // saveToInfluxDB(dataCopy, timestamp);
    // long saveTime = System.currentTimeMillis() - saveStart;
    // log.debug("DB 저장 완료: 시간={}, 소요시간={}ms", timestamp, saveTime);
    // } catch (Exception e) {
    // log.error("DB 저장 오류: {}", e.getMessage());
    // }
    // });

    // // 2. 별도로 DB 조회 스레드
    // dbQueryExecutor.submit(() -> {
    // try {
    // // DB 조회 시간 측정
    // long queryStart = System.currentTimeMillis();
    // Map<String, Object> latestData = influxDBService.getLatestOpcuaData("all");
    // long queryTime = System.currentTimeMillis() - queryStart;

    // // 조회한 데이터를 프론트엔드로 전송
    // sendDataToFrontend(latestData);

    // log.debug("DB 조회 및 전송 완료: 조회시간={}ms", queryTime);
    // } catch (Exception e) {
    // log.error("DB 조회 오류: {}", e.getMessage());
    // }
    // });

    // // 전체 작업 소요 시간 측정
    // long totalTime = System.currentTimeMillis() - startTime;
    // if (totalTime > 10) { // 10ms 이상 걸리면 로깅
    // log.warn("데이터 수집 작업 지연: {}ms (설정: 5ms) - 수집:{}ms",
    // totalTime, collectionTime);
    // }

    // } catch (Exception e) {
    // log.error("OPC UA 데이터 수집 중 오류: {}", e.getMessage());
    // }
    // }, 0, 10, TimeUnit.MILLISECONDS);

    // log.info("OPC UA 데이터 수집 시작됨 (5ms 간격)");
    // }

    public void startDataCollection() {
        stopDataCollection(); // 중복 방지

        // ✅ 1. 수집 쓰레드 (5ms 간격 → saveQueue로 전달 - put 사용)
        for (int i = 0; i < 4; i++) {
            collectorPool.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    long collectionCycleStartTime = System.currentTimeMillis();
                    try {
                        if (!opcuaClient.isConnected()) {
                            log.warn("OPC UA 서버 연결 끊김 → 재연결 시도");
                            if (autoReconnect)
                                opcuaClient.connect();
                            Thread.sleep(100); // 연결 재시도 간격
                            continue;
                        }

                        long readStartTime = System.currentTimeMillis();
                        Map<String, Map<String, Object>> data = opcuaClient.readAllValues();
                        long readEndTime = System.currentTimeMillis();

                        // log.debug("스레드 {}: readAllValues() {} ms",
                        // Thread.currentThread().getName(), readEndTime - readStartTime);

                        LocalDateTime collectionTimestamp = LocalDateTime.now();
                        saveQueue.put(new TimestampedData(data, collectionTimestamp));

                        // 수집 주기 조절 (과부하 방지)
                        Thread.sleep(5);

                        long collectionCycleEndTime = System.currentTimeMillis();
                        // log.debug("스레드 {}: 수집 사이클 완료. 총 {} ms",
                        // Thread.currentThread().getName(), collectionCycleEndTime -
                        // collectionCycleStartTime);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.error("스레드 {} 수집 오류: {}", Thread.currentThread().getName(), e.getMessage(), e);
                    }
                }
            });
        }

        log.info("✅ 수집 스레드 4개 시작됨 (병렬 수집)");

        // ✅ 2. 저장 스레드 (조회 제거, sendQueue에 타임스탬프 put)
        // for (int i = 0; i < 4; i++) {
        // saveExecutor.submit(() -> {
        // while (!Thread.currentThread().isInterrupted()) {
        // try {
        // TimestampedData timestampedData = saveQueue.take(); // 큐에서 데이터+타임스탬프 꺼내기
        // // DB 저장 시도
        // saveToInfluxDB(timestampedData.getData(), timestampedData.getTimestamp());

        // // === 수정: DB 조회 대신, 저장된 데이터의 타임스탬프를 sendQueue에 넣음 ===
        // try {
        // // sendQueue가 가득 차면 여기서 대기
        // sendQueue.put(timestampedData.getTimestamp());
        // } catch (InterruptedException e) {
        // log.warn("데이터 전송 큐(Timestamp) 대기 중 인터럽트 발생", e);
        // Thread.currentThread().interrupt();
        // break;
        // }

        // // --- 삭제: DB 조회 및 이전 put 로직 제거 ---
        // // Map<String, Object> latest = influxDBService.getLatestOpcuaData("all");
        // // try {
        // // sendQueue.put(latest); ...

        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // break;
        // } catch (Exception e) {
        // log.error("저장 처리 중 오류 발생 (saveExecutor): {}", e.getMessage(), e);
        // }
        // }
        // log.info("저장 스레드 종료됨.");
        // });

        // ✅ 2. 저장 스레드 (배치 + 병렬 처리 방식)
        // saveExecutor.submit(() -> {
        // while (!Thread.currentThread().isInterrupted()) {
        // try {
        // // 1. 배치 단위로 데이터 꺼내기
        // List<TimestampedData> batch = new ArrayList<>();
        // saveQueue.drainTo(batch, 50); // 최대 50개까지 한 번에 꺼냄

        // if (batch.isEmpty()) {
        // // 데이터 없으면 잠깐 대기 (CPU 낭비 방지)
        // Thread.sleep(5);
        // continue;
        // }

        // // 2. 병렬 저장 처리
        // batch.parallelStream().forEach(data -> {
        // try {
        // saveToInfluxDB(data.getData(), data.getTimestamp());

        // // 저장 후 전송 큐에 타임스탬프 전달
        // try {
        // sendQueue.put(data.getTimestamp());
        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // log.warn("sendQueue.put() 중 인터럽트 발생", e);
        // }

        // } catch (Exception e) {
        // log.error("배치 저장 중 오류", e);
        // }
        // });

        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // log.info("저장 스레드 인터럽트로 종료됨");
        // break;
        // } catch (Exception e) {
        // log.error("배치 저장 처리 오류", e);
        // }
        // }
        // log.info("저장 스레드 종료됨 (배치 + 병렬 처리)");
        // });
        // }

        // saveExecutor.submit(() -> {
        // while (!Thread.currentThread().isInterrupted()) {
        // try {
        // // 1. 배치 단위로 데이터 꺼내기
        // List<TimestampedData> batch = new ArrayList<>();
        // saveQueue.drainTo(batch, 50); // 최대 50개 꺼냄

        // if (batch.isEmpty()) {
        // Thread.sleep(5);
        // continue;
        // }

        // // 2. ExecutorService로 병렬 저장 작업 제출
        // for (TimestampedData data : batch) {
        // storageExecutor.submit(() -> {
        // try {
        // saveToInfluxDB(data.getData(), data.getTimestamp());

        // // 저장 후 전송 큐에 타임스탬프 전달
        // try {
        // sendQueue.put(data.getTimestamp());
        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // log.warn("sendQueue.put() 중 인터럽트 발생", e);
        // }

        // } catch (Exception e) {
        // log.error("저장 작업 중 오류", e);
        // }
        // });
        // }

        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // log.info("저장 스레드 인터럽트로 종료됨");
        // break;
        // } catch (Exception e) {
        // log.error("저장 스레드 처리 오류", e);
        // }
        // }

        // log.info("저장 스레드 종료됨 (ExecutorService 기반 병렬 처리)");
        // });
        for (int i = 0; i < 8; i++) {
            saveExecutor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        List<TimestampedData> batch = new ArrayList<>();
                        saveQueue.drainTo(batch, 100);

                        if (batch.isEmpty()) {
                            Thread.sleep(5);
                            continue;
                        }

                        for (TimestampedData data : batch) {
                            storageExecutor.submit(() -> {
                                try {
                                    saveToInfluxDB(data.getData(), data.getTimestamp());

                                    // ✅ 중복된 timestamp가 큐에 들어가지 않도록 처리
                                    if (seenTimestamps.add(data.getTimestamp())) {
                                        sendQueue.put(data.getTimestamp()); // 새 타임스탬프만 큐에 넣음
                                    }

                                } catch (Exception e) {
                                    log.error("저장 중 오류", e);
                                }
                            });
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.error("배치 저장 오류", e);
                    }
                }
            });
        }
        // ✅ 3. 전송 스레드 (큐에서 타임스탬프 꺼낸 후 DB 조회 및 전송)
        // sendExecutor.submit(() -> {
        // while (!Thread.currentThread().isInterrupted()) {
        // try {
        // // === 수정: 큐에서 타임스탬프 꺼내기 (데이터 자체 X) ===
        // LocalDateTime triggerTimestamp = sendQueue.take(); // 저장 완료 신호 (타임스탬프)
        // log.debug("전송 트리거 수신: {}", triggerTimestamp); // 디버그 로그 추가

        // // === 수정: 여기서 DB 최신 데이터 조회 ===
        // Map<String, Object> latestData = influxDBService.getLatestOpcuaData("all");

        // // 조회 결과를 프론트엔드로 전송
        // sendDataToFrontend(latestData);

        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // break;
        // } catch (Exception e) {
        // // getLatestOpcuaData 또는 sendDataToFrontend 오류 처리
        // log.error("조회/전송 오류 (sendExecutor): {}", e.getMessage(), e);
        // }
        // }
        // log.info("전송 스레드 종료됨.");
        // });
        // for (int i = 0; i < 2; i++) {
            sendExecutor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        LocalDateTime ts = sendQueue.take(); // 트리거 타임스탬프 꺼냄

                        // ✅ 전송이 끝났으면 중복 체크용 Set에서 제거
                        seenTimestamps.remove(ts);

                        Map<String, Object> latest = influxDBService.getLatestOpcuaData("all");
                        sendDataToFrontend(latest);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.error("조회/전송 오류", e);
                    }
                }
            });
        // }

    }

    public void stopDataCollection() {
        if (dataCollectionTask != null && !dataCollectionTask.isDone()) {
            dataCollectionTask.cancel(false);
            log.info("데이터 수집 중단됨");
        }
    }

    @PreDestroy
    public void cleanup() {
        stopDataCollection();
        opcuaClient.disconnect();
        webSocketHandler.clearAllSessions();
        saveExecutor.shutdownNow();
        sendExecutor.shutdownNow();
        storageExecutor.shutdownNow(); // 💡 추가됨
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
    // public void stopDataCollection() {
    // if (dataCollectionTask != null && !dataCollectionTask.isDone()) {
    // dataCollectionTask.cancel(false);
    // log.info("OPC UA 데이터 수집 중지됨");
    // }
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

            if (dataPoint.hasFields()) { // 필드가 하나라도 있는지 확인
                Instant saveTime = timestamp.atZone(ZoneId.systemDefault()).toInstant();
                dataPoint.time(saveTime, WritePrecision.NS);

                // <<< 추가: 쓰기 직전 데이터 로깅 >>>
                log.debug("InfluxDB 쓰기 시도: {}", dataPoint.toLineProtocol());

                WriteApiBlocking writeApi = influxDBService.getWriteApi();
                writeApi.writePoint(influxDBService.getBucket(), influxDBService.getOrg(), dataPoint);

                log.info("OPC UA 데이터 직접 저장 완료: 시간={}, ", timestamp); // 필드 수 로깅 변경
            } else {
                log.warn("저장할 유효한 필드가 없어 InfluxDB 쓰기를 건너<0xEB><0x9C><0x8D>니다.");
            }

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
            if (data == null || data.isEmpty() || data.containsKey("message")) {
                log.warn("전송할 유효한 데이터가 없습니다 (DB 조회 결과: {})", data);
                // 데이터 없는 경우 프론트에 알릴지 여부 결정 (예: 빈 데이터 대신 상태 메시지 전송)
                // return; // 또는 빈 메시지라도 전송?
            }

            Map<String, Object> wsMessage = new HashMap<>();
            wsMessage.put("type", "opcua");
            // === 수정: DB 조회 결과의 시간 사용 시도 (없으면 현재 시간) ===
            Object dbTime = data.get("time"); // OpcuaInfluxDBService.queryData 에서 넣는 키 확인 필요
            if (dbTime instanceof LocalDateTime) {
                wsMessage.put("timestamp", dbTime.toString());
            } else if (dbTime instanceof Instant) { // Instant 타입일 수도 있음
                wsMessage.put("timestamp",
                        LocalDateTime.ofInstant((Instant) dbTime, ZoneId.systemDefault()).toString());
            } else {
                wsMessage.put("timestamp", LocalDateTime.now().toString());
                log.trace("DB 조회 결과에 유효한 'time' 필드가 없어 현재 시간 사용");
            }

            // 데이터 구조 정리 (DB 조회 결과 기준)
            Map<String, Object> cleanedData = new HashMap<>(data);
            // DB 조회 메타데이터 필드 제거
            cleanedData.remove("time"); // wsMessage.timestamp 로 옮겼으므로 제거
            cleanedData.remove("_time");
            cleanedData.remove("table");
            cleanedData.remove("result");
            cleanedData.remove("_start");
            cleanedData.remove("_stop");
            cleanedData.remove("_measurement");
            cleanedData.remove("message"); // "데이터가 없습니다" 메시지 제거

            Map<String, Object> opcuaData = new HashMap<>();
            opcuaData.put("OPC_UA", cleanedData);
            wsMessage.put("data", opcuaData);

            eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));
            log.info("프론트엔드로 데이터 전송 완료: 필드 수={}", cleanedData.size());

            // 디버깅 로그 (기존과 동일)
            // ...

        } catch (Exception e) {
            log.error("데이터 전송 오류: {}", e.getMessage(), e);
        }
    }

    // 예시: 래퍼 클래스
    class TimestampedData {
        final Map<String, Map<String, Object>> data;
        final LocalDateTime timestamp;

        // 생성자, getter
        TimestampedData(Map<String, Map<String, Object>> data, LocalDateTime timestamp) {
            this.data = data;
            this.timestamp = timestamp;
        }

        Map<String, Map<String, Object>> getData() {
            return data;
        }

        LocalDateTime getTimestamp() {
            return timestamp;
        }
    }

}
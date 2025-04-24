package com.yth.realtime.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.yth.realtime.controller.OpcuaWebSocketHandler;
import com.yth.realtime.event.OpcuaDataEvent;
import com.yth.realtime.event.StartOpcuaCollectionEvent;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class OpcuaService {
    private static final Logger log = LoggerFactory.getLogger(OpcuaService.class);

    private final OpcuaClient opcuaClient;
    private final OpcuaWebSocketHandler webSocketHandler;
    private final OpcuaInfluxDBService influxDBService;
    private final ApplicationEventPublisher eventPublisher;

    ExecutorService collectorPool = Executors.newFixedThreadPool(1); // 수집 병렬 스레드 4개
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService saveExecutor = Executors.newFixedThreadPool(8); // 저장 스레드
    ExecutorService storageExecutor = Executors.newFixedThreadPool(16);

    ExecutorService sendExecutor = Executors.newFixedThreadPool(1);
    private final BlockingQueue<TimestampedData> saveQueue = new LinkedBlockingQueue<>(4096 * 2);

    private ScheduledFuture<?> dataCollectionTask;
    private boolean autoReconnect = true;

    // 클래스 멤버 변수 추가
    private long firstDisconnectTime = 0L;
    private long lastReconnectAttempt = 0L;

    @Autowired
    public OpcuaService(OpcuaClient opcuaClient, OpcuaWebSocketHandler opcuaWebSocketHandler,
            OpcuaInfluxDBService opcuaInfluxDBService, ApplicationEventPublisher eventPublisher) {
        this.opcuaClient = opcuaClient;
        this.webSocketHandler = opcuaWebSocketHandler;
        this.influxDBService = opcuaInfluxDBService;
        this.eventPublisher = eventPublisher;
        // 비동기 Write API 생성
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

    public void startDataCollection() {
        stopDataCollection(); // 중복 방지

        log.info("✅ 데이터 수집 시작 (5ms 주기 스케줄링)");

        dataCollectionTask = scheduler.scheduleAtFixedRate(() -> {
            long cycleStartTime = System.nanoTime(); // 더 정밀한 시간 측정

            try {
                // 1. 연결 확인
                // if (!opcuaClient.isConnected()) {
                // log.warn("OPC UA 서버 연결 끊김 → 재연결 시도");
                // if (autoReconnect) {
                // opcuaClient.connect(); // 연결 시도 (동기 방식이므로 완료될 때까지 대기)
                // }
                // // 연결 실패 시 다음 주기로 넘어감 (재연결 로직 보완 필요 시 여기에 추가)
                // if (!opcuaClient.isConnected()) {
                // log.warn("OPC UA 서버 재연결 실패. 다음 주기에 다시 시도합니다.");
                // return; // 현재 주기 작업 중단
                // }
                // }

                if (!opcuaClient.isConnected()) {
                    long now = System.currentTimeMillis();

                    // 최초 연결 끊김 시점 기록
                    if (firstDisconnectTime == 0L) {
                        firstDisconnectTime = now;
                    }

                    long timeSinceDisconnect = now - firstDisconnectTime;

                    long retryInterval;
                    if (timeSinceDisconnect < TimeUnit.MINUTES.toMillis(15)) {
                        retryInterval = 5; // 5ms
                    } else if (timeSinceDisconnect < TimeUnit.MINUTES.toMillis(30)) {
                        retryInterval = TimeUnit.MINUTES.toMillis(5); // 5분
                    } else {
                        retryInterval = TimeUnit.MINUTES.toMillis(10); // 10분
                    }

                    // 마지막 재시도 이후 지정한 간격이 지났으면 재시도
                    if (now - lastReconnectAttempt >= retryInterval) {
                        lastReconnectAttempt = now;

                        log.warn("OPC UA 서버 재연결 시도 (재시도 간격: {}ms)", retryInterval);
                        if (autoReconnect) {
                            opcuaClient.connect();
                        }
                    }

                    // 연결에 실패했으면 다음 주기로 넘어감
                    if (!opcuaClient.isConnected()) {
                        log.warn("OPC UA 서버 재연결 실패. 다음 주기에 다시 시도합니다.");
                        return;
                    }

                    // 재연결 성공 시 시간 초기화
                    firstDisconnectTime = 0L;
                    lastReconnectAttempt = 0L;
                }

                // 2. 데이터 읽기
                long readStartTime = System.nanoTime();
                // readAllValues()가 TimeoutException 던질 수 있음
                Map<String, Map<String, Object>> data = opcuaClient.readAllValues();
                long readEndTime = System.nanoTime();
                long readDurationMs = TimeUnit.NANOSECONDS.toMillis(readEndTime - readStartTime);
                // log.debug("readAllValues() 소요 시간: {} ms", readDurationMs);

                // 3. 큐에 데이터 넣기
                if (data != null && !data.isEmpty()) { // null 또는 빈 데이터 체크 강화
                    LocalDateTime collectionTimestamp = LocalDateTime.now();
                    try {
                        // saveQueue가 가득 차면 offer가 false 반환 (put은 블로킹)
                        // offer 대신 put을 사용하여 큐가 비워질 때까지 기다리거나,
                        // offer 사용 시 실패 로그를 남기고 데이터를 버리는 전략 선택 필요
                        // 여기서는 put 사용 (이전 로직과 동일하게)
                        saveQueue.put(new TimestampedData(data, collectionTimestamp));
                    } catch (InterruptedException e) {
                        log.warn("saveQueue.put() 대기 중 인터럽트 발생. 데이터 유실 가능성 있음.", e);
                        Thread.currentThread().interrupt(); // 인터럽트 상태 복원
                        // 스케줄된 작업 내에서는 break 대신 return 사용 고려
                        return; // 현재 작업 중단
                    } catch (Exception queueEx) {
                        log.error("saveQueue에 데이터 넣는 중 예상치 못한 오류 발생", queueEx);
                        // 필요 시 추가 오류 처리
                    }
                } else {
                    log.warn("OPC UA 서버로부터 유효한 데이터를 읽어오지 못했습니다. (결과: {})", data);
                }

            } catch (Exception e) {
                // readAllValues 타임아웃 등 모든 예외 처리
                log.error("데이터 수집 주기 작업 중 오류 발생: {}", e.getMessage(), e);
                // 특정 예외(예: TimeoutException)에 따라 재연결 시도 등 추가 로직 가능
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt(); // 인터럽트 상태 복원
                }
                // 여기서 오류가 발생해도 스케줄러는 다음 주기에 작업을 계속 실행함
            } finally {
                long cycleEndTime = System.nanoTime();
                long cycleDurationMs = TimeUnit.NANOSECONDS.toMillis(cycleEndTime - cycleStartTime);
                if (cycleDurationMs > 5) { // 5ms 이상 걸리면 경고
                    // 로깅로깅
                    // log.warn("데이터 수집 주기 작업 시간 초과: {}ms (설정: 5ms)", cycleDurationMs);
                }
                // log.debug("데이터 수집 주기 완료. 총 {} ms", cycleDurationMs);
            }
        }, 0, 5, TimeUnit.MILLISECONDS); // 초기 지연 0ms, 주기 5ms
        // ===========================================================

        log.info("✅ 저장 스레드 8개 시작됨 (병렬 처리)"); // 저장 스레드 시작 로그는 유지

        // ✅ 2. 저장 스레드 (기존 로직 유지)
        for (int i = 0; i < 8; i++) {
            saveExecutor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        List<TimestampedData> batch = new ArrayList<>();
                        saveQueue.drainTo(batch, 100);

                        if (batch.isEmpty()) {
                            Thread.sleep(10);
                            continue;
                        }

                        for (TimestampedData data : batch) {
                            storageExecutor.submit(() -> {
                                try {
                                    saveToInfluxDB(data.getData(), data.getTimestamp());
                                } catch (Exception e) {
                                    log.error("저장 작업 제출 또는 실행 중 오류 (Timestamp: {}): {}",
                                            data != null ? data.getTimestamp() : "unknown", e.getMessage(), e);
                                }
                            });
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("saveExecutor 스레드 인터럽트 발생. 종료 중...", e);
                        break;
                    } catch (Exception e) {
                        log.error("saveExecutor 배치 처리 루프 오류: {}", e.getMessage(), e);
                    }
                }
                log.info("saveExecutor 스레드 종료됨.");
            });
        }
        // ✅ 3. 전송 스레드는 이미 제거됨
    }

    public void stopDataCollection() {
        // dataCollectionTask 는 이제 scheduleAtFixedRate의 결과이므로, cancel 동작은 동일함
        if (dataCollectionTask != null && !dataCollectionTask.isDone()) {
            dataCollectionTask.cancel(false); // 진행 중인 작업은 완료하도록 false 사용 (true는 즉시 중단 시도)
            log.info("데이터 수집 작업 중단됨");
        }
    }

    @PreDestroy
    public void cleanup() {
        stopDataCollection();
        // --- 수정: collectorPool 제거, scheduler 종료 추가 ---
        // if (collectorPool != null) collectorPool.shutdownNow();
        shutdownExecutorService(scheduler, "Scheduler"); // 스케줄러 종료
        // ============================================
        shutdownExecutorService(saveExecutor, "SaveExecutor"); // 종료 로직 헬퍼 메서드 사용
        shutdownExecutorService(storageExecutor, "StorageExecutor"); // 종료 로직 헬퍼 메서드 사용
        // if (sendExecutor != null) sendExecutor.shutdownNow(); // 제거됨

        opcuaClient.disconnect();
        webSocketHandler.clearAllSessions();

        if (influxDBService.getAsyncWriteApi() != null) {
            try {
                influxDBService.getAsyncWriteApi().flush();
                influxDBService.getAsyncWriteApi().close();
                log.info("InfluxDB Async Write API flushed and closed.");
            } catch (Exception e) {
                log.error("Error closing InfluxDB Async Write API", e);
            }
        }
    }

    // ExecutorService 종료를 위한 헬퍼 메서드
    private void shutdownExecutorService(ExecutorService executor, String name) {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdown(); // 진행 중인 작업 완료 후 종료
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) { // 5초간 대기
                    executor.shutdownNow(); // 강제 종료
                    log.warn("{} 강제 종료됨.", name);
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS))
                        log.error("{} 가 정상적으로 종료되지 않음.", name);
                } else {
                    log.info("{} 정상 종료됨.", name);
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                log.error("{} 종료 중 인터럽트 발생.", name, ie);
            }
        }
    }

    private void saveToInfluxDB(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
        if (allData == null || allData.isEmpty())
            return;

        try {
            Map<String, Object> flattenedData = flattenData(allData);
            if (flattenedData.isEmpty())
                return;

            Point point = Point.measurement("opcua_data")
                    .addTag("system", "PCS_System")
                    .time(timestamp.atZone(ZoneId.systemDefault()).toInstant(), WritePrecision.NS);

            int fieldsAdded = 0;

            for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
                String field = entry.getKey().replaceAll("[^a-zA-Z0-9_]", "_");
                Object value = entry.getValue();

                if (value instanceof Number) {
                    double doubleValue = ((Number) value).doubleValue();
                    if (!Double.isNaN(doubleValue) && !Double.isInfinite(doubleValue)) {
                        point.addField(field, doubleValue);
                        fieldsAdded++;
                    } else {
                        log.warn("[SAVE_DB] Skipping invalid number (NaN or Infinite) for field '{}', timestamp {}",
                                field, timestamp);
                    }
                } else if (value instanceof Boolean) {
                    point.addField(field, (Boolean) value);
                    fieldsAdded++;
                }
            }

            if (fieldsAdded > 0) {
                influxDBService.getAsyncWriteApi().writePoint(
                        influxDBService.getBucket(),
                        influxDBService.getOrg(),
                        point);
                // 저장 확인
                // 로깅로깅
                // log.debug("✅ 비동기 InfluxDB 저장 요청 완료: {}", timestamp);
            } else {
                log.warn("[SAVE_DB] No valid fields found to write for timestamp {}. Skipping writePoint.", timestamp);
            }

        } catch (Exception e) {
            log.error("❌ InfluxDB 비동기 저장 중 오류 (Timestamp: {}): {}", timestamp, e.getMessage(), e);
        }
    }

    /**
     * 중첩된 맵 구조 평탄화
     */
    private Map<String, Object> flattenData(Map<String, Map<String, Object>> nestedData) {
        Map<String, Object> flattenedData = new HashMap<>();
        if (nestedData == null)
            return flattenedData; // Null 체크 추가

        for (Map.Entry<String, Map<String, Object>> groupEntry : nestedData.entrySet()) {
            String groupName = groupEntry.getKey();
            Map<String, Object> groupData = groupEntry.getValue();

            if (groupData != null) { // groupData Null 체크 추가
                for (Map.Entry<String, Object> fieldEntry : groupData.entrySet()) {
                    String fieldName = fieldEntry.getKey();
                    Object fieldValue = fieldEntry.getValue();

                    String flatKey = fieldName;
                    flattenedData.put(flatKey, fieldValue);
                }
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

    @PostConstruct
    public void initializeAndStartCollection() {
        log.info("애플리케이션 시작됨. OPC UA 연결 및 데이터 수집 시작 시도...");
        if (connect()) { // OPC UA 서버 연결 시도
            log.info("OPC UA 서버 연결 성공.");
            startDataCollection(); // 데이터 수집 시작
        } else {
            log.error("OPC UA 서버 초기 연결 실패. 데이터 수집을 시작할 수 없습니다. (자동 재연결은 시도될 수 있음)");
            // 연결 실패 시 추가적인 처리 (예: 재시도 로직, 상태 알림 등) 필요 시 여기에 구현
        }
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

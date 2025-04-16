
//틱
// package com.yth.realtime.service;

// import java.time.Instant;
// import java.time.LocalDateTime;
// import java.time.ZoneId;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.BlockingQueue;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.LinkedBlockingQueue;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.ScheduledFuture;
// import java.util.concurrent.TimeUnit;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.context.ApplicationEventPublisher;
// import org.springframework.context.event.EventListener;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.yth.realtime.controller.OpcuaWebSocketHandler;
// import com.yth.realtime.event.OpcuaDataEvent;
// import com.yth.realtime.event.StartOpcuaCollectionEvent;

// import jakarta.annotation.PostConstruct;
// import jakarta.annotation.PreDestroy;

// @Service
// public class OpcuaService {
//     private static final Logger log = LoggerFactory.getLogger(OpcuaService.class);

//     private final OpcuaClient opcuaClient;
//     private final OpcuaWebSocketHandler webSocketHandler;
//     private final OpcuaInfluxDBService influxDBService;
//     private final ApplicationEventPublisher eventPublisher;

//     ExecutorService collectorPool = Executors.newFixedThreadPool(4); // 수집 병렬 스레드 4개
//     private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//     private final ExecutorService saveExecutor = Executors.newFixedThreadPool(8); // 저장 스레드
//     ExecutorService storageExecutor = Executors.newFixedThreadPool(16);

//     ExecutorService sendExecutor = Executors.newFixedThreadPool(1);
//     private final BlockingQueue<TimestampedData> saveQueue = new LinkedBlockingQueue<>(1000);

//     private ScheduledFuture<?> dataCollectionTask;
//     private boolean autoReconnect = true;

//     @Autowired
//     public OpcuaService(OpcuaClient opcuaClient, OpcuaWebSocketHandler opcuaWebSocketHandler,
//             OpcuaInfluxDBService opcuaInfluxDBService, ApplicationEventPublisher eventPublisher) {
//         this.opcuaClient = opcuaClient;
//         this.webSocketHandler = opcuaWebSocketHandler;
//         this.influxDBService = opcuaInfluxDBService;
//         this.eventPublisher = eventPublisher;
//         // 비동기 Write API 생성
//     }

//     /**
//      * OPC UA 서버 연결
//      */
//     public boolean connect() {
//         return opcuaClient.connect();
//     }

//     /**
//      * OPC UA 서버 연결 해제
//      */
//     public void disconnect() {
//         opcuaClient.disconnect();
//     }

//     public void startDataCollection() {
//         stopDataCollection(); // 중복 방지

//         log.info("✅ 데이터 수집 시작 (5ms 주기 스케줄링)");

//         dataCollectionTask = scheduler.scheduleAtFixedRate(() -> {
//             long cycleStartTime = System.nanoTime(); // 더 정밀한 시간 측정
//             try {
//                 // 1. 연결 확인
//                 if (!opcuaClient.isConnected()) {
//                     log.warn("OPC UA 서버 연결 끊김 → 재연결 시도");
//                     if (autoReconnect) {
//                         opcuaClient.connect(); // 연결 시도 (동기 방식이므로 완료될 때까지 대기)
//                     }
//                     // 연결 실패 시 다음 주기로 넘어감 (재연결 로직 보완 필요 시 여기에 추가)
//                     if (!opcuaClient.isConnected()) {
//                         log.warn("OPC UA 서버 재연결 실패. 다음 주기에 다시 시도합니다.");
//                         return; // 현재 주기 작업 중단
//                     }
//                 }

//                 // 2. 데이터 읽기
//                 long readStartTime = System.nanoTime();
//                 // readAllValues()가 TimeoutException 던질 수 있음
//                 Map<String, Map<String, Object>> data = opcuaClient.readAllValues();
//                 long readEndTime = System.nanoTime();
//                 long readDurationMs = TimeUnit.NANOSECONDS.toMillis(readEndTime - readStartTime);
//                 // log.debug("readAllValues() 소요 시간: {} ms", readDurationMs);

//                 // 3. 큐에 데이터 넣기
//                 if (data != null && !data.isEmpty()) { // null 또는 빈 데이터 체크 강화
//                     LocalDateTime collectionTimestamp = LocalDateTime.now();
//                     try {
//                         // saveQueue가 가득 차면 offer가 false 반환 (put은 블로킹)
//                         // offer 대신 put을 사용하여 큐가 비워질 때까지 기다리거나,
//                         // offer 사용 시 실패 로그를 남기고 데이터를 버리는 전략 선택 필요
//                         // 여기서는 put 사용 (이전 로직과 동일하게)
//                         saveQueue.put(new TimestampedData(data, collectionTimestamp));
//                     } catch (InterruptedException e) {
//                         log.warn("saveQueue.put() 대기 중 인터럽트 발생. 데이터 유실 가능성 있음.", e);
//                         Thread.currentThread().interrupt(); // 인터럽트 상태 복원
//                         // 스케줄된 작업 내에서는 break 대신 return 사용 고려
//                         return; // 현재 작업 중단
//                     } catch (Exception queueEx) {
//                         log.error("saveQueue에 데이터 넣는 중 예상치 못한 오류 발생", queueEx);
//                         // 필요 시 추가 오류 처리
//                     }
//                 } else {
//                     log.warn("OPC UA 서버로부터 유효한 데이터를 읽어오지 못했습니다. (결과: {})", data);
//                 }

//             } catch (Exception e) {
//                 // readAllValues 타임아웃 등 모든 예외 처리
//                 log.error("데이터 수집 주기 작업 중 오류 발생: {}", e.getMessage(), e);
//                 // 특정 예외(예: TimeoutException)에 따라 재연결 시도 등 추가 로직 가능
//                 if (e instanceof InterruptedException) {
//                     Thread.currentThread().interrupt(); // 인터럽트 상태 복원
//                 }
//                 // 여기서 오류가 발생해도 스케줄러는 다음 주기에 작업을 계속 실행함
//             } finally {
//                 long cycleEndTime = System.nanoTime();
//                 long cycleDurationMs = TimeUnit.NANOSECONDS.toMillis(cycleEndTime - cycleStartTime);
//                 if (cycleDurationMs > 5) { // 5ms 이상 걸리면 경고
//                     log.warn("데이터 수집 주기 작업 시간 초과: {}ms (설정: 5ms)", cycleDurationMs);
//                 }
//                 // log.debug("데이터 수집 주기 완료. 총 {} ms", cycleDurationMs);
//             }
//         }, 0, 5, TimeUnit.MILLISECONDS); // 초기 지연 0ms, 주기 5ms
//         // ===========================================================

//         log.info("✅ 저장 스레드 8개 시작됨 (병렬 처리)"); // 저장 스레드 시작 로그는 유지

//         // ✅ 2. 저장 스레드 (기존 로직 유지)
//         for (int i = 0; i < 8; i++) {
//             saveExecutor.submit(() -> {
//                 while (!Thread.currentThread().isInterrupted()) {
//                     try {
//                         List<TimestampedData> batch = new ArrayList<>();
//                         saveQueue.drainTo(batch, 100);

//                         if (batch.isEmpty()) {
//                             Thread.sleep(10);
//                             continue;
//                         }

//                         for (TimestampedData data : batch) {
//                             storageExecutor.submit(() -> {
//                                 try {
//                                     saveToInfluxDB(data.getData(), data.getTimestamp());
//                                 } catch (Exception e) {
//                                     log.error("저장 작업 제출 또는 실행 중 오류 (Timestamp: {}): {}",
//                                             data != null ? data.getTimestamp() : "unknown", e.getMessage(), e);
//                                 }
//                             });
//                         }

//                     } catch (InterruptedException e) {
//                         Thread.currentThread().interrupt();
//                         log.warn("saveExecutor 스레드 인터럽트 발생. 종료 중...", e);
//                         break;
//                     } catch (Exception e) {
//                         log.error("saveExecutor 배치 처리 루프 오류: {}", e.getMessage(), e);
//                     }
//                 }
//                 log.info("saveExecutor 스레드 종료됨.");
//             });
//         }
//         // ✅ 3. 전송 스레드는 이미 제거됨
//     }

//     public void stopDataCollection() {
//         // dataCollectionTask 는 이제 scheduleAtFixedRate의 결과이므로, cancel 동작은 동일함
//         if (dataCollectionTask != null && !dataCollectionTask.isDone()) {
//             dataCollectionTask.cancel(false); // 진행 중인 작업은 완료하도록 false 사용 (true는 즉시 중단 시도)
//             log.info("데이터 수집 작업 중단됨");
//         }
//     }

//     @PreDestroy
//     public void cleanup() {
//         stopDataCollection();
//         // --- 수정: collectorPool 제거, scheduler 종료 추가 ---
//         // if (collectorPool != null) collectorPool.shutdownNow();
//         shutdownExecutorService(scheduler, "Scheduler"); // 스케줄러 종료
//         // ============================================
//         shutdownExecutorService(saveExecutor, "SaveExecutor"); // 종료 로직 헬퍼 메서드 사용
//         shutdownExecutorService(storageExecutor, "StorageExecutor"); // 종료 로직 헬퍼 메서드 사용
//         // if (sendExecutor != null) sendExecutor.shutdownNow(); // 제거됨

//         opcuaClient.disconnect();
//         webSocketHandler.clearAllSessions();

//         if (influxDBService.getAsyncWriteApi() != null) {
//             try {
//                 influxDBService.getAsyncWriteApi().flush();
//                 influxDBService.getAsyncWriteApi().close();
//                 log.info("InfluxDB Async Write API flushed and closed.");
//             } catch (Exception e) {
//                 log.error("Error closing InfluxDB Async Write API", e);
//             }
//         }
//     }

//     // ExecutorService 종료를 위한 헬퍼 메서드
//     private void shutdownExecutorService(ExecutorService executor, String name) {
//         if (executor != null && !executor.isShutdown()) {
//             executor.shutdown(); // 진행 중인 작업 완료 후 종료
//             try {
//                 if (!executor.awaitTermination(5, TimeUnit.SECONDS)) { // 5초간 대기
//                     executor.shutdownNow(); // 강제 종료
//                     log.warn("{} 강제 종료됨.", name);
//                     if (!executor.awaitTermination(5, TimeUnit.SECONDS))
//                         log.error("{} 가 정상적으로 종료되지 않음.", name);
//                 } else {
//                     log.info("{} 정상 종료됨.", name);
//                 }
//             } catch (InterruptedException ie) {
//                 executor.shutdownNow();
//                 Thread.currentThread().interrupt();
//                 log.error("{} 종료 중 인터럽트 발생.", name, ie);
//             }
//         }
//     }

//     private void saveToInfluxDB(Map<String, Map<String, Object>> allData, LocalDateTime timestamp) {
//         if (allData == null || allData.isEmpty())
//             return;

//         try {
//             Map<String, Object> flattenedData = flattenData(allData);
//             if (flattenedData.isEmpty())
//                 return;

//             Point point = Point.measurement("opcua_data")
//                     .addTag("system", "PCS_System")
//                     .time(timestamp.atZone(ZoneId.systemDefault()).toInstant(), WritePrecision.NS);

//             int fieldsAdded = 0;

//             for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
//                 String field = entry.getKey().replaceAll("[^a-zA-Z0-9_]", "_");
//                 Object value = entry.getValue();

//                 if (value instanceof Number) {
//                     double doubleValue = ((Number) value).doubleValue();
//                     if (!Double.isNaN(doubleValue) && !Double.isInfinite(doubleValue)) {
//                         point.addField(field, doubleValue);
//                         fieldsAdded++;
//                     } else {
//                         log.warn("[SAVE_DB] Skipping invalid number (NaN or Infinite) for field '{}', timestamp {}",
//                                 field, timestamp);
//                     }
//                 } else if (value instanceof Boolean) {
//                     point.addField(field, (Boolean) value);
//                     fieldsAdded++;
//                 }
//             }

//             if (fieldsAdded > 0) {
//                 influxDBService.getAsyncWriteApi().writePoint(
//                         influxDBService.getBucket(),
//                         influxDBService.getOrg(),
//                         point);
//             } else {
//                 log.warn("[SAVE_DB] No valid fields found to write for timestamp {}. Skipping writePoint.", timestamp);
//             }

//         } catch (Exception e) {
//             log.error("❌ InfluxDB 비동기 저장 중 오류 (Timestamp: {}): {}", timestamp, e.getMessage(), e);
//         }
//     }

//     /**
//      * 중첩된 맵 구조 평탄화
//      */
//     private Map<String, Object> flattenData(Map<String, Map<String, Object>> nestedData) {
//         Map<String, Object> flattenedData = new HashMap<>();
//         if (nestedData == null)
//             return flattenedData; // Null 체크 추가

//         for (Map.Entry<String, Map<String, Object>> groupEntry : nestedData.entrySet()) {
//             String groupName = groupEntry.getKey();
//             Map<String, Object> groupData = groupEntry.getValue();

//             if (groupData != null) { // groupData Null 체크 추가
//                 for (Map.Entry<String, Object> fieldEntry : groupData.entrySet()) {
//                     String fieldName = fieldEntry.getKey();
//                     Object fieldValue = fieldEntry.getValue();

//                     String flatKey = fieldName;
//                     flattenedData.put(flatKey, fieldValue);
//                 }
//             }
//         }
//         return flattenedData;
//     }

//     /**
//      * 특정 그룹의 데이터 조회
//      */
//     public Map<String, Object> getGroupData(String groupName) {
//         return opcuaClient.readGroupValues(groupName);
//     }

//     /**
//      * 모든 그룹의 데이터 조회
//      */
//     public Map<String, Map<String, Object>> getAllData() {
//         return opcuaClient.readAllValues();
//     }

//     /**
//      * 연결 상태 확인
//      */
//     public boolean isConnected() {
//         return opcuaClient.isConnected();
//     }

//     /**
//      * 자동 재연결 설정
//      */
//     public void setAutoReconnect(boolean autoReconnect) {
//         this.autoReconnect = autoReconnect;
//     }

//     /**
//      * 자동 재연결 상태 확인
//      */
//     public boolean isAutoReconnect() {
//         return autoReconnect;
//     }

//     @PostConstruct
//     public void initializeAndStartCollection() {
//         log.info("애플리케이션 시작됨. OPC UA 연결 및 데이터 수집 시작 시도...");
//         if (connect()) { // OPC UA 서버 연결 시도
//             log.info("OPC UA 서버 연결 성공.");
//             startDataCollection(); // 데이터 수집 시작
//         } else {
//             log.error("OPC UA 서버 초기 연결 실패. 데이터 수집을 시작할 수 없습니다. (자동 재연결은 시도될 수 있음)");
//             // 연결 실패 시 추가적인 처리 (예: 재시도 로직, 상태 알림 등) 필요 시 여기에 구현
//         }
//     }

//     @EventListener
//     public void onStartCollection(StartOpcuaCollectionEvent event) {
//         // log.info("StartOpcuaCollectionEvent 수신 → connect + startDataCollection 실행");
//         connect();
//         startDataCollection();
//         // startSubscriptionBasedCollection(); // ✅ 구독 방식 사용
//     }

//     /**
//      * 프론트엔드로 데이터 전송 메서드
//      * 
//      * @param data 전송할 OPC UA 데이터
//      */
//     private void sendDataToFrontend(Map<String, Object> data) {
//         try {
//             if (data == null || data.isEmpty() || data.containsKey("message")) {
//                 log.warn("전송할 유효한 데이터가 없습니다 (DB 조회 결과: {})", data);
//                 // 데이터 없는 경우 프론트에 알릴지 여부 결정 (예: 빈 데이터 대신 상태 메시지 전송)
//                 // return; // 또는 빈 메시지라도 전송?
//             }

//             Map<String, Object> wsMessage = new HashMap<>();
//             wsMessage.put("type", "opcua");
//             // === 수정: DB 조회 결과의 시간 사용 시도 (없으면 현재 시간) ===
//             Object dbTime = data.get("time"); // OpcuaInfluxDBService.queryData 에서 넣는 키 확인 필요
//             if (dbTime instanceof LocalDateTime) {
//                 wsMessage.put("timestamp", dbTime.toString());
//             } else if (dbTime instanceof Instant) { // Instant 타입일 수도 있음
//                 wsMessage.put("timestamp",
//                         LocalDateTime.ofInstant((Instant) dbTime, ZoneId.systemDefault()).toString());
//             } else {
//                 wsMessage.put("timestamp", LocalDateTime.now().toString());
//                 log.trace("DB 조회 결과에 유효한 'time' 필드가 없어 현재 시간 사용");
//             }

//             // 데이터 구조 정리 (DB 조회 결과 기준)
//             Map<String, Object> cleanedData = new HashMap<>(data);
//             // DB 조회 메타데이터 필드 제거
//             cleanedData.remove("time"); // wsMessage.timestamp 로 옮겼으므로 제거
//             cleanedData.remove("_time");
//             cleanedData.remove("table");
//             cleanedData.remove("result");
//             cleanedData.remove("_start");
//             cleanedData.remove("_stop");
//             cleanedData.remove("_measurement");
//             cleanedData.remove("message"); // "데이터가 없습니다" 메시지 제거

//             Map<String, Object> opcuaData = new HashMap<>();
//             opcuaData.put("OPC_UA", cleanedData);
//             wsMessage.put("data", opcuaData);

//             eventPublisher.publishEvent(new OpcuaDataEvent(this, wsMessage));
//             log.info("프론트엔드로 데이터 전송 완료: 필드 수={}", cleanedData.size());

//             // 디버깅 로그 (기존과 동일)
//             // ...

//         } catch (Exception e) {
//             log.error("데이터 전송 오류: {}", e.getMessage(), e);
//         }
//     }

//     // 예시: 래퍼 클래스
//     class TimestampedData {
//         final Map<String, Map<String, Object>> data;
//         final LocalDateTime timestamp;

//         // 생성자, getter
//         TimestampedData(Map<String, Map<String, Object>> data, LocalDateTime timestamp) {
//             this.data = data;
//             this.timestamp = timestamp;
//         }

//         Map<String, Map<String, Object>> getData() {
//             return data;
//         }

//         LocalDateTime getTimestamp() {
//             return timestamp;
//         }
//     }

// }




package com.yth.realtime.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
// import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
// import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

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
    // private final Set<LocalDateTime> seenTimestamps =
    // ConcurrentHashMap.newKeySet();
    // 중복 제거가 가능한 큐
    // private final LinkedBlockingDeque<LocalDateTime> sendQueue = new
    // LinkedBlockingDeque<>(1000);

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
                    long cycleStartTimeNanos = System.nanoTime(); // <<<--- 사이클 시작 시간 (나노초)
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

                        //추가 로깅
                        long cycleEndTimeNanos = System.nanoTime(); // <<<--- 사이클 종료 시간 (나노초)
                        double cycleDurationMs = (cycleEndTimeNanos - cycleStartTimeNanos) / 1_000_000.0; // 밀리초 변환

                        // 수집 주기 시간 로그 출력 (DEBUG 레벨 추천)
                        log.debug("수집 주기 완료 (스레드 {}): {} ms", Thread.currentThread().getName(), cycleDurationMs);

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
                            Thread.sleep(10);
                            continue;
                        }

                        for (TimestampedData data : batch) {
                            storageExecutor.submit(() -> {
                                try {
                                    saveToInfluxDB(data.getData(), data.getTimestamp());

                                    // ✅ 중복된 timestamp가 큐에 들어가지 않도록 처리
                                    // if (seenTimestamps.add(data.getTimestamp())) {
                                    // sendQueue.put(data.getTimestamp()); // 새 타임스탬프만 큐에 넣음
                                    // }

                                } catch (Exception e) {
                                    // saveToInfluxDB 내부 오류 또는 submit 자체 오류 처리
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
                        // drainTo, submit 등에서 발생 가능한 예외 처리
                        log.error("saveExecutor 배치 처리 루프 오류: {}", e.getMessage(), e);
                    }
                }
                log.info("saveExecutor 스레드 종료됨.");
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
        // sendExecutor.submit(() -> {
        // while (!Thread.currentThread().isInterrupted()) {
        // try {
        // LocalDateTime ts = sendQueue.take(); // 트리거 타임스탬프 꺼냄

        // // ✅ 전송이 끝났으면 중복 체크용 Set에서 제거
        // // seenTimestamps.remove(ts);

        // // Map<String, Object> latest = influxDBService.getLatestOpcuaData("all");
        // // sendDataToFrontend(latest);

        // } catch (InterruptedException e) {
        // Thread.currentThread().interrupt();
        // break;
        // } catch (Exception e) {
        // log.error("조회/전송 오류", e);
        // }
        // }
        // });
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
        if (collectorPool != null)
            collectorPool.shutdownNow(); // 수집 풀 종료
        if (saveExecutor != null)
            saveExecutor.shutdownNow();
        if (storageExecutor != null)
            storageExecutor.shutdownNow();
        // if (sendExecutor != null)
        // sendExecutor.shutdownNow();
        opcuaClient.disconnect();
        webSocketHandler.clearAllSessions();
        // saveExecutor.shutdownNow();
        // sendExecutor.shutdownNow();
        // storageExecutor.shutdownNow(); // 💡 추가됨
        if (influxDBService.getAsyncWriteApi() != null) { // Null 체크 추가
            influxDBService.getAsyncWriteApi().flush(); // 남은 데이터 저장
            influxDBService.getAsyncWriteApi().close();
        }
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
    // private void saveToInfluxDB(Map<String, Map<String, Object>> allData,
    // LocalDateTime timestamp) {
    // // ❗ [로그 추가] 메서드 시작 및 입력 데이터 확인
    // log.info("[SAVE_DB] Timestamp: {}, Received raw data size: {}", timestamp,
    // (allData != null ? allData.size() : "null"));
    // if (allData == null || allData.isEmpty()) {
    // log.warn("[SAVE_DB] Timestamp: {}, Raw data is null or empty. Skipping
    // save.", timestamp);
    // return;
    // }

    // try {
    // // 데이터를 평탄화
    // Map<String, Object> flattenedData = flattenData(allData);
    // // ❗ [로그 추가] 평탄화 결과 확인
    // log.debug("[SAVE_DB] Timestamp: {}, Flattened data size: {}", timestamp,
    // flattenedData.size());
    // // log.trace("[SAVE_DB] Timestamp: {}, Flattened data: {}", timestamp,
    // // flattenedData); // 필요시 상세 데이터 로깅

    // if (flattenedData.isEmpty()) {
    // log.warn("[SAVE_DB] Timestamp: {}, Flattened data is empty. Skipping save.",
    // timestamp);
    // return;
    // }

    // Point dataPoint = Point.measurement("opcua_data")
    // .addTag("system", "PCS_System");

    // int addedFieldsCount = 0; // ❗ 필드 추가 개수 카운트
    // for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
    // String fieldName = entry.getKey();
    // Object value = entry.getValue();
    // String originalFieldName = fieldName; // 로깅을 위해 원본 이름 저장

    // // 필드명 정리
    // fieldName = fieldName.replaceAll("[^a-zA-Z0-9_]", "_");

    // if (value == null) {
    // log.trace("[SAVE_DB] Timestamp: {}, Skipping null value for field: {}",
    // timestamp,
    // originalFieldName);
    // continue;
    // }

    // // ❗ [로그 추가] 필드 추가 시도 로깅 (TRACE 레벨)
    // log.trace(
    // "[SAVE_DB] Timestamp: {}, Attempting to add field: '{}' (cleaned: '{}'),
    // Value: '{}' (Type: {})",
    // timestamp, originalFieldName, fieldName, value,
    // value.getClass().getSimpleName());

    // boolean fieldAdded = false;
    // try {
    // if (value instanceof Double) {
    // Double doubleValue = (Double) value;
    // if (!Double.isNaN(doubleValue) && !Double.isInfinite(doubleValue)) {
    // dataPoint.addField(fieldName, doubleValue);
    // fieldAdded = true;
    // } else {
    // log.warn(
    // "[SAVE_DB] Timestamp: {}, Skipping invalid Double value (NaN or Infinite) for
    // field: {}",
    // timestamp, originalFieldName);
    // }
    // } else if (value instanceof Integer) {
    // dataPoint.addField(fieldName, (Integer) value);
    // fieldAdded = true;
    // } else if (value instanceof Long) {
    // dataPoint.addField(fieldName, (Long) value);
    // fieldAdded = true;
    // } else if (value instanceof Float) {
    // Float floatValue = (Float) value;
    // if (!Float.isNaN(floatValue) && !Float.isInfinite(floatValue)) {
    // dataPoint.addField(fieldName, floatValue); // Float 처리
    // fieldAdded = true;
    // } else {
    // log.warn(
    // "[SAVE_DB] Timestamp: {}, Skipping invalid Float value (NaN or Infinite) for
    // field: {}",
    // timestamp, originalFieldName);
    // }
    // } else if (value instanceof String) {
    // String strValue = (String) value;
    // if (!strValue.isEmpty()) {
    // try {
    // double numValue = Double.parseDouble(strValue);
    // if (!Double.isNaN(numValue) && !Double.isInfinite(numValue)) {
    // dataPoint.addField(fieldName, numValue);
    // fieldAdded = true;
    // } else {
    // log.warn(
    // "[SAVE_DB] Timestamp: {}, Skipping invalid Double value (NaN or Infinite)
    // parsed from string for field: {}",
    // timestamp, originalFieldName);
    // }
    // } catch (NumberFormatException e) {
    // // 문자열을 숫자로 변환 실패 시, 문자열 그대로 저장할지 결정
    // // dataPoint.addField(fieldName, strValue); // 필요하다면 주석 해제
    // // fieldAdded = true;
    // log.warn(
    // "[SAVE_DB] Timestamp: {}, Could not parse string '{}' to double for field
    // '{}'. Skipping or storing as string.",
    // timestamp, strValue, originalFieldName);
    // }
    // }
    // } else if (value instanceof Boolean) { // Boolean 타입 처리
    // dataPoint.addField(fieldName, (Boolean) value);
    // fieldAdded = true;
    // } else {
    // // 지원하지 않는 타입 로깅
    // log.warn("[SAVE_DB] Timestamp: {}, Unsupported data type '{}' for field '{}'.
    // Skipping.",
    // timestamp, value.getClass().getSimpleName(), originalFieldName);
    // }

    // if (fieldAdded) {
    // addedFieldsCount++;
    // log.trace("[SAVE_DB] Timestamp: {}, Successfully added field: '{}', Value:
    // {}", timestamp,
    // fieldName, value);
    // }
    // } catch (Exception fieldEx) {
    // log.error("[SAVE_DB] Timestamp: {}, Error adding field '{}' with value '{}':
    // {}", timestamp,
    // fieldName, value, fieldEx.getMessage(), fieldEx);
    // }
    // } // end of for loop

    // // ❗ [로그 추가] 최종 추가된 필드 수 확인
    // log.debug("[SAVE_DB] Timestamp: {}, Total valid fields added to Point: {}",
    // timestamp, addedFieldsCount);

    // if (addedFieldsCount > 0) { // ❗ 카운터로 확인
    // Instant saveTime = timestamp.atZone(ZoneId.systemDefault()).toInstant();
    // dataPoint.time(saveTime, WritePrecision.NS);

    // // ❗ [로그 추가] DB 쓰기 직전 데이터 (Line Protocol) 로깅
    // log.debug("[SAVE_DB] Timestamp: {}, Attempting InfluxDB write. Point data:
    // {}", timestamp,
    // dataPoint.toLineProtocol());

    // WriteApiBlocking writeApi = influxDBService.getWriteApi();
    // try {
    // writeApi.writePoint(influxDBService.getBucket(), influxDBService.getOrg(),
    // dataPoint);
    // // ❗ [로그 추가] DB 쓰기 성공
    // log.info("[SAVE_DB] Timestamp: {}, InfluxDB write successful.", timestamp);
    // } catch (Exception writeEx) {
    // // ❗ [로그 추가] DB 쓰기 실패 시 예외 상세 로깅
    // log.error("[SAVE_DB] Timestamp: {}, InfluxDB writePoint failed: {}",
    // timestamp,
    // writeEx.getMessage(), writeEx);
    // // 여기서 예외를 다시 던질지, 아니면 로깅만 할지 결정 필요 (현재는 로깅만 함)
    // throw writeEx; // ❗ 디버깅 위해 예외를 다시 던져서 상위 catch 블록에서 잡도록 변경 (원인 파악 후 제거 가능)
    // }

    // // 쓰기 성공 후 타임스탬프 처리 로직은 그대로 유지
    // if (seenTimestamps.add(timestamp)) {
    // try {
    // sendQueue.put(timestamp);
    // } catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // log.warn("[SAVE_DB] Interrupted while putting timestamp to sendQueue after
    // successful write.",
    // e);
    // }
    // }

    // } else {
    // log.warn("[SAVE_DB] Timestamp: {}, No valid fields were added. Skipping
    // InfluxDB write.", timestamp);
    // }

    // } catch (Exception e) {
    // // ❗ [로그 추가] saveToInfluxDB 메서드 전체를 감싸는 catch 블록
    // log.error("[SAVE_DB] Timestamp: {}, Unhandled exception in saveToInfluxDB:
    // {}", timestamp, e.getMessage(),
    // e);
    // // 이 예외가 발생하면 storageExecutor 스레드가 종료될 수 있음
    // }
    // }

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

            for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
                String field = entry.getKey().replaceAll("[^a-zA-Z0-9_]", "_");
                Object value = entry.getValue();

                if (value instanceof Number) {
                    point.addField(field, ((Number) value).doubleValue());
                } else if (value instanceof Boolean) {
                    point.addField(field, (Boolean) value);
                }
            }

            // ✅ 비동기 저장으로 변경
            influxDBService.getAsyncWriteApi().writePoint(
                    influxDBService.getBucket(),
                    influxDBService.getOrg(),
                    point);

            // 저장 확인
            log.debug("✅ 비동기 InfluxDB 저장 요청 완료: {}", timestamp);

        } catch (Exception e) {
            log.error("❌ InfluxDB 비동기 저장 중 오류: {}", e.getMessage(), e);
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
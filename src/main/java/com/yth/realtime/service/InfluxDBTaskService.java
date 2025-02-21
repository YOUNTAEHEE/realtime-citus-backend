package com.yth.realtime.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.influxdb.client.BucketsApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.query.FluxTable;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class InfluxDBTaskService {
    private final InfluxDBClient influxDBClient;
    private final Map<String, Long> lastAggregationTime = new ConcurrentHashMap<>(); // 마지막 집계 시간 저장
    private static final long MIN_INTERVAL = 60000; // 최소 집계 간격 (1분)

    // InfluxDB 버킷 정의
    private static final String SOURCE_BUCKET = "ydata"; // 원본 데이터 버킷
    private static final String AGG_BUCKET_2MIN = "ydata_2min_avg"; // 2분 평균 버킷
    private static final String AGG_BUCKET_4MIN = "ydata_4min_avg"; // 4분 평균 버킷
    private static final String ORG = "youn";
    private static final String MEASUREMENT_NAME = "sensor_data"; // 측정값 이름

    public InfluxDBTaskService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
    }

    @PostConstruct
    public void init() {
        try {
            initializeBuckets();
        } catch (Exception e) {
            log.error("❌ InfluxDBTaskService 초기화 실패", e);
            throw new RuntimeException("서비스 초기화 실패", e);
        }
    }

    private void initializeBuckets() {
        List.of(SOURCE_BUCKET, AGG_BUCKET_2MIN, AGG_BUCKET_4MIN).forEach(this::createBucketIfNotExists);
    }

    /**
     * ✅ 3분마다 실행 → `ydata`에서 최근 2분 데이터의 평균을 `ydata_2min_avg`에 저장
     */
    @Scheduled(fixedRate = 181000, initialDelay = 61000)
    public void aggregateDataTo2Min() {
        // String taskId = "2min";
        // if (!canExecuteTask(taskId)) {
        //     log.debug("2분 평균 데이터 집계 스킵 (최소 간격 미달)");
        //     return;
        // }

        log.info("⏳ 2분 평균 데이터 저장 시작...");
        try {
            String fluxQuery = String.format("""
                    from(bucket: "%s")
                      |> range(start: -10m)
                      |> filter(fn: (r) => r._measurement == "sensor_data")
                      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
                      |> group(columns: ["device", "host", "_field"])
                       |> sort(columns: ["_time"])  // 시간순 오름차순 정렬
                      |> aggregateWindow(
                            every: 2m,
                            fn: mean,
                            createEmpty: true
                        )
                      |> duplicate(column: "_stop", as: "_time")  // 시간 컬럼 복제
                      |> group()
                      |> set(key: "_measurement", value: "sensor_data_avg")
                      |> drop(columns: ["_start", "_stop"])  // 불필요한 시간 컬럼 제거
                      |> to(
                          bucket: "%s",
                          org: "%s"
                        )
                    """,
                    SOURCE_BUCKET, AGG_BUCKET_2MIN, ORG);

            executeFluxQuery(fluxQuery);
            // updateLastExecutionTime(taskId);

            // ✅ 2분 평균 저장 후 `sensor_data`에서 해당 데이터 삭제
            deleteOldData(SOURCE_BUCKET, "sensor_data");
        } catch (Exception e) {
            log.error("2분 평균 데이터 집계 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * ✅ 6분마다 실행 → `ydata_2min_avg`에서 최근 4분 데이터의 평균을 `ydata_4min_avg`에 저장
     */
    @Scheduled(fixedRate = 303000, initialDelay = 303000)
    public void aggregateDataTo4Min() {
        // String taskId = "4min";
        // if (!canExecuteTask(taskId)) {
        //     log.debug("4분 평균 데이터 집계 스킵 (최소 간격 미달)");
        //     return;
        // }

        log.info("⏳ 4분 평균 데이터 저장 시작...");
        try {
            String fluxQuery = String.format("""
                                from(bucket: "%s")
                      |> range(start: -10m)
                      |> filter(fn: (r) => r._measurement == "sensor_data_avg")
                      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
                      |> group(columns: ["device", "host", "_field"])
                       |> sort(columns: ["_time"])  // 시간순 오름차순 정렬
                      |> aggregateWindow(
                            every: 4m,
                            fn: mean,
                            createEmpty: true
                        )
                      |> duplicate(column: "_stop", as: "_time")  // 시간 컬럼 복제
                      |> group()
                      |> set(key: "_measurement", value: "sensor_data_4min_avg")
                      |> drop(columns: ["_start", "_stop"])  // 불필요한 시간 컬럼 제거
                      |> to(
                          bucket: "%s",
                          org: "%s"
                        )
                    """,
                    AGG_BUCKET_2MIN, AGG_BUCKET_4MIN, ORG);

            executeFluxQuery(fluxQuery);
            // updateLastExecutionTime(taskId);
            // ✅ 4분 평균 저장 후 `ydata_2min_avg`에서 해당 데이터 삭제
            deleteOldData(AGG_BUCKET_2MIN, "sensor_data_avg");
        } catch (Exception e) {
            log.error("4분 평균 데이터 집계 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * ✅ InfluxDB에서 Flux Query 실행하는 메서드
     */
    public void executeFluxQuery(String query) {
        try {
            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> result = queryApi.query(query, ORG);

            if (result.isEmpty()) {
                log.warn("⚠️ Flux Query 실행 결과가 없습니다.");
            } else {
                result.forEach(table -> {
                    table.getRecords().forEach(record -> {
                        log.info("저장된 데이터: measurement={}, field={}, value={}, device={}, host={}, time={}",
                                record.getMeasurement(),
                                record.getField(),
                                record.getValue(),
                                record.getValueByKey("device"),
                                record.getValueByKey("host"),
                                record.getTime());
                    });
                });
                log.info("✅ Flux Query 실행 성공! 저장된 데이터 개수: {}", result.size());
            }
        } catch (Exception e) {
            log.error("❌ Flux Query 실행 실패: {}", e.getMessage(), e);
            throw new RuntimeException("Flux Query 실행 실패", e);
        }
    }

    /**
     * ✅ 버킷이 없으면 자동으로 생성
     */
    private void createBucketIfNotExists(String bucketName) {
        try {
            BucketsApi bucketsApi = influxDBClient.getBucketsApi();
            if (bucketsApi.findBucketByName(bucketName) == null) {
                log.info("⚠️ 버킷 생성 시작: {}", bucketName);

                String orgId = influxDBClient.getOrganizationsApi().findOrganizations().get(0).getId();

                // ✅ Retention 정책 설정 (30일 = 30 * 24 * 60 * 60 초)
                long retentionSeconds = 30 * 24 * 60 * 60L; // 30일
                BucketRetentionRules retentionRule = new BucketRetentionRules();
                retentionRule.setShardGroupDurationSeconds(retentionSeconds);
                // ✅ 수정된 부분

                // ✅ 버킷 생성
                Bucket bucket = new Bucket();
                bucket.setName(bucketName);
                bucket.setOrgID(orgId);
                bucket.setRetentionRules(List.of(retentionRule));

                bucketsApi.createBucket(bucket);

                log.info("✅ 버킷 생성 완료: {}", bucketName);
            } else {
                log.info("✅ 기존 버킷 '{}'이 존재함", bucketName);
            }
        } catch (Exception e) {
            log.error("❌ 버킷 '{}' 생성 실패: {}", bucketName, e.getMessage(), e);
            throw new RuntimeException("버킷 생성 실패: " + bucketName, e);
        }
    }

    // private boolean canExecuteTask(String taskId) {
    //     long currentTime = System.currentTimeMillis();
    //     Long lastExecution = lastAggregationTime.get(taskId);
    //     return lastExecution == null || (currentTime - lastExecution) >= MIN_INTERVAL;
    // }

    // private void updateLastExecutionTime(String taskId) {
    //     lastAggregationTime.put(taskId, System.currentTimeMillis());
    // }

    /**
     * ✅ 특정 버킷에서 특정 measurement(예: sensor_data)의 데이터를 삭제하는 메서드
     */
    private void deleteOldData(String bucketName, String measurement) {
        try {
            String fluxQuery = String.format("""
                    from(bucket: "%s")
                      |> range(start: -10m)
                      |> filter(fn: (r) => r._measurement == "%s")
                      |> drop(columns: ["_measurement"]) // measurement 삭제 후 버킷에서 제거
                    """, bucketName, measurement);

            QueryApi queryApi = influxDBClient.getQueryApi();
            queryApi.query(fluxQuery, ORG);

            log.info("✅ {} 버킷에서 {} measurement 데이터 삭제 완료!", bucketName, measurement);
        } catch (Exception e) {
            log.error("❌ {} 버킷에서 {} 데이터 삭제 실패: {}", bucketName, measurement, e.getMessage(), e);
        }
    }

    // public void startScheduledTasks() {
    // try {
    // log.info("스케줄러 작업 시작");
    // initializeBuckets();

    // // 초기 실행 시에도 중복 체크
    // aggregateDataTo2Min();
    // Thread.sleep(300000);
    // aggregateDataTo4Min();

    // log.info("✅ 스케줄러 작업 초기화 완료");
    // } catch (Exception e) {
    // log.error("스케줄러 작업 시작 중 오류 발생: {}", e.getMessage(), e);
    // }
    // }
}

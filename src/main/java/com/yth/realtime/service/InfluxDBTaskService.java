package com.yth.realtime.service;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.influxdb.client.BucketsApi;
import com.influxdb.client.DeleteApi;
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
    private static final String AGG_BUCKET_15MIN = "ydata_15min_avg"; // 15분 평균 버킷
    private static final String AGG_BUCKET_1HOUR = "ydata_1hour_avg"; // 1시간 평균 버킷
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
        List.of(SOURCE_BUCKET, AGG_BUCKET_15MIN, AGG_BUCKET_1HOUR).forEach(this::createBucketIfNotExists);
    }

    // 7일 전 원본 데이터 삭제 스케줄러
    @Scheduled(cron = "0 0 0 * * *") // 매일 자정에 실행
    public void deleteOldSensorData() {
        log.info("⏳ 7일 이전 원본 데이터 삭제 검사 시작...");
        try {
            DeleteApi deleteApi = influxDBClient.getDeleteApi();

            // 삭제 전 데이터 확인
            String checkQuery = String.format("""
                    from(bucket: "%s")
                      |> range(start: -8d)
                      |> filter(fn: (r) => r._measurement == "sensor_data")
                    """, SOURCE_BUCKET);

            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> beforeDelete = queryApi.query(checkQuery, ORG);
            log.info("삭제 전 7일 이전 데이터 수: {}", beforeDelete.size());

            // 7일 이전 데이터만 삭제
            String predicate = "_measurement=\"sensor_data\"";
            deleteApi.delete(
                    OffsetDateTime.now().minusDays(7), // 7일 전
                    OffsetDateTime.now().minusDays(6), // 6일 전
                    predicate,
                    SOURCE_BUCKET,
                    ORG);

            // 삭제 후 데이터 확인
            List<FluxTable> afterDelete = queryApi.query(checkQuery, ORG);
            log.info("삭제 후 7일 이전 데이터 수: {}", afterDelete.size());
            log.info("✅ 7일 이전 원본 데이터 삭제 완료! ({} -> {} 개)",
                    beforeDelete.size(), afterDelete.size());

        } catch (Exception e) {
            log.error("❌ 7일 이전 원본 데이터 삭제 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * ✅ 15분 평균 데이터 집계
     */
    @Scheduled(fixedRate = 900000) // 15분 = 15 * 60 * 1000 밀리초
    public void aggregateDataTo15Min() {
        log.info("⏳ 15분 평균 데이터 저장 시작...");
        try {
            String fluxQuery = String.format("""
                    from(bucket: "%s")
                      |> range(start: -30m)
                      |> filter(fn: (r) => r._measurement == "sensor_data")
                      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
                      |> group(columns: ["device", "host", "_field"])
                       |> sort(columns: ["_time"])
                      |> aggregateWindow(
                            every: 15m,
                            fn: mean,
                            createEmpty: true
                        )
                      |> duplicate(column: "_stop", as: "_time")
                      |> group()
                      |> set(key: "_measurement", value: "sensor_data_15min_avg")
                      |> drop(columns: ["_start", "_stop"])
                      |> to(
                          bucket: "%s",
                          org: "%s"
                        )
                    """, SOURCE_BUCKET, AGG_BUCKET_15MIN, ORG);

            executeFluxQuery(fluxQuery);
            // deleteOldData(SOURCE_BUCKET, "sensor_data");
        } catch (Exception e) {
            log.error("15분 평균 데이터 집계 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * ✅ 1시간 평균 데이터 집계
     */
    @Scheduled(fixedRate = 3600000) // 1시간 = 60 * 60 * 1000 밀리초
    public void aggregateDataTo1Hour() {
        log.info("⏳ 1시간 평균 데이터 저장 시작...");
        try {
            String fluxQuery = String.format("""
                    from(bucket: "%s")
                      |> range(start: -2h)
                      |> filter(fn: (r) => r._measurement == "sensor_data_15min_avg")
                      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
                      |> group(columns: ["device", "host", "_field"])
                       |> sort(columns: ["_time"])
                      |> aggregateWindow(
                            every: 1h,
                            fn: mean,
                            createEmpty: true
                        )
                      |> duplicate(column: "_stop", as: "_time")
                      |> group()
                      |> set(key: "_measurement", value: "sensor_data_1hour_avg")
                      |> drop(columns: ["_start", "_stop"])
                      |> to(
                          bucket: "%s",
                          org: "%s"
                        )
                    """, AGG_BUCKET_15MIN, AGG_BUCKET_1HOUR, ORG);

            executeFluxQuery(fluxQuery);
            // deleteOldData(AGG_BUCKET_15MIN, "sensor_data_15min_avg");
        } catch (Exception e) {
            log.error("1시간 평균 데이터 집계 실패: {}", e.getMessage(), e);
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
                long retentionSeconds = (7 * 24 * 60 * 60L);// 30일
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

    /**
     * ✅ 데이터 삭제 로직 수정
     */
    private void deleteOldData(String bucketName, String measurement) {
        try {
            DeleteApi deleteApi = influxDBClient.getDeleteApi();
            String predicate = String.format("_measurement=\"%s\"", measurement);

            // SOURCE_BUCKET인 경우 (원본 데이터)
            if (bucketName.equals(SOURCE_BUCKET)) {
                deleteApi.delete(
                        OffsetDateTime.now().minusMinutes(30), // 30분 전 데이터부터
                        OffsetDateTime.now().minusMinutes(15), // 15분 전 데이터까지
                        predicate,
                        bucketName,
                        ORG);
            }
            // AGG_BUCKET_15MIN인 경우 (15분 평균 데이터)
            else if (bucketName.equals(AGG_BUCKET_15MIN)) {
                deleteApi.delete(
                        OffsetDateTime.now().minusHours(2), // 2시간 전 데이터부터
                        OffsetDateTime.now().minusHours(1), // 1시간 전 데이터까지
                        predicate,
                        bucketName,
                        ORG);
            }

            log.info("✅ {} 버킷에서 {} measurement 데이터 삭제 완료!", bucketName, measurement);
        } catch (Exception e) {
            log.error("❌ {} 버킷에서 {} 데이터 삭제 실패: {}", bucketName, measurement, e.getMessage(), e);
        }
    }
}

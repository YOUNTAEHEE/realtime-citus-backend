// package com.yth.realtime.service;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;

// @Service
// public class InfluxDBTaskService {
//     private static final Logger log = LoggerFactory.getLogger(ModbusService.class);

//     private final InfluxDBClient influxDBClient;

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//         log.info("InfluxDBTaskService 초기화됨");
//     }

//     @Scheduled(cron = "0 0 2 * * ?") // 매일 새벽 2시에 실행
//     public void aggregateDataTo15Min() {
//         log.info("15분 평균 데이터 집계 작업 시작");
//         try {
//         String query = """
//             from(bucket: "sensor_data")
//             |> range(start: -6d, stop: -5d)
//             |> filter(fn: (r) => r._measurement == "sensor_data")
//             |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
//             |> to(bucket: "sensor_data_15min_avg", org: "your_org")
//         """;
//         influxDBClient.getQueryApi().query(query);
//         System.out.println("15분 평균 데이터 저장 완료");
//     } catch (Exception e) {
//         log.error("15분 평균 데이터 저장 실패: {}", e.getMessage(), e);
//     }
//     }

//     @Scheduled(cron = "0 30 2 * * ?") // 매일 새벽 2시 30분에 실행
//     public void aggregateDataTo1Hour() {
//         log.info("1시간 평균 데이터 집계 작업 시작");
//         try {
//         String query = """
//             from(bucket: "sensor_data_15min_avg")
//             |> range(start: -6d, stop: -5d)
//             |> filter(fn: (r) => r._measurement == "sensor_data_15min_avg")
//             |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
//             |> to(bucket: "sensor_data_1h_avg", org: "your_org")
//         """;
//         influxDBClient.getQueryApi().query(query);
//         System.out.println("1시간 평균 데이터 저장 완료");
//     } catch (Exception e) {
//         log.error("1시간 평균 데이터 저장 실패: {}", e.getMessage(), e);
//     }
//     }
//        // 테스트용 메서드 추가
//        public void testConnection() {
//         try {
//             influxDBClient.ping();
//             log.info("InfluxDB 연결 테스트 성공 taskService");
//         } catch (Exception e) {
//             log.error("InfluxDB 연결 테스트 taskService 실패: {}", e.getMessage(), e);
//         }
//     }
// }

// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.List;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.influxdb.query.FluxTable;

// @Service
// public class InfluxDBTaskService {
//     private static final Logger log = LoggerFactory.getLogger(InfluxDBTaskService.class);

//     private final InfluxDBClient influxDBClient;
//     private static final String BUCKET = "ydata";  // InfluxDB 버킷 (변경 가능)
//     private static final String ORG = "youn";  // 조직명 (환경 변수에서 가져오는 게 좋음)

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//         log.info("✅ InfluxDBTaskService 초기화 완료");
//         testConnection();  
//     }

//     /**
//      * 1분 평균 데이터를 "sensor_data_1min_avg" Measurement로 저장 (매 1분 실행)
//      */
//     @Scheduled(initialDelay = 10000, fixedRate = 60000) // 애플리케이션 시작 후 10초 후에 첫 실행
//     public void aggregateDataTo1Min() {
//         log.info("⏳ 1분 평균 데이터 집계 시작...");
//         try {
//             // 먼저 데이터가 있는지 확인
//             String checkQuery = """
//                 from(bucket: "ydata")
//                 |> range(start: -1m)
//                 |> filter(fn: (r) => r._measurement == "sensor_data")
//                 |> count()
//             """;
            
//             QueryApi queryApi = influxDBClient.getQueryApi();
//             List<FluxTable> checkResults = queryApi.query(checkQuery);
            
//             if (checkResults.isEmpty() || checkResults.get(0).getRecords().isEmpty()) {
//                 log.warn("⚠️ 집계할 데이터가 없습니다.");
//                 return;
//             }
            
//             // 실제 집계 쿼리
//             String fluxQuery = """
//                 from(bucket: "ydata")
//                 |> range(start: -1m)
//                 |> filter(fn: (r) => r._measurement == "sensor_data")
//                 |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
//                 |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
//                 |> yield(name: "mean")
//             """;
            
//             log.debug("실행할 쿼리: {}", fluxQuery);
            
//             queryApi.query(fluxQuery).forEach(table -> {
//                 if (table.getRecords().isEmpty()) {
//                     log.warn("⚠️ 집계 결과가 없습니다.");
//                     return;
//                 }
                
//                 table.getRecords().forEach(record -> {
//                     String field = record.getField();
//                     Object value = record.getValue();
//                     String measurement = record.getMeasurement();
                    
//                     log.debug("레코드 정보: measurement={}, field={}, value={}", 
//                         measurement, field, value);
                    
//                     if (value != null) {
//                         saveToInfluxDB("sensor_data_1min_avg", field, value);
//                     } else {
//                         log.warn("⚠️ 값이 null입니다: field={}", field);
//                     }
//                 });
//             });

//             log.info("✅ 1분 평균 데이터 저장 완료");
//         } catch (Exception e) {
//             log.error("❌ 1분 평균 데이터 저장 실패: {}", e.getMessage());
//             log.error("상세 에러: ", e);  // 스택 트레이스 출력
//         }
//     }

//     /**
//      * 2분 평균 데이터를 "sensor_data_2min_avg" Measurement로 저장 (매 3분 실행)
//      */
//     @Scheduled(fixedRate = 180000) // 3분마다 실행 (테스트)
//     public void aggregateDataTo2Min() {
//         log.info("⏳ [테스트] 2분 평균 데이터 집계 시작...");
//         try {
//             String fluxQuery = """
//                 from(bucket: "ydata")
//                 |> range(start: -3m, stop: now()) // 최근 3분 데이터 사용
//                 |> filter(fn: (r) => r._measurement == "sensor_data_1min_avg")
//                 |> aggregateWindow(every: 2m, fn: mean, createEmpty: false)
//             """;

//             QueryApi queryApi = influxDBClient.getQueryApi();
//             queryApi.query(fluxQuery).forEach(table -> {
//                 table.getRecords().forEach(record -> {
//                     if (record.getValue() != null) {
//                         saveToInfluxDB("sensor_data_2min_avg", record.getField(), record.getValue());
//                     }
//                 });
//             });

//             log.info("✅ [테스트] 2분 평균 데이터 저장 완료");
//         } catch (Exception e) {
//             log.error("❌ 2분 평균 데이터 저장 실패: {}", e.getMessage(), e);
//         }
//     }

//     /**
//      * InfluxDB에 데이터 저장하는 메서드
//      */
//     private void saveToInfluxDB(String measurement, String field, Object value) {
//         try {
//             if (value == null) {
//                 log.warn("⚠️ 데이터 저장 스킵 - null 값: measurement={}, field={}", 
//                     measurement, field);
//                 return;
//             }

//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             Point point = Point.measurement(measurement)
//                 .addTag("aggregation_type", "1min_avg")
//                 .addTag("data_type", field)
                
//                 // field 이름을 그대로 사용하여 값 저장
//                 .addField(field, value instanceof Number ? 
//                     (Number)value : 
//                     Double.parseDouble(value.toString()))
                
//                 .time(Instant.now(), WritePrecision.NS);

//             log.debug("저장할 데이터 포인트: {}", point);
//             writeApi.writePoint(BUCKET, ORG, point);
//             log.info("✅ 데이터 저장 완료: measurement={}, field={}, value={}", 
//                 measurement, field, value);
//         } catch (Exception e) {
//             log.error("❌ 데이터 저장 실패: measurement={}, field={}, value={}, error={}", 
//                 measurement, field, value, e.getMessage());
//             log.error("상세 에러: ", e);
//         }
//     }

//     /**
//      * InfluxDB 연결 테스트
//      */
//     public void testConnection() {
//         try {
//             influxDBClient.ping();
//             log.info("✅ InfluxDB 연결 테스트 성공");
//         } catch (Exception e) {
//             log.error("❌ InfluxDB 연결 실패: {}", e.getMessage(), e);
//         }
//     }
// }

// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.List;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.BucketsApi;
// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.Bucket;
// import com.influxdb.client.domain.BucketRetentionRules;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.influxdb.query.FluxRecord;
// import com.influxdb.query.FluxTable;

// import jakarta.annotation.PostConstruct;
// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;
    
//     // 상수 정의
//     private static final String SOURCE_BUCKET = "ydata";
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg";
//     private static final String AGG_BUCKET_5MIN = "ydata_5min_avg";
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data";
//     private static final long RETENTION_DAYS = 30L;
    
//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//     }

//     @PostConstruct
//     public void init() {
//         try {
//             log.info("✅ InfluxDBTaskService 초기화 시작 - {}", Instant.now());
//             if (!testConnection()) {
//                 throw new RuntimeException("InfluxDB 연결 실패");
//             }
//             initializeBuckets();
//             log.info("✅ InfluxDBTaskService 초기화 완료");
//         } catch (Exception e) {
//             log.error("❌ InfluxDBTaskService 초기화 실패", e);
//             throw new RuntimeException("서비스 초기화 실패", e);
//         }
//     }

//     private void initializeBuckets() {
//         List.of(AGG_BUCKET_2MIN, AGG_BUCKET_5MIN).forEach(this::createBucketIfNotExists);
//     }

//     @Scheduled(fixedRate = 180000) // 3분
//     public void aggregateDataTo2Min() {
//         aggregateData(SOURCE_BUCKET, AGG_BUCKET_2MIN, "2m", "-3m", "2분");
//     }

//     @Scheduled(fixedRate = 360000) // 6분
//     public void aggregateDataTo5Min() {
//         aggregateData(AGG_BUCKET_2MIN, AGG_BUCKET_5MIN, "5m", "-6m", "5분");
//     }

//     private void aggregateData(String sourceBucket, String targetBucket, 
//                            String windowSize, String rangeStart, String logPrefix) {
//         log.info("⏳ {} 평균 데이터 집계 시작... - {}", logPrefix, Instant.now());
//         try {
//             // 1. 데이터 조회
//             String queryForRead = String.format("""
//                 from(bucket: "%s")
//                   |> range(start: %s)
//                   |> filter(fn: (r) => r._measurement == "%s")
//                   |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
//                   |> aggregateWindow(
//                       every: %s,
//                       fn: mean,
//                       createEmpty: false
//                   )
//                 """, 
//                 sourceBucket, rangeStart, MEASUREMENT_NAME, windowSize);

//             List<FluxTable> tables = influxDBClient.getQueryApi().query(queryForRead);
            
//             if (tables.isEmpty()) {
//                 log.warn("⚠️ 집계할 데이터가 없습니다");
//                 return;
//             }

//             // 2. 집계된 데이터를 새 버킷에 저장
//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             List<Point> points = new ArrayList<>();

//             for (FluxTable table : tables) {
//                 for (FluxRecord record : table.getRecords()) {
//                     String field = record.getField();
//                     Double value = record.getValue() instanceof Number ? 
//                         ((Number) record.getValue()).doubleValue() : 
//                         Double.parseDouble(record.getValue().toString());
//                     Instant time = record.getTime();

//                     Point point = Point.measurement(MEASUREMENT_NAME)
//                         .addTag("aggregation_type", windowSize + "_avg")
//                         .addField(field, value)
//                         .time(time, WritePrecision.NS);

//                     points.add(point);
//                 }
//             }

//             if (!points.isEmpty()) {
//                 writeApi.writePoints(targetBucket, ORG, points);
//                 log.info("✅ {} 평균 데이터 저장 완료 - 포인트 수: {}", logPrefix, points.size());
//             } else {
//                 log.warn("⚠️ 저장할 데이터 포인트가 없습니다");
//             }

//             // 3. 저장 확인
//             verifyData(targetBucket, rangeStart);

//         } catch (Exception e) {
//             log.error("❌ {} 평균 데이터 처리 실패: {}", logPrefix, e.getMessage());
//             log.error("상세 에러: ", e);
//         }
//     }

//     private void verifyData(String bucket, String timeRange) {
//         try {
//             String verifyQuery = String.format("""
//                 from(bucket: "%s")
//                   |> range(start: %s)
//                   |> filter(fn: (r) => r._measurement == "%s")
//                   |> count()
//                 """, 
//                 bucket, timeRange, MEASUREMENT_NAME);
            
//             List<FluxTable> results = influxDBClient.getQueryApi().query(verifyQuery);
            
//             if (!results.isEmpty() && !results.get(0).getRecords().isEmpty()) {
//                 long count = ((Number) results.get(0).getRecords().get(0).getValue()).longValue();
//                 log.info("✅ 저장된 데이터 수: {} records", count);
//             } else {
//                 log.warn("⚠️ 저장된 데이터가 없습니다");
//             }
//         } catch (Exception e) {
//             log.error("❌ 데이터 확인 중 오류 발생: {}", e.getMessage());
//         }
//     }

//     private boolean testConnection() {
//         try {
//             influxDBClient.ping();
//             log.info("✅ InfluxDB 연결 테스트 성공");
//             return true;
//         } catch (Exception e) {
//             log.error("❌ InfluxDB 연결 실패: {}", e.getMessage(), e);
//             return false;
//         }
//     }

//     private void createBucketIfNotExists(String bucketName) {
//         try {
//             BucketsApi bucketsApi = influxDBClient.getBucketsApi();
//             if (bucketsApi.findBucketByName(bucketName) == null) {
//                 log.info("⚠️ 버킷 생성 시작: {}", bucketName);
                
//                 String orgId = influxDBClient.getOrganizationsApi().findOrganizations().get(0).getId();
                
//                 // Retention 정책 설정 (데이터 유지 기간)
//                 long retentionSeconds = RETENTION_DAYS * 24 * 60 * 60L; // 30일을 초로 변환
//                 BucketRetentionRules retentionRule = new BucketRetentionRules();
//                 retentionRule.setShardGroupDurationSeconds(retentionSeconds);
                
//                 // 버킷 생성
//                 Bucket bucket = new Bucket();
//                 bucket.name(bucketName)
//                      .orgID(orgId)
//                      .retentionRules(List.of(retentionRule));
                
//                 bucketsApi.createBucket(bucket);
                
//                 log.info("✅ 버킷 생성 완료: {}", bucketName);
//             } else {
//                 log.info("✅ 기존 버킷 확인: {}", bucketName);
//             }
//         } catch (Exception e) {
//             log.error("❌ 버킷 생성 실패: {} - {}", bucketName, e.getMessage(), e);
//             throw new RuntimeException("버킷 생성 실패: " + bucketName, e);
//         }
//     }

//     protected void saveToInfluxDB(String bucket, String measurement, 
//                                 String field, Object value, Instant time) {
//         try {
//             if (value == null) {
//                 log.warn("⚠️ null 값 스킵: measurement={}, field={}", measurement, field);
//                 return;
//             }

//             Point point = Point.measurement(measurement)
//                 .addTag("aggregation_type", measurement)
//                 .addField(field, convertToNumber(value))
//                 .time(time != null ? time : Instant.now(), WritePrecision.NS);

//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             writeApi.writePoint(bucket, ORG, point);
            
//             log.debug("✅ 데이터 저장: bucket={}, measurement={}, field={}, value={}", 
//                 bucket, measurement, field, value);
//         } catch (Exception e) {
//             log.error("❌ 데이터 저장 실패: measurement={}, field={}, error={}", 
//                 measurement, field, e.getMessage(), e);
//             throw new RuntimeException("데이터 저장 실패", e);
//         }
//     }

//     private Number convertToNumber(Object value) {
//         if (value instanceof Number) {
//             return (Number) value;
//         }
//         try {
//             return Double.parseDouble(value.toString());
//         } catch (NumberFormatException e) {
//             throw new IllegalArgumentException("숫자로 변환할 수 없는 값: " + value);
//         }
//     }
// }


// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;

// import jakarta.annotation.PostConstruct;
// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;
    
//     // 버킷 정의
//     private static final String SOURCE_BUCKET = "ydata";
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg";
//     private static final String AGG_BUCKET_5MIN = "ydata_5min_avg";
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data";

//     // 데이터 저장 버퍼
//     private final List<Point> rawDataBuffer = new ArrayList<>();

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//     }

//     @PostConstruct
//     public void init() {
//         try {
//             log.info("✅ InfluxDBTaskService 초기화 완료 - {}", Instant.now());
//         } catch (Exception e) {
//             log.error("❌ InfluxDBTaskService 초기화 실패: {}", e.getMessage(), e);
//             throw new RuntimeException("서비스 초기화 실패", e);
//         }
//     }

//     /**
//      * ✅ 새로운 센서 데이터를 저장 (시뮬레이션)
//      */
//     public void saveSensorData(String deviceId, String deviceHost, double temperature, double humidity) {
//         Point point = Point.measurement(MEASUREMENT_NAME)
//             .addTag("device", deviceId)
//             .addTag("host", deviceHost)
//             .addField("temperature", temperature)
//             .addField("humidity", humidity)
//             .time(Instant.now(), WritePrecision.NS);

//         rawDataBuffer.add(point);
//         log.info("✅ 센서 데이터 저장: device={}, host={}, temperature={}, humidity={}", 
//                  deviceId, deviceHost, temperature, humidity);
//     }

//     /**
//      * ✅ 3분마다 실행 -> 2분 평균값 계산 후 `ydata_2min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 180000) // 3분마다 실행
//     public void aggregateDataTo2Min() {
//         aggregateAndSave(AGG_BUCKET_2MIN, "2m", "2분");
//     }

//     /**
//      * ✅ 6분마다 실행 -> 5분 평균값 계산 후 `ydata_5min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 360000) // 6분마다 실행
//     public void aggregateDataTo5Min() {
//         aggregateAndSave(AGG_BUCKET_5MIN, "5m", "5분");
//     }

//     /**
//      * ✅ 데이터를 그룹화하여 평균을 계산하고 InfluxDB에 저장
//      */
//     private void aggregateAndSave(String targetBucket, String windowSize, String logPrefix) {
//         log.info("⏳ {} 평균 데이터 집계 시작... - {}", logPrefix, Instant.now());
    
//         if (rawDataBuffer.isEmpty()) {
//             log.warn("⚠️ 데이터가 없어 평균을 계산할 수 없습니다.");
//             return;
//         }
    
//         Map<String, List<Point>> groupedData = rawDataBuffer.stream()
//             .collect(Collectors.groupingBy(point -> getTagValue(point, "device") + "_" + getTagValue(point, "host")));
    
//         List<Point> aggregatedPoints = new ArrayList<>();
    
//         for (Map.Entry<String, List<Point>> entry : groupedData.entrySet()) {
//             List<Point> records = entry.getValue();
//             int count = records.size();
    
//             String deviceId = getTagValue(records.get(0), "device");
//             String deviceHost = getTagValue(records.get(0), "host");
    
//             double tempSum = records.stream().mapToDouble(p -> getFieldValue(p, "temperature")).sum();
//             double humSum = records.stream().mapToDouble(p -> getFieldValue(p, "humidity")).sum();
    
//             double avgTemp = tempSum / count;
//             double avgHum = humSum / count;
    
//             Point aggregatedPoint = Point.measurement(MEASUREMENT_NAME)
//                 .addTag("device", deviceId)
//                 .addTag("host", deviceHost)
//                 .addTag("aggregation_type", windowSize + "_avg")
//                 .addField("temperature", avgTemp)
//                 .addField("humidity", avgHum)
//                 .time(Instant.now(), WritePrecision.NS);
    
//             aggregatedPoints.add(aggregatedPoint);
//         }
    
//         if (!aggregatedPoints.isEmpty()) {
//             log.info("📌 InfluxDB에 저장하기 전 데이터 확인: {}", aggregatedPoints);
//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             writeApi.writePoints(targetBucket, ORG, aggregatedPoints);
//             log.info("✅ {} 평균 데이터 저장 완료 - 포인트 수: {}", logPrefix, aggregatedPoints.size());
//         } else {
//             log.warn("⚠️ 저장할 데이터 포인트가 없습니다");
//         }
    
//         // 데이터가 저장된 후 1초 대기 후 버퍼 초기화
//         try {
//             Thread.sleep(1000);
//         } catch (InterruptedException e) {
//             Thread.currentThread().interrupt();
//         }
    
//         rawDataBuffer.clear();
//     }
    

//     /**
//      * ✅ Point에서 특정 태그 값을 안전하게 가져오는 메서드
//      */
//     private String getTagValue(Point point, String tagKey) {
//         try {
//             return (String) point.toLineProtocol().split(tagKey + "=")[1].split(",")[0];
//         } catch (Exception e) {
//             log.warn("⚠️ 태그 값 가져오기 실패: key={}, error={}", tagKey, e.getMessage());
//             return "unknown";
//         }
//     }

//     /**
//      * ✅ Point에서 특정 필드 값을 안전하게 가져오는 메서드
//      */
//     private double getFieldValue(Point point, String fieldKey) {
//         try {
//             String protocol = point.toLineProtocol();
//             String fieldPart = protocol.split(" ")[1];  // 필드 부분 추출
//             for (String field : fieldPart.split(",")) {
//                 String[] keyValue = field.split("=");
//                 if (keyValue[0].equals(fieldKey)) {
//                     return Double.parseDouble(keyValue[1]);
//                 }
//             }
//         } catch (Exception e) {
//             log.warn("⚠️ 필드 값 가져오기 실패: key={}, error={}", fieldKey, e.getMessage());
//         }
//         return 0.0;
//     }
// }

// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;

// import jakarta.annotation.PostConstruct;
// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;

//     // 버킷 정의
//     private static final String SOURCE_BUCKET = "ydata";
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg";
//     private static final String AGG_BUCKET_5MIN = "ydata_5min_avg";
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data";

//     // 데이터 저장 버퍼
//     private final List<Point> rawDataBuffer = new ArrayList<>();

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//     }

//     @PostConstruct
//     public void init() {
//         log.info("✅ InfluxDBTaskService 초기화 완료 - {}", Instant.now());
//     }

//     /**
//      * ✅ 새로운 센서 데이터를 저장 (시뮬레이션)
//      */
//     public void saveSensorData(String deviceId, String deviceHost, double temperature, double humidity) {
//         Point point = Point.measurement(MEASUREMENT_NAME)
//             .addTag("device", deviceId)
//             .addTag("host", deviceHost)
//             .addField("temperature", temperature)
//             .addField("humidity", humidity)
//             .time(Instant.now(), WritePrecision.NS);

//         rawDataBuffer.add(point);
//         log.info("✅ 센서 데이터 저장: device={}, host={}, temperature={}, humidity={}, bufferSize={}", 
//                  deviceId, deviceHost, temperature, humidity, rawDataBuffer.size());
//     }

//     /**
//      * ✅ 3분마다 실행 -> 2분 평균값 계산 후 `ydata_2min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 180000) // 3분마다 실행
//     public void aggregateDataTo2Min() {
//         aggregateAndSave(AGG_BUCKET_2MIN, "2m", "2분");
//     }

//     /**
//      * ✅ 6분마다 실행 -> 5분 평균값 계산 후 `ydata_5min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 360000) // 6분마다 실행
//     public void aggregateDataTo5Min() {
//         aggregateAndSave(AGG_BUCKET_5MIN, "5m", "5분");
//     }

//     /**
//      * ✅ 데이터를 그룹화하여 평균을 계산하고 InfluxDB에 저장
//      */
//     private void aggregateAndSave(String targetBucket, String windowSize, String logPrefix) {
//         log.info("⏳ {} 평균 데이터 집계 시작... - {}", logPrefix, Instant.now());

//         if (rawDataBuffer.isEmpty()) {
//             log.warn("⚠️ 데이터가 없어 평균을 계산할 수 없습니다.");
//             return;
//         }

//         // 1️⃣ 데이터 그룹화 (device + host 별)
//         Map<String, List<Point>> groupedData = rawDataBuffer.stream()
//             .collect(Collectors.groupingBy(point -> getTagValue(point, "device") + "_" + getTagValue(point, "host")));

//         List<Point> aggregatedPoints = new ArrayList<>();

//         for (Map.Entry<String, List<Point>> entry : groupedData.entrySet()) {
//             List<Point> records = entry.getValue();
//             int count = records.size();

//             String deviceId = getTagValue(records.get(0), "device");
//             String deviceHost = getTagValue(records.get(0), "host");

//             double tempSum = records.stream().mapToDouble(p -> getFieldValue(p, "temperature")).sum();
//             double humSum = records.stream().mapToDouble(p -> getFieldValue(p, "humidity")).sum();

//             double avgTemp = tempSum / count;
//             double avgHum = humSum / count;

//             Point aggregatedPoint = Point.measurement(MEASUREMENT_NAME)
//                 .addTag("device", deviceId)
//                 .addTag("host", deviceHost)
//                 .addTag("aggregation_type", windowSize + "_avg")
//                 .addField("temperature", avgTemp)
//                 .addField("humidity", avgHum)
//                 .time(Instant.now(), WritePrecision.NS);

//             aggregatedPoints.add(aggregatedPoint);
//         }

//         if (!aggregatedPoints.isEmpty()) {
//             log.info("📌 InfluxDB에 저장할 데이터: {}", aggregatedPoints);
//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             writeApi.writePoints(targetBucket, ORG, aggregatedPoints);
//             log.info("✅ {} 평균 데이터 저장 완료 - 포인트 수: {}", logPrefix, aggregatedPoints.size());
//         } else {
//             log.warn("⚠️ 저장할 데이터 포인트가 없습니다");
//         }

//         // 4️⃣ 데이터 버퍼 비우기 (저장 후 확인 로그 추가)
//         log.info("🧹 rawDataBuffer 초기화 전 크기: {}", rawDataBuffer.size());
//         rawDataBuffer.clear();
//         log.info("🧹 rawDataBuffer 초기화 후 크기: {}", rawDataBuffer.size());
//     }

//     /**
//      * ✅ Point에서 특정 태그 값을 안전하게 가져오는 메서드
//      */
//     private String getTagValue(Point point, String tagKey) {
//         try {
//             String lineProtocol = point.toLineProtocol(); // InfluxDB 저장용 포맷 문자열
//             String[] parts = lineProtocol.split(" "); // 공백 기준으로 분리
//             String tagPart = parts[0]; // 첫 번째 부분이 태그 정보
    
//             for (String tag : tagPart.split(",")) {
//                 if (tag.contains("=")) {
//                     String[] keyValue = tag.split("=");
//                     if (keyValue[0].equals(tagKey)) {
//                         return keyValue[1]; // 태그 값 반환
//                     }
//                 }
//             }
//         } catch (Exception e) {
//             log.warn("⚠️ 태그 값 가져오기 실패: key={}, error={}", tagKey, e.getMessage());
//         }
//         return "unknown"; // 기본값 반환
//     }
    

//     /**
//      * ✅ Point에서 특정 필드 값을 안전하게 가져오는 메서드
//      */
//     private double getFieldValue(Point point, String fieldKey) {
//         try {
//             String protocol = point.toLineProtocol(); // Point를 Line Protocol 형태로 변환
//             String[] parts = protocol.split(" "); // 공백 기준으로 필드 값 분리
    
//             if (parts.length < 2) return 0.0; // 필드 값이 없는 경우 기본값 반환
    
//             String fieldPart = parts[1]; // 두 번째 부분이 필드 정보
//             for (String field : fieldPart.split(",")) { // 쉼표 기준으로 필드 분리
//                 String[] keyValue = field.split("=");
//                 if (keyValue[0].equals(fieldKey)) {
//                     return Double.parseDouble(keyValue[1]); // 해당 필드 값 반환
//                 }
//             }
//         } catch (Exception e) {
//             log.warn("⚠️ 필드 값 가져오기 실패: key={}, error={}", fieldKey, e.getMessage());
//         }
//         return 0.0; // 기본값 반환
//     }
    
// }

// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.List;
// import java.util.stream.Collectors;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.influxdb.query.FluxRecord;
// import com.influxdb.query.FluxTable;

// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;

//     // InfluxDB 버킷 정의
//     private static final String SOURCE_BUCKET = "ydata";  // 원본 데이터가 저장된 버킷
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg"; // 2분 평균 버킷
//     private static final String AGG_BUCKET_5MIN = "ydata_5min_avg"; // 5분 평균 버킷
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data"; // 측정값 이름

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//     }

//     /**
//      * ✅ 3분마다 실행 → `ydata`에서 최근 2분 데이터의 평균을 `ydata_2min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 180000, initialDelay = 60000) // 3분마다 실행, 초기 1분 대기
//     public void aggregateDataTo2Min() {
//         aggregateAndSave(SOURCE_BUCKET, AGG_BUCKET_2MIN, "2m", "2분");
//     }

//     /**
//      * ✅ 6분마다 실행 → `ydata_2min_avg`에서 최근 5분 데이터의 평균을 `ydata_5min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 360000, initialDelay = 120000) // 6분마다 실행, 초기 2분 대기
//     public void aggregateDataTo5Min() {
//         aggregateAndSave(AGG_BUCKET_2MIN, AGG_BUCKET_5MIN, "5m", "5분");
//     }

//     /**
//      * ✅ InfluxDB에서 데이터를 조회하여 평균을 계산하고 저장
//      */
//     private void aggregateAndSave(String sourceBucket, String targetBucket, String windowSize, String logPrefix) {
//         log.info("⏳ {} 평균 데이터 집계 시작 (출처: {}) - {}", logPrefix, sourceBucket, Instant.now());

//         // 최근 데이터 조회 (이제 FluxRecord 리스트를 직접 반환받습니다)
//         List<FluxRecord> records = queryRecentDataFromInfluxDB(sourceBucket, windowSize);
//         if (records.isEmpty()) {
//             log.warn("⚠️ {} 평균을 계산할 데이터가 없습니다.", logPrefix);
//             return;
//         }

//         // 평균 계산
//         double avgTemp = records.stream()
//             .mapToDouble(record -> {
//                 Object value = record.getValueByKey("_value"); // FluxDB에서는 값이 "_value" 필드에 저장됨
//                 if (value instanceof Number) {
//                     return ((Number) value).doubleValue();
//                 }
//                 try {
//                     return Double.parseDouble(value.toString());
//                 } catch (Exception e) {
//                     log.warn("⚠️ 온도 값 변환 실패: {}", value);
//                     return 0.0;
//                 }
//             })
//             .average()
//             .orElse(0.0);

//         double avgHum = records.stream()
//             .mapToDouble(record -> {
//                 Object value = record.getValueByKey("_value");
//                 if (value instanceof Number) {
//                     return ((Number) value).doubleValue();
//                 }
//                 try {
//                     return Double.parseDouble(value.toString());
//                 } catch (Exception e) {
//                     log.warn("⚠️ 습도 값 변환 실패: {}", value);
//                     return 0.0;
//                 }
//             })
//             .average()
//             .orElse(0.0);

//         // 새로 저장할 데이터 포인트 생성
//         Point aggregatedPoint = Point.measurement(MEASUREMENT_NAME)
//                 .addTag("aggregation_type", windowSize + "_avg")
//                 .addField("temperature", avgTemp)
//                 .addField("humidity", avgHum)
//                 .time(Instant.now(), WritePrecision.NS);

//         // InfluxDB에 저장
//         WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//         writeApi.writePoint(targetBucket, ORG, aggregatedPoint);

//         log.info("✅ {} 평균 데이터 저장 완료 (출처: {} → 저장: {}) | temp_avg={}, hum_avg={}", 
//                  logPrefix, sourceBucket, targetBucket, avgTemp, avgHum);
//     }

//     /**
//      * ✅ InfluxDB에서 최근 N분(2분/5분) 동안의 데이터를 조회하는 메서드
//      */
//     private List<Point> queryRecentDataFromInfluxDB(String sourceBucket, String windowSize) {
//         QueryApi queryApi = influxDBClient.getQueryApi();
        
//         // Flux 쿼리 작성 (N분 동안의 데이터 조회)
//         String fluxQuery = String.format(
//             "from(bucket: \"%s\") |> range(start: -%s) |> filter(fn: (r) => r._measurement == \"%s\")",
//             sourceBucket, windowSize, MEASUREMENT_NAME
//         );

//         List<FluxTable> tables = queryApi.query(fluxQuery, ORG);
        
//         if (tables.isEmpty()) {
//             return List.of(); // 데이터가 없는 경우 빈 리스트 반환
//         }

//         return tables.stream()
//         .flatMap(table -> table.getRecords().stream())
//         .map(record -> Point.measurement(MEASUREMENT_NAME)
//             .addField("temperature", ((Number) record.getValueByKey("temperature")).doubleValue()) // 형 변환 추가
//             .addField("humidity", ((Number) record.getValueByKey("humidity")).doubleValue()) // 형 변환 추가
//             .time(record.getTime(), WritePrecision.NS))
//         .collect(Collectors.toList());
    
    
    
//     }
// }

// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.List;
// import java.util.stream.Collectors;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.influxdb.query.FluxRecord;
// import com.influxdb.query.FluxTable;

// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;

//     // InfluxDB 버킷 정의
//     private static final String SOURCE_BUCKET = "ydata";  // 원본 데이터 버킷
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg"; // 2분 평균 버킷
//     private static final String AGG_BUCKET_5MIN = "ydata_5min_avg"; // 5분 평균 버킷
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data"; // 측정값 이름

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//     }

//     /**
//      * ✅ 3분마다 실행 → `ydata`에서 최근 2분 데이터의 평균을 `ydata_2min_avg`에 저장
//      */
//     // @Scheduled(fixedRate = 180000, initialDelay = 61000) // 3분마다 실행, 초기 1분 대기
//     // public void aggregateDataTo2Min() {
//     //     aggregateAndSave(SOURCE_BUCKET, AGG_BUCKET_2MIN, "2m", "2분");
//     // }

//     @Scheduled(fixedRate = 121000, initialDelay = 61000) // 3분마다 실행, 초기 1분 1초 후 실행
// public void aggregateDataTo2Min() {
//     log.info("⏳ 1시간 동안의 데이터를 2분 단위로 그룹화하여 평균 계산 시작...");
    
//     String fluxQuery = String.format(
//         "from(bucket: \"%s\") |> range(start: -1h) " + // ✅ 1시간 동안의 데이터를 가져옴
//         "|> filter(fn: (r) => r._measurement == \"%s\") " +
//         "|> filter(fn: (r) => r._field == \"temperature\" or r._field == \"humidity\") " +
//         "|> aggregateWindow(every: 2m, fn: mean, createEmpty: false)", // ✅ 2분 단위로 그룹화하여 평균 계산
//         SOURCE_BUCKET, MEASUREMENT_NAME
//     );

//     log.info("🔥 실행할 2분 평균 Flux 쿼리: {}", fluxQuery);

//     QueryApi queryApi = influxDBClient.getQueryApi();
//     List<FluxTable> tables = queryApi.query(fluxQuery, ORG);

//     if (tables.isEmpty()) {
//         log.warn("⚠️ 1시간 동안의 데이터가 부족하여 2분 평균을 계산할 수 없습니다.");
//         return;
//     }

//     // 여러 개의 2분 평균 데이터를 저장할 리스트 생성
//     List<Point> aggregatedPoints = tables.stream()
//         .flatMap(table -> table.getRecords().stream())
//         .map(record -> {
//             double avgTemp = record.getValueByKey("_value") instanceof Number
//                 ? ((Number) record.getValueByKey("_value")).doubleValue()
//                 : 0.0;
//             String field = record.getValueByKey("_field").toString();
//             Instant time = record.getTime();

//             return Point.measurement(MEASUREMENT_NAME + "_avg")
//                 .addTag("aggregation_type", "2m_avg")
//                 .addTag("field", field)
//                 .addField("value", avgTemp)
//                 .time(time, WritePrecision.NS);
//         })
//         .collect(Collectors.toList());

//     // InfluxDB에 저장
//     if (!aggregatedPoints.isEmpty()) {
//         WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//         writeApi.writePoints(AGG_BUCKET_2MIN, ORG, aggregatedPoints);
//         log.info("✅ 2분 평균 데이터 저장 완료! 저장된 포인트 개수: {}", aggregatedPoints.size());
//     } else {
//         log.warn("⚠️ 저장할 2분 평균 데이터가 없습니다.");
//     }
// }
//     /**
//      * ✅ 6분마다 실행 → `ydata_2min_avg`에서 최근 5분 데이터의 평균을 `ydata_5min_avg`에 저장
//      */
//     // @Scheduled(fixedRate = 360000, initialDelay = 123000) // 6분마다 실행, 초기 2분 대기
//     // public void aggregateDataTo5Min() {
//     //     aggregateAndSave(AGG_BUCKET_2MIN, AGG_BUCKET_5MIN, "5m", "5분");
//     // }
//     @Scheduled(fixedRate = 363000, initialDelay = 183000) // 6분 3초마다 실행
//     public void aggregateDataTo5Min() {
//         log.info("⏳ 1시간 동안의 2분 평균 데이터를 5분 단위로 그룹화하여 평균 계산 시작...");
    
//         String fluxQuery = String.format(
//             "from(bucket: \"%s\") |> range(start: -1h) " + // ✅ 1시간 동안의 데이터를 가져옴
//             "|> filter(fn: (r) => r._measurement == \"%s\") " +
//             "|> filter(fn: (r) => r._field == \"temperature\" or r._field == \"humidity\") " +
//             "|> aggregateWindow(every: 4m, fn: mean, createEmpty: false)", // ✅ 5분 단위로 그룹화하여 평균 계산
//             AGG_BUCKET_2MIN, MEASUREMENT_NAME + "_avg"
//         );
    
//         log.info("🔥 실행할 5분 평균 Flux 쿼리: {}", fluxQuery);
    
//         QueryApi queryApi = influxDBClient.getQueryApi();
//         List<FluxTable> tables = queryApi.query(fluxQuery, ORG);
    
//         if (tables.isEmpty()) {
//             log.warn("⚠️ 1시간 동안의 2분 평균 데이터가 부족하여 5분 평균을 계산할 수 없습니다.");
//             return;
//         }
    
//         // 여러 개의 5분 평균 데이터를 저장할 리스트 생성
//         List<Point> aggregatedPoints = tables.stream()
//             .flatMap(table -> table.getRecords().stream())
//             .map(record -> {
//                 double avgTemp = record.getValueByKey("_value") instanceof Number
//                     ? ((Number) record.getValueByKey("_value")).doubleValue()
//                     : 0.0;
//                 String field = record.getValueByKey("_field").toString();
//                 Instant time = record.getTime();
    
//                 return Point.measurement(MEASUREMENT_NAME + "_avg")
//                     .addTag("aggregation_type", "5m_avg")
//                     .addTag("field", field)
//                     .addField("value", avgTemp)
//                     .time(time, WritePrecision.NS);
//             })
//             .collect(Collectors.toList());
    
//         // InfluxDB에 저장
//         if (!aggregatedPoints.isEmpty()) {
//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             writeApi.writePoints(AGG_BUCKET_5MIN, ORG, aggregatedPoints);
//             log.info("✅ 5분 평균 데이터 저장 완료! 저장된 포인트 개수: {}", aggregatedPoints.size());
//         } else {
//             log.warn("⚠️ 저장할 5분 평균 데이터가 없습니다.");
//         }
//     }
    
    

//     /**
//      * ✅ InfluxDB에서 데이터를 조회하여 평균을 계산하고 저장
//      */
//     private void aggregateAndSave(String sourceBucket, String targetBucket, String windowSize, String logPrefix) {
//         log.info("⏳ {} 평균 데이터 집계 시작 (출처: {}) - {}", logPrefix, sourceBucket, Instant.now());

//         // 최근 데이터 조회 (FluxRecord 리스트를 반환받음)
//         List<FluxRecord> records = queryRecentDataFromInfluxDB(sourceBucket, windowSize);
//         if (records.isEmpty()) {
//             log.warn("⚠️ {} 평균을 계산할 데이터가 없습니다.", logPrefix);
//             return;
//         }

//         // 온도 평균 계산
//         double avgTemp = records.stream()
//             .filter(record -> "temperature".equals(record.getValueByKey("_field"))) // 🔹 온도 필터링
//             .mapToDouble(record -> {
//                 Object value = record.getValueByKey("_value");
//                 return (value instanceof Number) ? ((Number) value).doubleValue() : 0.0;
//             })
//             .average()
//             .orElse(0.0);

//         // 습도 평균 계산
//         double avgHum = records.stream()
//             .filter(record -> "humidity".equals(record.getValueByKey("_field"))) // 🔹 습도 필터링
//             .mapToDouble(record -> {
//                 Object value = record.getValueByKey("_value");
//                 return (value instanceof Number) ? ((Number) value).doubleValue() : 0.0;
//             })
//             .average()
//             .orElse(0.0);

//        // 평균 데이터 저장 포인트 생성 (가장 오래된 데이터의 타임스탬프 유지)
// Point aggregatedPoint = Point.measurement(MEASUREMENT_NAME + "_avg") // ✅ "_avg" 추가하여 원본 데이터와 구분
// .addTag("aggregation_type", windowSize + "_avg")
// .addField("temperature", avgTemp)
// .addField("humidity", avgHum)
// .time(records.get(0).getTime(), WritePrecision.NS); // ✅ 가장 오래된 데이터의 타임스탬프 사용


//         // InfluxDB에 저장
//         WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//         writeApi.writePoint(targetBucket, ORG, aggregatedPoint);

//         log.info("✅ {} 평균 데이터 저장 완료 (출처: {} → 저장: {}) | temp_avg={}, hum_avg={}", 
//                  logPrefix, sourceBucket, targetBucket, avgTemp, avgHum);
//     }

//     /**
//      * ✅ InfluxDB에서 최근 N분(2분/5분) 동안의 데이터를 조회하는 메서드
//      */
//     public List<FluxRecord> queryRecentDataFromInfluxDB(String sourceBucket, String windowSize) {
//         QueryApi queryApi = influxDBClient.getQueryApi();
        
//         // Flux 쿼리 작성 (N분 동안의 데이터 조회)
//         String fluxQuery = String.format(
//             "from(bucket: \"%s\") |> range(start: -%s) " +
//             "|> filter(fn: (r) => r._measurement == \"%s\") " +
//             "|> filter(fn: (r) => r._field == \"temperature\" or r._field == \"humidity\")",
//             sourceBucket, windowSize, MEASUREMENT_NAME
//         );
//         log.info("🔥 실행할 2분 평균 Flux 쿼리: {}", fluxQuery);
//         List<FluxTable> tables = queryApi.query(fluxQuery, ORG);
//         log.info("✅ 2분 평균 데이터 개수: {}", tables.size());
//         if (tables.isEmpty()) {
//             return List.of(); // 데이터가 없는 경우 빈 리스트 반환
//         }

//         // FluxRecord 리스트 반환
//         return tables.stream()
//                      .flatMap(table -> table.getRecords().stream())
//                      .collect(Collectors.toList());
//     }
// }



// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.influxdb.query.FluxTable;

// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;

//     // InfluxDB 버킷 정의
//     private static final String SOURCE_BUCKET = "ydata";  // 원본 데이터 버킷
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg"; // 2분 평균 버킷
//     private static final String AGG_BUCKET_5MIN = "ydata_5min_avg"; // 5분 평균 버킷
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data"; // 측정값 이름

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
//     }

//     /**
//      * ✅ 3분마다 실행 → 원본 데이터를 가져와 2분 평균을 계산 후 저장
//      */
//     @Scheduled(fixedRate = 120000, initialDelay = 61000) // 2분마다 실행
//     public void aggregateDataTo2Min() {
//         log.info("⏳ 2분 평균 데이터 계산 시작...");

//         // 1️⃣ InfluxDB에서 최근 2분 데이터를 가져옴
//         List<Point> recentData = fetchRecentSensorData(2);

//         if (recentData.isEmpty()) {
//             log.warn("⚠️ 최근 2분간 데이터가 없어 평균을 계산할 수 없습니다.");
//             return;
//         }

//         // 2️⃣ 평균 계산
//         List<Point> aggregatedPoints = calculateAverages(recentData, "2m_avg");

//         // 3️⃣ InfluxDB에 저장
//         saveAggregatedData(AGG_BUCKET_2MIN, aggregatedPoints, "2m_avg");
//     }

//     /**
//      * ✅ 5분마다 실행 → `ydata_2min_avg` 데이터를 가져와 5분 평균을 계산 후 저장
//      */
//     @Scheduled(fixedRate = 300000, initialDelay = 183000) // 5분마다 실행
//     public void aggregateDataTo5Min() {
//         log.info("⏳ 5분 평균 데이터 계산 시작...");

//         // 1️⃣ InfluxDB에서 최근 5분 데이터를 가져옴
//         List<Point> recentData = fetchRecentSensorData(5);

//         if (recentData.isEmpty()) {
//             log.warn("⚠️ 최근 5분간 데이터가 없어 평균을 계산할 수 없습니다.");
//             return;
//         }

//         // 2️⃣ 평균 계산
//         List<Point> aggregatedPoints = calculateAverages(recentData, "5m_avg");

//         // 3️⃣ InfluxDB에 저장
//         saveAggregatedData(AGG_BUCKET_5MIN, aggregatedPoints, "5m_avg");
//     }

//     /**
//      * ✅ InfluxDB에서 최근 N분 동안의 데이터를 가져오는 메서드
//      */
//     private List<Point> fetchRecentSensorData(int minutes) {
//         try {
//             QueryApi queryApi = influxDBClient.getQueryApi();
//             Instant startTime = Instant.now().minusSeconds(minutes * 60);
//             List<Point> points = new ArrayList<>();

//             String fluxQuery = String.format(
//                 "from(bucket: \"%s\") " +
//                 "|> range(start: -%dm) " +
//                 "|> filter(fn: (r) => r._measurement == \"%s\") " +
//                 "|> filter(fn: (r) => r._field == \"temperature\" or r._field == \"humidity\")",
//                 SOURCE_BUCKET, minutes, MEASUREMENT_NAME
//             );

//             List<FluxTable> tables = queryApi.query(fluxQuery, ORG);

//             tables.forEach(table -> table.getRecords().forEach(record -> {
//                 try {
//                     String device = record.getValueByKey("device") != null ? 
//                         record.getValueByKey("device").toString() : "unknown";
//                     String host = record.getValueByKey("host") != null ? 
//                         record.getValueByKey("host").toString() : "unknown";
//                     String field = record.getField();
//                     Double value = Double.parseDouble(record.getValue().toString());

//                     Point point = Point.measurement(MEASUREMENT_NAME)
//                         .addTag("device", device)
//                         .addTag("host", host)
//                         .addField(field, value)
//                         .time(record.getTime(), WritePrecision.NS);
//                     points.add(point);
//                 } catch (NumberFormatException | NullPointerException e) {
//                     log.error("데이터 변환 중 오류 발생: {}", e.getMessage());
//                 }
//             }));

//             return points;
//         } catch (Exception e) {
//             log.error("InfluxDB 데이터 조회 중 오류 발생: {}", e.getMessage());
//             return new ArrayList<>();
//         }
//     }

//     /**
//      * ✅ 데이터를 그룹화하고 평균을 계산하는 메서드
//      */
//     private List<Point> calculateAverages(List<Point> rawData, String aggregationType) {
//         // Point를 문자열로 변환하여 태그 정보를 추출
//         Map<String, List<Point>> groupedData = rawData.stream()
//             .collect(Collectors.groupingBy(point -> {
//                 String deviceTag = point.toLineProtocol().split(",device=")[1].split(",")[0];
//                 String hostTag = point.toLineProtocol().split(",host=")[1].split(",")[0];
//                 return deviceTag + "_" + hostTag;
//             }));

//         List<Point> aggregatedPoints = new ArrayList<>();

//         for (Map.Entry<String, List<Point>> entry : groupedData.entrySet()) {
//             List<Point> records = entry.getValue();
//             if (records.isEmpty()) continue;

//             int count = records.size();
//             double tempSum = records.stream().mapToDouble(p -> getFieldValue(p, "temperature")).sum();
//             double humSum = records.stream().mapToDouble(p -> getFieldValue(p, "humidity")).sum();

//             double avgTemp = tempSum / count;
//             double avgHum = humSum / count;

//             // 첫 번째 레코드에서 device와 host 태그 추출
//             String[] tags = entry.getKey().split("_");
//             String device = tags[0];
//             String host = tags[1];

//             Point aggregatedPoint = Point.measurement(MEASUREMENT_NAME + "_avg")
//                 .addTag("device", device)
//                 .addTag("host", host)
//                 .addTag("aggregation_type", aggregationType)
//                 .addField("temperature", avgTemp)
//                 .addField("humidity", avgHum)
//                 .time(Instant.now(), WritePrecision.NS);

//             aggregatedPoints.add(aggregatedPoint);
//         }

//         return aggregatedPoints;
//     }

//     /**
//      * ✅ 평균 데이터를 InfluxDB에 저장하는 메서드
//      */
//     private void saveAggregatedData(String targetBucket, List<Point> aggregatedPoints, String logPrefix) {
//         if (!aggregatedPoints.isEmpty()) {
//             WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
//             writeApi.writePoints(targetBucket, ORG, aggregatedPoints);
//             log.info("✅ {} 평균 데이터 저장 완료 - 포인트 수: {}", logPrefix, aggregatedPoints.size());
//         } else {
//             log.warn("⚠️ 저장할 평균 데이터가 없습니다.");
//         }
//     }

//     /**
//      * ✅ Point에서 특정 필드 값을 가져오는 메서드
//      */
//     private double getFieldValue(Point point, String fieldKey) {
//         try {
//             String lineProtocol = point.toLineProtocol();
//             String[] parts = lineProtocol.split(" ");
//             if (parts.length >= 2) {
//                 String fieldsPart = parts[1];  // 필드 부분 추출
//                 for (String field : fieldsPart.split(",")) {
//                     String[] keyValue = field.split("=");
//                     if (keyValue.length == 2 && keyValue[0].equals(fieldKey)) {
//                         return Double.parseDouble(keyValue[1]);
//                     }
//                 }
//             }
//         } catch (Exception e) {
//             log.warn("⚠️ 필드 값 가져오기 실패: key={}, error={}", fieldKey, e.getMessage());
//         }
//         return 0.0;  // 기본값 반환
//     }
// }

// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.List;

// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.BucketsApi;
// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.client.WriteApiBlocking;
// import com.influxdb.client.domain.Bucket;
// import com.influxdb.client.domain.BucketRetentionRules;
// import com.influxdb.client.domain.WritePrecision;
// import com.influxdb.client.write.Point;
// import com.influxdb.query.FluxTable;

// import jakarta.annotation.PostConstruct;
// import lombok.extern.slf4j.Slf4j;

// @Service
// @Slf4j
// public class InfluxDBTaskService {
//     private final InfluxDBClient influxDBClient;

//     // InfluxDB 버킷 정의
//     private static final String SOURCE_BUCKET = "ydata";  // 원본 데이터 버킷
//     private static final String AGG_BUCKET_2MIN = "ydata_2min_avg"; // 2분 평균 버킷
//     private static final String AGG_BUCKET_4MIN = "ydata_4min_avg"; // 5분 평균 버킷
//     private static final String ORG = "youn";
//     private static final String MEASUREMENT_NAME = "sensor_data"; // 측정값 이름

//     public InfluxDBTaskService(InfluxDBClient influxDBClient) {
//         this.influxDBClient = influxDBClient;
        
//     }
//     @PostConstruct
//     public void init() {
//         try {
//             log.info("✅ InfluxDBTaskService 초기화 시작 - {}", Instant.now());
          
//             initializeBuckets();
//             log.info("✅ InfluxDBTaskService 초기화 완료");
//         } catch (Exception e) {
//             log.error("❌ InfluxDBTaskService 초기화 실패", e);
//             throw new RuntimeException("서비스 초기화 실패", e);
//         }
//     }

//     private void initializeBuckets() {
//         List.of(AGG_BUCKET_2MIN, AGG_BUCKET_4MIN).forEach(this::createBucketIfNotExists);
//     }
//     /**
//      * ✅ 3분마다 실행 → `ydata`에서 최근 2분 데이터의 평균을 `ydata_2min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 181000, initialDelay = 61000) // 3분마다 실행
//     public void aggregateDataTo2Min() {
//         log.info("⏳ 2분 평균 데이터 저장 시작...");

//         String fluxQuery = String.format("""
//             data = from(bucket: "%s")
//               |> range(start: -10m)
//               |> filter(fn: (r) => r._measurement == "sensor_data")
//               |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
//               |> group(columns: ["device", "host", "_field"])
//               |> aggregateWindow(
//                   every: 2m,
//                   fn: mean,
//                   createEmpty: false
//                 )
//               |> set(key: "_measurement", value: "sensor_data_avg")
//               |> set(key: "bucket_name", value: "%s")

//             data
//               |> yield(name: "mean")
//               |> to(
//                   bucket: "%s",
//                   org: "%s"
//                 )
//             """,
//             SOURCE_BUCKET, AGG_BUCKET_2MIN, AGG_BUCKET_2MIN, ORG);

//         log.info("🔥 실행할 2분 평균 Flux 쿼리: {}", fluxQuery);
//         executeFluxQuery(fluxQuery);
//     }

//     /**
//      * ✅ 6분마다 실행 → `ydata_2min_avg`에서 최근 5분 데이터의 평균을 `ydata_5min_avg`에 저장
//      */
//     @Scheduled(fixedRate = 303000, initialDelay = 303000)// 6분마다 실행
//     public void aggregateDataTo4Min() {
//         log.info("⏳ 4분 평균 데이터 저장 시작...");

//         String fluxQuery = String.format("""
//             data = from(bucket: "%s")
//               |> range(start: -10m)
//               |> filter(fn: (r) => r._measurement == "sensor_data_avg")
//               |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
//               |> group(columns: ["device", "host", "_field"])
//               |> aggregateWindow(
//                   every: 4m,
//                   fn: mean,
//                   createEmpty: false
//                 )
//               |> set(key: "_measurement", value: "sensor_data_4min_avg")
//               |> set(key: "bucket_name", value: "%s")

//             data
//               |> yield(name: "mean")
//               |> to(
//                   bucket: "%s",
//                   org: "%s"
//                 )
//             """,
//             AGG_BUCKET_2MIN, AGG_BUCKET_4MIN, AGG_BUCKET_4MIN, ORG);

//         log.info("🔥 실행할 4분 평균 Flux 쿼리: {}", fluxQuery);
//         executeFluxQuery(fluxQuery);
//     }

//     /**
//      * ✅ InfluxDB에서 Flux Query 실행하는 메서드
//      */
//     public void executeFluxQuery(String query) {
//         try {
//             QueryApi queryApi = influxDBClient.getQueryApi();
//             List<FluxTable> result = queryApi.query(query, ORG);
            
//             if (result.isEmpty()) {
//                 log.warn("⚠️ Flux Query 실행 결과가 없습니다.");
//             } else {
//                 // 결과 상세 로깅 추가
//                 result.forEach(table -> {
//                     table.getRecords().forEach(record -> {
//                         log.info("저장된 데이터: measurement={}, field={}, value={}, device={}, host={}, time={}",
//                             record.getMeasurement(),
//                             record.getField(),
//                             record.getValue(),
//                             record.getValueByKey("device"),
//                             record.getValueByKey("host"),
//                             record.getTime()
//                         );
//                     });
//                 });
//                 log.info("✅ Flux Query 실행 성공! 저장된 데이터 개수: {}", result.size());
//             }
//         } catch (Exception e) {
//             log.error("❌ Flux Query 실행 실패: {}", e.getMessage(), e);
//             throw new RuntimeException("Flux Query 실행 실패", e);
//         }
//     }
//     private void createBucketIfNotExists(String bucketName) {
//         try {
//             BucketsApi bucketsApi = influxDBClient.getBucketsApi();
//             if (bucketsApi.findBucketByName(bucketName) == null) {
//                 log.info("⚠️ 버킷 생성 시작: {}", bucketName);
                
//                 String orgId = influxDBClient.getOrganizationsApi().findOrganizations().get(0).getId();
                
//                 // Retention 정책 설정 (데이터 유지 기간)
//                 long retentionSeconds = 30 * 24 * 60 * 60L; // 30일을 초로 변환
//                 BucketRetentionRules retentionRule = new BucketRetentionRules();
//                 retentionRule.setShardGroupDurationSeconds(retentionSeconds);
                
//                 // 버킷 생성
//                 Bucket bucket = new Bucket();
//                 bucket.name(bucketName)
//                      .orgID(orgId)
//                      .retentionRules(List.of(retentionRule));
                
//                 bucketsApi.createBucket(bucket);
                
//                 log.info("✅ 버킷 생성 완료: {}", bucketName);
//             } else {
//                 log.info("✅ 기존 버킷 확인: {}", bucketName);
//             }
//         } catch (Exception e) {
//             log.error("❌ 버킷 생성 실패: {} - {}", bucketName, e.getMessage(), e);
//             throw new RuntimeException("버킷 생성 실패: " + bucketName, e);
//         }
//     }

//     /**
//      * ✅ 테스트: InfluxDB에 임의의 센서 데이터 저장
//      */
//     public void saveSensorData(String deviceId, String deviceHost, double temperature, double humidity) {
//         WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        
//         Point point = Point.measurement(MEASUREMENT_NAME)
//             .addTag("device", deviceId)
//             .addTag("host", deviceHost)
//             .addField("temperature", temperature)
//             .addField("humidity", humidity)
//             .time(Instant.now(), WritePrecision.NS);

//         writeApi.writePoint(SOURCE_BUCKET, ORG, point);
//         log.info("✅ 센서 데이터 저장: device={}, host={}, temperature={}, humidity={}", 
//                  deviceId, deviceHost, temperature, humidity);
//     }
// }


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

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class InfluxDBTaskService {
    private final InfluxDBClient influxDBClient;
    private final Map<String, Long> lastAggregationTime = new ConcurrentHashMap<>(); // 마지막 집계 시간 저장
    private static final long MIN_INTERVAL = 60000; // 최소 집계 간격 (1분)

    // InfluxDB 버킷 정의
    private static final String SOURCE_BUCKET = "ydata";  // 원본 데이터 버킷
    private static final String AGG_BUCKET_2MIN = "ydata_2min_avg"; // 2분 평균 버킷
    private static final String AGG_BUCKET_4MIN = "ydata_4min_avg"; // 4분 평균 버킷
    private static final String ORG = "youn";
    private static final String MEASUREMENT_NAME = "sensor_data"; // 측정값 이름

    public InfluxDBTaskService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
    }

    private void initializeBuckets() {
        List.of(SOURCE_BUCKET, AGG_BUCKET_2MIN, AGG_BUCKET_4MIN).forEach(this::createBucketIfNotExists);
    }

    /**
     * ✅ 3분마다 실행 → `ydata`에서 최근 2분 데이터의 평균을 `ydata_2min_avg`에 저장
     */
    @Scheduled(fixedRate = 181000,initialDelay = 60000) // 3분마다 실행
    public void aggregateDataTo2Min() {
        String taskId = "2min";
        if (!canExecuteTask(taskId)) {
            log.debug("2분 평균 데이터 집계 스킵 (최소 간격 미달)");
            return;
        }

        log.info("⏳ 2분 평균 데이터 저장 시작...");
        try {
            String fluxQuery = String.format("""
                from(bucket: "%s")
                  |> range(start: -10m)
                  |> filter(fn: (r) => r._measurement == "sensor_data")
                  |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
                  |> group(columns: ["device", "host", "_field"])
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
            updateLastExecutionTime(taskId);
        } catch (Exception e) {
            log.error("2분 평균 데이터 집계 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * ✅ 6분마다 실행 → `ydata_2min_avg`에서 최근 4분 데이터의 평균을 `ydata_4min_avg`에 저장
     */
    @Scheduled(fixedRate = 303000, initialDelay = 300000) // 6분마다 실행
    public void aggregateDataTo4Min() {
        String taskId = "4min";
        if (!canExecuteTask(taskId)) {
            log.debug("4분 평균 데이터 집계 스킵 (최소 간격 미달)");
            return;
        }

        log.info("⏳ 4분 평균 데이터 저장 시작...");
        try {
            String fluxQuery = String.format("""
                from(bucket: "%s")
      |> range(start: -10m)
      |> filter(fn: (r) => r._measurement == "sensor_data_avg")
      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity")
      |> group(columns: ["device", "host", "_field"])
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
            updateLastExecutionTime(taskId);
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
                            record.getTime()
                        );
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

    private boolean canExecuteTask(String taskId) {
        long currentTime = System.currentTimeMillis();
        Long lastExecution = lastAggregationTime.get(taskId);
        return lastExecution == null || (currentTime - lastExecution) >= MIN_INTERVAL;
    }

    private void updateLastExecutionTime(String taskId) {
        lastAggregationTime.put(taskId, System.currentTimeMillis());
    }

    public void startScheduledTasks() {
        try {
            log.info("스케줄러 작업 시작");
            initializeBuckets();
            
            // 초기 실행 시에도 중복 체크
            aggregateDataTo2Min();
            Thread.sleep(300000); 
            aggregateDataTo4Min();
            
            log.info("✅ 스케줄러 작업 초기화 완료");
        } catch (Exception e) {
            log.error("스케줄러 작업 시작 중 오류 발생: {}", e.getMessage(), e);
        }
    }
}

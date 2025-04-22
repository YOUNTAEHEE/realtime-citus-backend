// // package com.yth.realtime.service;

// // import java.time.Instant;
// // import java.util.ArrayList;
// // import java.util.HashMap;
// // import java.util.List;
// // import java.util.Map;

// // import org.springframework.beans.factory.annotation.Value;
// // import org.springframework.stereotype.Service;

// // import com.influxdb.client.InfluxDBClient;
// // import com.influxdb.client.QueryApi;
// // import com.influxdb.query.FluxRecord;
// // import com.influxdb.query.FluxTable;
// // import com.yth.realtime.dto.HistoricalDataResponse;

// // import lombok.RequiredArgsConstructor;
// // import lombok.extern.slf4j.Slf4j;

// // @Slf4j
// // @Service
// // @RequiredArgsConstructor
// // public class OpcuaHistoricalService {

// //     private final InfluxDBClient influxDBClient;

// //     @Value("${influxdb.bucket}")
// //     private String bucket;

// //     @Value("${influxdb.organization}")
// //     private String organization;

// //     /**
// //      * 과거 OPC UA 데이터를 조회하는 메소드
// //      * 
// //      * @param startTimeStr 시작 시간 (ISO-8601 형식)
// //      * @param endTimeStr   종료 시간 (ISO-8601 형식)
// //      * @param deviceGroup  장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
// //      * @return 과거 데이터 응답 객체
// //      */
// //     public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup) {
// //         try {

// //             Instant startTime = Instant.parse(startTimeStr);
// //             Instant endTime = Instant.parse(endTimeStr);

// //             if (startTime.isAfter(endTime)) {
// //                 return HistoricalDataResponse.builder()
// //                         .success(false)
// //                         .message("시작 시간이 종료 시간보다 늦을 수 없습니다")
// //                         .build();
// //             }

// //             // 최대 3시간으로 쿼리 제한
// //             if (endTime.toEpochMilli() - startTime.toEpochMilli() > 3 * 60 * 60 * 1000) {
// //                 log.warn("요청된 시간 범위가 3시간을 초과합니다. 3시간으로 제한합니다.");
// //                 startTime = Instant.ofEpochMilli(endTime.toEpochMilli() - 3 * 60 * 60 * 1000);
// //             }

// //             List<Map<String, Object>> timeSeriesData = queryInfluxDB(startTime, endTime, deviceGroup);

// //             HistoricalDataResponse response = HistoricalDataResponse.builder()
// //                     .success(true)
// //                     .message("데이터를 성공적으로 조회했습니다")
// //                     .build();

// //             response.setTimeSeriesData(timeSeriesData);
// //             return response;

// //         } catch (Exception e) {
// //             log.error("과거 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
// //             return HistoricalDataResponse.builder()
// //                     .success(false)
// //                     .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
// //                     .build();
// //         }
// //     }

// //     /**
// //      * InfluxDB에서 데이터를 쿼리하는 메소드
// //      */
// //     private List<Map<String, Object>> queryInfluxDB(Instant startTime, Instant endTime, String deviceGroup) {
// //         QueryApi queryApi = influxDBClient.getQueryApi();

// //         // 테이블명, 필드 지정
// //         String measurementName = "opcua_data";

// //         // 디바이스 그룹에 따른 필터 설정
// //         String deviceFilter = "";
// //         // if (!"TOTAL".equalsIgnoreCase(deviceGroup)) {
// //         //     deviceFilter = String.format(" and r[\"_field\"] =~ /%s_.*/", deviceGroup);
// //         //     log.info("디바이스 그룹: {}, 쿼리 필터: {}", deviceGroup, deviceFilter);
// //         // }
// //         if (!"total".equalsIgnoreCase(deviceGroup)) {
// //             // 공통 필드(Filtered_Grid_Freq)와 디바이스 그룹 필드를 모두 포함
// //             deviceFilter = String.format(" and (r[\"_field\"] =~ /^%s_.*/ or r[\"_field\"] == \"Filtered_Grid_Freq\")", deviceGroup);
// //             log.info("디바이스 그룹: {}, 쿼리 필터: {}", deviceGroup, deviceFilter);
// //         }

// //         // Flux 쿼리 작성
// //         String flux = String.format(
// //                 "from(bucket: \"%s\")" +
// //                         " |> range(start: %s, stop: %s)" +
// //                         " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
// //                         " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")" +
// //                         " |> sort(columns: [\"_time\"])",
// //                 bucket,
// //                 startTime,
// //                 endTime,
// //                 measurementName,
// //                 deviceFilter);

// //         log.debug("실행할 Flux 쿼리: {}", flux);

// //         // 쿼리 실행
// //         List<FluxTable> tables = queryApi.query(flux, organization);
// //         log.debug("조회된 테이블 수: {}", tables.size());

// //         // 결과 변환
// //         List<Map<String, Object>> resultList = new ArrayList<>();

// //         for (FluxTable table : tables) {
// //             for (FluxRecord record : table.getRecords()) {
// //                 Map<String, Object> dataPoint = new HashMap<>();

// //                 // 시간 정보 추가
// //                 dataPoint.put("timestamp", record.getTime().toString());

// //                 // 필드 값 추가
// //                 record.getValues().forEach((key, value) -> {
// //                     if (!key.startsWith("_") && value != null) {
// //                         if (value instanceof Double) {
// //                             // NaN 값 처리
// //                             double doubleValue = (Double) value;
// //                             if (Double.isNaN(doubleValue)) {
// //                                 dataPoint.put(key, -1); // NaN을 -1로 대체
// //                             } else {
// //                                 dataPoint.put(key, doubleValue);
// //                             }
// //                         } else {
// //                             dataPoint.put(key, value);
// //                         }
// //                     }
// //                 });

// //                 resultList.add(dataPoint);
// //             }
// //         }

// //         log.info("조회된 데이터 포인트 수: {}", resultList.size());
// //         return resultList;
// //     }

// // }

// //5분 단위로 쿼리 병렬 처리
// package com.yth.realtime.service;

// import java.io.IOException;
// import java.io.StringWriter;
// import java.time.Duration;
// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.Comparator;
// import java.util.HashMap;
// import java.util.LinkedHashSet;
// import java.util.List;
// import java.util.Map;
// import java.util.Set;
// import java.util.concurrent.CompletableFuture;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.TimeUnit;

// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;
// import org.springframework.util.StringUtils;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.exceptions.InfluxException;
// import com.influxdb.query.FluxRecord;
// import com.influxdb.query.FluxTable;
// import com.yth.realtime.dto.HistoricalDataResponse;

// import jakarta.annotation.PreDestroy;
// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// @Service
// @RequiredArgsConstructor
// public class OpcuaHistoricalService {

//     private final InfluxDBClient influxDBClient;

//     @Value("${influxdb.bucket}")
//     private String bucket;

//     @Value("${influxdb.organization}")
//     private String organization;

//     // === 추가: 병렬 쿼리를 위한 ExecutorService ===
//     private final ExecutorService queryExecutor = Executors
//             .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//     // =============================================

//     /**
//      * 과거 OPC UA 데이터를 조회하는 메소드 (병렬 처리 방식)
//      *
//      * @param startTimeStr        시작 시간 (ISO-8601 형식)
//      * @param endTimeStr          종료 시간 (ISO-8601 형식)
//      * @param deviceGroup         장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
//      * @param aggregationInterval 집계 간격 (예: "raw", "10ms", "1s")
//      * @return 과거 데이터 응답 객체
//      */
//     public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup,
//             String aggregationInterval) {
//         log.info("➡️ [조회 시작] deviceGroup: {}, startTime: {}, endTime: {}, aggregationInterval: {}",
//                 deviceGroup, startTimeStr, endTimeStr, aggregationInterval);

//         Instant originalStartTime = Instant.parse(startTimeStr);
//         Instant originalEndTime = Instant.parse(endTimeStr);

//         if (originalStartTime.isAfter(originalEndTime)) {
//             return HistoricalDataResponse.builder()
//                     .success(false)
//                     .message("시작 시간이 종료 시간보다 늦을 수 없습니다")
//                     .build();
//         }

//         // 최대 3시간으로 쿼리 제한 (요청 자체는 3시간까지 받음)
//         Instant queryStartTime = originalStartTime;
//         Instant queryEndTime = originalEndTime;
//         if (Duration.between(queryStartTime, queryEndTime).toHours() > 3) {
//             log.warn("요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
//             queryStartTime = queryEndTime.minus(Duration.ofHours(3));
//         }

//         final Instant finalStartTime = queryStartTime;
//         final Instant finalEndTime = queryEndTime;

//         try {
//             // 데이터 조회 (다운샘플링 없이 원본 데이터 조회)
//             List<Map<String, Object>> allResults = queryHistoricalDataParallel(finalStartTime, finalEndTime,
//                     deviceGroup, aggregationInterval);

//             HistoricalDataResponse response = HistoricalDataResponse.builder()
//                     .success(true)
//                     .message("데이터를 성공적으로 조회했습니다 (병렬 처리, 집계: "
//                             + (StringUtils.hasText(aggregationInterval) ? aggregationInterval : "raw") + ")")
//                     .build();
//             response.setTimeSeriesData(allResults);

//             log.info("✅ [조회 성공] deviceGroup: {}, aggregationInterval: {}, 조회된 데이터 포인트 수: {}",
//                     deviceGroup, aggregationInterval, allResults.size());

//             return response;

//         } catch (Exception e) {
//             log.error("❌ [조회 실패] deviceGroup: {}, aggregationInterval: {}. 오류: {}",
//                     deviceGroup, aggregationInterval, e.getMessage(), e);
//             return HistoricalDataResponse.builder()
//                     .success(false)
//                     .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
//                     .build();
//         }
//     }

//     /**
//      * 과거 OPC UA 데이터를 조회하여 CSV 문자열로 반환하는 메소드 (다운샘플링 없음)
//      *
//      * @param startTimeStr        시작 시간 (ISO-8601 형식)
//      * @param endTimeStr          종료 시간 (ISO-8601 형식)
//      * @param deviceGroup         장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
//      * @param aggregationInterval 집계 간격 (예: "raw", "10ms", "1s")
//      * @return CSV 형식의 문자열 데이터
//      * @throws IOException CSV 생성 중 오류 발생 시
//      */
//     public String exportHistoricalDataToCsv(String startTimeStr, String endTimeStr, String deviceGroup,
//             String aggregationInterval)
//             throws IOException {
//         log.info("➡️ [내보내기 시작] deviceGroup: {}, startTime: {}, endTime: {}, aggregationInterval: {}",
//                 deviceGroup, startTimeStr, endTimeStr, aggregationInterval);

//         Instant originalStartTime = Instant.parse(startTimeStr);
//         Instant originalEndTime = Instant.parse(endTimeStr);

//         if (originalStartTime.isAfter(originalEndTime)) {
//             throw new IllegalArgumentException("시작 시간이 종료 시간보다 늦을 수 없습니다");
//         }

//         // 최대 3시간으로 쿼리 제한
//         Instant queryStartTime = originalStartTime;
//         Instant queryEndTime = originalEndTime;
//         if (Duration.between(queryStartTime, queryEndTime).toHours() > 3) {
//             log.warn("요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
//             queryStartTime = queryEndTime.minus(Duration.ofHours(3));
//         }

//         final Instant finalStartTime = queryStartTime;
//         final Instant finalEndTime = queryEndTime;

//         List<Map<String, Object>> allResults;
//         try {
//             // 데이터 조회 (다운샘플링 없이 원본 데이터 조회)
//             allResults = queryHistoricalDataParallel(finalStartTime, finalEndTime, deviceGroup, aggregationInterval);

//             // 조회된 원본 데이터를 CSV 문자열로 변환
//             String csvData = convertToCsvString(allResults);

//             log.info("✅ [내보내기 성공] deviceGroup: {}, aggregationInterval: {}, 변환된 데이터 포인트 수: {}",
//                     deviceGroup, aggregationInterval, allResults.size());

//             return csvData;

//         } catch (Exception e) {
//             log.error("❌ [내보내기 실패] deviceGroup: {}, aggregationInterval: {}. 오류: {}",
//                     deviceGroup, aggregationInterval, e.getMessage(), e);
//             if (e instanceof IOException) {
//                 throw (IOException) e;
//             }
//             throw new RuntimeException("데이터 조회 또는 CSV 변환 중 오류가 발생했습니다: " + e.getMessage(), e);
//         }
//     }

//     /**
//      * 병렬 쿼리를 통해 과거 데이터를 조회하는 내부 헬퍼 메서드 (다운샘플링 없음)
//      */
//     private List<Map<String, Object>> queryHistoricalDataParallel(Instant startTime, Instant endTime,
//             String deviceGroup, String aggregationInterval) {
//         log.info("⏳ [병렬 조회 시작] deviceGroup: {}, 범위: {} ~ {}, aggregationInterval: {}",
//                 deviceGroup, startTime, endTime, aggregationInterval);

//         List<Map<String, Object>> allResults = Collections.synchronizedList(new ArrayList<>());
//         List<CompletableFuture<Void>> futures = new ArrayList<>();
//         Duration interval = Duration.ofMinutes(5); // 5분 간격으로 분할

//         Instant currentStart = startTime;
//         int queryCount = 0;
//         while (currentStart.isBefore(endTime)) {
//             Instant currentEnd = currentStart.plus(interval);
//             if (currentEnd.isAfter(endTime)) {
//                 currentEnd = endTime;
//             }

//             final Instant subStart = currentStart;
//             final Instant subEnd = currentEnd;
//             queryCount++;

//             CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//                 log.debug("  Sub-query executing: {} to {}, interval: {}", subStart, subEnd, aggregationInterval);
//                 try {
//                     // 내부 쿼리 메서드 호출 (다운샘플링 없이 조회)
//                     List<Map<String, Object>> resultChunk = queryInfluxDBInternal(subStart, subEnd, deviceGroup,
//                             aggregationInterval);
//                     if (resultChunk != null && !resultChunk.isEmpty()) {
//                         allResults.addAll(resultChunk);
//                         log.debug("  Sub-query success ({} ~ {}): {} points added", subStart, subEnd,
//                                 resultChunk.size());
//                     } else {
//                         log.debug("  Sub-query success ({} ~ {}): 0 points found", subStart, subEnd);
//                     }
//                 } catch (Exception e) {
//                     log.warn("  ⚠️ Sub-query failed ({} ~ {}): {}", subStart, subEnd, e.getMessage());
//                 }
//             }, queryExecutor);
//             futures.add(future);

//             currentStart = currentEnd;
//         }

//         log.info("  {}개의 5분 단위 병렬 쿼리 실행 요청됨 (집계 간격: {}).", queryCount, aggregationInterval);
//         CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

//         try {
//             allFutures.get(5, TimeUnit.MINUTES);
//             log.debug("  모든 병렬 쿼리 작업 완료됨.");
//         } catch (Exception e) {
//             log.error("  ❌ 병렬 쿼리 작업 대기 중 오류 발생", e);
//             throw new RuntimeException("과거 데이터 조회 중 병렬 처리 오류", e);
//         }

//         log.debug("  총 {}개의 결과 취합 완료 (정렬 전)", allResults.size());

//         // 결과 정렬 (시간 순서 보장)
//         // Collections.sort() 는 동기화된 리스트에 안전하지 않을 수 있으므로, 새 리스트로 복사 후 정렬
//         List<Map<String, Object>> sortedResults = new ArrayList<>(allResults);
//         sortedResults.sort(Comparator.comparing(m -> {
//             Object ts = m.get("timestamp");
//             return (ts instanceof Instant) ? (Instant) ts : Instant.parse((String) ts);
//         }));

//         log.info("✅ [병렬 조회 완료] deviceGroup: {}, aggregationInterval: {}, 최종 데이터 포인트 수: {}",
//                 deviceGroup, aggregationInterval, sortedResults.size());
//         return sortedResults; // 정렬된 결과 반환
//     }

//     /**
//      * InfluxDB에서 특정 시간 범위의 데이터를 쿼리하는 내부 메소드 (집계 기능 추가)
//      */
//     private List<Map<String, Object>> queryInfluxDBInternal(Instant startTime, Instant endTime, String deviceGroup,
//             String aggregationInterval) {
//         QueryApi queryApi = influxDBClient.getQueryApi();
//         String measurementName = "opcua_data";
//         String deviceFilter = "";

//         // 디바이스 그룹 필터링 로직
//         if (!"total".equalsIgnoreCase(deviceGroup)) {
//             // PCS 그룹 필터링: 해당 PCS 관련 필드 + 공통 필드(Filtered_Grid_Freq 등)
//             // 정규표현식을 사용하여 필드 이름 시작 부분을 체크
//             deviceFilter = String.format(
//                     " and (r[\"_field\"] =~ /^%s_.*/ or r[\"_field\"] == \"Filtered_Grid_Freq\" or r[\"_field\"] == \"T_Simul_P_REAL\")",
//                     deviceGroup.toUpperCase()); // PCS1, PCS2 등
//             // 필요하다면 여기에 Total에서 필요한 다른 공통 필드도 or 조건으로 추가
//         } else {
//             // Total 그룹: 특정 필드들 또는 전체 (여기서는 예시로 전체 필터링 없음)
//             // 필요시 Total에만 해당하는 필터 로직 추가
//             // deviceFilter = " and (r[\"_field\"] == \"Total_TPWR_P_REAL\" or ...)";
//         }

//         // 집계 쿼리 생성
//         String aggregationFunction = "";
//         if (StringUtils.hasText(aggregationInterval) && !"raw".equalsIgnoreCase(aggregationInterval)) {
//             aggregationFunction = String.format(
//                     " |> aggregateWindow(every: %s, fn: mean, createEmpty: true)",
//                     aggregationInterval);
//             log.debug("Applying aggregation interval: {}", aggregationInterval);
//         } else {
//             log.debug("No aggregation applied (raw data requested).");
//         }

//         // Flux 쿼리 구성 (기본 쿼리 + 집계 함수 + 피벗)
//         String flux = String.format(
//                 "from(bucket: \"%s\")" +
//                         " |> range(start: %s, stop: %s)" +
//                         " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
//                         "%s" +
//                         " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")",
//                 bucket,
//                 startTime.toString(),
//                 endTime.toString(),
//                 measurementName,
//                 deviceFilter,
//                 aggregationFunction);

//         log.debug("    Executing Flux query ({} ~ {}): {}", startTime, endTime, flux);

//         List<Map<String, Object>> resultList = new ArrayList<>();
//         try {
//             List<FluxTable> tables = queryApi.query(flux, organization);
//             log.debug("    InfluxDB query returned {} tables.", tables.size());

//             // --- 추가: 반환된 타임스탬프 로깅 (처음 5개) ---
//             log.debug("    First 5 timestamps returned by InfluxDB for interval '{}':", aggregationInterval);
//             int tsCount = 0;
//             for (FluxTable table : tables) {
//                 for (FluxRecord record : table.getRecords()) {
//                     if (tsCount < 5) {
//                         log.debug("      - {}", record.getTime());
//                         tsCount++;
//                     }
//                     Map<String, Object> dataPoint = new HashMap<>();
//                     dataPoint.put("timestamp", record.getTime());
//                     record.getValues().forEach((key, value) -> {
//                         if (!key.startsWith("_") && value != null) {
//                             if (value instanceof Double && Double.isNaN((Double) value)) {
//                                 dataPoint.put(key, null);
//                             } else {
//                                 dataPoint.put(key, value);
//                             }
//                         }
//                     });
//                     if (dataPoint.size() > 1) {
//                         resultList.add(dataPoint);
//                     }
//                     if (tsCount >= 5 && table == tables.get(0))
//                         break; // 첫 테이블의 처음 5개만 확인
//                 }
//                 if (tsCount >= 5)
//                     break; // 전체 테이블에서 5개 채웠으면 중단
//             }
//             if (tsCount == 0) {
//                 log.debug("      - No timestamps returned.");
//             }
//             // ------------------------------------------

//             log.debug("    Query successful and transformed ({} ~ {}): {} points", startTime, endTime,
//                     resultList.size());
//         } catch (InfluxException ie) {
//             log.error("    ❌ InfluxDB query error ({} ~ {}): {}", startTime, endTime, ie.getMessage());
//             throw ie;
//         } catch (Exception e) {
//             log.error("    ❌ Error processing query results ({} ~ {}): {}", startTime, endTime, e.getMessage(), e);
//             throw new RuntimeException("쿼리 결과 처리 중 오류", e);
//         }

//         return resultList;
//     }

//     /**
//      * 데이터 리스트를 CSV 형식 문자열로 변환하는 헬퍼 메서드
//      */
//     private String convertToCsvString(List<Map<String, Object>> dataList) throws IOException {
//         if (dataList == null || dataList.isEmpty()) {
//             return ""; // 데이터 없으면 빈 문자열 반환
//         }

//         StringWriter stringWriter = new StringWriter();

//         // 헤더 생성 (모든 데이터에서 발견된 키 사용, 순서 유지를 위해 LinkedHashSet)
//         // 'timestamp'를 맨 앞에 추가하고 나머지 키는 알파벳 순으로 정렬하여 일관성 유지
//         Set<String> headers = new LinkedHashSet<>();
//         Set<String> valueHeaders = new java.util.TreeSet<>(); // 알파벳 순 정렬
//         headers.add("timestamp"); // timestamp는 항상 첫 번째

//         for (Map<String, Object> dataPoint : dataList) {
//             for (String key : dataPoint.keySet()) {
//                 if (!"timestamp".equals(key)) {
//                     valueHeaders.add(key);
//                 }
//             }
//         }
//         headers.addAll(valueHeaders); // 정렬된 값 헤더 추가

//         // 헤더 쓰기
//         stringWriter.append(String.join(",", headers)).append("\n");

//         // 데이터 행 쓰기
//         for (Map<String, Object> dataPoint : dataList) {
//             List<String> row = new ArrayList<>();
//             for (String header : headers) {
//                 Object value = dataPoint.get(header);
//                 String valueStr;
//                 if (value instanceof Instant) {
//                     // Instant를 ISO-8601 형식 문자열로 변환
//                     valueStr = ((Instant) value).toString();
//                 } else {
//                     valueStr = (value == null) ? "" : String.valueOf(value);
//                 }

//                 // CSV 값 이스케이프 처리 (쉼표, 따옴표, 줄바꿈 문자 포함 시)
//                 if (valueStr.contains(",") || valueStr.contains("\"") || valueStr.contains("\n")) {
//                     valueStr = "\"" + valueStr.replace("\"", "\"\"") + "\"";
//                 }
//                 row.add(valueStr);
//             }
//             stringWriter.append(String.join(",", row)).append("\n");
//         }

//         return stringWriter.toString();
//     }

//     // ExecutorService 종료 로직
//     @PreDestroy
//     public void shutdownExecutor() {
//         log.info("Shutting down OpcuaHistoricalService query executor...");
//         if (queryExecutor != null && !queryExecutor.isShutdown()) {
//             queryExecutor.shutdown();
//             try {
//                 if (!queryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
//                     queryExecutor.shutdownNow();
//                     log.warn("Query executor forcefully shut down.");
//                 } else {
//                     log.info("Query executor shut down gracefully.");
//                 }
//             } catch (InterruptedException e) {
//                 queryExecutor.shutdownNow();
//                 Thread.currentThread().interrupt();
//                 log.error("Interrupted while shutting down query executor.", e);
//             }
//         }
//     }
// }


// package com.yth.realtime.service;

// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;

// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.query.FluxRecord;
// import com.influxdb.query.FluxTable;
// import com.yth.realtime.dto.HistoricalDataResponse;

// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// @Service
// @RequiredArgsConstructor
// public class OpcuaHistoricalService {

//     private final InfluxDBClient influxDBClient;

//     @Value("${influxdb.bucket}")
//     private String bucket;

//     @Value("${influxdb.organization}")
//     private String organization;

//     /**
//      * 과거 OPC UA 데이터를 조회하는 메소드
//      * 
//      * @param startTimeStr 시작 시간 (ISO-8601 형식)
//      * @param endTimeStr   종료 시간 (ISO-8601 형식)
//      * @param deviceGroup  장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
//      * @return 과거 데이터 응답 객체
//      */
//     public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup) {
//         try {

//             Instant startTime = Instant.parse(startTimeStr);
//             Instant endTime = Instant.parse(endTimeStr);

//             if (startTime.isAfter(endTime)) {
//                 return HistoricalDataResponse.builder()
//                         .success(false)
//                         .message("시작 시간이 종료 시간보다 늦을 수 없습니다")
//                         .build();
//             }

//             // 최대 3시간으로 쿼리 제한
//             if (endTime.toEpochMilli() - startTime.toEpochMilli() > 3 * 60 * 60 * 1000) {
//                 log.warn("요청된 시간 범위가 3시간을 초과합니다. 3시간으로 제한합니다.");
//                 startTime = Instant.ofEpochMilli(endTime.toEpochMilli() - 3 * 60 * 60 * 1000);
//             }

//             List<Map<String, Object>> timeSeriesData = queryInfluxDB(startTime, endTime, deviceGroup);

//             HistoricalDataResponse response = HistoricalDataResponse.builder()
//                     .success(true)
//                     .message("데이터를 성공적으로 조회했습니다")
//                     .build();

//             response.setTimeSeriesData(timeSeriesData);
//             return response;

//         } catch (Exception e) {
//             log.error("과거 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
//             return HistoricalDataResponse.builder()
//                     .success(false)
//                     .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
//                     .build();
//         }
//     }

//     /**
//      * InfluxDB에서 데이터를 쿼리하는 메소드
//      */
//     private List<Map<String, Object>> queryInfluxDB(Instant startTime, Instant endTime, String deviceGroup) {
//         QueryApi queryApi = influxDBClient.getQueryApi();

//         // 테이블명, 필드 지정
//         String measurementName = "opcua_data";

//         // 디바이스 그룹에 따른 필터 설정
//         String deviceFilter = "";
//         // if (!"TOTAL".equalsIgnoreCase(deviceGroup)) {
//         //     deviceFilter = String.format(" and r[\"_field\"] =~ /%s_.*/", deviceGroup);
//         //     log.info("디바이스 그룹: {}, 쿼리 필터: {}", deviceGroup, deviceFilter);
//         // }
//         if (!"total".equalsIgnoreCase(deviceGroup)) {
//             // 공통 필드(Filtered_Grid_Freq)와 디바이스 그룹 필드를 모두 포함
//             deviceFilter = String.format(" and (r[\"_field\"] =~ /^%s_.*/ or r[\"_field\"] == \"Filtered_Grid_Freq\")", deviceGroup);
//             log.info("디바이스 그룹: {}, 쿼리 필터: {}", deviceGroup, deviceFilter);
//         }

//         // Flux 쿼리 작성
//         String flux = String.format(
//                 "from(bucket: \"%s\")" +
//                         " |> range(start: %s, stop: %s)" +
//                         " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
//                         " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")" +
//                         " |> sort(columns: [\"_time\"])",
//                 bucket,
//                 startTime,
//                 endTime,
//                 measurementName,
//                 deviceFilter);

//         log.debug("실행할 Flux 쿼리: {}", flux);

//         // 쿼리 실행
//         List<FluxTable> tables = queryApi.query(flux, organization);
//         log.debug("조회된 테이블 수: {}", tables.size());

//         // 결과 변환
//         List<Map<String, Object>> resultList = new ArrayList<>();

//         for (FluxTable table : tables) {
//             for (FluxRecord record : table.getRecords()) {
//                 Map<String, Object> dataPoint = new HashMap<>();

//                 // 시간 정보 추가
//                 dataPoint.put("timestamp", record.getTime().toString());

//                 // 필드 값 추가
//                 record.getValues().forEach((key, value) -> {
//                     if (!key.startsWith("_") && value != null) {
//                         if (value instanceof Double) {
//                             // NaN 값 처리
//                             double doubleValue = (Double) value;
//                             if (Double.isNaN(doubleValue)) {
//                                 dataPoint.put(key, -1); // NaN을 -1로 대체
//                             } else {
//                                 dataPoint.put(key, doubleValue);
//                             }
//                         } else {
//                             dataPoint.put(key, value);
//                         }
//                     }
//                 });

//                 resultList.add(dataPoint);
//             }
//         }

//         log.info("조회된 데이터 포인트 수: {}", resultList.size());
//         return resultList;
//     }

// }

//5분 단위로 쿼리 병렬 처리
package com.yth.realtime.service;

import java.time.Duration; // Duration import 추가
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections; // Collections import 추가
import java.util.Comparator; // Comparator import 추가
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture; // CompletableFuture import 추가
import java.util.concurrent.ExecutorService; // ExecutorService import 추가
import java.util.concurrent.Executors; // Executors import 추가
import java.util.concurrent.TimeUnit; // TimeUnit import 추가

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.exceptions.InfluxException; // InfluxException import 추가
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.yth.realtime.dto.HistoricalDataResponse;

import jakarta.annotation.PreDestroy; // PreDestroy import 추가
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class OpcuaHistoricalService {

    private final InfluxDBClient influxDBClient;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.organization}")
    private String organization;

    // === 추가: 병렬 쿼리를 위한 ExecutorService ===
    private final ExecutorService queryExecutor = Executors.newFixedThreadPool(3); // 예시: 3개로 줄임
    // =============================================

    /**
     * 과거 OPC UA 데이터를 조회하는 메소드 (병렬 처리 방식)
     *
     * @param startTimeStr 시작 시간 (ISO-8601 형식)
     * @param endTimeStr   종료 시간 (ISO-8601 형식)
     * @param deviceGroup  장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
     * @return 과거 데이터 응답 객체
     */
    public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup) {
        Instant originalStartTime = Instant.parse(startTimeStr);
        Instant originalEndTime = Instant.parse(endTimeStr);

        if (originalStartTime.isAfter(originalEndTime)) {
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("시작 시간이 종료 시간보다 늦을 수 없습니다")
                    .build();
        }

        // 최대 3시간으로 쿼리 제한 (요청 자체는 3시간까지 받음)
        Instant queryStartTime = originalStartTime;
        Instant queryEndTime = originalEndTime;
        if (Duration.between(queryStartTime, queryEndTime).toHours() > 3) {
            log.warn("요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
            queryStartTime = queryEndTime.minus(Duration.ofHours(3));
        }

        final Instant finalStartTime = queryStartTime;
        final Instant finalEndTime = queryEndTime;

        try {
            // --- 수정: 시간 범위 분할 및 병렬 쿼리 ---
            List<Map<String, Object>> allResults = new ArrayList<>();
            List<CompletableFuture<List<Map<String, Object>>>> futures = new ArrayList<>();
            Duration interval = Duration.ofMinutes(5); // 5분 간격

            Instant currentStart = finalStartTime;
            while (currentStart.isBefore(finalEndTime)) {
                Instant currentEnd = currentStart.plus(interval);
                if (currentEnd.isAfter(finalEndTime)) {
                    currentEnd = finalEndTime;
                }

                final Instant subStart = currentStart;
                final Instant subEnd = currentEnd;

                // 각 5분 범위 쿼리를 비동기 실행
                CompletableFuture<List<Map<String, Object>>> future = CompletableFuture.supplyAsync(() -> {
                    log.info("Executing query for range: {} to {}", subStart, subEnd);
                    // 내부 쿼리 메서드 호출
                    return queryInfluxDBInternal(subStart, subEnd, deviceGroup);
                }, queryExecutor); // 생성한 스레드 풀 사용
                futures.add(future);

                currentStart = currentEnd; // 다음 구간 시작 시간 설정
            }

            // 모든 비동기 작업이 완료될 때까지 대기
            log.info("{}개의 5분 단위 병렬 쿼리 실행 시작...", futures.size());
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            try {
                // 전체 작업에 대한 타임아웃 설정 (예: 1분, 개별 30초 타임아웃과 별개)
                allFutures.get(5, TimeUnit.MINUTES); // 1분 내 모든 작업 완료 대기
                log.info("모든 병렬 쿼리 작업 완료.");
            } catch (Exception e) {
                log.error("병렬 쿼리 작업 대기 중 오류 발생 (타임아웃 또는 개별 작업 실패)", e);
                // 실패 시 빈 리스트 또는 부분 결과 반환 고려 (여기서는 실패 메시지 반환)
                throw new RuntimeException("과거 데이터 조회 중 병렬 처리 오류", e);
            }

            // 결과 취합
            for (CompletableFuture<List<Map<String, Object>>> future : futures) {
                // 개별 작업에서 예외가 발생했을 수 있으므로 확인 후 결과 추가
                if (!future.isCompletedExceptionally()) {
                    allResults.addAll(future.join()); // join()은 검사 예외를 던지지 않음
                } else {
                    log.warn("병렬 쿼리 중 일부 작업 실패 (결과 누락 가능성 있음)");
                    // 실패한 작업에 대한 추가 처리 필요 시 여기에 구현
                }
            }
            log.info("총 {}개의 결과 취합 완료. (정렬 전)", allResults.size());

            // --- 추가: 결과 정렬 (시간 순서 보장) ---
            allResults.sort(Comparator.comparing(m -> Instant.parse((String) m.get("timestamp"))));
            log.info("결과 정렬 완료.");
            // =====================================

            HistoricalDataResponse response = HistoricalDataResponse.builder()
                    .success(true)
                    .message("데이터를 성공적으로 조회했습니다 (병렬 처리)")
                    .build();

            response.setTimeSeriesData(allResults); // 합쳐지고 정렬된 결과 설정
            return response;

        } catch (Exception e) {
            log.error("과거 데이터 조회 중 오류 발생 (병렬 처리 중): {}", e.getMessage(), e);
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
                    .build();
        }
    }

    /**
     * InfluxDB에서 특정 시간 범위의 데이터를 쿼리하는 내부 메소드
     */
    private List<Map<String, Object>> queryInfluxDBInternal(Instant startTime, Instant endTime, String deviceGroup) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        String measurementName = "opcua_data";
        String deviceFilter = "";

        if (!"total".equalsIgnoreCase(deviceGroup)) {
            deviceFilter = String.format(" and (r[\"_field\"] =~ /^%s_.*/ or r[\"_field\"] == \"Filtered_Grid_Freq\")",
                    deviceGroup);
            // log.info("디바이스 그룹: {}, 쿼리 필터: {}", deviceGroup, deviceFilter); // 로그 레벨 조정 또는
            // 제거 고려
        }

        // --- 중요: 쿼리 최적화를 위해 pivot, sort는 여기서 제거하는 것을 강력히 권장 ---
        // 이 예제에서는 일단 기존 쿼리 구조 유지 (병렬 처리 효과만 확인)
        // 만약 pivot/sort 제거 시, 아래 결과 변환 로직도 함께 수정 필요
        String flux = String.format(
                "from(bucket: \"%s\")" +
                        " |> range(start: %s, stop: %s)" +
                        " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
                        " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")",
                // " |> sort(columns: [\"_time\"])", // 개별 쿼리에서 sort 제거 (나중에 전체 결과 정렬)
                bucket,
                startTime,
                endTime,
                measurementName,
                deviceFilter);

        // log.debug("실행할 Flux 쿼리 ({} ~ {}): {}", startTime, endTime, flux); // 로그 레벨 조정

        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
            // 쿼리 실행 (클라이언트 타임아웃 설정이 적용됨, 예: 30초)
            List<FluxTable> tables = queryApi.query(flux, organization);
            // log.debug("조회된 테이블 수 ({} ~ {}): {}", startTime, endTime, tables.size());

            // --- 결과 변환 (기존 로직 유지) ---
            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("timestamp", record.getTime().toString());
                    record.getValues().forEach((key, value) -> {
                        if (!key.startsWith("_") && value != null) {
                            if (value instanceof Double) {
                                double doubleValue = (Double) value;
                                dataPoint.put(key, Double.isNaN(doubleValue) ? -1 : doubleValue); // NaN 처리
                            } else {
                                dataPoint.put(key, value);
                            }
                        }
                    });
                    resultList.add(dataPoint);
                }
            }
            log.info("쿼리 성공 및 변환 완료 ({} ~ {}): {} 포인트", startTime, endTime, resultList.size());
            // -----------------------------
        } catch (InfluxException ie) {
            // 타임아웃 등 InfluxDB 관련 예외 처리
            log.error("InfluxDB 쿼리 오류 ({} ~ {}): {}", startTime, endTime, ie.getMessage());
            // 여기서 빈 리스트 반환 또는 예외 다시 던지기 선택
            // CompletableFuture에서 처리할 것이므로 빈 리스트 반환
            return Collections.emptyList();
        } catch (Exception e) {
            log.error("쿼리 결과 처리 중 오류 ({} ~ {}): {}", startTime, endTime, e.getMessage(), e);
            return Collections.emptyList();
        }

        return resultList;
    }

    // === 추가: ExecutorService 종료 로직 ===
    @PreDestroy
    public void shutdownExecutor() {
        log.info("Shutting down OpcuaHistoricalService query executor...");
        if (queryExecutor != null && !queryExecutor.isShutdown()) {
            queryExecutor.shutdown();
            try {
                if (!queryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    queryExecutor.shutdownNow();
                    log.warn("Query executor forcefully shut down.");
                } else {
                    log.info("Query executor shut down gracefully.");
                }
            } catch (InterruptedException e) {
                queryExecutor.shutdownNow();
                Thread.currentThread().interrupt();
                log.error("Interrupted while shutting down query executor.", e);
            }
        }
    }
    // =====================================
}



// //다운샘플링 export 1초단위로 보기
// package com.yth.realtime.service;

// import java.time.Duration;
// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.Comparator;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.CompletableFuture;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.TimeUnit;

// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;

// import com.influxdb.client.InfluxDBClient;
// import com.influxdb.client.QueryApi;
// import com.influxdb.exceptions.InfluxException;
// import com.influxdb.query.FluxRecord;
// import com.influxdb.query.FluxTable;
// import com.yth.realtime.dto.HistoricalDataResponse;

// import jakarta.annotation.PreDestroy;
// import lombok.Data;
// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// @Service
// @RequiredArgsConstructor
// public class OpcuaHistoricalService {

//     private final InfluxDBClient influxDBClient;

//     @Value("${influxdb.bucket}")
//     private String bucket;

//     @Value("${influxdb.organization}")
//     private String organization;

//     // 동시 실행 스레드 수를 줄여서 테스트 (예: 3개)
//     private final ExecutorService queryExecutor = Executors.newFixedThreadPool(3);

//     // 다운샘플링 적용 기준 시간 (예: 1시간)
//     private static final Duration DOWNSAMPLING_THRESHOLD = Duration.ofHours(1);
//     // 다운샘플링 시 목표 데이터 포인트 수 (조정 가능)
//     private static final int TARGET_DOWNSAMPLED_POINTS = 2000;

//     public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup) {

//         try {
//             // 데이터 조회 로직 호출
//             List<Map<String, Object>> timeSeriesData = queryTimeSeriesData(startTimeStr, endTimeStr, deviceGroup);

//             // 다운샘플링 여부 판단 (조회 로직에서 결정된 방식 확인 필요 - 여기서는 간단히 시간 차이로 다시 판단)
//             Instant queryStartTime = Instant.parse(startTimeStr);
//             Instant queryEndTime = Instant.parse(endTimeStr);
//             Duration adjustedDuration = Duration.between(queryStartTime, queryEndTime);
//             boolean wasDownsampled = adjustedDuration.compareTo(DOWNSAMPLING_THRESHOLD) > 0; // 실제 조회 방식과 일치해야 함

//             HistoricalDataResponse response = HistoricalDataResponse.builder()
//                     .success(true)
//                     .message(wasDownsampled ? "데이터를 성공적으로 조회했습니다 (다운샘플링 적용)" : "데이터를 성공적으로 조회했습니다 (병렬 처리)")
//                     .data(Map.of("timeSeries", timeSeriesData)) // 조회된 데이터 설정
//                     .build();

//             return response;

//         } catch (IllegalArgumentException iae) { // 시간 파싱 또는 순서 오류
//             log.warn("잘못된 시간 파라미터: {}", iae.getMessage());
//             return HistoricalDataResponse.builder()
//                     .success(false)
//                     .message(iae.getMessage()) // 오류 메시지 전달
//                     .build();
//         } catch (Exception e) {
//             log.error("과거 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
//             return HistoricalDataResponse.builder()
//                     .success(false)
//                     .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
//                     .build();
//         }
//     }

//     // --- 데이터 조회 로직을 분리한 private 메서드 ---
//     private List<Map<String, Object>> queryTimeSeriesData(String startTimeStr, String endTimeStr, String deviceGroup)
//             throws IllegalArgumentException {
//         Instant originalStartTime;
//         Instant originalEndTime;
//         try {
//             originalStartTime = Instant.parse(startTimeStr);
//             originalEndTime = Instant.parse(endTimeStr);
//         } catch (Exception e) {
//             throw new IllegalArgumentException("시간 형식이 잘못되었습니다: " + e.getMessage());
//         }

//         if (originalStartTime.isAfter(originalEndTime)) {
//             throw new IllegalArgumentException("시작 시간이 종료 시간보다 늦을 수 없습니다");
//         }

//         Instant queryStartTime = originalStartTime;
//         Instant queryEndTime = originalEndTime;
//         final Duration queryDuration = Duration.between(queryStartTime, queryEndTime);

//         // 최대 3시간 제한은 유지 (요청 범위 자체 제한)
//         if (queryDuration.toHours() > 3) {
//             log.warn("요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
//             queryStartTime = queryEndTime.minus(Duration.ofHours(3));
//         }

//         final Instant finalStartTime = queryStartTime;
//         final Instant finalEndTime = queryEndTime;
//         final Duration adjustedDuration = Duration.between(finalStartTime, finalEndTime); // 실제 쿼리할 기간

//         List<Map<String, Object>> timeSeriesData;

//         // 시간 범위에 따라 다운샘플링 여부 결정
//         boolean applyDownsampling = adjustedDuration.compareTo(DOWNSAMPLING_THRESHOLD) > 0;

//         if (applyDownsampling) {
//             log.info("요청 시간 범위({}시간)가 임계값({}시간)을 초과하여 다운샘플링을 적용합니다.",
//                     adjustedDuration.toHours(), DOWNSAMPLING_THRESHOLD.toHours());
//             // 다운샘플링 쿼리 실행
//             timeSeriesData = queryDownsampledData(finalStartTime, finalEndTime, deviceGroup, adjustedDuration);
//         } else {
//             log.info("요청 시간 범위({}분)가 임계값 미만이므로 원본 데이터를 조회합니다. (병렬 처리)",
//                     adjustedDuration.toMinutes());
//             // 기존 병렬 쿼리 방식 사용 (pivot/sort 제거된 쿼리)
//             timeSeriesData = queryRawDataParallel(finalStartTime, finalEndTime, deviceGroup);
//         }
//         return timeSeriesData;
//     }
//     // =======================================================

//     // --- 추가: 다운샘플링 데이터 조회 메서드 (기존 코드 유지) ---
//     private List<Map<String, Object>> queryDownsampledData(Instant startTime, Instant endTime, String deviceGroup,
//             Duration totalDuration) {
//         QueryApi queryApi = influxDBClient.getQueryApi();
//         String measurementName = "opcua_data";
//         String deviceFilter = "";
//         if (!"total".equalsIgnoreCase(deviceGroup)) {
//             deviceFilter = String.format(" and (r._field =~ /^%s_.*/ or r._field == \"Filtered_Grid_Freq\")",
//                     deviceGroup); // 필드 참조 시 r. 사용
//         }

//         // 목표 포인트 수에 맞춰 집계 간격 동적 계산 (최소 1초)
//         long intervalSeconds = Math.max(1, totalDuration.getSeconds() / TARGET_DOWNSAMPLED_POINTS);
//         String windowPeriod = intervalSeconds + "s";
//         log.info("다운샘플링 간격 계산: 전체 {}초 / 목표 {}개 = {}초 -> windowPeriod: {}",
//                 totalDuration.getSeconds(), TARGET_DOWNSAMPLED_POINTS, intervalSeconds, windowPeriod);

//         // 다운샘플링 쿼리 (예: 1분 평균 - fn: mean, pivot 없음)
//         // 시각화에는 mean 또는 first/last 가 적합할 수 있음. 여기서는 mean 사용
//         String flux = String.format(
//                 "from(bucket: \"%s\")" +
//                         " |> range(start: %s, stop: %s)" +
//                         " |> filter(fn: (r) => r._measurement == \"%s\"%s)" + // _field 참조 수정
//                         // 집계: windowPeriod 간격으로 평균 계산
//                         " |> aggregateWindow(every: %s, fn: mean, createEmpty: false)" +
//                         // 집계 후 필요한 데이터 형태로 재구성 (pivot과 유사 효과)
//                         " |> group(columns: [\"_time\"])" + // 시간 기준으로 그룹화
//                         " |> map(fn: (r) => ({ _time: r._time, _field: r._field, _value: r._value }))" + // 필요한 컬럼만 선택
//                         " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")" + // 다운샘플링 후
//                                                                                                             // pivot
//                         " |> sort(columns: [\"_time\"])", // 정렬 추가
//                 bucket,
//                 startTime,
//                 endTime,
//                 measurementName,
//                 deviceFilter,
//                 windowPeriod // %s 에 windowPeriod 삽입
//         // "mean" // 집계 함수 지정 (주석 제거)
//         );

//         log.debug("실행할 다운샘플링 Flux 쿼리: {}", flux);

//         List<Map<String, Object>> resultList = new ArrayList<>();
//         try {
//             List<FluxTable> tables = queryApi.query(flux, organization);
//             log.debug("다운샘플링 쿼리 결과 테이블 수: {}", tables.size());

//             // 다운샘플링된 결과 변환 (pivot 적용했으므로 기존 로직과 유사하게 처리 가능)
//             for (FluxTable table : tables) {
//                 for (FluxRecord record : table.getRecords()) {
//                     Map<String, Object> dataPoint = new HashMap<>();
//                     dataPoint.put("timestamp", record.getTime().toString());
//                     record.getValues().forEach((key, value) -> {
//                         if (!key.startsWith("_") && value != null) {
//                             if (value instanceof Double) {
//                                 double doubleValue = (Double) value;
//                                 dataPoint.put(key, Double.isNaN(doubleValue) ? -1 : doubleValue);
//                             } else {
//                                 dataPoint.put(key, value); // 다른 타입도 그대로 넣기
//                             }
//                         }
//                     });
//                     if (!dataPoint.keySet().stream().allMatch(k -> k.equals("timestamp"))) { // timestamp 외 다른 키가 있는지 확인
//                         resultList.add(dataPoint);
//                     }
//                 }
//             }
//             log.info("다운샘플링 쿼리 성공 및 변환 완료: {} 포인트", resultList.size());

//         } catch (InfluxException ie) {
//             log.error("InfluxDB 다운샘플링 쿼리 오류: {}", ie.getMessage());
//             throw ie;
//         } catch (Exception e) {
//             log.error("다운샘플링 결과 처리 중 오류: {}", e.getMessage(), e);
//             throw new RuntimeException("다운샘플링 데이터 처리 오류", e);
//         }
//         return resultList;
//     }
//     // ======================================

//     // --- 기존 병렬 쿼리 메서드 이름 변경 및 수정 (기존 코드 유지) ---
//     private List<Map<String, Object>> queryRawDataParallel(Instant startTime, Instant endTime, String deviceGroup) {
//         List<Map<String, Object>> allResults = new ArrayList<>();
//         List<CompletableFuture<List<Map<String, Object>>>> futures = new ArrayList<>();
//         Duration interval = Duration.ofMinutes(5); // 5분 간격

//         Instant currentStart = startTime;
//         while (currentStart.isBefore(endTime)) {
//             Instant currentEnd = currentStart.plus(interval);
//             if (currentEnd.isAfter(endTime)) {
//                 currentEnd = endTime;
//             }
//             final Instant subStart = currentStart;
//             final Instant subEnd = currentEnd;

//             CompletableFuture<List<Map<String, Object>>> future = CompletableFuture.supplyAsync(() -> {
//                 // log.info("Executing raw query for range: {} to {}", subStart, subEnd);
//                 // 원본 데이터 조회 (pivot/sort 제거된 쿼리 사용)
//                 return queryInfluxDBInternalRaw(subStart, subEnd, deviceGroup);
//             }, queryExecutor);
//             futures.add(future);
//             currentStart = currentEnd;
//         }

//         log.info("{}개의 5분 단위 병렬 원본 데이터 쿼리 실행 시작...", futures.size());
//         CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

//         try {
//             allFutures.get(1, TimeUnit.MINUTES); // 타임아웃 조정 가능
//             log.info("모든 병렬 원본 데이터 쿼리 작업 완료.");
//         } catch (Exception e) {
//             log.error("병렬 원본 데이터 쿼리 작업 대기 중 오류 발생", e);
//             throw new RuntimeException("과거 데이터 조회 중 병렬 처리 오류", e);
//         }

//         for (CompletableFuture<List<Map<String, Object>>> future : futures) {
//             if (!future.isCompletedExceptionally()) {
//                 allResults.addAll(future.join());
//             } else {
//                 log.warn("병렬 원본 데이터 쿼리 중 일부 작업 실패");
//             }
//         }
//         log.info("총 {}개의 원본 데이터 결과 취합 완료. (정렬 전)", allResults.size());

//         // 결과 정렬
//         allResults.sort(Comparator.comparing(m -> Instant.parse((String) m.get("timestamp"))));
//         log.info("원본 데이터 결과 정렬 완료.");

//         return allResults;
//     }
//     // =========================================

//     // --- 추가: Pivot/Sort 없는 원본 데이터 조회 내부 메서드 (기존 코드 유지) ---
//     private List<Map<String, Object>> queryInfluxDBInternalRaw(Instant startTime, Instant endTime, String deviceGroup) {
//         QueryApi queryApi = influxDBClient.getQueryApi();
//         String measurementName = "opcua_data";
//         String deviceFilter = "";
//         if (!"total".equalsIgnoreCase(deviceGroup)) {
//             deviceFilter = String.format(" and (r._field =~ /^%s_.*/ or r._field == \"Filtered_Grid_Freq\")",
//                     deviceGroup);
//         }

//         // Pivot과 Sort가 제거된 쿼리
//         String flux = String.format(
//                 "from(bucket: \"%s\")" +
//                         " |> range(start: %s, stop: %s)" +
//                         " |> filter(fn: (r) => r._measurement == \"%s\"%s)", // pivot/sort 제거됨
//                 bucket,
//                 startTime,
//                 endTime,
//                 measurementName,
//                 deviceFilter);

//         // log.debug("실행할 Raw Flux 쿼리 ({} ~ {}): {}", startTime, endTime, flux);

//         List<Map<String, Object>> resultList = new ArrayList<>();
//         try {
//             List<FluxTable> tables = queryApi.query(flux, organization);
//             // log.debug("Raw 쿼리 테이블 수 ({} ~ {}): {}", startTime, endTime, tables.size());

//             // 결과 변환: Pivot되지 않은 형태이므로 구조가 다름
//             // 각 레코드는 하나의 필드와 값을 가짐 (_time, _field, _value)
//             // 이를 timestamp를 키로 하는 Map으로 재구성해야 함
//             Map<Instant, Map<String, Object>> timeBasedMap = new HashMap<>();

//             for (FluxTable table : tables) {
//                 for (FluxRecord record : table.getRecords()) {
//                     Instant recordTime = record.getTime();
//                     String field = record.getField();
//                     Object value = record.getValue();

//                     // 해당 타임스탬프의 Map을 가져오거나 새로 생성
//                     Map<String, Object> dataPoint = timeBasedMap.computeIfAbsent(recordTime, k -> {
//                         Map<String, Object> newPoint = new HashMap<>();
//                         newPoint.put("timestamp", k.toString()); // 타임스탬프 문자열 추가
//                         return newPoint;
//                     });

//                     // 필드 값 추가
//                     if (value != null) {
//                         if (value instanceof Double) {
//                             double doubleValue = (Double) value;
//                             dataPoint.put(field, Double.isNaN(doubleValue) ? -1 : doubleValue);
//                         } else {
//                             dataPoint.put(field, value);
//                         }
//                     }
//                 }
//             }
//             // Map의 value들(각 timestamp별 dataPoint Map)을 리스트로 변환
//             resultList.addAll(timeBasedMap.values());

//             log.info("Raw 쿼리 성공 및 변환 완료 ({} ~ {}): {} 타임스탬프 포인트", startTime, endTime, resultList.size());

//         } catch (InfluxException ie) {
//             log.error("InfluxDB Raw 쿼리 오류 ({} ~ {}): {}", startTime, endTime, ie.getMessage());
//             throw ie;
//         } catch (Exception e) {
//             log.error("Raw 쿼리 결과 처리 중 오류 ({} ~ {}): {}", startTime, endTime, e.getMessage(), e);
//             throw new RuntimeException("Raw 쿼리 데이터 처리 오류", e);
//         }
//         return resultList;
//     }
//     // ============================================

//     // --- 2단계: CSV 내보내기 메서드 추가 ---
//     public String exportHistoricalDataToCsv(String startTimeStr, String endTimeStr, String deviceGroup)
//             throws Exception {
//         // 1. 데이터 조회 (리팩토링된 메서드 사용)
//         List<Map<String, Object>> data = queryTimeSeriesData(startTimeStr, endTimeStr, deviceGroup);

//         if (data == null || data.isEmpty()) {
//             log.warn("내보낼 데이터가 없습니다. startTime={}, endTime={}, group={}", startTimeStr, endTimeStr, deviceGroup);
//             return ""; // 데이터 없으면 빈 문자열 반환
//         }

//         // 2. CSV 문자열 생성
//         StringBuilder csvBuilder = new StringBuilder();

//         // UTF-8 BOM 추가 (Excel 호환성)
//         csvBuilder.append('\uFEFF');

//         // 3. 헤더 생성 (첫 번째 데이터의 키 사용, 순서 보장 위해 정렬 권장)
//         Map<String, Object> firstRow = data.get(0);
//         List<String> headers = new ArrayList<>(firstRow.keySet());
//         // timestamp를 맨 앞으로, 나머지는 알파벳 순으로 정렬 (선택 사항)
//         headers.sort((h1, h2) -> {
//             if ("timestamp".equals(h1))
//                 return -1;
//             if ("timestamp".equals(h2))
//                 return 1;
//             return h1.compareTo(h2);
//         });

//         for (int i = 0; i < headers.size(); i++) {
//             csvBuilder.append(escapeCsvValue(headers.get(i)));
//             if (i < headers.size() - 1) {
//                 csvBuilder.append(',');
//             }
//         }
//         csvBuilder.append('\n'); // 줄바꿈

//         // 4. 데이터 행 생성
//         for (Map<String, Object> row : data) {
//             for (int i = 0; i < headers.size(); i++) {
//                 String header = headers.get(i);
//                 Object value = row.getOrDefault(header, ""); // 키가 없는 경우 빈 문자열 처리
//                 csvBuilder.append(escapeCsvValue(value != null ? value.toString() : "")); // null 값 처리
//                 if (i < headers.size() - 1) {
//                     csvBuilder.append(',');
//                 }
//             }
//             csvBuilder.append('\n'); // 줄바꿈
//         }

//         log.info("CSV 데이터 생성 완료: {} 행 (헤더 포함)", data.size() + 1);
//         return csvBuilder.toString();
//     }

//     // CSV 값 이스케이프 헬퍼 메서드 (쉼표, 큰따옴표, 줄바꿈 처리)
//     private String escapeCsvValue(String value) {
//         if (value == null) {
//             return "";
//         }
//         // 쉼표, 큰따옴표, 줄바꿈 문자가 포함되어 있는지 확인
//         if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
//             // 큰따옴표를 이중 큰따옴표로 치환
//             String escapedValue = value.replace("\"", "\"\"");
//             // 전체 값을 큰따옴표로 감쌈
//             return "\"" + escapedValue + "\"";
//         }
//         return value; // 특수 문자가 없으면 그대로 반환
//     }
//     // ======================================

//     @PreDestroy
//     public void shutdownExecutor() {
//         log.info("Shutting down OpcuaHistoricalService query executor...");
//         if (queryExecutor != null && !queryExecutor.isShutdown()) {
//             queryExecutor.shutdown();
//             try {
//                 if (!queryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
//                     queryExecutor.shutdownNow();
//                     log.warn("Query executor forcefully shut down.");
//                 } else {
//                     log.info("Query executor shut down gracefully.");
//                 }
//             } catch (InterruptedException e) {
//                 queryExecutor.shutdownNow();
//                 Thread.currentThread().interrupt();
//                 log.error("Interrupted while shutting down query executor.", e);
//             }
//         }
//     }
// }
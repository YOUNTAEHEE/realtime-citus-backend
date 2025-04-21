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

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.yth.realtime.dto.HistoricalDataResponse;

import jakarta.annotation.PreDestroy;
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
    private final ExecutorService queryExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    // =============================================

    /**
     * 과거 OPC UA 데이터를 조회하는 메소드 (병렬 처리 방식)
     *
     * @param startTimeStr        시작 시간 (ISO-8601 형식)
     * @param endTimeStr          종료 시간 (ISO-8601 형식)
     * @param deviceGroup         장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
     * @param aggregationInterval 집계 간격 (예: "raw", "10ms", "1s")
     * @return 과거 데이터 응답 객체
     */
    public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup,
            String aggregationInterval) {
        log.info("➡️ [조회 시작] deviceGroup: {}, startTime: {}, endTime: {}, aggregationInterval: {}",
                deviceGroup, startTimeStr, endTimeStr, aggregationInterval);

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
            // 데이터 조회 (다운샘플링 없이 원본 데이터 조회)
            List<Map<String, Object>> allResults = queryHistoricalDataParallel(finalStartTime, finalEndTime,
                    deviceGroup, aggregationInterval);

            HistoricalDataResponse response = HistoricalDataResponse.builder()
                    .success(true)
                    .message("데이터를 성공적으로 조회했습니다 (병렬 처리, 집계: "
                            + (StringUtils.hasText(aggregationInterval) ? aggregationInterval : "raw") + ")")
                    .build();
            response.setTimeSeriesData(allResults);

            log.info("✅ [조회 성공] deviceGroup: {}, aggregationInterval: {}, 조회된 데이터 포인트 수: {}",
                    deviceGroup, aggregationInterval, allResults.size());

            return response;

        } catch (Exception e) {
            log.error("❌ [조회 실패] deviceGroup: {}, aggregationInterval: {}. 오류: {}",
                    deviceGroup, aggregationInterval, e.getMessage(), e);
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
                    .build();
        }
    }

    /**
     * 과거 OPC UA 데이터를 조회하여 CSV 문자열로 반환하는 메소드 (다운샘플링 없음)
     *
     * @param startTimeStr        시작 시간 (ISO-8601 형식)
     * @param endTimeStr          종료 시간 (ISO-8601 형식)
     * @param deviceGroup         장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
     * @param aggregationInterval 집계 간격 (예: "raw", "10ms", "1s")
     * @return CSV 형식의 문자열 데이터
     * @throws IOException CSV 생성 중 오류 발생 시
     */
    public String exportHistoricalDataToCsv(String startTimeStr, String endTimeStr, String deviceGroup,
            String aggregationInterval)
            throws IOException {
        log.info("➡️ [내보내기 시작] deviceGroup: {}, startTime: {}, endTime: {}, aggregationInterval: {}",
                deviceGroup, startTimeStr, endTimeStr, aggregationInterval);

        Instant originalStartTime = Instant.parse(startTimeStr);
        Instant originalEndTime = Instant.parse(endTimeStr);

        if (originalStartTime.isAfter(originalEndTime)) {
            throw new IllegalArgumentException("시작 시간이 종료 시간보다 늦을 수 없습니다");
        }

        // 최대 3시간으로 쿼리 제한
        Instant queryStartTime = originalStartTime;
        Instant queryEndTime = originalEndTime;
        if (Duration.between(queryStartTime, queryEndTime).toHours() > 3) {
            log.warn("요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
            queryStartTime = queryEndTime.minus(Duration.ofHours(3));
        }

        final Instant finalStartTime = queryStartTime;
        final Instant finalEndTime = queryEndTime;

        List<Map<String, Object>> allResults;
        try {
            // 데이터 조회 (다운샘플링 없이 원본 데이터 조회)
            allResults = queryHistoricalDataParallel(finalStartTime, finalEndTime, deviceGroup, aggregationInterval);

            // 조회된 원본 데이터를 CSV 문자열로 변환
            String csvData = convertToCsvString(allResults);

            log.info("✅ [내보내기 성공] deviceGroup: {}, aggregationInterval: {}, 변환된 데이터 포인트 수: {}",
                    deviceGroup, aggregationInterval, allResults.size());

            return csvData;

        } catch (Exception e) {
            log.error("❌ [내보내기 실패] deviceGroup: {}, aggregationInterval: {}. 오류: {}",
                    deviceGroup, aggregationInterval, e.getMessage(), e);
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new RuntimeException("데이터 조회 또는 CSV 변환 중 오류가 발생했습니다: " + e.getMessage(), e);
        }
    }

    /**
     * 병렬 쿼리를 통해 과거 데이터를 조회하는 내부 헬퍼 메서드 (다운샘플링 없음)
     */
    private List<Map<String, Object>> queryHistoricalDataParallel(Instant startTime, Instant endTime,
            String deviceGroup, String aggregationInterval) {
        log.info("⏳ [병렬 조회 시작] deviceGroup: {}, 범위: {} ~ {}, aggregationInterval: {}",
                deviceGroup, startTime, endTime, aggregationInterval);

        List<Map<String, Object>> allResults = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Duration interval = Duration.ofMinutes(5); // 5분 간격으로 분할

        Instant currentStart = startTime;
        int queryCount = 0;
        while (currentStart.isBefore(endTime)) {
            Instant currentEnd = currentStart.plus(interval);
            if (currentEnd.isAfter(endTime)) {
                currentEnd = endTime;
            }

            final Instant subStart = currentStart;
            final Instant subEnd = currentEnd;
            queryCount++;

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                log.debug("  Sub-query executing: {} to {}, interval: {}", subStart, subEnd, aggregationInterval);
                try {
                    // 내부 쿼리 메서드 호출 (다운샘플링 없이 조회)
                    List<Map<String, Object>> resultChunk = queryInfluxDBInternal(subStart, subEnd, deviceGroup,
                            aggregationInterval);
                    if (resultChunk != null && !resultChunk.isEmpty()) {
                        allResults.addAll(resultChunk);
                        log.debug("  Sub-query success ({} ~ {}): {} points added", subStart, subEnd,
                                resultChunk.size());
                    } else {
                        log.debug("  Sub-query success ({} ~ {}): 0 points found", subStart, subEnd);
                    }
                } catch (Exception e) {
                    log.warn("  ⚠️ Sub-query failed ({} ~ {}): {}", subStart, subEnd, e.getMessage());
                }
            }, queryExecutor);
            futures.add(future);

            currentStart = currentEnd;
        }

        log.info("  {}개의 5분 단위 병렬 쿼리 실행 요청됨 (집계 간격: {}).", queryCount, aggregationInterval);
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allFutures.get(5, TimeUnit.MINUTES);
            log.debug("  모든 병렬 쿼리 작업 완료됨.");
        } catch (Exception e) {
            log.error("  ❌ 병렬 쿼리 작업 대기 중 오류 발생", e);
            throw new RuntimeException("과거 데이터 조회 중 병렬 처리 오류", e);
        }

        log.debug("  총 {}개의 결과 취합 완료 (정렬 전)", allResults.size());

        // 결과 정렬 (시간 순서 보장)
        // Collections.sort() 는 동기화된 리스트에 안전하지 않을 수 있으므로, 새 리스트로 복사 후 정렬
        List<Map<String, Object>> sortedResults = new ArrayList<>(allResults);
        sortedResults.sort(Comparator.comparing(m -> {
            Object ts = m.get("timestamp");
            return (ts instanceof Instant) ? (Instant) ts : Instant.parse((String) ts);
        }));

        log.info("✅ [병렬 조회 완료] deviceGroup: {}, aggregationInterval: {}, 최종 데이터 포인트 수: {}",
                deviceGroup, aggregationInterval, sortedResults.size());
        return sortedResults; // 정렬된 결과 반환
    }

    /**
     * InfluxDB에서 특정 시간 범위의 데이터를 쿼리하는 내부 메소드 (집계 기능 추가)
     */
    private List<Map<String, Object>> queryInfluxDBInternal(Instant startTime, Instant endTime, String deviceGroup,
            String aggregationInterval) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        String measurementName = "opcua_data";
        String deviceFilter = "";

        // 디바이스 그룹 필터링 로직
        if (!"total".equalsIgnoreCase(deviceGroup)) {
            // PCS 그룹 필터링: 해당 PCS 관련 필드 + 공통 필드(Filtered_Grid_Freq 등)
            // 정규표현식을 사용하여 필드 이름 시작 부분을 체크
            deviceFilter = String.format(
                    " and (r[\"_field\"] =~ /^%s_.*/ or r[\"_field\"] == \"Filtered_Grid_Freq\" or r[\"_field\"] == \"T_Simul_P_REAL\")",
                    deviceGroup.toUpperCase()); // PCS1, PCS2 등
            // 필요하다면 여기에 Total에서 필요한 다른 공통 필드도 or 조건으로 추가
        } else {
            // Total 그룹: 특정 필드들 또는 전체 (여기서는 예시로 전체 필터링 없음)
            // 필요시 Total에만 해당하는 필터 로직 추가
            // deviceFilter = " and (r[\"_field\"] == \"Total_TPWR_P_REAL\" or ...)";
        }

        // 집계 쿼리 생성
        String aggregationFunction = "";
        if (StringUtils.hasText(aggregationInterval) && !"raw".equalsIgnoreCase(aggregationInterval)) {
            aggregationFunction = String.format(
                    " |> aggregateWindow(every: %s, fn: mean, createEmpty: true)",
                    aggregationInterval);
            log.debug("Applying aggregation interval: {}", aggregationInterval);
        } else {
            log.debug("No aggregation applied (raw data requested).");
        }

        // Flux 쿼리 구성 (기본 쿼리 + 집계 함수 + 피벗)
        String flux = String.format(
                "from(bucket: \"%s\")" +
                        " |> range(start: %s, stop: %s)" +
                        " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
                        "%s" +
                        " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")",
                bucket,
                startTime.toString(),
                endTime.toString(),
                measurementName,
                deviceFilter,
                aggregationFunction);

        log.debug("    Executing Flux query ({} ~ {}): {}", startTime, endTime, flux);

        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
            List<FluxTable> tables = queryApi.query(flux, organization);
            log.debug("    InfluxDB query returned {} tables.", tables.size());

            // --- 추가: 반환된 타임스탬프 로깅 (처음 5개) ---
            log.debug("    First 5 timestamps returned by InfluxDB for interval '{}':", aggregationInterval);
            int tsCount = 0;
            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    if (tsCount < 5) {
                        log.debug("      - {}", record.getTime());
                        tsCount++;
                    }
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("timestamp", record.getTime());
                    record.getValues().forEach((key, value) -> {
                        if (!key.startsWith("_") && value != null) {
                            if (value instanceof Double && Double.isNaN((Double) value)) {
                                dataPoint.put(key, null);
                            } else {
                                dataPoint.put(key, value);
                            }
                        }
                    });
                    if (dataPoint.size() > 1) {
                        resultList.add(dataPoint);
                    }
                    if (tsCount >= 5 && table == tables.get(0))
                        break; // 첫 테이블의 처음 5개만 확인
                }
                if (tsCount >= 5)
                    break; // 전체 테이블에서 5개 채웠으면 중단
            }
            if (tsCount == 0) {
                log.debug("      - No timestamps returned.");
            }
            // ------------------------------------------

            log.debug("    Query successful and transformed ({} ~ {}): {} points", startTime, endTime,
                    resultList.size());
        } catch (InfluxException ie) {
            log.error("    ❌ InfluxDB query error ({} ~ {}): {}", startTime, endTime, ie.getMessage());
            throw ie;
        } catch (Exception e) {
            log.error("    ❌ Error processing query results ({} ~ {}): {}", startTime, endTime, e.getMessage(), e);
            throw new RuntimeException("쿼리 결과 처리 중 오류", e);
        }

        return resultList;
    }

    /**
     * 데이터 리스트를 CSV 형식 문자열로 변환하는 헬퍼 메서드
     */
    private String convertToCsvString(List<Map<String, Object>> dataList) throws IOException {
        if (dataList == null || dataList.isEmpty()) {
            return ""; // 데이터 없으면 빈 문자열 반환
        }

        StringWriter stringWriter = new StringWriter();

        // 헤더 생성 (모든 데이터에서 발견된 키 사용, 순서 유지를 위해 LinkedHashSet)
        // 'timestamp'를 맨 앞에 추가하고 나머지 키는 알파벳 순으로 정렬하여 일관성 유지
        Set<String> headers = new LinkedHashSet<>();
        Set<String> valueHeaders = new java.util.TreeSet<>(); // 알파벳 순 정렬
        headers.add("timestamp"); // timestamp는 항상 첫 번째

        for (Map<String, Object> dataPoint : dataList) {
            for (String key : dataPoint.keySet()) {
                if (!"timestamp".equals(key)) {
                    valueHeaders.add(key);
                }
            }
        }
        headers.addAll(valueHeaders); // 정렬된 값 헤더 추가

        // 헤더 쓰기
        stringWriter.append(String.join(",", headers)).append("\n");

        // 데이터 행 쓰기
        for (Map<String, Object> dataPoint : dataList) {
            List<String> row = new ArrayList<>();
            for (String header : headers) {
                Object value = dataPoint.get(header);
                String valueStr;
                if (value instanceof Instant) {
                    // Instant를 ISO-8601 형식 문자열로 변환
                    valueStr = ((Instant) value).toString();
                } else {
                    valueStr = (value == null) ? "" : String.valueOf(value);
                }

                // CSV 값 이스케이프 처리 (쉼표, 따옴표, 줄바꿈 문자 포함 시)
                if (valueStr.contains(",") || valueStr.contains("\"") || valueStr.contains("\n")) {
                    valueStr = "\"" + valueStr.replace("\"", "\"\"") + "\"";
                }
                row.add(valueStr);
            }
            stringWriter.append(String.join(",", row)).append("\n");
        }

        return stringWriter.toString();
    }

    // ExecutorService 종료 로직
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
}
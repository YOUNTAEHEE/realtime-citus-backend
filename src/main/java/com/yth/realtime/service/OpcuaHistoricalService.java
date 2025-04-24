//5분 단위로 쿼리 병렬 처리
package com.yth.realtime.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration; // Duration import 추가
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections; // Collections import 추가
import java.util.Comparator; // Comparator import 추가
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture; // CompletableFuture import 추가
import java.util.concurrent.ExecutorService; // ExecutorService import 추가
import java.util.concurrent.Executors; // Executors import 추가
import java.util.concurrent.TimeUnit; // TimeUnit import 추가

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
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

    // CSV 임시 파일 저장 경로
    @Value("${app.csv.temp-dir:/tmp/csv_exports}")
    private String csvTempDir;

    /**
     * [공통 로직] 과거 데이터를 병렬로 조회하고 피벗된 결과를 반환 (getHistoricalData,
     * exportPivotedDataToCsvFile 에서 사용)
     */
    private List<Map<String, Object>> retrievePivotedDataParallel(Instant startTime, Instant endTime,
            String deviceGroup) {
        log.debug("Executing retrievePivotedDataParallel for range: {} to {}", startTime, endTime);
        List<Map<String, Object>> allResults = Collections.synchronizedList(new ArrayList<>()); // 동기화된 리스트 사용
        List<CompletableFuture<List<Map<String, Object>>>> futures = new ArrayList<>();
        Duration interval = Duration.ofMinutes(5); // 5분 간격

        Instant currentStart = startTime;
        while (currentStart.isBefore(endTime)) {
            Instant currentEnd = currentStart.plus(interval);
            if (currentEnd.isAfter(endTime)) {
                currentEnd = endTime;
            }
            final Instant subStart = currentStart;
            final Instant subEnd = currentEnd;

            // 각 5분 범위 쿼리를 비동기 실행 (pivot 사용하는 내부 메서드 호출)
            CompletableFuture<List<Map<String, Object>>> future = CompletableFuture.supplyAsync(() -> {
                // --- 중요: pivot을 사용하는 queryInfluxDBInternal 호출 ---
                return queryInfluxDBInternal(subStart, subEnd, deviceGroup);
                // ----------------------------------------------------
            }, queryExecutor);
            futures.add(future);
            currentStart = currentEnd;
        }

        log.info("{}개의 5분 단위 병렬 쿼리 실행 시작 (pivot 포함)...", futures.size());
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allFutures.get(8, TimeUnit.MINUTES); // 타임아웃 설정
            log.info("모든 병렬 쿼리 작업 완료 (pivot 포함).");
        } catch (Exception e) {
            log.error("병렬 쿼리 작업(pivot 포함) 대기 중 오류 발생", e);
            // InterruptedException 처리 추가
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("과거 데이터 조회 중 병렬 처리 오류", e);
        }

        // 결과 취합
        for (CompletableFuture<List<Map<String, Object>>> future : futures) {
            if (!future.isCompletedExceptionally()) {
                allResults.addAll(future.join());
            } else {
                log.warn("병렬 쿼리(pivot 포함) 중 일부 작업 실패 (결과 누락 가능성 있음)");
            }
        }
        log.info("총 {}개의 결과 취합 완료 (pivot 포함, 정렬 전)", allResults.size());

        // 결과 정렬 (시간 순서 보장)
        // queryInfluxDBInternal이 timestamp를 문자열로 반환하므로 Instant.parse 사용
        allResults.sort(Comparator.comparing(m -> {
            Object ts = m.get("timestamp");
            if (ts instanceof String) {
                try {
                    return Instant.parse((String) ts);
                } catch (Exception e) {
                    log.error("Timestamp 파싱 오류: {}", ts);
                    return Instant.EPOCH;
                }
            }
            log.warn("예상치 못한 timestamp 타입: {}", ts != null ? ts.getClass().getName() : "null");
            return Instant.EPOCH; // 기본값 처리
        }));
        log.info("결과 정렬 완료 (pivot 포함).");

        // 동기화된 리스트 대신 일반 리스트 복사본 반환 (스레드 안전성 위해)
        return new ArrayList<>(allResults);
    }

    /**
     * [화면 조회용] 과거 OPC UA 데이터를 조회하는 메소드 (병렬 처리, 피벗된 데이터)
     *
     * @param startTimeStr 시작 시간 (ISO-8601 형식)
     * @param endTimeStr   종료 시간 (ISO-8601 형식)
     * @param deviceGroup  장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
     * @return 과거 데이터 응답 객체
     */
    public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup) {
        log.info("➡️ [조회 시작 - 피벗/병렬] deviceGroup: {}, startTime: {}, endTime: {}",
                deviceGroup, startTimeStr, endTimeStr);
        Instant originalStartTime;
        Instant originalEndTime;
        try {
            originalStartTime = Instant.parse(startTimeStr);
            originalEndTime = Instant.parse(endTimeStr);
        } catch (Exception e) {
            log.error("시간 파싱 오류: {}", e.getMessage());
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("시간 형식이 잘못되었습니다: " + e.getMessage())
                    .build();
        }

        if (originalStartTime.isAfter(originalEndTime)) {
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("시작 시간이 종료 시간보다 늦을 수 없습니다")
                    .build();
        }

        Instant queryStartTime = originalStartTime;
        Instant queryEndTime = originalEndTime;
        if (Duration.between(queryStartTime, queryEndTime).toHours() > 3) {
            log.warn("[UI Query] 요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
            queryStartTime = queryEndTime.minus(Duration.ofHours(3));
        }

        final Instant finalStartTime = queryStartTime;
        final Instant finalEndTime = queryEndTime;

        try {
            // 공통 로직 호출하여 피벗된 데이터 조회
            log.info("[UI Query] 피벗된 데이터 조회 시작 (병렬)...");
            List<Map<String, Object>> timeSeriesData = retrievePivotedDataParallel(finalStartTime, finalEndTime,
                    deviceGroup);
            log.info("[UI Query] 피벗된 데이터 조회 완료: {} 타임스탬프 포인트", timeSeriesData.size());

            HistoricalDataResponse response = HistoricalDataResponse.builder()
                    .success(true)
                    .message("데이터를 성공적으로 조회했습니다 (피벗 조회 - 병렬 처리)") // 메시지 수정
                    .build();
            response.setTimeSeriesData(timeSeriesData);
            return response;

        } catch (Exception e) {
            log.error("❌ [UI Query - 피벗/병렬] 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("UI 데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
                    .build();
        }
    }

    /**
     * [CSV 내보내기용] 과거 OPC UA 데이터를 조회(getHistoricalData와 동일 로직)하여 서버에 CSV 파일로 생성하고 파일
     * 경로를 반환.
     *
     * @param startTimeStr 시작 시간 (ISO-8601 형식)
     * @param endTimeStr   종료 시간 (ISO-8601 형식)
     * @param deviceGroup  장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
     * @return 생성된 CSV 파일의 Path 객체
     * @throws IOException              파일 생성 또는 데이터 조회/처리 중 오류 발생 시
     * @throws IllegalArgumentException 시간 파라미터 오류 시
     */
    public Path exportPivotedDataToCsvFile(String startTimeStr, String endTimeStr, String deviceGroup)
            throws IOException, IllegalArgumentException {

        log.info("➡️ [CSV 파일 생성 시작 - 피벗 데이터] deviceGroup: {}, startTime: {}, endTime: {}",
                deviceGroup, startTimeStr, endTimeStr);

        Instant originalStartTime;
        Instant originalEndTime;
        try {
            originalStartTime = Instant.parse(startTimeStr);
            originalEndTime = Instant.parse(endTimeStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("시간 형식이 잘못되었습니다: " + e.getMessage());
        }

        if (originalStartTime.isAfter(originalEndTime)) {
            throw new IllegalArgumentException("시작 시간이 종료 시간보다 늦을 수 없습니다");
        }

        Instant queryStartTime = originalStartTime;
        Instant queryEndTime = originalEndTime;
        if (Duration.between(queryStartTime, queryEndTime).toHours() > 3) {
            log.warn("[CSV Export] 요청된 시간 범위가 3시간을 초과합니다. 최근 3시간으로 제한합니다.");
            queryStartTime = queryEndTime.minus(Duration.ofHours(3));
        }

        final Instant finalStartTime = queryStartTime;
        final Instant finalEndTime = queryEndTime;

        List<Map<String, Object>> pivotedData;
        try {
            // 공통 로직 호출하여 피벗된 데이터 조회
            log.info("[CSV Export] 피벗된 데이터 조회 시작 (병렬)...");
            pivotedData = retrievePivotedDataParallel(finalStartTime, finalEndTime, deviceGroup);
            log.info("[CSV Export] 피벗된 데이터 조회 완료: {} 타임스탬프 포인트", pivotedData.size());

        } catch (Exception e) {
            log.error("❌ [CSV 파일 생성 실패] 피벗된 데이터 조회 중 오류: {}", e.getMessage(), e);
            throw new IOException("피벗된 데이터 조회 중 오류가 발생했습니다: " + e.getMessage(), e);
        }

        if (pivotedData == null || pivotedData.isEmpty()) {
            log.warn("[CSV Export] 내보낼 피벗된 데이터가 없습니다. startTime={}, endTime={}, group={}", startTimeStr, endTimeStr,
                    deviceGroup);
        }

        Path tempDirPath = Path.of(csvTempDir);
        Files.createDirectories(tempDirPath);
        String uniqueFileName = "opcua_export_pivoted_" + UUID.randomUUID().toString() + ".csv";
        Path tempFilePath = tempDirPath.resolve(uniqueFileName);
        log.info("[CSV Export] CSV 파일 생성 경로: {}", tempFilePath);

        try (BufferedWriter writer = Files.newBufferedWriter(tempFilePath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            writer.write('\uFEFF'); // BOM

            if (!pivotedData.isEmpty()) {
                Map<String, Object> firstRow = pivotedData.get(0);
                List<String> headers = new ArrayList<>(firstRow.keySet());
                headers.sort((h1, h2) -> {
                    if ("timestamp".equals(h1))
                        return -1;
                    if ("timestamp".equals(h2))
                        return 1;
                    return h1.compareTo(h2);
                });

                writer.write(String.join(",", headers.stream().map(this::escapeCsvValue).toList()));
                writer.newLine();

                for (Map<String, Object> row : pivotedData) {
                    List<String> values = new ArrayList<>();
                    for (String header : headers) {
                        Object value = row.getOrDefault(header, "");
                        values.add(escapeCsvValue(value != null ? value.toString() : ""));
                    }
                    writer.write(String.join(",", values));
                    writer.newLine();
                }
            } else {
                writer.write("No pivoted data available for the selected range.");
                writer.newLine();
            }

            log.info("✅ [CSV 파일 생성 성공 - 피벗 데이터] 경로: {}", tempFilePath);
            return tempFilePath;

        } catch (IOException e) {
            log.error("❌ [CSV 파일 생성 실패 - 피벗 데이터] 파일 쓰기 중 오류: {}", tempFilePath, e);
            try {
                Files.deleteIfExists(tempFilePath);
            } catch (IOException ignored) {
            }
            throw e;
        }
    }

    /**
     * InfluxDB에서 특정 시간 범위의 데이터를 쿼리하는 내부 메소드 (pivot 포함 - 수정 없음)
     */
    private List<Map<String, Object>> queryInfluxDBInternal(Instant startTime, Instant endTime, String deviceGroup) {
        // --- 이 메서드는 제공해주신 원래 코드를 그대로 사용 (pivot 포함) ---
        log.debug("Executing queryInfluxDBInternal (with pivot) for range: {} to {}", startTime, endTime);
        QueryApi queryApi = influxDBClient.getQueryApi();
        String measurementName = "opcua_data";
        String deviceFilter = "";

        if (!"total".equalsIgnoreCase(deviceGroup)) {
            deviceFilter = String.format(
                    " and (r[\"_field\"] =~ /^%s_.*/ or r[\"_field\"] == \"Filtered_Grid_Freq\" or r[\"_field\"] == \"T_Simul_P_REAL\")", // T_Simul_P_REAL
                                                                                                                                          // 추가
                                                                                                                                          // (Total과
                                                                                                                                          // PCS공통)
                    deviceGroup.toUpperCase()); // 대문자 PCS 사용 가정
        } else {
            // Total 그룹: Total 관련 필드 + 공통 필드 (PCS 관련 필드 제외 필요)
            // 예시: PCS로 시작하지 않는 필드 또는 특정 필드 목록 지정
            // deviceFilter = " and (r[\"_field\"] !~ /^PCS.*/ or r[\"_field\"] ==
            // \"Filtered_Grid_Freq\")"; // 예시
            // 여기서는 필터 없이 진행 (모든 필드가 올 수 있음 - 이후 로직에서 처리되거나 불필요 데이터 포함 가능성)
        }

        String flux = String.format(
                "from(bucket: \"%s\")" +
                        " |> range(start: %s, stop: %s)" +
                        " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
                        " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")",
                bucket, startTime.toString(), endTime.toString(), measurementName, deviceFilter);

        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
            List<FluxTable> tables = queryApi.query(flux, organization);
            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("timestamp", record.getTime().toString()); // 문자열로 저장
                    record.getValues().forEach((key, value) -> {
                        if (!key.startsWith("_") && value != null) {
                            if (value instanceof Double) {
                                double doubleValue = (Double) value;
                                dataPoint.put(key, Double.isNaN(doubleValue) ? null : doubleValue); // NaN은 null
                            } else {
                                dataPoint.put(key, value);
                            }
                        }
                    });
                    // timestamp 외에 다른 키가 하나라도 있는지 확인 후 추가 (pivot 결과가 비어있을 수 있음)
                    if (dataPoint.keySet().stream().anyMatch(k -> !k.equals("timestamp"))) {
                        resultList.add(dataPoint);
                    }
                }
            }
            log.debug("queryInfluxDBInternal (with pivot) successful for range: {} to {}. Points: {}", startTime,
                    endTime, resultList.size());
        } catch (Exception e) {
            log.error("Error in queryInfluxDBInternal (with pivot) for range: {} to {}: {}", startTime, endTime,
                    e.getMessage(), e);
            return Collections.emptyList(); // 오류 시 빈 리스트 반환
        }
        return resultList;
    }

    // CSV 값 이스케이프 헬퍼 메서드
    private String escapeCsvValue(String value) {
        if (value == null)
            return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
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

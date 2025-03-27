package com.yth.realtime.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.yth.realtime.dto.HistoricalDataResponse;

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

    /**
     * 과거 OPC UA 데이터를 조회하는 메소드
     * 
     * @param startTimeStr 시작 시간 (ISO-8601 형식)
     * @param endTimeStr   종료 시간 (ISO-8601 형식)
     * @param deviceGroup  장치 그룹 ("total", "pcs1", "pcs2", "pcs3", "pcs4")
     * @return 과거 데이터 응답 객체
     */
    public HistoricalDataResponse getHistoricalData(String startTimeStr, String endTimeStr, String deviceGroup) {
        try {

            Instant startTime = Instant.parse(startTimeStr);
            Instant endTime = Instant.parse(endTimeStr);

            if (startTime.isAfter(endTime)) {
                return HistoricalDataResponse.builder()
                        .success(false)
                        .message("시작 시간이 종료 시간보다 늦을 수 없습니다")
                        .build();
            }

            // 최대 3시간으로 쿼리 제한
            if (endTime.toEpochMilli() - startTime.toEpochMilli() > 3 * 60 * 60 * 1000) {
                log.warn("요청된 시간 범위가 3시간을 초과합니다. 3시간으로 제한합니다.");
                startTime = Instant.ofEpochMilli(endTime.toEpochMilli() - 3 * 60 * 60 * 1000);
            }

            List<Map<String, Object>> timeSeriesData = queryInfluxDB(startTime, endTime, deviceGroup);

            HistoricalDataResponse response = HistoricalDataResponse.builder()
                    .success(true)
                    .message("데이터를 성공적으로 조회했습니다")
                    .build();

            response.setTimeSeriesData(timeSeriesData);
            return response;

        } catch (Exception e) {
            log.error("과거 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
            return HistoricalDataResponse.builder()
                    .success(false)
                    .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
                    .build();
        }
    }

    /**
     * InfluxDB에서 데이터를 쿼리하는 메소드
     */
    private List<Map<String, Object>> queryInfluxDB(Instant startTime, Instant endTime, String deviceGroup) {
        QueryApi queryApi = influxDBClient.getQueryApi();

        // 테이블명, 필드 지정
        String measurementName = "opcua_data";

        // 디바이스 그룹에 따른 필터 설정
        String deviceFilter = "";
        if (!"total".equalsIgnoreCase(deviceGroup)) {
            deviceFilter = String.format(" and r[\"_field\"] =~ /%s.*/i", deviceGroup);
        }

        // Flux 쿼리 작성
        String flux = String.format(
                "from(bucket: \"%s\")" +
                        " |> range(start: %s, stop: %s)" +
                        " |> filter(fn: (r) => r[\"_measurement\"] == \"%s\"%s)" +
                        " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")" +
                        " |> sort(columns: [\"_time\"])",
                bucket,
                startTime,
                endTime,
                measurementName,
                deviceFilter);

        log.debug("실행할 Flux 쿼리: {}", flux);

        // 쿼리 실행
        List<FluxTable> tables = queryApi.query(flux, organization);
        log.debug("조회된 테이블 수: {}", tables.size());

        // 결과 변환
        List<Map<String, Object>> resultList = new ArrayList<>();

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                Map<String, Object> dataPoint = new HashMap<>();

                // 시간 정보 추가
                dataPoint.put("timestamp", record.getTime().toString());

                // 필드 값 추가
                record.getValues().forEach((key, value) -> {
                    if (!key.startsWith("_") && value != null) {
                        if (value instanceof Double) {
                            // NaN 값 처리
                            double doubleValue = (Double) value;
                            if (Double.isNaN(doubleValue)) {
                                dataPoint.put(key, -1); // NaN을 -1로 대체
                            } else {
                                dataPoint.put(key, doubleValue);
                            }
                        } else {
                            dataPoint.put(key, value);
                        }
                    }
                });

                resultList.add(dataPoint);
            }
        }

        log.info("조회된 데이터 포인트 수: {}", resultList.size());
        return resultList;
    }

}
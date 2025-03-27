package com.yth.realtime.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.yth.realtime.model.InfluxDBMeasurement;

@Service
@Transactional
public class OpcuaInfluxDBService {
    private static final Logger log = LoggerFactory.getLogger(OpcuaInfluxDBService.class);

    private final InfluxDBClient influxDBClient;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.organization}")
    private String organization;

    public OpcuaInfluxDBService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
        log.info("OPC UA InfluxDB 서비스 초기화 완료");
    }

    /**
     * OPC UA 데이터 저장
     * 
     * @param measurement InfluxDB 측정 이름
     * @param tags        태그 맵
     * @param fields      필드 맵
     * @param timestamp   타임스탬프
     */
    public void saveOpcuaData(String measurement, Map<String, String> tags, Map<String, Object> fields,
            Instant timestamp) {
        Point point = Point.measurement(measurement);

        // 태그 추가
        tags.forEach(point::addTag);

        // 필드 추가
        fields.forEach((key, value) -> {
            if (value instanceof Double) {
                point.addField(key, (Double) value);
            } else if (value instanceof Float) {
                point.addField(key, (Float) value);
            } else if (value instanceof Integer) {
                point.addField(key, (Integer) value);
            } else if (value instanceof Long) {
                point.addField(key, (Long) value);
            } else if (value instanceof Boolean) {
                point.addField(key, (Boolean) value);
            } else if (value instanceof String) {
                point.addField(key, (String) value);
            } else if (value != null) {
                // 기타 형식은 문자열로 변환 시도
                point.addField(key, value.toString());
            }
        });

        // 타임스탬프 설정
        point.time(timestamp, WritePrecision.NS);

        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writePoint(bucket, organization, point);
            log.debug("OPC UA 데이터 저장 성공 - 측정: {}, 태그: {}", measurement, tags);
        } catch (Exception e) {
            log.error("OPC UA 데이터 저장 실패: {}", e.getMessage(), e);
            throw new RuntimeException("OPC UA 데이터 저장 실패", e);
        }
    }

    /**
     * InfluxDBMeasurement 객체를 사용하여 데이터 저장
     * 
     * @param measurement InfluxDBMeasurement 객체
     */
    public void saveData(InfluxDBMeasurement measurement) {
        try {
            log.info("InfluxDB 저장 호출: 측정명={}, 태그 수={}, 필드 수={}",
                    measurement.getMeasurement(),
                    measurement.getTags().size(),
                    measurement.getFields().size());

            // 데이터 형식 검증
            for (Map.Entry<String, Object> field : measurement.getFields().entrySet()) {
                if (field.getValue() == null) {
                    log.warn("필드 {} 값이 null입니다. 이 필드는 저장되지 않습니다.", field.getKey());
                }
            }

            Map<String, String> tags = measurement.getTags();
            Map<String, Object> fields = measurement.getFields();
            Instant timestamp = measurement.getTimestamp();

            // 저장 실행
            Point point = Point.measurement(measurement.getMeasurement());

            // 태그 추가
            for (Map.Entry<String, String> tag : tags.entrySet()) {
                point.addTag(tag.getKey(), tag.getValue());
                log.debug("추가된 태그: {}={}", tag.getKey(), tag.getValue());
            }

            // 필드 로깅 (첫 5개만 상세 출력)
            int fieldCounter = 0;
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                if (fieldCounter < 5) {
                    log.debug("필드 상세 {}. {}={} (타입: {})",
                            fieldCounter + 1,
                            entry.getKey(),
                            entry.getValue(),
                            entry.getValue() != null ? entry.getValue().getClass().getSimpleName() : "null");
                }
                fieldCounter++;
            }

            // 필드 추가 (기존 코드와 동일)
            fields.forEach((key, value) -> {
                if (value instanceof Double) {
                    point.addField(key, (Double) value);
                } else if (value instanceof Float) {
                    point.addField(key, (Float) value);
                } else if (value instanceof Integer) {
                    point.addField(key, (Integer) value);
                } else if (value instanceof Long) {
                    point.addField(key, (Long) value);
                } else if (value instanceof Boolean) {
                    point.addField(key, (Boolean) value);
                } else if (value instanceof String) {
                    point.addField(key, (String) value);
                } else if (value != null) {
                    point.addField(key, value.toString());
                }
            });

            // 타임스탬프 설정
            point.time(timestamp, WritePrecision.NS);

            // 로그 추가: 저장 직전 Point 정보
            log.info("Point 생성 완료: 측정명={}, 시간={}",
                    measurement.getMeasurement(), timestamp);

            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writePoint(bucket, organization, point);

            log.info("InfluxDB 저장 성공: 버킷={}, 측정명={}, 필드 수={}, 시간={}",
                    bucket, measurement.getMeasurement(), fields.size(), timestamp);

        } catch (Exception e) {
            log.error("InfluxDB 저장 실패 - 상세 오류: {} ({})",
                    e.getMessage(), e.getClass().getSimpleName(), e);
            throw new RuntimeException("InfluxDB 데이터 저장 실패", e);
        }
    }

    /**
     * OPC UA 쿼리 실행 및 결과 반환
     * 
     * @param query Flux 쿼리 문자열
     * @return 쿼리 결과 목록
     */
    public List<Map<String, Object>> queryData(String query) {
        try {
            List<Map<String, Object>> resultList = new ArrayList<>();
            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> tables = queryApi.query(query, organization);

            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    Map<String, Object> dataPoint = new HashMap<>();

                    // 타임스탬프 처리 - 수정된 부분
                    if (record.getTime() != null) {
                        // InfluxDB의 Instant를 LocalDateTime으로 변환
                        dataPoint.put("time", LocalDateTime.ofInstant(
                                record.getTime(), ZoneId.systemDefault()));

                        // 디버깅용 로그 추가
                        log.debug("시간 변환: {} -> {}", record.getTime(),
                                dataPoint.get("time"));
                    }

                    // 모든 필드 추출
                    for (Map.Entry<String, Object> entry : record.getValues().entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();

                        // 내부 필드는 무시
                        if (!key.startsWith("_") || key.equals("_time")) {
                            dataPoint.put(key, value);
                        }
                    }

                    resultList.add(dataPoint);
                }
            }

            log.debug("OPC UA 데이터 조회 성공: {} 레코드", resultList.size());
            return resultList;
        } catch (Exception e) {
            log.error("OPC UA 데이터 조회 실패: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * OPC UA 측정의 최근 데이터 조회
     * 
     * @param deviceGroup 장치 그룹 (예: PCS1, Grid 등)
     * @param minutes     조회할 시간 범위(분)
     * @return 조회된 데이터 목록
     */
    public List<Map<String, Object>> getRecentOpcuaData(String deviceGroup, int minutes) {
        String fieldFilter = deviceGroup.equals("all") ? "" : String.format("and (r._field =~ /^%s_.*/)", deviceGroup);

        String query = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: -%dm) " +
                        "|> filter(fn: (r) => r._measurement == \"opcua_data\" %s) " +
                        "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
                        "|> sort(columns: [\"_time\"], desc: false)",
                bucket, minutes, fieldFilter);

        return queryData(query);
    }

    /**
     * OPC UA 측정의 최신 데이터 조회
     * 
     * @param deviceGroup 장치 그룹 (예: PCS1, Grid 등)
     * @return 최신 데이터
     */
    public Map<String, Object> getLatestOpcuaData(String deviceGroup) {
        String fieldFilter = deviceGroup.equals("all") ? "" : String.format("and (r._field =~ /^%s_.*/)", deviceGroup);

        String query = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: -5m) " +
                        "|> filter(fn: (r) => r._measurement == \"opcua_data\" %s) " +
                        "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
                        "|> sort(columns: [\"_time\"], desc: true) " +
                        "|> limit(n: 1)",
                bucket, fieldFilter);

        List<Map<String, Object>> results = queryData(query);

        if (!results.isEmpty()) {
            return results.get(0);
        } else {
            Map<String, Object> emptyResult = new HashMap<>();
            emptyResult.put("time", LocalDateTime.now());
            emptyResult.put("message", "데이터가 없습니다");
            return emptyResult;
        }
    }

    /**
     * 시간 범위 내 OPC UA 데이터 조회
     * 
     * @param deviceGroup 장치 그룹 (예: PCS1, Grid 등)
     * @param startTime   시작 시간 (Flux 형식, 예: -24h, 2023-10-24T00:00:00Z)
     * @param endTime     종료 시간 (Flux 형식, 예: now(), 2023-10-24T23:59:59Z)
     * @return 조회된 데이터 목록
     */
    public List<Map<String, Object>> getOpcuaDataByTimeRange(String deviceGroup, String startTime, String endTime) {
        String fieldFilter = deviceGroup.equals("all") ? "" : String.format("and (r._field =~ /^%s_.*/)", deviceGroup);

        String query = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: %s, stop: %s) " +
                        "|> filter(fn: (r) => r._measurement == \"opcua_data\" %s) " +
                        "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
                        "|> sort(columns: [\"_time\"], desc: false)",
                bucket, startTime, endTime, fieldFilter);

        return queryData(query);
    }

    /**
     * 특정 필드에 대한 통계 데이터 생성
     * 
     * @param deviceGroup 장치 그룹 (예: PCS1, Grid 등)
     * @param fieldName   필드 이름 (예: SOC, STATUS 등)
     * @param hours       데이터 조회 시간 범위(시간)
     * @return 통계 데이터
     */
    public Map<String, Object> getOpcuaFieldStatistics(String deviceGroup, String fieldName, int hours) {
        String fullFieldName = deviceGroup.equals("Common") ? fieldName : deviceGroup + "_" + fieldName;

        String query = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: -%dh) " +
                        "|> filter(fn: (r) => r._measurement == \"opcua_data\" and r._field == \"%s\") " +
                        "|> timedMovingAverage(every: 5m, period: 10m) " + // 5분마다 10분 평균
                        "|> group() " +
                        "|> mean() " + // 전체 평균
                        "|> yield(name: \"mean\")",
                bucket, hours, fullFieldName);

        String minMaxQuery = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: -%dh) " +
                        "|> filter(fn: (r) => r._measurement == \"opcua_data\" and r._field == \"%s\") " +
                        "|> group() " +
                        "|> reduce(fn: (r, accumulator) => ({min: if r._value < accumulator.min then r._value else accumulator.min, max: if r._value > accumulator.max then r._value else accumulator.max}), identity: {min: 1000000.0, max: -1000000.0}) "
                        +
                        "|> yield()",
                bucket, hours, fullFieldName);

        Map<String, Object> stats = new HashMap<>();
        stats.put("field", fullFieldName);
        stats.put("period", hours + "h");

        // 평균값 조회
        List<Map<String, Object>> meanResults = queryData(query);
        if (!meanResults.isEmpty() && meanResults.get(0).containsKey("_value")) {
            stats.put("mean", meanResults.get(0).get("_value"));
        } else {
            stats.put("mean", 0.0);
        }

        // 최소/최대값 조회
        List<Map<String, Object>> minMaxResults = queryData(minMaxQuery);
        if (!minMaxResults.isEmpty()) {
            Map<String, Object> minMax = minMaxResults.get(0);
            stats.put("min", minMax.getOrDefault("min", 0.0));
            stats.put("max", minMax.getOrDefault("max", 0.0));
        } else {
            stats.put("min", 0.0);
            stats.put("max", 0.0);
        }

        return stats;
    }

    /**
     * 현재 설정된 버킷 이름 반환
     * 
     * @return 버킷 이름
     */
    public String getBucket() {
        return this.bucket;
    }

    /**
     * WriteApi 객체 가져오기
     * 
     * @return WriteApiBlocking 인스턴스
     */
    public WriteApiBlocking getWriteApi() {
        return influxDBClient.getWriteApiBlocking();
    }

    /**
     * 조직 이름 가져오기
     * 
     * @return InfluxDB 조직 이름
     */
    public String getOrg() {
        return this.organization;
    }

}
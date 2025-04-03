package com.yth.realtime.service;

import java.time.Instant;
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
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

@Service
@Transactional
public class InfluxDBService {
    private static final Logger log = LoggerFactory.getLogger(InfluxDBService.class);

    private final InfluxDBClient influxDBClient;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.organization}")
    private String organization;

    public InfluxDBService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
    }

    public void saveSensorData(double temperature, double humidity, String deviceHost, String deviceId) {
        Point point = Point.measurement("sensor_data")
                .addTag("device", deviceId)
                .addTag("host", deviceHost)
                .addField("temperature", temperature)
                .addField("humidity", humidity)
                .time(Instant.now(), WritePrecision.NS);

        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writePoint(bucket, organization, point);
            log.info("데이터 저장 성공 - 장치: {}, 호스트: {}, 온도: {}°C, 습도: {}%",
                    deviceId, deviceHost, temperature, humidity);
        } catch (Exception e) {
            log.error("데이터 저장 실패: {}", e.getMessage());
        }
    }

    /**
     * 여러 데이터 포인트를 InfluxDB에 배치로 저장합니다.
     *
     * @param points 저장할 Point 객체 리스트
     */
    public void savePoints(List<Point> points) {
        if (points == null || points.isEmpty()) {
            log.warn("저장할 데이터 포인트가 없습니다.");
            return;
        }

        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writePoints(bucket, organization, points);
            log.info("InfluxDB 배치 쓰기 성공: {} 개의 포인트 저장", points.size());
        } catch (InfluxException e) {
            log.error("InfluxDB 배치 쓰기 실패: {} - 상태 코드: {}, 메시지: {}",
                    points.size(), e.status(), e.getMessage(), e);
        } catch (Exception e) {
            log.error("InfluxDB 배치 쓰기 중 예상치 못한 오류 발생: {} points - {}",
                    points.size(), e.getMessage(), e);
        }
    }

    /**
     * 특정 장치의 최근 센서 데이터를 조회
     * 
     * @param deviceId 장치 ID
     * @param minutes  조회할 시간 범위(분)
     * @return 센서 데이터 목록
     */
    public List<Map<String, Object>> getRecentSensorData(String deviceId, int minutes) {
        try {
            String query = String.format(
                    "from(bucket: \"%s\") " +
                            "|> range(start: -%dm) " +
                            "|> filter(fn: (r) => r._measurement == \"sensor_data\" and r.device == \"%s\") " +
                            "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
                            "|> sort(columns: [\"_time\"], desc: false)",
                    bucket, minutes, deviceId);

            List<Map<String, Object>> resultList = new ArrayList<>();

            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> tables = queryApi.query(query, organization);

            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("timestamp", record.getTime().toEpochMilli());
                    dataPoint.put("temperature", record.getValueByKey("temperature"));
                    dataPoint.put("humidity", record.getValueByKey("humidity"));
                    resultList.add(dataPoint);
                }
            }

            log.info("조회된 데이터 수: {}", resultList.size());
            return resultList;
        } catch (Exception e) {
            log.error("InfluxDB 데이터 조회 실패: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    // /**
    // * 특정 장치의 최신 센서 데이터를 조회
    // *
    // * @param deviceId 장치 ID
    // * @return 최신 센서 데이터
    // */
    public Map<String, Object> getLatestSensorData(String deviceId) {
        try {
            String query = String.format(
                    "from(bucket: \"%s\") " +
                            "|> range(start: -5m) " +
                            "|> filter(fn: (r) => r._measurement == \"sensor_data\" and r.device == \"%s\") " +
                            "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
                            "|> sort(columns: [\"_time\"], desc: true) " +
                            "|> limit(n: 1)",
                    bucket, deviceId);

            Map<String, Object> result = new HashMap<>();
            result.put("timestamp", System.currentTimeMillis());
            result.put("temperature", 0.0);
            result.put("humidity", 0.0);

            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> tables = queryApi.query(query, organization);

            if (!tables.isEmpty() && !tables.get(0).getRecords().isEmpty()) {
                FluxRecord record = tables.get(0).getRecords().get(0);
                result.put("timestamp", record.getTime().toEpochMilli());
                result.put("temperature", record.getValueByKey("temperature"));
                result.put("humidity", record.getValueByKey("humidity"));
            }

            return result;
        } catch (Exception e) {
            log.error("InfluxDB 최신 데이터 조회 실패: {}", e.getMessage());
            Map<String, Object> emptyResult = new HashMap<>();
            emptyResult.put("timestamp", System.currentTimeMillis());
            emptyResult.put("temperature", 0.0);
            emptyResult.put("humidity", 0.0);
            return emptyResult;
        }
    }
}
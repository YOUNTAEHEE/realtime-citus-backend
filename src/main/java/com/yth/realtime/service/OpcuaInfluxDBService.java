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
import com.influxdb.client.WriteApi;
@Service
@Transactional
public class OpcuaInfluxDBService {
    private static final Logger log = LoggerFactory.getLogger(OpcuaInfluxDBService.class);

    private final InfluxDBClient influxDBClient;
    private final WriteApi asyncWriteApi;
    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.organization}")
    private String organization;

    public OpcuaInfluxDBService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
        log.info("OPC UA InfluxDB 서비스 초기화 완료");
        this.asyncWriteApi = influxDBClient.makeWriteApi(); // 비동기 Write API 생성
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
     * OPC UA 측정의 최신 데이터 조회
     * 
     * @param deviceGroup 장치 그룹 (예: PCS1, Grid 등)
     * @return 최신 데이터
     */
    public Map<String, Object> getLatestOpcuaData(String deviceGroup) {
        log.debug("getLatestOpcuaData 호출됨 - deviceGroup: {}", deviceGroup);

        String fieldFilter = deviceGroup.equals("all") ? "" : String.format("and (r._field =~ /^%s_.*/)", deviceGroup);

        String query = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: -10m) " +
                        "|> filter(fn: (r) => r._measurement == \"opcua_data\" %s) " +
                        "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
                        "|> sort(columns: [\"_time\"], desc: true) " +
                        "|> limit(n: 1)",
                bucket, fieldFilter);

        List<Map<String, Object>> results = queryData(query);

        if (!results.isEmpty()) {
            log.info("최신 데이터 조회 성공. 결과 레코드 수: {}", results.size()); // <<< 로그 추가
            return results.get(0);
            
        } else {
            log.warn("최신 데이터 조회 결과 없음.  또는 해당 시간 범위 내 데이터 확인 필요.");
            Map<String, Object> emptyResult = new HashMap<>();
            emptyResult.put("time", LocalDateTime.now());
            emptyResult.put("message", "데이터가 없습니다");
            return emptyResult;
        }
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
    public WriteApi getAsyncWriteApi() {
        return asyncWriteApi;
    }
}
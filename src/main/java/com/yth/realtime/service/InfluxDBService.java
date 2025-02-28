package com.yth.realtime.service;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

@Service
@Transactional
public class InfluxDBService {
    private static final Logger log = LoggerFactory.getLogger(InfluxDBService.class);
    
    private final InfluxDBClient influxDBClient;
    
    @Value("${influxdb.bucket}")
    private String bucket;
    
    @Value("${influxdb.org}")
    private String org;

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
            writeApi.writePoint(bucket, org, point);
            log.info("데이터 저장 성공 - 장치: {}, 호스트: {}, 온도: {}°C, 습도: {}%", 
                deviceId, deviceHost, temperature, humidity);
        } catch (Exception e) {
            log.error("데이터 저장 실패: {}", e.getMessage());
        }
    }
} 
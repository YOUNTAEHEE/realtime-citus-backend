package com.yth.realtime.service;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

@Service
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

    public void saveSensorData(double temperature, double humidity) {
        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            
            Point point = Point
                .measurement("sensor_data")
                .addTag("device", "DHT22")
                .addField("temperature", temperature)
                .addField("humidity", humidity)
                .time(Instant.now(), WritePrecision.MS);

            writeApi.writePoint(bucket, org, point);
            log.info("InfluxDB 데이터 저장 완료 - 온도: {}°C, 습도: {}%", temperature, humidity);
            
        } catch (Exception e) {
            log.error("InfluxDB 데이터 저장 실패: {}", e.getMessage(), e);
        }
    }
} 
package com.yth.realtime.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Configuration
public class InfluxDBConfig {
    private static final Logger log = LoggerFactory.getLogger(InfluxDBConfig.class);
    
    @Value("${influxdb.url:http://localhost:8086}")
    private String url;
    
    @Value("${influxdb.token}")
    private String token;
    
    @Value("${influxdb.org:youn}")
    private String org;
    
    @Value("${influxdb.bucket:ydata}")
    private String bucket;
    
    private InfluxDBClient influxDBClient;
    
    @PostConstruct
    public void init() {
        log.info("InfluxDB 설정 확인:");
        log.info("URL: {}", url);
        log.info("Token 존재 여부: {}", token != null && !token.isEmpty() ? "Yes" : "No");
        log.info("Organization: {}", org);
        log.info("Bucket: {}", bucket);
    }
    
    @Bean
    public InfluxDBClient influxDBClient() {
        try {
            log.info("InfluxDB 클라이언트 초기화 시작");
            if (token == null || token.isEmpty()) {
                log.error("InfluxDB 토큰이 설정되지 않았습니다. 기본 토큰을 사용합니다.");
                token = "your-default-token-here";  // 기본 토큰 설정
            }
            
            influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());
            
            try {
                boolean ready = influxDBClient.ping();
                if (ready) {
                    log.info("InfluxDB 연결 성공 - URL: {}, Org: {}, Bucket: {}", url, org, bucket);
                } else {
                    log.warn("InfluxDB 연결 실패 - 서버가 응답하지 않습니다. 계속 진행합니다.");
                }
            } catch (Exception e) {
                log.warn("InfluxDB ping 실패. 계속 진행합니다.: {}", e.getMessage());
            }
            
            return influxDBClient;
        } catch (Exception e) {
            log.error("InfluxDB 클라이언트 초기화 실패: {}", e.getMessage(), e);
            // 예외를 던지지 않고 null을 반환하거나 더미 클라이언트를 반환
            return null;  // 또는 더미 클라이언트 반환
        }
    }
    
    @PreDestroy
    public void close() {
        if (influxDBClient != null) {
            try {
                influxDBClient.close();
                log.info("InfluxDB 클라이언트 연결 종료 성공");
            } catch (Exception e) {
                log.error("InfluxDB 클라이언트 연결 종료 실패: {}", e.getMessage(), e);
            }
        }
    }
} 
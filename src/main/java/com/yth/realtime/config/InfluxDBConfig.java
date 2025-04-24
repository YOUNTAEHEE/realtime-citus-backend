package com.yth.realtime.config;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;

import io.github.cdimascio.dotenv.Dotenv;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import okhttp3.OkHttpClient;

@Configuration
public class InfluxDBConfig {
    private static final Logger log = LoggerFactory.getLogger(InfluxDBConfig.class);

    private final String url = "http://localhost:8086";
    private String token;
    private final String organization = "youn";
    private final String bucket = "ydata";

    private InfluxDBClient influxDBClient;

    @PostConstruct
    public void init() {
        Dotenv dotenv = Dotenv.configure()
                .directory("./") // .env 파일 위치 지정
                .ignoreIfMissing()
                .load();

        // .env 파일에서 토큰 읽기
        token = dotenv.get("INFLUX_TOKEN");

        log.info("InfluxDB 설정 확인:");
        log.info("URL: {}", url);
        log.info("Token 존재 여부: {}", token != null && !token.isEmpty() ? "Yes" : "No");
        log.info("Organization: {}", organization);
        log.info("Bucket: {}", bucket);
    }

    @Bean
    public InfluxDBClient influxDBClient() {
        try {
            log.info("InfluxDB 클라이언트 초기화 시작");
            if (token == null || token.isEmpty()) {
                log.error("InfluxDB 토큰이 설정되지 않았습니다. 기본 토큰을 사용합니다.");
                token = "your-default-token-here"; // 기본 토큰 설정
            }

            // influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());

            long readTimeoutSeconds = 60;

            // 1. OkHttpClient.Builder 생성 및 타임아웃 설정
            OkHttpClient.Builder okHttpBuilder = new OkHttpClient.Builder()
                    .readTimeout(Duration.ofSeconds(readTimeoutSeconds));
            // .writeTimeout(Duration.ofSeconds(writeTimeoutSeconds)); // 필요시
            // .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds)); // 필요시
            // .addInterceptor(...) // 다른 커스텀 설정이 필요하면 추가

            // 2. InfluxDBClientOptions에 OkHttpClient.Builder 전달
            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(url)
                    .authenticateToken(token.toCharArray())
                    .org(organization)
                    .bucket(bucket)
                    .okHttpClient(okHttpBuilder) // OkHttpClient.Builder 객체 전달
                    .build();

            influxDBClient = InfluxDBClientFactory.create(options);
            // 5. 설정된 타임아웃 값 로그 출력
            log.info("InfluxDB Client initialized with read timeout: {} seconds", readTimeoutSeconds);

            try {
                boolean ready = influxDBClient.ping();
                if (ready) {
                    log.info("InfluxDB 연결 성공 - URL: {}, Org: {}, Bucket: {}", url, organization, bucket);
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
            return null; // 또는 더미 클라이언트 반환
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
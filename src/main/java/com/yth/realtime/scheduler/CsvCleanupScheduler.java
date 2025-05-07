package com.yth.realtime.scheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value; // @Value import 추가
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class CsvCleanupScheduler {

    // --- csvTempDir 필드 선언 및 @Value 주입 ---
    @Value("${app.csv.temp-dir:C:/tmp/csv_exports}") // Windows 경로로 수정
    private String csvTempDir;
    // ------------------------------------------

    // --- 임시 파일 보관 기간 설정 (예: 1시간) ---
    @Value("${app.csv.cleanup.max-age-hours:1}") // 프로퍼티 또는 기본값 사용
    private long maxAgeHours;
    // private static final String TEMP_DIR = "/tmp/csv_exports";

    // @Scheduled(fixedRate = 3600000) // 1시간마다
    // public void cleanOldTempCsvFiles() {
    // try (Stream<Path> files = Files.walk(Paths.get(TEMP_DIR))) {
    // files.filter(Files::isRegularFile)
    // .filter(p -> {
    // try {
    // return Files.getLastModifiedTime(p)
    // .toInstant()
    // .isBefore(Instant.now().minus(Duration.ofHours(1)));
    // } catch (IOException e) {
    // return false;
    // }
    // })
    // .forEach(p -> {
    // try {
    // Files.deleteIfExists(p);
    // log.info("🔁 오래된 CSV 삭제됨: {}", p);
    // } catch (IOException e) {
    // log.warn("❌ 삭제 실패: {}", p);
    // }
    // });
    // } catch (IOException e) {
    // log.error("CSV 정리 작업 중 오류", e);
    // }
    // }
    @Scheduled(fixedRate = 3600000)
    public void cleanOldTempCsvFiles() {
        try {
            // 디렉토리가 없으면 생성
            Path tempDirPath = Path.of(csvTempDir);
            Files.createDirectories(tempDirPath);

            try (Stream<Path> files = Files.walk(tempDirPath)) {
                files.filter(Files::isRegularFile)
                        .filter(p -> {
                            try {
                                return Files.getLastModifiedTime(p)
                                        .toInstant()
                                        .isBefore(Instant.now().minus(Duration.ofHours(1)));
                            } catch (IOException e) {
                                return false;
                            }
                        })
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                                log.info("🔁 오래된 CSV 삭제됨: {}", p);
                            } catch (IOException e) {
                                log.warn("❌ 삭제 실패: {}", p);
                            }
                        });
            }
        } catch (IOException e) {
            log.error("CSV 정리 작업 중 오류", e);
        }
    }
}

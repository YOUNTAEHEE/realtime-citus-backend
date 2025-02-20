package com.yth.realtime.runner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.yth.realtime.service.InfluxDBTaskService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ScheduledTestRunner implements CommandLineRunner {

    private final InfluxDBTaskService influxDBTaskService;

    public ScheduledTestRunner(InfluxDBTaskService influxDBTaskService) {
        this.influxDBTaskService = influxDBTaskService;
    }
    // @Override
    // public void run(String... args) throws Exception {
    //     try {
    //         log.info("2분 평균 데이터 집계 시작...");
    //         influxDBTaskService.aggregateDataTo2Min();
            
    //         // 2분 평균 데이터가 저장될 때까지 대기
    //         Thread.sleep(300000); // ✅ 6분 후 실행되도록 변경
            
    //         log.info("5분 평균 데이터 집계 시작...");
    //         influxDBTaskService.aggregateDataTo4Min();
            
    //         log.info("데이터 집계 완료");
    //     } catch (Exception e) {
    //         log.error("데이터 집계 중 오류 발생: {}", e.getMessage(), e);
    //     }
    // }
    @Override
    public void run(String... args) {
        try {
            log.info("데이터 집계 스케줄러 시작...");
            // 스케줄러 설정을 통해 실행하도록 변경
            influxDBTaskService.startScheduledTasks();
        } catch (Exception e) {
            log.error("데이터 집계 스케줄러 시작 중 오류 발생: {}", e.getMessage(), e);
        }
    }
}

package com.yth.realtime.runner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.yth.realtime.service.InfluxDBTaskService;

@Component
public class ScheduledTestRunner implements CommandLineRunner {

    private final InfluxDBTaskService influxDBTaskService;

    public ScheduledTestRunner(InfluxDBTaskService influxDBTaskService) {
        this.influxDBTaskService = influxDBTaskService;
    }

    @Override
    public void run(String... args) throws Exception {
        influxDBTaskService.aggregateDataTo2Min(); // 수동 실행
        influxDBTaskService.aggregateDataTo5Min(); // 수동 실행 (5분 평균)
    }
}

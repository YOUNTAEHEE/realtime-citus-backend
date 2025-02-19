package com.yth.realtime.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.service.InfluxDBTaskService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SchedulerController {
    
    private final InfluxDBTaskService influxDBTaskService;
    
    // @GetMapping("/api/scheduler/test")
    // public String testScheduler() {
    //     log.info("스케줄러 수동 실행 테스트");
    //     influxDBTaskService.aggregateDataTo1Min();
    //     return "스케줄러 테스트 실행 완료";
    // }
    
    // @GetMapping("/api/scheduler/status")
    // public ResponseEntity<Map<String, Object>> getSchedulerStatus() {
    //     try {
    //         Map<String, Object> status = influxDBTaskService.getStatus();
    //         return ResponseEntity.ok(status);
    //     } catch (Exception e) {
    //         log.error("스케줄러 상태 확인 실패: {}", e.getMessage(), e);
    //         return ResponseEntity.internalServerError().build();
    //     }
    // }
}
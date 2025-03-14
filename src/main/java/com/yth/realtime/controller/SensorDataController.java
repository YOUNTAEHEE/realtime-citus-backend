package com.yth.realtime.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.service.InfluxDBService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/sensor-data")
@RequiredArgsConstructor
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001"})
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001", "https://realtime-citus-nagp.vercel.app" })
// @CrossOrigin(origins = "*", allowCredentials = "false")
@Slf4j
public class SensorDataController {

    private final InfluxDBService influxDBService;

    @GetMapping("/{deviceId}")
    public ResponseEntity<List<Map<String, Object>>> getSensorData(
            @PathVariable String deviceId,
            @RequestParam(defaultValue = "1440") int minutes) {

        log.info("센서 데이터 조회 요청: deviceId={}, minutes={}", deviceId, minutes);
        List<Map<String, Object>> data = influxDBService.getRecentSensorData(deviceId, minutes);
        return ResponseEntity.ok(data);
    }

    // @GetMapping("/latest/{deviceId}")
    // public ResponseEntity<Map<String, Object>> getLatestSensorData(
    //         @PathVariable String deviceId) {

    //     log.info("최신 센서 데이터 조회 요청: deviceId={}", deviceId);
    //     Map<String, Object> data = influxDBService.getLatestSensorData(deviceId);
    //     return ResponseEntity.ok(data);
    // }
}
package com.yth.realtime.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.entity.WeatherStation;
import com.yth.realtime.service.WeatherService;
import com.yth.realtime.service.WeatherStationService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/stations")
@RequiredArgsConstructor
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001"})
// @CrossOrigin(origins = {"http://localhost:3000", "http://localhost:3001", "https://realtime-citus-nagp.vercel.app"})
// @CrossOrigin(origins = "*", allowCredentials = "false")
public class WeatherStationController {
    
    private final WeatherStationService weatherStationService;
    
    // @GetMapping
    // public ResponseEntity<?> getAllStations() {
    //     try {
    //         return ResponseEntity.ok(weatherStationService.getAllStations());
    //     } catch (Exception e) {
    //         return ResponseEntity.internalServerError().body(e.getMessage());
    //     }
    // }

    @GetMapping("/nearest")
    public ResponseEntity<?> getNearestStationData(
        @RequestParam("lat") double lat,
        @RequestParam("lng") double lng) {
        try {
            WeatherStation nearestStation = weatherStationService.findNearestStation(lat, lng);
            
            if (nearestStation == null) {
                return ResponseEntity.notFound().build();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("stnName", nearestStation.getStnName());
            response.put("stnId", nearestStation.getStnId());
            
            // 20km 이상 떨어진 경우 경고 메시지 포함
            if (nearestStation.getDistance() > 20) {
                response.put("warning", "선택한 위치에서 20km 이상 떨어진 관측소입니다. 데이터의 정확도가 떨어질 수 있습니다.");
            }

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
} 
package com.yth.realtime.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.service.ShortTermForecastService;
import com.yth.realtime.service.WeatherService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001" }) // React 서버주소
@Slf4j
public class WeatherController {

    private final WeatherService weatherService;
    private final ShortTermForecastService shortTermForecastService;

    @Autowired
    public WeatherController(WeatherService weatherService, ShortTermForecastService shortTermForecastService) {
        this.weatherService = weatherService;
        this.shortTermForecastService = shortTermForecastService;
    }

    @GetMapping("/weather")
    public ResponseEntity<?> getWeatherData(
            @RequestParam("date-first") String dateFirst,
            @RequestParam("date-last") String dateLast,
            @RequestParam("region") String region) {
        try {
            List<Map<String, String>> data = weatherService.fetchWeatherData(dateFirst, dateLast, region);
            if (data.isEmpty()) {
                return ResponseEntity.noContent().build();
            }
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("날씨 데이터 조회 중 오류: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("데이터 조회 실패");
        }
    }

    @GetMapping("/temp-search")
    public ResponseEntity<?> searchTemperature(
            @RequestParam("date") String date,
            @RequestParam("type") String type, // "hight_temp" 또는 "low_temp"
            @RequestParam("region") String region) {
        Map<String, Object> result = weatherService.findTemperatureExtreme(date, type, region);
        return ResponseEntity.ok(result);
    }

    // 단기 예보
    @GetMapping("/short-term-forecast")
    public ResponseEntity<?> getShortTermForecast(
            @RequestParam("region") String region) {
        try {
            System.out.println("region: " + region);
            List<Map<String, String>> forecast = shortTermForecastService.fetchShortTermForecast(region);
            if (forecast.isEmpty()) {
                return ResponseEntity.noContent().build();
            }
            return ResponseEntity.ok(forecast);
        } catch (Exception e) {
            log.error("날씨 데이터 조회 중 오류: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("데이터 조회 실패");
        }
    }
}
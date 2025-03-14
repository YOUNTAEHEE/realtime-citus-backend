package com.yth.realtime.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.service.ShortTermForecastService;
import com.yth.realtime.service.TwoWeatherService;
import com.yth.realtime.service.WeatherService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api")
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001"})
// @CrossOrigin(origins = { "http://localhost:3000", "http://localhost:3001" ,
// "https://realtime-citus-nagp.vercel.app"}) // React 서버주소
// @CrossOrigin(origins = "*", allowCredentials = "false")
@Slf4j
public class WeatherController {

    private final WeatherService weatherService;
    private final ShortTermForecastService shortTermForecastService;
    private final TwoWeatherService twoWeatherService;

    @Autowired
    public WeatherController(WeatherService weatherService, ShortTermForecastService shortTermForecastService,
            TwoWeatherService twoWeatherService) {
        this.weatherService = weatherService;
        this.shortTermForecastService = shortTermForecastService;
        this.twoWeatherService = twoWeatherService;
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

    @GetMapping("/temp-diff")
    public ResponseEntity<?> calculateTempDiff(
            @RequestParam("region1") String region1,
            @RequestParam("region2") String region2,
            @RequestParam("dateFirst1") String dateFirst1,
            @RequestParam("dateLast1") String dateLast1,
            @RequestParam("dateFirst2") String dateFirst2,
            @RequestParam("dateLast2") String dateLast2) {
        try {
            log.info(
                    "기온 차이 계산 요청 파라미터: region1={}, region2={}, dateFirst1={}, dateLast1={}, dateFirst2={}, dateLast2={}",
                    region1, region2, dateFirst1, dateLast1, dateFirst2, dateLast2);

            // 파라미터 검증
            if (region1 == null || region1.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "region1이 비어 있습니다."));
            }
            if (region2 == null || region2.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "region2가 비어 있습니다."));
            }

            try {
                // 문자열을 long으로 변환 (문자열이 UNIX 타임스탬프라 가정)
                long startTime1 = Long.parseLong(dateFirst1);
                long endTime1 = Long.parseLong(dateLast1);
                long startTime2 = Long.parseLong(dateFirst2);
                long endTime2 = Long.parseLong(dateLast2);

                log.info("변환된 타임스탬프: startTime1={}, endTime1={}, startTime2={}, endTime2={}",
                        startTime1, endTime1, startTime2, endTime2);

                Map<String, Double> tempDiff = twoWeatherService.getTemperatureDifference(region1, region2, startTime1,
                        endTime1, startTime2, endTime2);
                return ResponseEntity.ok(tempDiff);
            } catch (NumberFormatException e) {
                log.error("타임스탬프 변환 중 오류: {}", e.getMessage());
                return ResponseEntity.badRequest().body(Map.of("error", "날짜 형식이 올바르지 않습니다: " + e.getMessage()));
            }
        } catch (Exception e) {
            log.error("기온 차이 계산 중 오류: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", "기온 차이 계산 실패: " + e.getMessage()));
        }
    }

}
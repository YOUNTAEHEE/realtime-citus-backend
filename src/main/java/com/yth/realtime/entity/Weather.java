package com.yth.realtime.entity;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

@Data
@Document(collection = "weather")
public class Weather {
    @Id
    private String id;
    private String dateTime;  // YYMMDDHHMI
    private String temperature;  // TA
    private String windSpeed;   // WS
    private String pressure;    // PR
    private String dewPoint;    // TD
    private String region;
    private LocalDateTime createdAt;

    public static Weather fromMap(Map<String, String> data, String region) {
        Weather weather = new Weather();
        String rawDateTime = data.get("YYMMDDHHMI");
        // 날짜 형식이 12자리가 되도록 보정
        if (rawDateTime != null && !rawDateTime.isEmpty()) {
            if (rawDateTime.length() < 12) {
                rawDateTime = String.format("%012d", Long.parseLong(rawDateTime));
            }
            weather.setDateTime(rawDateTime);
        }
        weather.setTemperature(data.get("TA"));
        weather.setWindSpeed(data.get("WS"));
        weather.setPressure(data.get("PR"));
        weather.setDewPoint(data.get("TD"));
        weather.setRegion(region);
        weather.setCreatedAt(LocalDateTime.now());
        return weather;
    }

    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("YYMMDDHHMI", dateTime);
        map.put("TA", temperature);
        map.put("WS", windSpeed);
        map.put("PR", pressure);
        map.put("TD", dewPoint);
        return map;
    }
    
    // 날짜 비교를 위한 메서드 추가
    public boolean isInDateRange(String startDate, String endDate) {
        String date = this.dateTime.substring(0, 8);  // YYMMDDHHMI -> YYMMDD
        return date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0;
    }
}
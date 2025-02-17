package com.yth.realtime.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Data;

@Data
@Document(collection = "weather_stations")
public class WeatherStation {
    @Id
    private String id;
    private String stnId;        // 지점번호
    private String stnName;      // 지점명(한글)
    private double latitude;     // 위도
    private double longitude;    // 경도
    
    @Transient  // MongoDB에 저장하지 않을 필드
    private double distance;     // 선택한 위치로부터의 거리 (km)
} 
package com.yth.realtime.dto;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HistoricalDataRequest {
    private String startTime;
    private String endTime;
    private String deviceGroup;
    private String aggregationInterval;

    // ISO 문자열을 Instant로 변환하는 메소드
    public Instant getStartTimeAsInstant() {
        return Instant.parse(startTime);
    }

    public Instant getEndTimeAsInstant() {
        return Instant.parse(endTime);
    }
}
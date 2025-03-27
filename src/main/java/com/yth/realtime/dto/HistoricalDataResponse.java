package com.yth.realtime.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HistoricalDataResponse {
    private boolean success;
    private String message;

    @Builder.Default
    private Map<String, Object> data = Map.of();

    // 시계열 데이터를 추가하기 위한 헬퍼 메소드
    public void setTimeSeriesData(List<Map<String, Object>> timeSeriesData) {
        this.data = Map.of("timeSeries", timeSeriesData);
    }
}
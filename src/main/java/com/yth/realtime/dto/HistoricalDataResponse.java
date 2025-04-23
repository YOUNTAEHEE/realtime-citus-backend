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
    private List<Map<String, Object>> timeSeriesData;
}

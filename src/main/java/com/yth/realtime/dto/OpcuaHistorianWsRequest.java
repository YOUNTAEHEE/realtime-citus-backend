package com.yth.realtime.dto;

import lombok.AllArgsConstructor; // 모든 필드 생성자
import lombok.Data;
import lombok.NoArgsConstructor; // 기본 생성자

@Data // Getter, Setter, toString, equals, hashCode 등 자동 생성
@NoArgsConstructor
@AllArgsConstructor
public class OpcuaHistorianWsRequest {
    private String type; // 예: "getHistoricalData"
    private Object payload; // 실제 요청 내용 (Map 또는 특정 DTO)
}

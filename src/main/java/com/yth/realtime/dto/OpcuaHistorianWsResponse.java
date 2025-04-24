package com.yth.realtime.dto;

import lombok.AllArgsConstructor; // 모든 필드 생성자
import lombok.Builder; // 빌더 패턴 사용
import lombok.Data;
import lombok.NoArgsConstructor; // 기본 생성자

@Data // Getter, Setter, toString, equals, hashCode 등 자동 생성
@Builder // 빌더 패턴 자동 생성
@NoArgsConstructor
@AllArgsConstructor
public class OpcuaHistorianWsResponse {
    private String type; // 예: "historicalData", "error"
    private Object payload; // 실제 응답 내용 또는 에러 정보
}

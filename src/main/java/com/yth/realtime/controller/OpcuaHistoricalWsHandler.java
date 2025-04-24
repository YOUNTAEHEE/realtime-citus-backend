package com.yth.realtime.controller; // 패키지 경로는 실제 프로젝트 구조에 맞게 조정하세요.

import java.io.IOException;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.fasterxml.jackson.databind.ObjectMapper; // Jackson 라이브러리 사용
import com.yth.realtime.dto.HistoricalDataRequest; // 프론트엔드 요청 형식에 맞는 DTO (필요시 생성 또는 수정)
import com.yth.realtime.dto.HistoricalDataResponse;
import com.yth.realtime.dto.OpcuaHistorianWsRequest;
import com.yth.realtime.dto.OpcuaHistorianWsResponse;
import com.yth.realtime.service.OpcuaHistoricalService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component // Spring Bean으로 등록
@Slf4j
@RequiredArgsConstructor
public class OpcuaHistoricalWsHandler extends TextWebSocketHandler {

    private final OpcuaHistoricalService opcuaHistoricalService;
    private final ObjectMapper objectMapper; // JSON 파싱/생성을 위한 ObjectMapper (Spring Boot 자동 구성)

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("WebSocket 연결됨: {}", session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        log.info("WebSocket 메시지 수신 ({}): {}", session.getId(), payload);

        try {
            // 1. 수신 메시지 파싱 (WsRequestMessage 형식 가정)
            OpcuaHistorianWsRequest request = objectMapper.readValue(payload, OpcuaHistorianWsRequest.class);

            // 2. 요청 타입에 따른 처리 ("getHistoricalData" 가정)
            if ("getHistoricalData".equals(request.getType())) {
                // payload를 HistoricalDataRequest 형식으로 변환 (Map 또는 특정 DTO 사용)
                HistoricalDataRequest dataRequest = objectMapper.convertValue(request.getPayload(),
                        HistoricalDataRequest.class);

                // 3. 서비스 호출 (기존 로직 재사용)
                // 중요: 이 서비스 호출이 여전히 1시간 전체 데이터를 조회할 수 있음
                HistoricalDataResponse serviceResponse = opcuaHistoricalService.getHistoricalData(
                        dataRequest.getStartTime(),
                        dataRequest.getEndTime(),
                        dataRequest.getDeviceGroup());

                // 4. 성공 응답 메시지 생성 (수정된 부분)
                OpcuaHistorianWsResponse responseMessage = OpcuaHistorianWsResponse.builder() // 클래스 이름 수정 및 빌더 사용
                        .type("historicalData") // 프론트엔드와 약속된 응답 타입
                        .payload(serviceResponse) // 서비스 결과를 payload에 넣음
                        .build();
                sendMessage(session, responseMessage);

            } else {
                // 알 수 없는 요청 타입 처리
                sendErrorMessage(session, "알 수 없는 요청 타입입니다: " + request.getType());
            }

        } catch (Exception e) {
            log.error("WebSocket 메시지 처리 중 오류 발생 ({}): {}", session.getId(), e.getMessage(), e);
            // 오류 응답 메시지 전송
            sendErrorMessage(session, "메시지 처리 중 오류 발생: " + e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        log.info("WebSocket 연결 종료: {}, 상태: {}", session.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("WebSocket 전송 오류 ({}): {}", session.getId(), exception.getMessage());
    }

    // --- Helper Methods ---

    private void sendMessage(WebSocketSession session, OpcuaHistorianWsResponse message) throws IOException {
        String jsonMessage = objectMapper.writeValueAsString(message);
        log.debug("WebSocket 메시지 발송 ({}): {}", session.getId(),
                jsonMessage.substring(0, Math.min(jsonMessage.length(), 200)) + "..."); // 너무 길면 자르기
        session.sendMessage(new TextMessage(jsonMessage));
    }

    private void sendErrorMessage(WebSocketSession session, String errorMessage) throws IOException {
        // 빌더 사용하도록 수정
        OpcuaHistorianWsResponse errorResponse = OpcuaHistorianWsResponse.builder()
                .type("error")
                .payload(Map.of("message", errorMessage)) // 간단한 오류 메시지 형식
                .build();
        sendMessage(session, errorResponse);
    }
}

// --- 웹소켓 메시지 DTO 예시 (WsRequestMessage.java, WsResponseMessage.java) ---
// 필요에 따라 별도 파일로 생성하세요. Lombok 사용 예시.

/*
 * package com.yth.realtime.dto;
 * import lombok.Data;
 * 
 * @Data
 * public class WsRequestMessage {
 * private String type; // 예: "getHistoricalData"
 * private Object payload; // 실제 요청 내용 (Map 또는 특정 DTO)
 * }
 */

/*
 * package com.yth.realtime.dto;
 * import lombok.Builder;
 * import lombok.Data;
 * 
 * @Data
 * 
 * @Builder
 * public class WsResponseMessage {
 * private String type; // 예: "historicalData", "error"
 * private Object payload; // 실제 응답 내용 또는 에러 정보
 * }
 */

/*
 * // HistoricalDataRequest DTO가 없다면 생성 (Controller에서 사용하던 것과 동일하게)
 * package com.yth.realtime.dto;
 * import lombok.Data;
 * 
 * @Data
 * public class HistoricalDataRequest {
 * private String startTime;
 * private String endTime;
 * private String deviceGroup;
 * }
 */

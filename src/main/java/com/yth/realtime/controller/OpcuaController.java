package com.yth.realtime.controller;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.yth.realtime.dto.HistoricalDataRequest;
import com.yth.realtime.dto.HistoricalDataResponse;
import com.yth.realtime.service.OpcuaHistoricalService;
import com.yth.realtime.service.OpcuaService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/opcua")
@Slf4j
@RequiredArgsConstructor
public class OpcuaController {

    private final OpcuaService opcuaService;
    private final OpcuaWebSocketHandler webSocketHandler;
    private final OpcuaHistoricalService opcuaHistoricalService;

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("connected", opcuaService.isConnected());
        status.put("autoReconnect", opcuaService.isAutoReconnect());
        return ResponseEntity.ok(status);
    }

    @PostMapping("/connect")
    public ResponseEntity<Map<String, Object>> connect() {
        boolean success = opcuaService.connect();
        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        response.put("message", success ? "OPC UA 서버에 연결되었습니다" : "OPC UA 서버 연결 실패");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/disconnect")
    public ResponseEntity<Map<String, Object>> disconnect() {
        opcuaService.disconnect();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "OPC UA 서버 연결이 해제되었습니다");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startDataCollection() {
        opcuaService.startDataCollection();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "OPC UA 데이터 수집이 시작되었습니다");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stopOpcuaService() {
        log.info("OPC UA 서비스 중지 요청 수신 - 웹소켓 연결만 해제");
        try {
            // 웹소켓 세션만 정리하고 데이터 수집은 계속 유지
            webSocketHandler.clearAllSessions();
            return ResponseEntity.ok("OPC UA 웹소켓 연결이 해제되었지만 데이터 수집은 계속됩니다.");
        } catch (Exception e) {
            log.error("OPC UA 웹소켓 연결 해제 실패: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("웹소켓 연결 해제 실패: " + e.getMessage());
        }
    }

    @PostMapping("/stop-service")
    public ResponseEntity<Map<String, Object>> stopDataCollection() {
        opcuaService.stopDataCollection();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "OPC UA 데이터 수집이 중지되었습니다");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/data")
    public ResponseEntity<Map<String, Map<String, Object>>> getAllData() {
        Map<String, Map<String, Object>> data = opcuaService.getAllData();
        return ResponseEntity.ok(data);
    }

    @GetMapping("/data/{groupName}")
    public ResponseEntity<Map<String, Object>> getGroupData(@PathVariable String groupName) {
        Map<String, Object> data = opcuaService.getGroupData(groupName);
        return ResponseEntity.ok(data);
    }

    @PostMapping("/auto-reconnect/{enabled}")
    public ResponseEntity<Map<String, Object>> setAutoReconnect(@PathVariable boolean enabled) {
        opcuaService.setAutoReconnect(enabled);
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("autoReconnect", enabled);
        response.put("message", enabled ? "자동 재연결이 활성화되었습니다" : "자동 재연결이 비활성화되었습니다");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/historical")
    public ResponseEntity<HistoricalDataResponse> getHistoricalData(@RequestBody HistoricalDataRequest request) {
        log.warn("===== 과거 데이터 조회 요청 수신: {} =====", request);

        try {
            // 서비스 호출 직전 로그
            log.warn("서비스 호출 직전: {}", request.getDeviceGroup());
            HistoricalDataResponse response = opcuaHistoricalService.getHistoricalData(
                    request.getStartTime(),
                    request.getEndTime(),
                    request.getDeviceGroup());
            log.warn("서비스 호출 완료: 성공={}", response.isSuccess());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("과거 데이터 조회 중 오류 발생: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().body(
                    HistoricalDataResponse.builder()
                            .success(false)
                            .message("데이터 조회 중 오류가 발생했습니다: " + e.getMessage())
                            .build());
        }
    }

    // @PostMapping("/historical/export")
    // public ResponseEntity<?> exportHistoricalData(@RequestBody
    // HistoricalDataRequest request) {
    // log.info("===== 과거 데이터 내보내기 요청 수신: {} =====", request);
    // try {
    // // OpcuaHistoricalService의 CSV 내보내기 메서드 호출
    // String csvData = opcuaHistoricalService.exportHistoricalDataToCsv(
    // request.getStartTime(),
    // request.getEndTime(),
    // request.getDeviceGroup());

    // if (csvData == null || csvData.isEmpty()) {
    // log.warn("내보낼 데이터가 없습니다. 요청: {}", request);
    // // 데이터 없음 응답 (예: 204 No Content 또는 메시지와 함께 200 OK)
    // Map<String, Object> responseBody = new HashMap<>();
    // responseBody.put("success", true);
    // responseBody.put("message", "지정된 기간에 내보낼 데이터가 없습니다.");
    // return ResponseEntity.ok(responseBody); // 또는
    // ResponseEntity.noContent().build();
    // }

    // // HTTP 헤더 설정
    // HttpHeaders headers = new HttpHeaders();
    // headers.setContentType(MediaType.parseMediaType("text/csv; charset=utf-8"));
    // // Content-Type 설정 (UTF-8 명시)
    // // 파일 이름 설정 (예: export_data_그룹명_시작시간.csv) - 필요에 따라 수정
    // String filename = String.format("export_data_%s_%s.csv",
    // request.getDeviceGroup(),
    // request.getStartTime().replaceAll("[:.]", "-")); // 파일명에 부적합한 문자 제거
    // headers.setContentDispositionFormData("attachment", filename); // 다운로드 파일명 지정

    // // CSV 데이터와 헤더를 포함한 ResponseEntity 반환
    // return new ResponseEntity<>(csvData, headers, HttpStatus.OK);

    // } catch (IllegalArgumentException iae) {
    // log.warn("과거 데이터 내보내기 요청 처리 중 잘못된 파라미터: {}", iae.getMessage());
    // Map<String, Object> responseBody = new HashMap<>();
    // responseBody.put("success", false);
    // responseBody.put("message", "잘못된 요청 파라미터: " + iae.getMessage());
    // return ResponseEntity.badRequest().body(responseBody);
    // } catch (Exception e) {
    // log.error("과거 데이터 내보내기 중 오류 발생: {}", e.getMessage(), e);
    // // 오류 응답 (JSON 형태)
    // Map<String, Object> responseBody = new HashMap<>();
    // responseBody.put("success", false);
    // responseBody.put("message", "데이터 내보내기 중 서버 오류가 발생했습니다: " + e.getMessage());
    // return ResponseEntity.internalServerError().body(responseBody);
    // }
    // }

    // @GetMapping("/historical/export")
    // public ResponseEntity<Resource> exportHistoricalData(
    // @RequestParam String startTime,
    // @RequestParam String endTime,
    // @RequestParam String deviceGroup) {

    // log.info("Received request to export PIVOTED CSV: startTime={}, endTime={},
    // deviceGroup={}", startTime, endTime,
    // deviceGroup);
    // Path csvFilePath = null;
    // try {
    // // 서비스 호출하여 CSV 파일 생성 및 경로 얻기
    // csvFilePath = opcuaHistoricalService.exportPivotedDataToCsvFile(startTime,
    // endTime, deviceGroup);

    // // 파일 리소스 생성
    // Resource resource = new FileSystemResource(csvFilePath);

    // // 파일 존재 및 읽기 가능 여부 확인 강화
    // if (!resource.exists() || !resource.isReadable()) {
    // // --- log.error 수정: format: 제거 ---
    // log.error("생성된 CSV 파일을 찾거나 읽을 수 없습니다: {}", csvFilePath);
    // // ---------------------------------
    // throw new RuntimeException("생성된 CSV 파일을 찾거나 읽을 수 없습니다: " + csvFilePath);
    // }

    // // --- 파일 크기를 먼저 가져오기 (IOException 가능성 있음) ---
    // // 이 메서드는 IOException을 던질 수 있으며, 아래 catch 블록에서 처리됩니다.
    // long contentLength = resource.contentLength();

    // // 다운로드될 파일명 생성
    // String currentDateTime =
    // LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss"));
    // String filename = String.format("historical_pivoted_data_%s_%s.csv",
    // deviceGroup, currentDateTime);

    // // HTTP 헤더 설정
    // HttpHeaders headers = new HttpHeaders();
    // headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" +
    // filename + "\"");
    // headers.add(HttpHeaders.CACHE_CONTROL, "no-cache, no-store,
    // must-revalidate");
    // headers.add(HttpHeaders.PRAGMA, "no-cache");
    // headers.add(HttpHeaders.EXPIRES, "0");

    // // 파일 스트리밍 응답 반환
    // return ResponseEntity.ok()
    // .headers(headers)
    // // .contentLength(contentLength) // 계산된 파일 크기 사용
    // .contentType(MediaType.parseMediaType("text/csv; charset=UTF-8")) // UTF-8 명시
    // .body(resource);

    // } catch (IllegalArgumentException | IOException e) { // IOException 포함
    // (contentLength 등에서 발생 가능)
    // log.error("CSV 내보내기(피벗) 처리 중 오류 발생: {}", e.getMessage(), e);
    // // 오류 발생 시 빈 응답 반환 고려
    // return ResponseEntity.internalServerError().build();
    // } finally {
    // // 임시 파일 삭제 로직 (동일)
    // if (csvFilePath != null) {
    // try {
    // Files.deleteIfExists(csvFilePath);
    // log.info("임시 CSV 파일 삭제 완료: {}", csvFilePath);
    // } catch (IOException e) {
    // // 파일 삭제 실패는 로깅만 함
    // log.error("임시 CSV 파일 삭제 중 오류: {}", csvFilePath, e);
    // }
    // }
    // }
    // }

    @GetMapping("/historical/export")
    public ResponseEntity<StreamingResponseBody> exportHistoricalData(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam String deviceGroup) {

        log.info("Streaming CSV export: {}, {}", startTime, endTime);
        Path csvFilePath;

        try {
            csvFilePath = opcuaHistoricalService.exportPivotedDataToCsvFile(startTime, endTime, deviceGroup);

            String filename = URLEncoder.encode(
                    "historical_data_" + deviceGroup + ".csv", StandardCharsets.UTF_8).replace("+", "%20");

            StreamingResponseBody stream = outputStream -> {
                try (var inputStream = Files.newInputStream(csvFilePath)) {
                    inputStream.transferTo(outputStream);
                    outputStream.flush();
                } finally {
                    // ✅ 스트리밍 후 삭제
                    Files.deleteIfExists(csvFilePath);
                    log.info("CSV 파일 삭제 완료: {}", csvFilePath);
                }
            };

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename*=UTF-8''" + filename)
                    .contentType(MediaType.parseMediaType("text/csv; charset=UTF-8"))
                    .body(stream);

        } catch (Exception e) {
            log.error("CSV 스트리밍 중 오류 발생", e);
            return ResponseEntity.internalServerError().build();
        }
    }

}